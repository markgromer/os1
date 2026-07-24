import { createVerificationResult, makeOperationId, normalizeDesktopCorrelation, normalizeProviderAction, normalizeVerificationResult, nowIso, safeObject, safeString } from './operation_types.js';

export const VERIFICATION_TYPES = Object.freeze([
  'build', 'test', 'lint', 'typecheck', 'repository_cleanliness', 'diff_review', 'manual_review', 'url_health', 'artifact_present',
]);

const SCRIPT_CHECKS = new Set(['build', 'test', 'lint', 'typecheck']);

function requirementFrom(value) {
  if (typeof value === 'string') return { type: value, required: true };
  const raw = safeObject(value);
  return {
    type: VERIFICATION_TYPES.includes(safeString(raw.type, 100)) ? safeString(raw.type, 100) : 'manual_review',
    required: raw.required !== false,
    target: safeString(raw.target, 2_000),
    command: safeString(raw.command, 500),
  };
}

export class OperationVerification {
  constructor({ queueDesktopAction = null, store = null } = {}) {
    this.queueDesktopAction = typeof queueDesktopAction === 'function' ? queueDesktopAction : null;
    this.store = store;
  }

  buildRequirements(registryRecord, requested = []) {
    const record = safeObject(registryRecord);
    const commands = safeObject(record.commands);
    const requirements = [];
    for (const type of ['build', 'test', 'lint', 'typecheck']) {
      if (safeString(commands[type], 500)) requirements.push({ type, required: true, command: commands[type] });
    }
    requirements.push({ type: 'artifact_present', required: true });
    requirements.push({ type: 'diff_review', required: true });
    if (!requirements.some((item) => SCRIPT_CHECKS.has(item.type))) requirements.push({ type: 'manual_review', required: true });
    for (const item of Array.isArray(requested) ? requested : []) {
      const requirement = requirementFrom(item);
      const existing = requirements.find((candidate) => candidate.type === requirement.type);
      if (existing) Object.assign(existing, requirement);
      else requirements.push(requirement);
    }
    return requirements.slice(0, 20);
  }

  async run({ operation, step, registryRecord, idempotencyKey = '' }) {
    const requirements = (Array.isArray(step?.input?.requirements) ? step.input.requirements : [])
      .map(requirementFrom);
    const effective = requirements.length ? requirements : this.buildRequirements(registryRecord);
    const current = Array.isArray(operation.verification) ? operation.verification : [];
    const results = [];
    let queuedDesktopAction = false;

    for (const requirement of effective) {
      const prior = current.find((item) => item.type === requirement.type && item.stepId === step.id)
        || current.find((item) => item.type === requirement.type && ['passed', 'failed'].includes(item.status));
      if (prior && (prior.status === 'passed' || prior.waived === true || prior.status === 'failed')) {
        results.push(normalizeVerificationResult({ ...prior, required: requirement.required }, { operationId: operation.id, stepId: step.id }));
        continue;
      }

      if (requirement.type === 'artifact_present') {
        const evidenceArtifacts = (operation.artifacts || []).filter((artifact) => ['codex_result', 'codex_diff', 'commit', 'external_job', 'implementation_result'].includes(artifact.type));
        results.push(normalizeVerificationResult({
          id: prior?.id || makeOperationId('verify'), type: requirement.type, required: requirement.required,
          status: evidenceArtifacts.length ? 'passed' : 'failed', startedAt: nowIso(), completedAt: nowIso(),
          output: evidenceArtifacts.length ? `${evidenceArtifacts.length} implementation evidence artifact(s) registered.` : '',
          error: evidenceArtifacts.length ? '' : 'No implementation result, diff, commit, or external job artifact is registered.',
          evidence: evidenceArtifacts.map((artifact) => ({ id: artifact.id, type: artifact.type, name: artifact.name })),
        }, { operationId: operation.id, stepId: step.id }));
        continue;
      }

      if (SCRIPT_CHECKS.has(requirement.type)) {
        const workspacePath = safeString(registryRecord?.localWorkspace?.path, 2_000);
        const configured = safeString(registryRecord?.commands?.[requirement.type], 500);
        if (prior?.status === 'running' && prior?.evidence?.actionId) {
          results.push(normalizeVerificationResult(prior, { operationId: operation.id, stepId: step.id }));
          queuedDesktopAction = true;
        } else if (workspacePath && configured && registryRecord?.localWorkspace?.trustStatus === 'approved'
          && registryRecord?.localWorkspace?.desktopAgentId && this.queueDesktopAction && this.store) {
          const verification = createVerificationResult({
            type: requirement.type, required: requirement.required, status: 'running', command: configured, target: workspacePath,
            output: 'Verification was durably correlated and queued through the desktop agent. It has not been treated as passed.',
          }, { operationId: operation.id, stepId: step.id });
          const actionId = makeOperationId('desktop');
          const correlationKey = `${idempotencyKey}:${requirement.type}`;
          verification.evidence = { actionId, provider: 'desktop', idempotencyKey: correlationKey };
          const correlation = normalizeDesktopCorrelation({
            actionId, operationId: operation.id, stepId: step.id, businessKey: operation.businessKey, verificationId: verification.id,
            verificationType: requirement.type, actionType: 'run-project-script', projectRegistryId: operation.projectRegistryId,
            desktopAgentId: registryRecord.localWorkspace.desktopAgentId, idempotencyKey: correlationKey,
            attemptNumber: step.attemptCount, queuedAt: nowIso(), updatedAt: nowIso(), status: 'queued',
          });
          await this.store.update(operation.businessKey, operation.id, (draft) => {
            const existing = draft.desktopCorrelations.find((item) => item.idempotencyKey === correlationKey);
            if (!existing) draft.desktopCorrelations.push(correlation);
            if (!draft.verification.some((item) => item.id === verification.id)) draft.verification.push(verification);
            if (!draft.providerActions.some((item) => item.idempotencyKey === correlationKey)) {
              draft.providerActions.push(normalizeProviderAction({
                id: makeOperationId('action'), operationId: operation.id, stepId: step.id,
                provider: 'desktop', action: 'run-project-script', idempotencyKey: correlationKey,
                externalId: actionId, status: 'queued', issuedAt: nowIso(), updatedAt: nowIso(),
                metadata: { attempt: step.attemptCount, verificationId: verification.id },
              }));
            }
            return draft;
          });
          try {
            const action = await this.queueDesktopAction({
              id: actionId,
              idempotencyKey: correlationKey,
              type: 'run-project-script',
              payload: {
                path: workspacePath, scriptName: requirement.type, projectRegistryId: operation.projectRegistryId,
                desktopAgentId: registryRecord.localWorkspace.desktopAgentId, businessKey: operation.businessKey,
                operationId: operation.id, stepId: step.id, idempotencyKey: correlationKey, attemptNumber: step.attemptCount,
              },
              requestedBy: `operation:${operation.id}`,
            });
            if (action?.id !== actionId) throw new Error('Desktop queue returned a mismatched action id.');
            results.push(verification);
            queuedDesktopAction = true;
          } catch (error) {
            verification.status = 'needs_manual_review';
            verification.error = safeString(error?.message, 2_000) || 'Desktop verification queue failed.';
            await this.store.update(operation.businessKey, operation.id, (draft) => {
              const savedCorrelation = draft.desktopCorrelations.find((item) => item.actionId === actionId);
              if (savedCorrelation) {
                savedCorrelation.status = 'recovery_required'; savedCorrelation.updatedAt = nowIso(); savedCorrelation.error = verification.error;
              }
              const savedAction = draft.providerActions.find((item) => item.externalId === actionId);
              if (savedAction) {
                savedAction.status = 'failed'; savedAction.updatedAt = nowIso(); savedAction.completedAt = savedAction.updatedAt;
              }
              const savedVerification = draft.verification.find((item) => item.id === verification.id);
              if (savedVerification) Object.assign(savedVerification, verification);
              return draft;
            });
            results.push(verification);
          }
        } else {
          results.push(normalizeVerificationResult({
            id: prior?.id || makeOperationId('verify'), type: requirement.type, required: requirement.required,
            status: 'needs_manual_review', command: configured, target: workspacePath,
            error: workspacePath && configured ? 'Desktop verification is not available.' : `No allowed ${requirement.type} script is configured in the project registry.`,
          }, { operationId: operation.id, stepId: step.id }));
        }
        continue;
      }

      if (requirement.type === 'diff_review') {
        const diff = (operation.artifacts || []).find((artifact) => artifact.type === 'codex_diff' || artifact.type === 'diff');
        results.push(normalizeVerificationResult({
          id: prior?.id || makeOperationId('verify'), type: requirement.type, required: requirement.required,
          status: 'needs_manual_review',
          output: diff ? 'A diff artifact is available for review.' : '',
          error: diff ? '' : 'No diff artifact is available.',
          evidence: diff ? { artifactId: diff.id, name: diff.name } : {},
        }, { operationId: operation.id, stepId: step.id }));
        continue;
      }

      results.push(normalizeVerificationResult({
        id: prior?.id || makeOperationId('verify'), type: requirement.type, required: requirement.required,
        status: 'needs_manual_review', target: requirement.target, command: requirement.command,
        error: `${requirement.type} requires recorded external or manual evidence.`,
      }, { operationId: operation.id, stepId: step.id }));
    }

    const required = results.filter((result) => result.required !== false);
    const failed = required.filter((result) => result.status === 'failed' && !result.waived);
    const pending = required.filter((result) => !['passed', 'failed'].includes(result.status) && !result.waived);
    if (failed.length) return { status: 'failed', results, error: `${failed.length} required verification check(s) failed.` };
    if (pending.length || queuedDesktopAction) return { status: 'waiting', results, error: `${pending.length || 1} required verification check(s) still need evidence.` };
    return { status: 'completed', results, output: 'All required verification checks passed or were explicitly waived.' };
  }
}
