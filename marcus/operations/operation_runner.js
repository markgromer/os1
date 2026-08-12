import {
  createArtifact,
  makeOperationId,
  normalizeDesktopCorrelation,
  normalizeProviderAction,
  normalizeVerificationResult,
  nowIso,
  requiredVerificationPassed,
  safeObject,
  safeString,
  sanitizeStructured,
  TERMINAL_OPERATION_STATUSES,
} from './operation_types.js';
import { normalizeProviderStatus } from '../providers/codex_provider.js';
import { createUnavailableCodexReviewArtifact } from './codex_result_reviewer.js';
import { appendEvent, transition } from './operation_service.js';

function promiseWithTimeout(promise, timeoutMs) {
  let timeout;
  const guard = new Promise((_, reject) => {
    timeout = setTimeout(() => reject(Object.assign(new Error(`Provider timed out after ${timeoutMs}ms.`), { code: 'PROVIDER_TIMEOUT' })), timeoutMs);
    if (typeof timeout.unref === 'function') timeout.unref();
  });
  return Promise.race([promise, guard]).finally(() => clearTimeout(timeout));
}

function dependenciesComplete(operation, step) {
  return step.dependsOn.every((id) => operation.steps.some((item) => item.id === id && ['completed', 'skipped'].includes(item.status)));
}

function hasFailedDependency(operation, step) {
  return step.dependsOn.some((id) => operation.steps.some((item) => item.id === id && ['failed', 'blocked', 'cancelled'].includes(item.status)));
}

function activeCodexStep(operation) {
  return operation.steps.find((step) => step.type === 'codex' && step.status === 'running'
    && safeObject(operation.metadata?.codexJobs)[step.id]);
}

function createCodexDiffArtifact(diff, defaults = {}) {
  return createArtifact({
    type: 'codex_diff',
    name: 'Codex diff',
    mimeType: 'application/json',
    content: JSON.stringify(diff),
    metadata: {
      source: safeString(diff?.source, 100),
      authoritative: diff?.authoritative === true,
      evidenceDigest: safeString(diff?.evidenceDigest, 100),
      repository: safeString(diff?.repository, 500),
      headSha: safeString(diff?.headSha, 100),
    },
  }, defaults);
}

export class OperationRunner {
  constructor({ store, registry, service, policy, approvalService, providers, verification, codexResultReviewer = null, maxStepsPerCycle = 10, cycleTimeoutMs = 60_000, providerTimeoutMs = 45_000 }) {
    Object.assign(this, { store, registry, service, policy, approvalService, providers, verification, codexResultReviewer });
    this.maxStepsPerCycle = Math.max(1, Math.min(25, Number(maxStepsPerCycle) || 10));
    this.cycleTimeoutMs = Math.max(1_000, Math.min(5 * 60_000, Number(cycleTimeoutMs) || 60_000));
    this.providerTimeoutMs = Math.max(1_000, Math.min(2 * 60_000, Number(providerTimeoutMs) || 45_000));
    this.queues = new Map();
  }

  async tick(businessKey, operationId) {
    const queueKey = `${businessKey}:${operationId}`;
    const previous = this.queues.get(queueKey) || Promise.resolve();
    const run = previous.catch(() => {}).then(() => this.runCycle(businessKey, operationId));
    this.queues.set(queueKey, run);
    try { return await run; } finally { if (this.queues.get(queueKey) === run) this.queues.delete(queueKey); }
  }

  async runCycle(businessKey, operationId) {
    const cycleStarted = Date.now();
    let executed = 0;
    let operation = await this.store.get(businessKey, operationId);
    if (!operation) throw Object.assign(new Error('Operation not found.'), { code: 'OPERATION_NOT_FOUND' });

    const activeJobStep = activeCodexStep(operation);
    if (activeJobStep) {
      operation = await this.pollCodexJob(businessKey, operation, activeJobStep);
      if (!['queued', 'running', 'verifying'].includes(operation.status)) return operation;
    }
    if (!['queued', 'running', 'verifying'].includes(operation.status)) return operation;

    if (operation.status === 'queued') {
      operation = await this.store.update(businessKey, operationId, (draft) => {
        transition(draft, 'running');
        appendEvent(draft, { type: 'runner_cycle_started', actor: 'runner', message: 'A bounded runner cycle started.' });
        return draft;
      });
    }

    while (executed < this.maxStepsPerCycle && Date.now() - cycleStarted < this.cycleTimeoutMs) {
      operation = await this.store.get(businessKey, operationId);
      if (!operation || ['cancelled', 'failed', 'completed', 'paused', 'blocked', 'recovery_required', 'waiting_for_approval', 'awaiting_provider'].includes(operation.status)) return operation;

      const waitingRunning = operation.steps.find((step) => step.status === 'running');
      if (waitingRunning) {
        const job = safeObject(operation.metadata?.codexJobs)[waitingRunning.id];
        if (waitingRunning.type === 'codex' && job) return this.pollCodexJob(businessKey, operation, waitingRunning);
        return this.store.update(businessKey, operationId, (draft) => {
          draft.status = 'recovery_required';
          const step = draft.steps.find((item) => item.id === waitingRunning.id);
          step.status = 'blocked';
          step.error = 'A running provider action has no durable state that can prove its outcome.';
          draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: 'recovery_required', status: 'active', message: step.error, createdAt: nowIso() });
          appendEvent(draft, { type: 'provider_state_unknown', actor: 'runner', stepId: step.id, message: step.error });
          return draft;
        });
      }

      const failedDependencyStep = operation.steps.find((step) => ['pending', 'ready'].includes(step.status) && hasFailedDependency(operation, step));
      if (failedDependencyStep) {
        return this.store.update(businessKey, operationId, (draft) => {
          const step = draft.steps.find((item) => item.id === failedDependencyStep.id);
          step.status = 'blocked'; step.error = 'A dependency did not complete.';
          draft.status = 'blocked'; draft.currentStepId = step.id;
          draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: 'dependency_failed', status: 'active', message: `Step "${step.title}" is blocked by an incomplete dependency.`, createdAt: nowIso() });
          appendEvent(draft, { type: 'step_blocked_dependency', actor: 'runner', stepId: step.id, message: step.error, data: { dependsOn: step.dependsOn } });
          return draft;
        });
      }

      const next = operation.steps.filter((step) => ['pending', 'ready'].includes(step.status) && dependenciesComplete(operation, step))
        .sort((a, b) => a.sequence - b.sequence || a.id.localeCompare(b.id))[0];
      if (!next) return this.finishOrBlock(businessKey, operation);

      const liveRegistryRecord = operation.projectRegistryId ? await this.registry.get(businessKey, operation.projectRegistryId) : null;
      if (operation.projectRegistryId && !liveRegistryRecord) return this.markRecoveryRequired(businessKey, operationId, next.id, 'The bound project registry record no longer exists in this business.');
      const executionTarget = safeObject(operation.metadata?.executionTarget);
      if (executionTarget.projectRegistryId && (
        executionTarget.businessKey !== operation.businessKey
        || executionTarget.projectRegistryId !== operation.projectRegistryId
        || JSON.stringify(executionTarget.repository || {}) !== JSON.stringify(liveRegistryRecord?.repo || {})
        || JSON.stringify(executionTarget.localWorkspace || {}) !== JSON.stringify(liveRegistryRecord?.localWorkspace || {})
        || JSON.stringify(executionTarget.deployments || {}) !== JSON.stringify(liveRegistryRecord?.deployments || {})
        || JSON.stringify(executionTarget.commands || {}) !== JSON.stringify(liveRegistryRecord?.commands || {})
      )) return this.markRecoveryRequired(businessKey, operationId, next.id, 'The registered execution target changed after this operation started. Provider execution was refused.');
      const registryRecord = executionTarget.projectRegistryId ? {
        ...liveRegistryRecord,
        id: executionTarget.projectRegistryId,
        projectId: executionTarget.projectId,
        canonicalName: executionTarget.canonicalName,
        repo: executionTarget.repository,
        localWorkspace: executionTarget.localWorkspace,
        deployments: executionTarget.deployments,
        commands: executionTarget.commands,
      } : liveRegistryRecord;
      const input = safeObject(next.input);
      const environment = safeString(input.environment, 100) || 'development';
      const approvalTarget = safeString(input.approvalTarget, 300);
      const policyAction = `${next.toolName || next.type}${approvalTarget ? `:${approvalTarget}` : ''}`;
      const classification = this.policy.classify({
        business: businessKey,
        projectRegistryId: operation.projectRegistryId,
        environment,
        provider: next.provider,
        action: policyAction,
        actionClass: next.toolName || next.type,
        authorization: operation.metadata?.authorizationProvenance,
      });
      if (next.attemptCount > 0 && ['high', 'critical'].includes(classification.riskLevel)) {
        classification.action = `${classification.action}:retry-${next.attemptCount + 1}`;
        classification.approvalRequired = true;
        classification.approvalRequirement = classification.riskLevel === 'critical' ? 'explicit_strong_confirmation' : 'explicit';
        classification.reason = `${classification.riskLevel} risk retry requires a fresh approval.`;
      } else if (next.attemptCount > 0 && classification.riskLevel === 'medium' && !next.idempotencyKey) {
        classification.approvalRequired = true;
        classification.reason = 'A medium-risk retry without a proven idempotency key requires approval.';
      }

      const approved = operation.approvals.find((approval) => approval.stepId === next.id && approval.action === classification.action && approval.status === 'approved');
      if (classification.approvalRequired && !approved) return this.requestApproval(businessKey, operationId, next, classification);

      operation = await this.store.update(businessKey, operationId, (draft) => {
        const step = draft.steps.find((item) => item.id === next.id);
        if (!step || !['pending', 'ready'].includes(step.status)) throw Object.assign(new Error('Step changed before execution.'), { code: 'STEP_REVISION_CHANGED' });
        if (step.type === 'verification' && draft.status === 'running') transition(draft, 'verifying');
        step.status = 'running'; step.attemptCount += 1; step.startedAt = nowIso(); step.error = '';
        step.idempotencyKey = `${operationId}:${step.id}:${step.attemptCount}`;
        step.riskLevel = classification.riskLevel; draft.currentStepId = step.id;
        if (!draft.providerActions.some((action) => action.idempotencyKey === step.idempotencyKey)) {
          draft.providerActions.push(normalizeProviderAction({
            id: makeOperationId('action'), operationId, stepId: step.id, provider: step.provider, action: step.toolName || step.type,
            idempotencyKey: step.idempotencyKey, status: 'started', issuedAt: nowIso(), updatedAt: nowIso(), metadata: { attempt: step.attemptCount },
          }));
        }
        if (step.type === 'desktop' && !draft.desktopCorrelations.some((item) => item.idempotencyKey === step.idempotencyKey)) {
          const actionId = makeOperationId('desktop');
          draft.desktopCorrelations.push(normalizeDesktopCorrelation({
            actionId, operationId, stepId: step.id, businessKey: draft.businessKey,
            actionType: step.toolName || step.type, projectRegistryId: draft.projectRegistryId,
            desktopAgentId: registryRecord?.localWorkspace?.desktopAgentId,
            idempotencyKey: step.idempotencyKey, attemptNumber: step.attemptCount,
            queuedAt: nowIso(), updatedAt: nowIso(), status: 'queued',
          }));
          const providerAction = draft.providerActions.find((action) => action.idempotencyKey === step.idempotencyKey);
          if (providerAction) providerAction.externalId = actionId;
        }
        appendEvent(draft, { type: 'provider_action_issued', actor: 'runner', stepId: step.id, message: `Issued ${step.title} through ${step.provider}.`, data: { attempt: step.attemptCount, idempotencyKey: step.idempotencyKey } });
        return draft;
      });

      let runningStep = operation.steps.find((step) => step.id === next.id);
      if (runningStep.type === 'verification' && runningStep.attemptCount > 1 && this.codexResultReviewer) {
        operation = await this.refreshCodexCompletionEvidence(businessKey, operation, runningStep, registryRecord);
        if (!['running', 'verifying'].includes(operation.status)) return operation;
        runningStep = operation.steps.find((step) => step.id === next.id);
      }
      let result;
      try {
        result = await promiseWithTimeout(this.invokeProvider({ operation, step: runningStep, registryRecord }), this.providerTimeoutMs);
      } catch (error) {
        result = { status: ['PROVIDER_TIMEOUT', 'PROVIDER_STATE_UNKNOWN'].includes(error?.code) ? 'unknown' : 'failed', error: error?.message || 'Provider failed.' };
      }
      if (runningStep.type === 'codex' && normalizeProviderStatus(result?.status) === 'completed' && result?.job) {
        result = await this.collectCodexCompletion(result, { operation, step: runningStep, registryRecord })
          .catch((error) => ({ status: 'unknown', job: result.job, error: `Codex completed but its artifacts could not be collected: ${error?.message || 'unknown error'}` }));
      }
      executed += 1;
      operation = await this.applyProviderResult(businessKey, operationId, runningStep, result);
      if (!['running', 'queued', 'verifying'].includes(operation.status)) return operation;
    }

    operation = await this.store.get(businessKey, operationId);
    if (operation?.status === 'running') {
      operation = await this.store.update(businessKey, operationId, (draft) => {
        transition(draft, 'queued');
        appendEvent(draft, { type: 'runner_cycle_yielded', actor: 'runner', message: `Runner cycle yielded after ${executed} step(s); another explicit tick is required.` });
        return draft;
      });
    }
    return operation;
  }

  async collectCodexCompletion(result, { operation = null, step = null, registryRecord = null } = {}) {
    const provider = this.providers.codex;
    const [artifacts, diff] = await Promise.all([
      promiseWithTimeout(provider.getArtifacts(result.job), this.providerTimeoutMs),
      promiseWithTimeout(provider.getDiff(result.job), this.providerTimeoutMs),
    ]);
    const collectedArtifacts = (Array.isArray(artifacts) ? artifacts : []).map((artifact) => {
      const raw = safeObject(artifact);
      if (safeString(raw.type, 100) !== 'codex_result_review') return raw;
      return {
        ...raw,
        type: 'untrusted_codex_result_review_claim',
        name: 'Provider-supplied result review claim (untrusted)',
        metadata: { ...safeObject(raw.metadata), trustedForVerification: false },
      };
    });
    if (this.codexResultReviewer) {
      try {
        const review = await this.codexResultReviewer.review({ operation, step, registryRecord, job: result.job, artifacts: collectedArtifacts, diff });
        if (review) collectedArtifacts.push(review);
      } catch (error) {
        collectedArtifacts.push(createUnavailableCodexReviewArtifact({
          diff,
          reason: `Independent result review failed: ${error?.message || 'unknown error'}`,
        }));
      }
    }
    return { ...result, status: 'completed', artifacts: collectedArtifacts, diff };
  }

  async refreshCodexCompletionEvidence(businessKey, operation, verificationStep, registryRecord) {
    const codexStep = operation.steps.find((step) => step.type === 'codex' && step.status === 'completed');
    const job = codexStep ? safeObject(operation.metadata?.codexJobs)[codexStep.id] : null;
    if (!codexStep || !job || this.providers.codex?.mode !== 'direct') return operation;
    let result;
    try {
      if (typeof this.providers.codex.directAdapter?.invalidateEvidence === 'function') {
        this.providers.codex.directAdapter.invalidateEvidence(job);
      }
      result = await this.collectCodexCompletion({ status: 'completed', job }, { operation, step: codexStep, registryRecord });
    } catch (error) {
      return this.store.update(businessKey, operation.id, (draft) => {
        appendEvent(draft, {
          type: 'codex_result_evidence_refresh_failed',
          actor: 'runner',
          stepId: verificationStep.id,
          message: `Codex result evidence refresh failed: ${safeString(error?.message, 2_000) || 'unknown error'}`,
        });
        return draft;
      });
    }
    return this.store.update(businessKey, operation.id, (draft) => {
      const liveStep = draft.steps.find((step) => step.id === verificationStep.id);
      if (!liveStep || liveStep.status !== 'running' || liveStep.idempotencyKey !== verificationStep.idempotencyKey
        || !['running', 'verifying'].includes(draft.status)) return draft;
      for (const artifact of Array.isArray(result.artifacts) ? result.artifacts : []) {
        draft.artifacts.push(createArtifact(artifact, { operationId: draft.id, stepId: codexStep.id }));
      }
      if (result.diff && (result.diff.summary || Object.keys(result.diff).length)) {
        draft.artifacts.push(createCodexDiffArtifact(result.diff, { operationId: draft.id, stepId: codexStep.id }));
      }
      draft.verification = draft.verification.filter((item) => !(item.stepId === liveStep.id && item.type === 'diff_review'));
      appendEvent(draft, {
        type: 'codex_result_evidence_refreshed',
        actor: 'runner',
        stepId: liveStep.id,
        message: 'Refreshed authoritative Codex result evidence before verification retry.',
        data: { codexStepId: codexStep.id, evidenceDigest: safeString(result.diff?.evidenceDigest, 100) },
      });
      return draft;
    });
  }

  async pollCodexJob(businessKey, operation, step) {
    const provider = this.providers.codex;
    const job = safeObject(operation.metadata?.codexJobs)[step.id];
    if (!provider || provider.mode !== 'direct' || !safeString(job.jobId, 300)) {
      if (job.provider === 'external_handoff') return operation;
      return this.markRecoveryRequired(businessKey, operation.id, step.id, 'The Codex action was issued but no provider job ID is available for reconciliation.');
    }
    let polled;
    try { polled = await promiseWithTimeout(provider.getJobStatus(job), this.providerTimeoutMs); }
    catch (error) { polled = { ...job, status: 'unknown', error: error?.message || 'Codex status check failed.' }; }
    const status = normalizeProviderStatus(polled?.status);
    if (status === 'completed') {
      let result;
      try {
        const registryRecord = operation.projectRegistryId ? await this.registry.get(businessKey, operation.projectRegistryId) : null;
        result = await this.collectCodexCompletion({ status, job: { ...job, ...polled } }, { operation, step, registryRecord });
      }
      catch (error) { result = { status: 'unknown', job: { ...job, ...polled }, error: `Codex completion evidence could not be collected: ${error?.message || 'unknown error'}` }; }
      return this.applyProviderResult(businessKey, operation.id, step, result);
    }
    return this.applyProviderResult(businessKey, operation.id, step, { status, job: { ...job, ...polled }, error: polled?.error });
  }

  async applyProviderResult(businessKey, operationId, runningStep, rawResult = {}) {
    const result = safeObject(rawResult);
    const status = normalizeProviderStatus(result.status);
    let deferredControl = null;
    let updated = await this.store.update(businessKey, operationId, (draft) => {
      const step = draft.steps.find((item) => item.id === runningStep.id);
      const timestamp = nowIso();
      const expectedKey = safeString(runningStep.idempotencyKey, 240);
      const expectedAttempt = Number(runningStep.attemptCount) || 0;
      const action = draft.providerActions.find((item) => item.stepId === runningStep.id && item.idempotencyKey === expectedKey);
      const desktopCorrelation = draft.desktopCorrelations.find((item) => item.stepId === runningStep.id && item.idempotencyKey === expectedKey);
      const resultKey = safeString(result.job?.idempotencyKey, 240);
      const actionAttempt = Number(safeObject(action?.metadata).attempt) || 0;
      const eligibleStatus = ['running', 'awaiting_provider', 'verifying', 'queued'].includes(draft.status);
      const eligible = Boolean(step && eligibleStatus && step.status === 'running'
        && draft.currentStepId === step.id && safeString(step.idempotencyKey, 240) === expectedKey
        && step.attemptCount === expectedAttempt && action && actionAttempt === expectedAttempt
        && (!resultKey || resultKey === expectedKey));

      if (!eligible) {
        const safeJob = result.job ? sanitizeStructured(result.job, 30_000) : {};
        if (safeJob && typeof safeJob === 'object') delete safeJob.prompt;
        const evidence = sanitizeStructured({
          status, job: safeJob, error: result.error || '', output: result.output ?? '',
          receivedAt: timestamp, expectedIdempotencyKey: expectedKey, expectedAttempt,
          observedOperationStatus: draft.status, observedStepStatus: step?.status || 'missing',
        }, 30_000);
        if (action) {
          action.updatedAt = timestamp;
          if (result.job?.jobId) action.externalId = safeString(result.job.jobId, 300);
          action.metadata = sanitizeStructured({ ...safeObject(action.metadata), lateProviderResult: evidence }, 15_000);
          if (draft.status === 'paused') action.status = status;
        }
        if (step && result.job && (draft.status === 'paused' || TERMINAL_OPERATION_STATUSES.includes(draft.status))) {
          draft.metadata = {
            ...safeObject(draft.metadata),
            codexJobs: { ...safeObject(draft.metadata?.codexJobs), [step.id]: safeJob },
          };
        }
        const signature = `${step?.id || runningStep.id}:${expectedKey}:${expectedAttempt}:${status}:${safeString(result.job?.jobId, 300)}`;
        if (!draft.activityLog.some((event) => event.type === 'late_provider_result_recorded' && event.data?.signature === signature)) {
          draft.artifacts.push(createArtifact({
            operationId, stepId: step?.id || runningStep.id, type: 'late_provider_result',
            name: 'Late provider result (audit only)', mimeType: 'application/json', content: JSON.stringify(evidence),
            metadata: { terminalStatus: TERMINAL_OPERATION_STATUSES.includes(draft.status) ? draft.status : '', signature },
          }));
          appendEvent(draft, {
            type: 'late_provider_result_recorded', actor: 'runner', stepId: step?.id || runningStep.id,
            message: `A provider result arrived after its attempt was no longer eligible; operation status ${draft.status} was retained.`,
            data: { signature, providerStatus: status, operationStatus: draft.status, idempotencyKey: expectedKey, attempt: expectedAttempt },
          });
        }
        if (result.job?.jobId && step?.type === 'codex' && (draft.status === 'cancelled' || draft.status === 'paused')) {
          deferredControl = { operationStatus: draft.status, stepId: step.id, actionId: action?.id || '', job: safeJob };
        }
        return draft;
      }

      if (action) {
        action.status = status; action.updatedAt = timestamp;
        if (result.job?.jobId) action.externalId = safeString(result.job.jobId, 300);
        if (['completed', 'failed', 'cancelled'].includes(status)) action.completedAt = timestamp;
      }
      if (desktopCorrelation) {
        desktopCorrelation.updatedAt = timestamp;
        if (status === 'completed') {
          desktopCorrelation.status = 'completed'; desktopCorrelation.completedAt = timestamp;
        } else if (['failed', 'cancelled'].includes(status)) {
          desktopCorrelation.status = 'failed'; desktopCorrelation.completedAt = timestamp;
          desktopCorrelation.error = safeString(result.error, 8_000) || `Desktop provider returned ${status}.`;
        } else if (status === 'unknown') {
          desktopCorrelation.status = 'recovery_required';
          desktopCorrelation.error = safeString(result.error, 8_000) || 'Desktop provider outcome is unknown.';
        } else {
          desktopCorrelation.status = 'running';
        }
      }
      if (result.job) {
        const safeJob = sanitizeStructured(result.job, 30_000);
        delete safeJob.prompt;
        draft.metadata = { ...safeObject(draft.metadata), codexJobs: { ...safeObject(draft.metadata?.codexJobs), [step.id]: safeJob } };
      }
      if (step.type === 'verification' && Array.isArray(result.results)) {
        const other = draft.verification.filter((item) => item.stepId !== step.id);
        draft.verification = [...other, ...result.results.map((item) => normalizeVerificationResult(item, { operationId, stepId: step.id }))].slice(-100);
      }

      if (status === 'completed') {
        step.status = 'completed'; step.completedAt = timestamp; step.output = typeof result.output === 'string' ? result.output : JSON.stringify(sanitizeStructured(result.output ?? {}, 20_000)); step.error = '';
        if (['github_write', 'cloudflare_write'].includes(step.type)) {
          const evidence = sanitizeStructured(result.output ?? {}, 20_000);
          draft.verification.push(normalizeVerificationResult({
            type: 'provider_readback', status: 'passed', required: true, startedAt: step.startedAt, completedAt: timestamp,
            output: `The ${step.provider} mutation returned authoritative read-back verification.`,
            evidence: { provider: step.provider, action: step.toolName, result: evidence },
          }, { operationId, stepId: step.id }));
          draft.artifacts.push(createArtifact({
            type: 'provider_mutation_evidence', name: `${step.provider} ${step.toolName} verified result`,
            mimeType: 'application/json', content: JSON.stringify(evidence),
            metadata: { provider: step.provider, action: step.toolName, authoritative: true, verified: true },
          }, { operationId, stepId: step.id }));
        }
        for (const artifact of Array.isArray(result.artifacts) ? result.artifacts : []) draft.artifacts.push(createArtifact(artifact, { operationId, stepId: step.id }));
        if (result.diff && (result.diff.summary || Object.keys(result.diff).length)) {
          draft.artifacts.push(createCodexDiffArtifact(result.diff, { operationId, stepId: step.id }));
        }
        draft.status = step.type === 'verification' ? 'verifying' : 'running';
        appendEvent(draft, { type: 'step_completed', actor: 'runner', stepId: step.id, message: `Completed ${step.title}.`, data: { attempt: step.attemptCount } });
        return draft;
      }
      if (['started', 'queued', 'running'].includes(status) || (status === 'waiting' && result.job)) {
        step.status = 'running'; step.output = `Provider job ${safeString(result.job?.jobId, 300) || '(pending id)'} is ${status}.`;
        draft.status = 'awaiting_provider';
        appendEvent(draft, { type: 'provider_job_waiting', actor: 'runner', stepId: step.id, message: `Provider job is ${status}; completion was not assumed.`, data: { jobId: result.job?.jobId || '' } });
        return draft;
      }
      if (status === 'waiting_external') {
        step.status = 'blocked'; step.output = result.message || 'Waiting for external Codex execution.'; draft.status = 'blocked';
        if (result.artifact) draft.artifacts.push(createArtifact(result.artifact, { operationId, stepId: step.id }));
        draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: 'external_codex_required', status: 'active', message: result.message || step.output, createdAt: timestamp });
        appendEvent(draft, { type: 'external_codex_handoff_ready', actor: 'codex_provider', stepId: step.id, message: result.message || step.output });
        return draft;
      }
      if (status === 'waiting') {
        step.status = 'blocked'; step.output = result.output || ''; step.error = result.error || 'Provider evidence is still pending.'; draft.status = 'blocked';
        draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: step.type === 'verification' ? 'verification_required' : 'provider_result_required', status: 'active', message: step.error, createdAt: timestamp });
        appendEvent(draft, { type: 'step_waiting_evidence', actor: 'runner', stepId: step.id, message: step.error, data: result.evidence || {} });
        return draft;
      }
      if (status === 'paused') {
        step.status = 'blocked'; step.error = result.error || 'Provider paused the job.'; draft.status = 'paused'; draft.pausedAt = timestamp;
        appendEvent(draft, { type: 'provider_job_paused', actor: 'runner', stepId: step.id, message: step.error });
        return draft;
      }
      if (status === 'unknown') {
        step.status = 'blocked'; step.error = result.error || 'Provider outcome is unknown; retry is unsafe until reconciled.'; draft.status = 'recovery_required';
        draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: 'recovery_required', status: 'active', message: step.error, createdAt: timestamp });
        appendEvent(draft, { type: 'provider_state_unknown', actor: 'runner', stepId: step.id, message: step.error });
        return draft;
      }
      if (status === 'cancelled') {
        step.status = 'cancelled'; step.error = result.error || 'Provider cancelled the job.'; draft.status = 'cancelled'; draft.cancelledAt = timestamp;
        if (action) action.cancellationConfirmed = true;
        appendEvent(draft, { type: 'provider_job_cancelled', actor: 'runner', stepId: step.id, message: step.error, data: { externalCancellationConfirmed: true } });
        return draft;
      }
      step.status = 'failed'; step.failedAt = timestamp; step.error = safeString(result.error, 8_000) || 'Provider execution failed.';
      draft.status = step.type === 'verification' ? 'blocked' : 'failed'; if (draft.status === 'failed') draft.failedAt = timestamp;
      draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: step.type === 'verification' ? 'verification_failed' : 'provider_failed', status: 'active', message: step.error, createdAt: timestamp });
      appendEvent(draft, { type: step.type === 'verification' ? 'verification_failed' : 'step_failed', actor: 'runner', stepId: step.id, message: step.error });
      return draft;
    });

    if (!deferredControl || !this.providers.codex || this.providers.codex.mode !== 'direct') return updated;
    let controlledJob = null;
    let controlError = '';
    let controlType = '';
    try {
      if (deferredControl.operationStatus === 'cancelled') {
        controlType = 'cancel';
        controlledJob = await promiseWithTimeout(this.providers.codex.cancelJob(deferredControl.job), this.providerTimeoutMs);
      } else if (deferredControl.operationStatus === 'paused' && this.providers.codex.supportsPause()) {
        controlType = 'pause';
        controlledJob = await promiseWithTimeout(this.providers.codex.pauseJob(deferredControl.job), this.providerTimeoutMs);
      }
    } catch (error) {
      controlError = safeString(error?.message, 4_000) || `Provider ${controlType || 'control'} failed.`;
    }
    if (!controlType) return updated;
    updated = await this.store.update(businessKey, operationId, (draft) => {
      if (draft.status !== deferredControl.operationStatus) return draft;
      const timestamp = nowIso();
      const action = draft.providerActions.find((item) => item.id === deferredControl.actionId);
      const normalizedStatus = normalizeProviderStatus(controlledJob?.status);
      const confirmed = controlType === 'cancel' ? normalizedStatus === 'cancelled' : normalizedStatus === 'paused';
      if (action) {
        action.updatedAt = timestamp;
        action.cancellationConfirmed = controlType === 'cancel' && confirmed;
        if (confirmed) action.status = normalizedStatus;
        action.metadata = sanitizeStructured({
          ...safeObject(action.metadata), lateProviderControl: { type: controlType, confirmed, error: controlError, attemptedAt: timestamp },
        }, 15_000);
      }
      if (controlledJob) {
        const safeJob = sanitizeStructured(controlledJob, 30_000);
        delete safeJob.prompt;
        draft.metadata.codexJobs = { ...safeObject(draft.metadata?.codexJobs), [deferredControl.stepId]: safeJob };
      }
      appendEvent(draft, {
        type: `late_provider_${controlType}_attempted`, actor: 'runner', stepId: deferredControl.stepId,
        message: confirmed ? `Late external provider ${controlType} was confirmed.` : `Late external provider ${controlType} was not confirmed.`,
        data: { confirmed, error: controlError },
      });
      return draft;
    });
    return updated;
  }

  async requestApproval(businessKey, operationId, next, classification) {
    return this.store.update(businessKey, operationId, (draft) => {
      const step = draft.steps.find((item) => item.id === next.id);
      const approval = this.approvalService.buildRequest({ operation: draft, step, classification });
      if (!draft.approvals.some((item) => item.id === approval.id)) draft.approvals.push(approval);
      step.status = 'waiting_for_approval'; step.approvalRequired = true; step.approvalId = approval.id; step.riskLevel = classification.riskLevel;
      draft.currentStepId = step.id; draft.status = 'waiting_for_approval';
      appendEvent(draft, { type: 'approval_requested', actor: 'policy', stepId: step.id, message: classification.reason, data: { approvalId: approval.id, action: classification.action, riskLevel: classification.riskLevel } });
      return draft;
    });
  }

  async finishOrBlock(businessKey, operation) {
    const allFinished = operation.steps.length && operation.steps.every((step) => ['completed', 'skipped'].includes(step.status));
    if (allFinished && requiredVerificationPassed(operation)) {
      if (operation.status !== 'verifying') await this.store.update(businessKey, operation.id, (draft) => { draft.status = 'verifying'; return draft; });
      return this.service.completeOperation(businessKey, operation.id, { actor: 'runner' });
    }
    return this.store.update(businessKey, operation.id, (draft) => {
      draft.status = 'blocked';
      const type = allFinished ? 'verification_required' : 'no_runnable_step';
      const message = allFinished ? 'Required verification has not passed or been explicitly waived.' : 'No runnable step is available.';
      draft.blockers.push({ id: makeOperationId('blocker'), operationId: draft.id, type, status: 'active', message, createdAt: nowIso() });
      appendEvent(draft, { type: `runner_${type}`, actor: 'runner', message });
      return draft;
    });
  }

  async markRecoveryRequired(businessKey, operationId, stepId, message) {
    return this.store.update(businessKey, operationId, (draft) => {
      const step = draft.steps.find((item) => item.id === stepId);
      if (step) { step.status = 'blocked'; step.error = message; }
      draft.status = 'recovery_required'; draft.currentStepId = stepId;
      draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId, type: 'recovery_required', status: 'active', message, createdAt: nowIso() });
      appendEvent(draft, { type: 'recovery_required', actor: 'runner', stepId, message });
      return draft;
    });
  }

  async invokeProvider({ operation, step, registryRecord }) {
    if (step.type === 'approval') return { status: 'completed', output: `Approval checkpoint satisfied by record ${step.approvalId || 'unknown'}.` };
    if (step.type === 'internal') {
      const allowed = new Set(['prepare_operation_context', 'prepare_context', 'create_internal_note', 'generate_plan', 'create_handoff']);
      if (!allowed.has(step.toolName)) return { status: 'failed', error: `Internal action is not allowlisted: ${step.toolName || '(missing)'}.` };
      return { status: 'completed', output: { projectRegistryId: operation.projectRegistryId, repository: registryRecord?.repo?.fullName || '', workspace: registryRecord?.localWorkspace?.path || '', acceptanceCriteriaCount: operation.acceptanceCriteria.length } };
    }
    if (step.type === 'verification') return this.verification.run({ operation, step, registryRecord, idempotencyKey: step.idempotencyKey });
    const provider = this.providers[step.provider] || this.providers[step.type];
    if (!provider) return { status: 'failed', error: `No provider is registered for ${step.provider || step.type}.` };
    if (step.type === 'codex') return provider.startJob({ operation, step, registryRecord, idempotencyKey: step.idempotencyKey });
    return provider.execute({ operation, step, registryRecord, idempotencyKey: step.idempotencyKey });
  }
}

export { dependenciesComplete, hasFailedDependency, promiseWithTimeout };
