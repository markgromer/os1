import {
  makeOperationId,
  normalizeArtifact,
  nowIso,
  requiredVerificationPassed,
  safeObject,
  safeString,
} from './operation_types.js';
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
  return step.dependsOn.every((id) => {
    const dependency = operation.steps.find((item) => item.id === id);
    return dependency && ['completed', 'skipped'].includes(dependency.status);
  });
}

function hasFailedDependency(operation, step) {
  return step.dependsOn.some((id) => {
    const dependency = operation.steps.find((item) => item.id === id);
    return dependency && ['failed', 'blocked', 'cancelled'].includes(dependency.status);
  });
}

export class OperationRunner {
  constructor({ store, registry, service, policy, approvalService, providers, verification, maxStepsPerCycle = 10, cycleTimeoutMs = 60_000, providerTimeoutMs = 45_000 }) {
    this.store = store;
    this.registry = registry;
    this.service = service;
    this.policy = policy;
    this.approvalService = approvalService;
    this.providers = providers;
    this.verification = verification;
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
    if (!['queued', 'running', 'verifying'].includes(operation.status)) return operation;

    if (operation.status === 'queued') {
      operation = await this.store.update(businessKey, operationId, (draft) => {
        transition(draft, 'running');
        appendEvent(draft, { type: 'runner_cycle_started', actor: 'runner', message: 'A real runner cycle started.', data: {}, timestamp: nowIso() });
        return draft;
      });
    }

    while (executed < this.maxStepsPerCycle && (Date.now() - cycleStarted) < this.cycleTimeoutMs) {
      operation = await this.store.get(businessKey, operationId);
      if (!operation || ['cancelled', 'failed', 'completed', 'paused', 'blocked', 'waiting_for_approval'].includes(operation.status)) return operation;

      const waitingRunning = operation.steps.find((step) => step.status === 'running');
      if (waitingRunning) {
        return this.store.update(businessKey, operationId, (draft) => {
          draft.status = 'paused';
          draft.pausedAt = nowIso();
          appendEvent(draft, { type: 'runner_waiting_provider', actor: 'runner', stepId: waitingRunning.id, message: 'Provider work remains in progress; the runner stopped instead of assuming completion.', data: {}, timestamp: nowIso() });
          return draft;
        });
      }

      const failedDependencyStep = operation.steps.find((step) => ['pending', 'ready'].includes(step.status) && hasFailedDependency(operation, step));
      if (failedDependencyStep) {
        return this.store.update(businessKey, operationId, (draft) => {
          const step = draft.steps.find((item) => item.id === failedDependencyStep.id);
          step.status = 'blocked';
          step.error = 'A dependency did not complete.';
          draft.status = 'blocked';
          draft.currentStepId = step.id;
          draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: 'dependency_failed', status: 'active', message: `Step "${step.title}" is blocked by an incomplete dependency.`, createdAt: nowIso() });
          appendEvent(draft, { type: 'step_blocked_dependency', actor: 'runner', stepId: step.id, message: step.error, data: { dependsOn: step.dependsOn }, timestamp: nowIso() });
          return draft;
        });
      }

      const next = operation.steps
        .filter((step) => ['pending', 'ready'].includes(step.status) && dependenciesComplete(operation, step))
        .sort((a, b) => a.sequence - b.sequence || a.id.localeCompare(b.id))[0];

      if (!next) {
        const allFinished = operation.steps.length > 0 && operation.steps.every((step) => ['completed', 'skipped'].includes(step.status));
        if (allFinished) {
          if (!requiredVerificationPassed(operation)) {
            return this.store.update(businessKey, operationId, (draft) => {
              if (draft.status === 'running') transition(draft, 'verifying');
              draft.status = 'blocked';
              draft.blockers.push({ id: makeOperationId('blocker'), operationId, type: 'verification_required', status: 'active', message: 'Required verification has not passed or been explicitly waived.', createdAt: nowIso() });
              appendEvent(draft, { type: 'completion_blocked_verification', actor: 'runner', message: 'Completion refused because required verification is incomplete.', data: {}, timestamp: nowIso() });
              return draft;
            });
          }
          if (operation.status === 'running') {
            await this.store.update(businessKey, operationId, (draft) => {
              transition(draft, 'verifying');
              appendEvent(draft, { type: 'operation_verifying', actor: 'runner', message: 'All steps finished; final verification gate evaluated.', data: {}, timestamp: nowIso() });
              return draft;
            });
          }
          return this.service.completeOperation(businessKey, operationId, { actor: 'runner' });
        }
        return this.store.update(businessKey, operationId, (draft) => {
          draft.status = 'blocked';
          draft.blockers.push({ id: makeOperationId('blocker'), operationId, type: 'no_runnable_step', status: 'active', message: 'No runnable step is available.', createdAt: nowIso() });
          appendEvent(draft, { type: 'runner_no_runnable_step', actor: 'runner', message: 'Runner stopped because no step was runnable.', data: {}, timestamp: nowIso() });
          return draft;
        });
      }

      const registryRecord = operation.projectRegistryId ? await this.registry.get(businessKey, operation.projectRegistryId) : null;
      const input = safeObject(next.input);
      const classification = this.policy.classify({
        business: businessKey,
        environment: safeString(input.environment, 100) || 'development',
        provider: next.provider,
        action: next.toolName || next.type,
        riskLevel: next.riskLevel,
        autonomyMode: operation.autonomyMode,
        explicitRequest: input.explicitRequest === true,
        configuredAutonomy: operation.autonomyMode === 'configured',
      });
      if (next.attemptCount > 0 && ['high', 'critical'].includes(classification.riskLevel)) {
        classification.action = `${classification.action}:retry-${next.attemptCount + 1}`;
        classification.approvalRequired = true;
        classification.approvalRequirement = classification.riskLevel === 'critical' ? 'explicit_strong_confirmation' : 'explicit';
        classification.reason = `${classification.riskLevel} risk retry requires a fresh approval to prevent duplicate irreversible effects.`;
      }

      const approved = operation.approvals.find((approval) => approval.stepId === next.id && approval.action === classification.action && approval.status === 'approved');
      if (classification.approvalRequired && !approved) {
        return this.store.update(businessKey, operationId, (draft) => {
          const step = draft.steps.find((item) => item.id === next.id);
          const approval = this.approvalService.buildRequest({ operation: draft, step, classification });
          if (!draft.approvals.some((item) => item.id === approval.id)) draft.approvals.push(approval);
          step.status = 'waiting_for_approval';
          step.approvalRequired = true;
          step.approvalId = approval.id;
          step.riskLevel = classification.riskLevel;
          draft.riskLevel = classification.riskLevel === 'critical' ? 'critical' : (classification.riskLevel === 'high' && draft.riskLevel !== 'critical' ? 'high' : draft.riskLevel);
          draft.currentStepId = step.id;
          draft.status = 'waiting_for_approval';
          appendEvent(draft, { type: 'approval_requested', actor: 'policy', stepId: step.id, message: classification.reason, data: { approvalId: approval.id, action: classification.action, riskLevel: classification.riskLevel }, timestamp: nowIso() });
          return draft;
        });
      }

      operation = await this.store.update(businessKey, operationId, (draft) => {
        const step = draft.steps.find((item) => item.id === next.id);
        if (!step || !['pending', 'ready'].includes(step.status)) throw Object.assign(new Error('Step changed before execution.'), { code: 'STEP_REVISION_CHANGED' });
        if (step.type === 'verification' && draft.status === 'running') {
          transition(draft, 'verifying');
          appendEvent(draft, { type: 'operation_verifying', actor: 'runner', stepId: step.id, message: 'Implementation finished; required verification started.', data: {}, timestamp: nowIso() });
        }
        step.status = 'running';
        step.attemptCount += 1;
        step.startedAt = nowIso();
        step.error = '';
        step.idempotencyKey = step.idempotencyKey || `${operationId}:${step.id}:${step.attemptCount}`;
        step.riskLevel = classification.riskLevel;
        draft.currentStepId = step.id;
        appendEvent(draft, { type: 'step_started', actor: 'runner', stepId: step.id, message: `Started ${step.title} through ${step.provider}.`, data: { attempt: step.attemptCount, idempotencyKey: step.idempotencyKey }, timestamp: nowIso() });
        return draft;
      });

      const runningStep = operation.steps.find((step) => step.id === next.id);
      let result;
      try {
        result = await promiseWithTimeout(this.invokeProvider({ operation, step: runningStep, registryRecord }), this.providerTimeoutMs);
      } catch (error) {
        result = { status: 'failed', error: error?.message || 'Provider failed.' };
      }
      executed += 1;

      operation = await this.store.update(businessKey, operationId, (draft) => {
        const step = draft.steps.find((item) => item.id === runningStep.id);
        if (!step) return draft;
        const timestamp = nowIso();

        if (step.type === 'verification' && Array.isArray(result.results)) {
          const otherResults = draft.verification.filter((item) => item.stepId !== step.id);
          draft.verification = [...otherResults, ...result.results].slice(-100);
        }

        if (result.status === 'completed') {
          step.status = 'completed';
          step.completedAt = timestamp;
          step.output = result.output || '';
          step.error = '';
          appendEvent(draft, { type: 'step_completed', actor: 'runner', stepId: step.id, message: `Completed ${step.title}.`, data: { attempt: step.attemptCount }, timestamp });
          return draft;
        }

        if (result.status === 'waiting_external') {
          step.status = 'blocked';
          step.output = result.message || 'Waiting for external Codex execution.';
          const job = { ...safeObject(result.job) };
          delete job.prompt;
          draft.metadata = { ...safeObject(draft.metadata), codexJobs: { ...safeObject(draft.metadata?.codexJobs), [step.id]: job } };
          if (result.artifact) draft.artifacts.push(normalizeArtifact(result.artifact, { operationId, stepId: step.id }));
          draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: 'external_codex_required', status: 'active', message: result.message, createdAt: timestamp });
          draft.status = 'blocked';
          appendEvent(draft, { type: 'external_codex_handoff_ready', actor: 'codex_provider', stepId: step.id, message: result.message, data: { providerMode: 'external_handoff', artifactId: result.artifact?.id || '' }, timestamp });
          return draft;
        }

        if (result.status === 'waiting') {
          step.status = 'blocked';
          step.output = result.output || '';
          step.error = result.error || 'Provider evidence is still pending.';
          draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: step.type === 'verification' ? 'verification_required' : 'provider_result_required', status: 'active', message: step.error, createdAt: timestamp });
          draft.status = 'blocked';
          appendEvent(draft, { type: 'step_waiting_evidence', actor: 'runner', stepId: step.id, message: step.error, data: result.evidence || {}, timestamp });
          return draft;
        }

        step.status = 'failed';
        step.failedAt = timestamp;
        step.error = safeString(result.error, 8_000) || 'Provider execution failed.';
        draft.status = step.type === 'verification' ? 'blocked' : 'failed';
        if (draft.status === 'failed') draft.failedAt = timestamp;
        draft.blockers.push({ id: makeOperationId('blocker'), operationId, stepId: step.id, type: step.type === 'verification' ? 'verification_failed' : 'provider_failed', status: 'active', message: step.error, createdAt: timestamp });
        appendEvent(draft, { type: step.type === 'verification' ? 'verification_failed' : 'step_failed', actor: 'runner', stepId: step.id, message: step.error, data: { attempt: step.attemptCount, maxAttempts: step.maxAttempts }, timestamp });
        return draft;
      });
      if (['blocked', 'failed', 'paused', 'waiting_for_approval'].includes(operation.status)) return operation;
    }

    operation = await this.store.get(businessKey, operationId);
    if (operation && operation.status === 'running') {
      operation = await this.store.update(businessKey, operationId, (draft) => {
        transition(draft, 'queued');
        appendEvent(draft, { type: 'runner_cycle_yielded', actor: 'runner', message: `Runner cycle yielded after ${executed} step(s); another explicit tick is required.`, data: { maxStepsPerCycle: this.maxStepsPerCycle }, timestamp: nowIso() });
        return draft;
      });
    }
    return operation;
  }

  async invokeProvider({ operation, step, registryRecord }) {
    if (step.type === 'approval') {
      return { status: 'completed', output: `Approval checkpoint satisfied by record ${step.approvalId || 'unknown'}.` };
    }
    if (step.type === 'internal') {
      const allowedInternalActions = new Set(['prepare_operation_context', 'prepare_context', 'create_internal_note', 'generate_plan', 'create_handoff']);
      if (!allowedInternalActions.has(step.toolName)) {
        return { status: 'failed', error: `Internal action is not allowlisted: ${step.toolName || '(missing)'}.` };
      }
      return {
        status: 'completed',
        output: {
          projectRegistryId: operation.projectRegistryId,
          repository: registryRecord?.repo?.fullName || registryRecord?.repo?.url || '',
          workspace: registryRecord?.localWorkspace?.path || '',
          deployments: registryRecord?.deployments || {},
          acceptanceCriteriaCount: operation.acceptanceCriteria.length,
        },
      };
    }
    if (step.type === 'verification') return this.verification.run({ operation, step, registryRecord });
    const provider = this.providers[step.provider] || this.providers[step.type];
    if (!provider) return { status: 'failed', error: `No provider is registered for ${step.provider || step.type}.` };
    if (step.type === 'codex') return provider.startJob({ operation, step, registryRecord });
    return provider.execute({ operation, step, registryRecord });
  }
}

export { dependenciesComplete, hasFailedDependency };
