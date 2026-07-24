import {
  createArtifact,
  makeOperationId,
  normalizeProviderAction,
  normalizeVerificationResult,
  nowIso,
  requiredVerificationPassed,
  safeObject,
  safeString,
  sanitizeStructured,
} from './operation_types.js';
import { normalizeProviderStatus } from '../providers/codex_provider.js';
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

export class OperationRunner {
  constructor({ store, registry, service, policy, approvalService, providers, verification, maxStepsPerCycle = 10, cycleTimeoutMs = 60_000, providerTimeoutMs = 45_000 }) {
    Object.assign(this, { store, registry, service, policy, approvalService, providers, verification });
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
      const classification = this.policy.classify({
        business: businessKey,
        projectRegistryId: operation.projectRegistryId,
        environment,
        provider: next.provider,
        action: next.toolName || next.type,
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
        step.idempotencyKey = step.idempotencyKey || `${operationId}:${step.id}:${step.attemptCount}`;
        step.riskLevel = classification.riskLevel; draft.currentStepId = step.id;
        if (!draft.providerActions.some((action) => action.idempotencyKey === step.idempotencyKey)) {
          draft.providerActions.push(normalizeProviderAction({
            id: makeOperationId('action'), operationId, stepId: step.id, provider: step.provider, action: step.toolName || step.type,
            idempotencyKey: step.idempotencyKey, status: 'started', issuedAt: nowIso(), updatedAt: nowIso(), metadata: { attempt: step.attemptCount },
          }));
        }
        appendEvent(draft, { type: 'provider_action_issued', actor: 'runner', stepId: step.id, message: `Issued ${step.title} through ${step.provider}.`, data: { attempt: step.attemptCount, idempotencyKey: step.idempotencyKey } });
        return draft;
      });

      const runningStep = operation.steps.find((step) => step.id === next.id);
      let result;
      try {
        result = await promiseWithTimeout(this.invokeProvider({ operation, step: runningStep, registryRecord }), this.providerTimeoutMs);
      } catch (error) {
        result = { status: error?.code === 'PROVIDER_TIMEOUT' ? 'unknown' : 'failed', error: error?.message || 'Provider failed.' };
      }
      if (runningStep.type === 'codex' && normalizeProviderStatus(result?.status) === 'completed' && result?.job) {
        result = await this.collectCodexCompletion(result).catch((error) => ({ status: 'unknown', job: result.job, error: `Codex completed but its artifacts could not be collected: ${error?.message || 'unknown error'}` }));
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

  async collectCodexCompletion(result) {
    const provider = this.providers.codex;
    const [artifacts, diff] = await Promise.all([
      promiseWithTimeout(provider.getArtifacts(result.job), this.providerTimeoutMs),
      promiseWithTimeout(provider.getDiff(result.job), this.providerTimeoutMs),
    ]);
    return { ...result, status: 'completed', artifacts, diff };
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
      try { result = await this.collectCodexCompletion({ status, job: { ...job, ...polled } }); }
      catch (error) { result = { status: 'unknown', job: { ...job, ...polled }, error: `Codex completion evidence could not be collected: ${error?.message || 'unknown error'}` }; }
      return this.applyProviderResult(businessKey, operation.id, step, result);
    }
    return this.applyProviderResult(businessKey, operation.id, step, { status, job: { ...job, ...polled }, error: polled?.error });
  }

  async applyProviderResult(businessKey, operationId, runningStep, rawResult = {}) {
    const result = safeObject(rawResult);
    const status = normalizeProviderStatus(result.status);
    return this.store.update(businessKey, operationId, (draft) => {
      const step = draft.steps.find((item) => item.id === runningStep.id);
      if (!step) return draft;
      const timestamp = nowIso();
      const action = draft.providerActions.find((item) => item.idempotencyKey === step.idempotencyKey);
      if (action) {
        action.status = status; action.updatedAt = timestamp;
        if (result.job?.jobId) action.externalId = safeString(result.job.jobId, 300);
        if (['completed', 'failed', 'cancelled'].includes(status)) action.completedAt = timestamp;
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
        for (const artifact of Array.isArray(result.artifacts) ? result.artifacts : []) draft.artifacts.push(createArtifact(artifact, { operationId, stepId: step.id }));
        if (result.diff && (result.diff.summary || Object.keys(result.diff).length)) draft.artifacts.push(createArtifact({ type: 'codex_diff', name: 'Codex diff', mimeType: 'application/json', content: JSON.stringify(result.diff) }, { operationId, stepId: step.id }));
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
