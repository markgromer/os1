import { makeOperationId, nowIso, requiredVerificationPassed, safeObject, safeString, TERMINAL_OPERATION_STATUSES } from './operation_types.js';

const RECOVERABLE_DESKTOP_ACTIONS = new Set(['run-project-script', 'prepare-publish', 'open-vscode']);
const ACTIVE_CODEX_JOB_STATUSES = new Set(['started', 'queued', 'running', 'waiting', 'paused']);

function codexJobNeedsReconciliation(step, job) {
  const status = safeString(job?.status, 100).toLowerCase();
  return ACTIVE_CODEX_JOB_STATUSES.has(status) || (status === 'completed' && step?.status !== 'completed');
}

export class OperationRecovery {
  constructor({ store, registry = null, queueDesktopAction = null }) {
    this.store = store;
    this.registry = registry;
    this.queueDesktopAction = typeof queueDesktopAction === 'function' ? queueDesktopAction : null;
  }

  async restoreDesktopDispatch(operation, correlation) {
    if (!this.registry || !this.queueDesktopAction || operation.status === 'paused') return false;
    if (!RECOVERABLE_DESKTOP_ACTIONS.has(correlation.actionType)) return false;
    const project = await this.registry.get(operation.businessKey, correlation.projectRegistryId);
    const step = operation.steps.find((item) => item.id === correlation.stepId);
    if (!project || !step || project.localWorkspace?.trustStatus !== 'approved'
      || project.localWorkspace?.desktopAgentId !== correlation.desktopAgentId) return false;
    const workspacePath = safeString(project.localWorkspace?.path, 2_000);
    if (!workspacePath) return false;
    const verification = correlation.verificationId
      ? operation.verification.find((item) => item.id === correlation.verificationId)
      : null;
    const payload = {
      path: workspacePath,
      businessKey: operation.businessKey,
      operationId: operation.id,
      stepId: step.id,
      projectRegistryId: project.id,
      desktopAgentId: correlation.desktopAgentId,
      idempotencyKey: correlation.idempotencyKey,
      attemptNumber: correlation.attemptNumber,
    };
    if (correlation.actionType === 'run-project-script') {
      payload.scriptName = safeString(verification?.type || step.input?.scriptName, 100);
      if (!['build', 'test', 'lint', 'typecheck', 'dev', 'install'].includes(payload.scriptName)) return false;
    }
    const action = await this.queueDesktopAction({
      id: correlation.actionId,
      idempotencyKey: correlation.idempotencyKey,
      type: correlation.actionType,
      payload,
      requestedBy: `operation:${operation.id}`,
    });
    return action?.id === correlation.actionId;
  }

  async recoverBusiness(businessKey) {
    const operations = await this.store.listAll(businessKey, { nonterminal: true });
    const recovered = [];
    for (const operation of operations) {
      if (TERMINAL_OPERATION_STATUSES.includes(operation.status)) continue;
      const stepById = new Map(operation.steps.map((step) => [step.id, step]));
      const activeJobs = Object.entries(safeObject(operation.metadata?.codexJobs))
        .filter(([stepId, job]) => codexJobNeedsReconciliation(stepById.get(stepId), job));
      const queuedDesktop = operation.desktopCorrelations.filter((item) => ['queued', 'running'].includes(item.status));
      const unknownActions = operation.providerActions.filter((item) => item.status === 'unknown');
      const runningWithoutJob = operation.steps.filter((step) => step.status === 'running' && !activeJobs.some(([stepId]) => stepId === step.id));
      const expiredApprovals = operation.approvals.filter((approval) => approval.status === 'pending' && approval.expiresAt && Date.parse(approval.expiresAt) <= Date.now());
      const implementationFinished = operation.steps.some((step) => step.type === 'codex' && step.status === 'completed');
      const verificationIncomplete = implementationFinished && !requiredVerificationPassed(operation);
      const verificationNeedsClassification = verificationIncomplete && (operation.status !== 'blocked'
        || !operation.blockers.some((item) => item.type === 'verification_required' && item.status === 'active'));
      const inconsistentJobStatus = activeJobs.length && !['awaiting_provider', 'running', 'queued', 'paused'].includes(operation.status);
      if (!activeJobs.length && !queuedDesktop.length && !unknownActions.length && !runningWithoutJob.length && !expiredApprovals.length && !verificationNeedsClassification && !inconsistentJobStatus) continue;

      const restoredDesktopIds = new Set();
      for (const correlation of queuedDesktop) {
        try {
          if (await this.restoreDesktopDispatch(operation, correlation)) restoredDesktopIds.add(correlation.actionId);
        } catch {
          // The operation is moved to recovery-required below; no action is assumed queued.
        }
      }

      await this.store.update(businessKey, operation.id, (draft) => {
        const timestamp = nowIso();
        let state = '';
        for (const approval of draft.approvals) {
          if (approval.status === 'pending' && approval.expiresAt && Date.parse(approval.expiresAt) <= Date.now()) {
            approval.status = 'expired';
            const step = draft.steps.find((item) => item.id === approval.stepId);
            if (step) step.status = 'waiting_for_approval';
            state = 'waiting_for_approval';
          }
        }
        for (const correlation of draft.desktopCorrelations) {
          if (!['queued', 'running'].includes(correlation.status)) continue;
          if (draft.status === 'paused') continue;
          if (restoredDesktopIds.has(correlation.actionId)) {
            correlation.updatedAt = timestamp;
            state = state || 'awaiting_provider';
            const verification = draft.verification.find((item) => item.id === correlation.verificationId);
            if (verification) verification.error = '';
            continue;
          }
          correlation.status = 'recovery_required';
          correlation.updatedAt = timestamp;
          state = 'recovery_required';
          const verification = draft.verification.find((item) => item.id === correlation.verificationId);
          if (verification) verification.error = 'Server restarted while desktop verification was awaiting a result. A matching late result may still reconcile it.';
        }
        for (const step of draft.steps) {
          const job = safeObject(draft.metadata?.codexJobs)[step.id];
          if (job && codexJobNeedsReconciliation(step, job)) {
            step.status = 'running';
            if (draft.status !== 'paused') state = state || 'awaiting_provider';
          } else if (step.status === 'running') {
            const restoredDesktop = draft.desktopCorrelations.some((item) => item.stepId === step.id && restoredDesktopIds.has(item.actionId));
            if (restoredDesktop) {
              state = state || 'awaiting_provider';
              continue;
            }
            step.status = 'blocked'; step.error = 'Execution was interrupted and no provider state proves its outcome.'; state = 'recovery_required';
          }
        }
        if (draft.providerActions.some((item) => item.status === 'unknown')) state = 'recovery_required';
        if (verificationNeedsClassification && !['recovery_required', 'awaiting_provider', 'waiting_for_approval'].includes(state)) state = 'blocked';
        draft.status = draft.status === 'paused' ? 'paused' : (state || draft.status);
        draft.currentStepId = draft.steps.find((step) => ['running', 'blocked', 'waiting_for_approval'].includes(step.status))?.id || draft.currentStepId;
        if (state === 'recovery_required' && !draft.blockers.some((item) => item.type === 'recovery_required' && item.status === 'active')) {
          draft.blockers.push({ id: makeOperationId('blocker'), operationId: draft.id, stepId: draft.currentStepId, type: 'recovery_required', status: 'active', message: 'One or more asynchronous actions require provider or desktop reconciliation after restart.', createdAt: timestamp });
        }
        if (state === 'blocked' && !draft.blockers.some((item) => item.type === 'verification_required' && item.status === 'active')) {
          draft.blockers.push({ id: makeOperationId('blocker'), operationId: draft.id, type: 'verification_required', status: 'active', message: 'Implementation is complete but required verification is incomplete.', createdAt: timestamp });
        }
        draft.activityLog.push({ id: makeOperationId('evt'), operationId: draft.id, stepId: draft.currentStepId, type: 'operation_recovered', actor: 'system', message: `Startup reconciliation classified this operation as ${draft.status}; no completion was assumed.`, data: { activeJobs: activeJobs.length, queuedDesktop: queuedDesktop.length, restoredDesktop: restoredDesktopIds.size, unknownActions: unknownActions.length, expiredApprovals: expiredApprovals.length }, timestamp });
        return draft;
      });
      recovered.push(operation.id);
    }
    return recovered;
  }
}
