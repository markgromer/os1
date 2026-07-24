import { makeOperationId, nowIso, requiredVerificationPassed, safeObject } from './operation_types.js';

export class OperationRecovery {
  constructor({ store }) { this.store = store; }

  async recoverBusiness(businessKey) {
    const operations = await this.store.listAll(businessKey, { nonterminal: true });
    const recovered = [];
    for (const operation of operations) {
      const activeJobs = Object.entries(safeObject(operation.metadata?.codexJobs))
        .filter(([, job]) => ['started', 'queued', 'running', 'waiting'].includes(job?.status));
      const queuedDesktop = operation.desktopCorrelations.filter((item) => ['queued', 'running'].includes(item.status));
      const unknownActions = operation.providerActions.filter((item) => item.status === 'unknown');
      const runningWithoutJob = operation.steps.filter((step) => step.status === 'running' && !activeJobs.some(([stepId]) => stepId === step.id));
      const expiredApprovals = operation.approvals.filter((approval) => approval.status === 'pending' && approval.expiresAt && Date.parse(approval.expiresAt) <= Date.now());
      const implementationFinished = operation.steps.some((step) => step.type === 'codex' && step.status === 'completed');
      const verificationIncomplete = implementationFinished && !requiredVerificationPassed(operation);
      const inconsistentJobStatus = activeJobs.length && !['awaiting_provider', 'running', 'queued'].includes(operation.status);
      if (!activeJobs.length && !queuedDesktop.length && !unknownActions.length && !runningWithoutJob.length && !expiredApprovals.length && !verificationIncomplete && !inconsistentJobStatus) continue;

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
          correlation.status = 'recovery_required';
          state = 'recovery_required';
          const verification = draft.verification.find((item) => item.id === correlation.verificationId);
          if (verification) verification.error = 'Server restarted while desktop verification was awaiting a result. A matching late result may still reconcile it.';
        }
        for (const step of draft.steps) {
          const job = safeObject(draft.metadata?.codexJobs)[step.id];
          if (job && ['started', 'queued', 'running', 'waiting'].includes(job.status)) {
            step.status = 'running'; state = state || 'awaiting_provider';
          } else if (step.status === 'running') {
            step.status = 'blocked'; step.error = 'Execution was interrupted and no provider state proves its outcome.'; state = 'recovery_required';
          }
        }
        if (draft.providerActions.some((item) => item.status === 'unknown')) state = 'recovery_required';
        if (verificationIncomplete && !['recovery_required', 'awaiting_provider', 'waiting_for_approval'].includes(state)) state = 'blocked';
        draft.status = state || draft.status;
        draft.currentStepId = draft.steps.find((step) => ['running', 'blocked', 'waiting_for_approval'].includes(step.status))?.id || draft.currentStepId;
        if (state === 'recovery_required' && !draft.blockers.some((item) => item.type === 'recovery_required' && item.status === 'active')) {
          draft.blockers.push({ id: makeOperationId('blocker'), operationId: draft.id, stepId: draft.currentStepId, type: 'recovery_required', status: 'active', message: 'One or more asynchronous actions require provider or desktop reconciliation after restart.', createdAt: timestamp });
        }
        if (state === 'blocked' && !draft.blockers.some((item) => item.type === 'verification_required' && item.status === 'active')) {
          draft.blockers.push({ id: makeOperationId('blocker'), operationId: draft.id, type: 'verification_required', status: 'active', message: 'Implementation is complete but required verification is incomplete.', createdAt: timestamp });
        }
        draft.activityLog.push({ id: makeOperationId('evt'), operationId: draft.id, stepId: draft.currentStepId, type: 'operation_recovered', actor: 'system', message: `Startup reconciliation classified this operation as ${state || draft.status}; no completion was assumed.`, data: { activeJobs: activeJobs.length, queuedDesktop: queuedDesktop.length, unknownActions: unknownActions.length, expiredApprovals: expiredApprovals.length }, timestamp });
        return draft;
      });
      recovered.push(operation.id);
    }
    return recovered;
  }
}
