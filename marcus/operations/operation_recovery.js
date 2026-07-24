import { makeOperationId, nowIso } from './operation_types.js';

export class OperationRecovery {
  constructor({ store }) {
    this.store = store;
  }

  async recoverBusiness(businessKey) {
    const operations = await this.store.list(businessKey, { limit: 500 });
    const recovered = [];
    for (const operation of operations) {
      const interruptedSteps = operation.steps.filter((step) => step.status === 'running');
      if (!interruptedSteps.length) continue;
      const updated = await this.store.update(businessKey, operation.id, (draft) => {
        const timestamp = nowIso();
        for (const step of draft.steps) {
          if (step.status !== 'running') continue;
          step.status = 'blocked';
          step.error = 'Execution was interrupted by a server stop. Resume or retry after checking provider state.';
          step.failedAt = '';
          draft.blockers.push({
            id: makeOperationId('blocker'), operationId: draft.id, stepId: step.id, type: 'recovery_required', status: 'active',
            message: `Step "${step.title}" was running when the server stopped. Its outcome was not assumed.`, createdAt: timestamp,
          });
        }
        draft.status = 'paused';
        draft.pausedAt = timestamp;
        draft.currentStepId = interruptedSteps[0].id;
        draft.activityLog.push({
          id: makeOperationId('evt'), operationId: draft.id, stepId: interruptedSteps[0].id, type: 'operation_recovered', actor: 'system',
          message: `${interruptedSteps.length} interrupted running step(s) moved to recovery-required state.`, data: {}, timestamp,
        });
        return draft;
      });
      recovered.push(updated.id);
    }
    return recovered;
  }
}
