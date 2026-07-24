import { safeString } from '../operations/operation_types.js';

const ALLOWED_DESKTOP_ACTIONS = new Set([
  'run-project-script', 'prepare-publish', 'open-vscode',
]);

export class DesktopProvider {
  constructor({ queueAction = null } = {}) {
    this.queueAction = typeof queueAction === 'function' ? queueAction : null;
  }

  async execute({ operation, step, registryRecord, idempotencyKey }) {
    if (!this.queueAction) return { status: 'failed', error: 'Desktop agent execution is not available.' };
    const toolName = safeString(step.toolName, 100);
    if (!ALLOWED_DESKTOP_ACTIONS.has(toolName)) return { status: 'failed', error: `Desktop action is not allowlisted: ${toolName || '(missing)'}` };
    const workspacePath = safeString(registryRecord?.localWorkspace?.path, 2_000);
    if (!workspacePath) return { status: 'failed', error: 'The project registry has no local workspace path.' };
    if (registryRecord?.localWorkspace?.trustStatus !== 'approved' || !registryRecord?.localWorkspace?.desktopAgentId) {
      return { status: 'failed', error: 'The project workspace has not been explicitly approved and bound to a desktop agent.' };
    }
    const input = step.input && typeof step.input === 'object' ? step.input : {};
    const correlation = (operation.desktopCorrelations || []).find((item) => item.stepId === step.id && item.idempotencyKey === idempotencyKey);
    if (!correlation?.actionId || correlation.attemptNumber !== step.attemptCount) {
      return { status: 'failed', error: 'The desktop action has no durable correlation for this exact attempt.' };
    }
    const payload = {
      path: workspacePath,
      businessKey: operation.businessKey,
      operationId: operation.id,
      stepId: step.id,
      projectRegistryId: registryRecord.id,
      desktopAgentId: registryRecord.localWorkspace.desktopAgentId,
      idempotencyKey,
      attemptNumber: step.attemptCount,
    };
    if (toolName === 'run-project-script') {
      const scriptName = safeString(input.scriptName, 100);
      if (!['build', 'test', 'lint', 'typecheck', 'dev', 'install'].includes(scriptName)) return { status: 'failed', error: 'Project script is not allowlisted.' };
      payload.scriptName = scriptName;
    }
    const action = await this.queueAction({ id: correlation.actionId, type: toolName, payload, idempotencyKey, requestedBy: `operation:${operation.id}` });
    if (action?.id !== correlation.actionId) return { status: 'failed', error: 'Desktop agent returned an action id that did not match the durable correlation.' };
    return {
      status: 'waiting', output: 'Desktop action queued; completion has not been assumed.',
      evidence: { actionId: action.id, provider: 'desktop', idempotencyKey, attemptNumber: step.attemptCount },
    };
  }
}

export { ALLOWED_DESKTOP_ACTIONS };
