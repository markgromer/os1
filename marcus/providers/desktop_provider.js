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
    const payload = { path: workspacePath, projectRegistryId: registryRecord.id, desktopAgentId: registryRecord.localWorkspace.desktopAgentId };
    if (toolName === 'run-project-script') {
      const scriptName = safeString(input.scriptName, 100);
      if (!['build', 'test', 'lint', 'typecheck', 'dev', 'install'].includes(scriptName)) return { status: 'failed', error: 'Project script is not allowlisted.' };
      payload.scriptName = scriptName;
    }
    const action = await this.queueAction({ type: toolName, payload, idempotencyKey, requestedBy: `operation:${operation.id}` });
    if (!action?.id) return { status: 'failed', error: 'Desktop agent did not return an action id.' };
    return { status: 'waiting', output: 'Desktop action queued; completion has not been assumed.', evidence: { actionId: action.id, provider: 'desktop' } };
  }
}

export { ALLOWED_DESKTOP_ACTIONS };
