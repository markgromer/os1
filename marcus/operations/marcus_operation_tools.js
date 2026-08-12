import { safeObject, safeString, summarizeOperationProgress } from './operation_types.js';
import { isStrongConfirmation } from '../approvals/approval_policy.js';

const TOOL_NAMES = new Set([
  'create_operation', 'get_operation', 'list_operations', 'plan_operation', 'start_operation', 'pause_operation',
  'resume_operation', 'cancel_operation', 'approve_operation_step', 'reject_operation_step',
  'register_external_codex_job', 'resolve_project',
]);

export function getMarcusOperationToolDefinitions() {
  const idSchema = { type: 'object', properties: { operationId: { type: 'string' } }, required: ['operationId'] };
  return [
    {
      type: 'function',
      function: {
        name: 'create_operation',
        description: 'Create a durable, restart-safe operation from the authenticated chat request. Model suggestions cannot grant runtime authority or select a trusted project. This does not claim execution has started.',
        parameters: {
          type: 'object',
          properties: {
            objective: { type: 'string' }, title: { type: 'string' },
            acceptanceCriteria: { type: 'array', items: { type: 'string' } }, autoPlan: { type: 'boolean' },
          },
        },
      },
    },
    { type: 'function', function: { name: 'get_operation', description: 'Get a durable operation and its real current state.', parameters: idSchema } },
    {
      type: 'function', function: {
        name: 'list_operations', description: 'List durable operations for the active business only.',
        parameters: { type: 'object', properties: { status: { type: 'string' }, projectId: { type: 'string' }, limit: { type: 'number' } } },
      },
    },
    { type: 'function', function: { name: 'plan_operation', description: 'Create or refresh the validated deterministic plan for a draft operation.', parameters: idSchema } },
    { type: 'function', function: { name: 'start_operation', description: 'Start an explicit runner cycle for a planned operation. Policy gates still apply.', parameters: idSchema } },
    { type: 'function', function: { name: 'pause_operation', description: 'Safely pause an active operation without assuming an in-flight provider completed.', parameters: idSchema } },
    { type: 'function', function: { name: 'resume_operation', description: 'Resume a paused operation when its blockers are resolved.', parameters: idSchema } },
    { type: 'function', function: { name: 'cancel_operation', description: 'Cancel a nonterminal operation.', parameters: idSchema } },
    {
      type: 'function', function: {
        name: 'approve_operation_step', description: 'Approve one pending operation approval. Critical actions require strong confirmation.',
        parameters: { type: 'object', properties: { operationId: { type: 'string' }, approvalId: { type: 'string' }, message: { type: 'string' } }, required: ['operationId', 'approvalId'] },
      },
    },
    {
      type: 'function', function: {
        name: 'reject_operation_step', description: 'Reject one pending operation approval and block its step.',
        parameters: { type: 'object', properties: { operationId: { type: 'string' }, approvalId: { type: 'string' }, message: { type: 'string' } }, required: ['operationId', 'approvalId'] },
      },
    },
    {
      type: 'function', function: {
        name: 'register_external_codex_job', description: 'Attach a real external Codex job, branch, commit, diff, or result to the same durable operation.',
        parameters: {
          type: 'object', properties: {
            operationId: { type: 'string' }, jobId: { type: 'string' }, status: { type: 'string', enum: ['running', 'completed', 'failed', 'cancelled'] },
            branch: { type: 'string' }, commit: { type: 'string' }, diffSummary: { type: 'string' }, result: { type: 'string' },
          }, required: ['operationId'],
        },
      },
    },
    {
      type: 'function', function: {
        name: 'resolve_project', description: 'Deterministically resolve a request to the active business project registry.',
        parameters: { type: 'object', properties: { request: { type: 'string' }, projectId: { type: 'string' }, registryId: { type: 'string' } }, required: ['request'] },
      },
    },
  ];
}

export function isMarcusOperationTool(name) {
  return TOOL_NAMES.has(name);
}

export async function executeMarcusOperationTool({ name, args, engine, businessKey, requestMessage = '' }) {
  const input = safeObject(args);
  if (name === 'create_operation') {
    const authenticatedRequest = safeString(requestMessage, 12_000);
    if (!authenticatedRequest) return { ok: false, error: 'The authenticated user request is required to create an operation from chat.' };
    return engine.createFromRequest(businessKey, {
      originalRequest: authenticatedRequest,
      objective: safeString(input.objective, 8_000),
      title: safeString(input.title, 300),
      acceptanceCriteria: Array.isArray(input.acceptanceCriteria) ? input.acceptanceCriteria : [],
      requestedBy: 'marcus-chat', source: 'marcus_chat', autoPlan: input.autoPlan !== false,
    });
  }
  if (name === 'get_operation') return { ok: true, operation: await engine.getOperation(businessKey, input.operationId) };
  if (name === 'list_operations') return { ok: true, operations: await engine.listOperations(businessKey, input) };
  if (name === 'plan_operation') return { ok: true, operation: await engine.planOperation(businessKey, input.operationId, input) };
  if (name === 'start_operation') return { ok: true, operation: await engine.startOperation(businessKey, input.operationId, { actor: 'marcus-chat', runCycle: true }) };
  if (name === 'pause_operation') return { ok: true, operation: await engine.pauseOperation(businessKey, input.operationId, { actor: 'marcus-chat', reason: input.reason }) };
  if (name === 'resume_operation') return { ok: true, operation: await engine.resumeOperation(businessKey, input.operationId, { actor: 'marcus-chat', runCycle: true }) };
  if (name === 'cancel_operation') return { ok: true, operation: await engine.cancelOperation(businessKey, input.operationId, { actor: 'marcus-chat', reason: input.reason }) };
  if (name === 'approve_operation_step') {
    const actualRequest = safeString(requestMessage, 4_000);
    if (!/\b(approve|approved|approval granted|go ahead|proceed|do it|get it done|run it|start it)\b/i.test(actualRequest)) {
      return { ok: false, approvalRequired: true, error: 'The current user message does not explicitly approve this operation action.' };
    }
    const operation = await engine.getOperation(businessKey, input.operationId);
    const approval = operation?.approvals?.find((item) => item.id === input.approvalId);
    const allOperations = await engine.listOperations(businessKey, { limit: 100 });
    const pendingCount = allOperations.reduce((count, item) => count + (item.approvals || []).filter((candidate) => candidate.status === 'pending').length, 0);
    const requestLower = actualRequest.toLowerCase();
    const identifiesTarget = [operation?.id, operation?.title, operation?.projectName, approval?.action]
      .map((value) => safeString(value, 500).toLowerCase()).filter((value) => value.length >= 3)
      .some((value) => requestLower.includes(value));
    if (pendingCount > 1 && !identifiesTarget) {
      return { ok: false, approvalRequired: true, error: 'Multiple approvals are pending; the current user message does not identify which operation or action to approve.' };
    }
    const strongConfirmation = approval?.riskLevel === 'critical' ? isStrongConfirmation(actualRequest) : false;
    if (approval?.riskLevel === 'critical' && !strongConfirmation) {
      return { ok: false, approvalRequired: true, error: 'The current user message does not contain strong confirmation for this critical action.' };
    }
    return { ok: true, operation: await engine.approveOperationStep(businessKey, input.operationId, input.approvalId, { approvedBy: 'mark', message: actualRequest, runCycle: true }) };
  }
  if (name === 'reject_operation_step') {
    const actualRequest = safeString(requestMessage, 4_000);
    if (!/\b(reject|deny|decline|do not approve|don't approve|cancel approval)\b/i.test(actualRequest)) {
      return { ok: false, error: 'The current user message does not explicitly reject this operation action.' };
    }
    return { ok: true, operation: await engine.rejectOperationStep(businessKey, input.operationId, input.approvalId, { rejectedBy: 'mark', message: actualRequest }) };
  }
  if (name === 'register_external_codex_job') {
    const actualRequest = safeString(requestMessage, 12_000);
    if (!/\b(codex|job|run|branch|commit|diff)\b/i.test(actualRequest) || !/\b(register|attach|record|is running|started|finished|completed|failed|cancelled)\b/i.test(actualRequest)) {
      return { ok: false, error: 'The current user message does not explicitly provide or ask to register an external Codex job/result.' };
    }
    const operation = await engine.getOperation(businessKey, input.operationId);
    const candidates = (await engine.listOperations(businessKey, { limit: 100 })).filter((item) =>
      (item.steps || []).some((step) => step.type === 'codex' && ['blocked', 'running', 'failed', 'waiting_for_approval'].includes(step.status)));
    const requestLower = actualRequest.toLowerCase();
    const identifiesTarget = [operation?.id, operation?.title, operation?.projectName]
      .map((value) => safeString(value, 500).toLowerCase()).filter((value) => value.length >= 3)
      .some((value) => requestLower.includes(value));
    if (candidates.length > 1 && !identifiesTarget) {
      return { ok: false, error: 'Multiple Codex handoffs are waiting; the current user message does not identify which operation or project owns this result.' };
    }
    const supplied = { ...input };
    for (const field of ['jobId', 'branch', 'commit']) {
      const value = safeString(supplied[field], 500);
      if (value && !actualRequest.toLowerCase().includes(value.toLowerCase())) supplied[field] = '';
    }
    if (supplied.status === 'completed' && !/\b(completed|finished|done)\b/i.test(actualRequest)) supplied.status = 'running';
    if (supplied.result) supplied.result = actualRequest;
    return { ok: true, operation: await engine.registerExternalCodexJob(businessKey, input.operationId, { ...supplied, registeredBy: 'mark' }) };
  }
  if (name === 'resolve_project') return { ok: true, resolution: await engine.resolveProject(businessKey, input.request, input) };
  return { ok: false, error: `Unknown operation tool: ${name}` };
}

export function shouldCreateDurableOperationForRequest(message) {
  const text = safeString(message, 12_000).toLowerCase();
  if (!text || text.length < 12) return false;
  const asksWork = /\b(fix|build|implement|refactor|audit|publish|deploy|migrate|own|work on|take care of|get .* working)\b/.test(text);
  const durableSignal = /\b(codex|repository|repo|branch|pull request|deploy|production|mobile|across systems|end to end|own the problem)\b/.test(text);
  const trivial = /^\s*(what|who|when|where|why|how|explain|summarize|list|show|tell me)\b/.test(text) && !/\b(fix|build|implement|publish|deploy)\b/.test(text);
  return asksWork && durableSignal && !trivial;
}

export function formatOperationStatusForMarcus(operation, resolution = null, { reused = false } = {}) {
  if (!operation) return 'The durable operation could not be loaded.';
  const progress = summarizeOperationProgress(operation);
  const current = operation.steps.find((step) => step.id === operation.currentStepId);
  const pendingApproval = operation.approvals.find((approval) => approval.status === 'pending');
  const handoff = operation.artifacts.find((artifact) => artifact.type === 'codex_handoff');
  return [
    reused
      ? `I found the existing durable operation ${operation.id}: ${operation.title}. No duplicate was created.`
      : `I created durable operation ${operation.id}: ${operation.title}.`,
    `Outcome: ${operation.objective}`,
    `Project: ${operation.projectName || 'unresolved'}${resolution?.confidence ? ` (${resolution.confidence} confidence)` : ''}.`,
    `Risk: ${operation.riskLevel}. Status: ${operation.status}. Progress: ${progress.completed}/${progress.total} steps.`,
    current ? `Current step: ${current.title} (${current.status}).` : '',
    pendingApproval ? `Approval required: ${pendingApproval.action} (${pendingApproval.riskLevel}).` : '',
    handoff && operation.status === 'blocked' ? 'A complete Codex handoff is saved, but no direct Codex API is configured. Nothing is being falsely reported as running; the operation is waiting for a real external Codex job or result.' : '',
    operation.blockers.filter((blocker) => blocker.status === 'active').map((blocker) => `Blocker: ${blocker.message}`).join('\n'),
  ].filter(Boolean).join('\n');
}
