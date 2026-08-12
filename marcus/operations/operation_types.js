import crypto from 'node:crypto';

export const OPERATION_STATUSES = Object.freeze([
  'draft',
  'planned',
  'waiting_for_approval',
  'queued',
  'running',
  'awaiting_provider',
  'paused',
  'blocked',
  'recovery_required',
  'verifying',
  'completed',
  'failed',
  'cancelled',
]);

export const STEP_STATUSES = Object.freeze([
  'pending',
  'ready',
  'waiting_for_approval',
  'running',
  'completed',
  'failed',
  'skipped',
  'blocked',
  'cancelled',
]);

export const APPROVAL_STATUSES = Object.freeze([
  'pending', 'approved', 'rejected', 'expired', 'cancelled',
]);

export const VERIFICATION_STATUSES = Object.freeze([
  'pending', 'running', 'passed', 'failed', 'skipped', 'needs_manual_review',
]);

export const RISK_LEVELS = Object.freeze(['low', 'medium', 'high', 'critical']);

export const STEP_TYPES = Object.freeze([
  'internal', 'desktop', 'github_read', 'github_write', 'cloudflare_write', 'codex', 'verification', 'approval',
]);

export const TERMINAL_OPERATION_STATUSES = Object.freeze(['completed', 'failed', 'cancelled']);

export const OPERATION_TRANSITIONS = Object.freeze({
  draft: ['planned', 'cancelled'],
  planned: ['queued', 'waiting_for_approval', 'paused', 'cancelled'],
  waiting_for_approval: ['queued', 'blocked', 'paused', 'cancelled'],
  queued: ['running', 'waiting_for_approval', 'paused', 'blocked', 'failed', 'cancelled'],
  running: ['queued', 'waiting_for_approval', 'paused', 'blocked', 'verifying', 'failed', 'cancelled'],
  awaiting_provider: ['queued', 'running', 'paused', 'blocked', 'recovery_required', 'verifying', 'failed', 'cancelled'],
  paused: ['queued', 'waiting_for_approval', 'blocked', 'cancelled'],
  blocked: ['queued', 'waiting_for_approval', 'paused', 'failed', 'cancelled'],
  recovery_required: ['queued', 'waiting_for_approval', 'paused', 'blocked', 'failed', 'cancelled'],
  verifying: ['queued', 'waiting_for_approval', 'blocked', 'completed', 'failed', 'cancelled'],
  completed: [],
  failed: ['queued', 'cancelled'],
  cancelled: [],
});

export const MAX_ACTIVITY_EVENTS = 500;
export const MAX_ARTIFACTS = 100;
export const MAX_APPROVALS = 100;
export const MAX_BLOCKERS = 100;
export const MAX_VERIFICATION_RESULTS = 100;
export const MAX_STEPS = 50;
export const MAX_STORED_OUTPUT_CHARS = 40_000;
export const MAX_ARTIFACT_CONTENT_CHARS = 100_000;
export const PROVIDER_RESULT_STATUSES = Object.freeze([
  'completed', 'started', 'queued', 'running', 'waiting_external', 'waiting', 'failed', 'cancelled', 'paused', 'unknown',
]);

const SECRET_PATTERNS = [
  /(["']?(?:api[_-]?key|token|secret|password|passwd|private[_-]?key|client[_-]?secret)["']?\s*[:=]\s*["'])[^"'\r\n]*(?=["'])/gi,
  /(authorization\s*[:=]\s*(?:bearer\s+)?)[^\s,;]+/gi,
  /((?:api[_-]?key|token|secret|password|passwd|private[_-]?key|client[_-]?secret)\s*[:=]\s*)[^\s,;]+/gi,
  /\b(?:sk|pat|ghp|gho|github_pat|xox[baprs]|eyJ)[A-Za-z0-9._-]{12,}\b/g,
];

export function makeOperationId(prefix = 'op') {
  return `${prefix}_${crypto.randomBytes(10).toString('base64url')}`;
}

export function nowIso() {
  return new Date().toISOString();
}

export function safeString(value, max = 2_000) {
  return typeof value === 'string' ? value.trim().slice(0, max) : '';
}

export function safeBusinessKey(value, fallback = 'personal') {
  const normalized = safeString(value, 80)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64);
  return normalized || fallback;
}

export function safeEnum(value, allowed, fallback) {
  const normalized = safeString(value, 80).toLowerCase();
  return allowed.includes(normalized) ? normalized : fallback;
}

export function safeIso(value) {
  const text = safeString(value, 64);
  if (!text) return '';
  const time = Date.parse(text);
  return Number.isFinite(time) ? new Date(time).toISOString() : '';
}

export function safeHttpUrl(value) {
  const text = safeString(value, 2_000);
  try {
    const parsed = new URL(text);
    if (!['http:', 'https:'].includes(parsed.protocol) || parsed.username || parsed.password) return '';
    return parsed.toString();
  } catch {
    return '';
  }
}

export function safeInteger(value, fallback = 0, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(number)));
}

export function safeObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

export function redactSecrets(value, maxChars = MAX_STORED_OUTPUT_CHARS) {
  let text;
  if (typeof value === 'string') text = value;
  else {
    try {
      text = JSON.stringify(value);
    } catch {
      text = String(value ?? '');
    }
  }
  for (const pattern of SECRET_PATTERNS) {
    text = text.replace(pattern, (_, prefix) => `${typeof prefix === 'string' ? prefix : ''}[REDACTED]`);
  }
  return text.slice(0, maxChars);
}

export function sanitizeStructured(value, maxChars = 20_000) {
  const visit = (entry, depth = 0) => {
    if (depth > 8) return '[TRUNCATED]';
    if (entry === null || entry === undefined) return entry ?? null;
    if (typeof entry === 'string') return redactSecrets(entry, Math.min(maxChars, 8_000));
    if (typeof entry === 'number' || typeof entry === 'boolean') return entry;
    if (Array.isArray(entry)) return entry.slice(0, 100).map((item) => visit(item, depth + 1));
    if (typeof entry !== 'object') return safeString(String(entry), 1_000);
    const output = {};
    for (const [key, item] of Object.entries(entry).slice(0, 100)) {
      const safeKey = safeString(key, 200);
      if (!safeKey) continue;
      if (/(?:token|secret|password|passwd|private.?key|api.?key|authorization|cookie)/i.test(safeKey)) {
        output[safeKey] = '[REDACTED]';
      } else {
        output[safeKey] = visit(item, depth + 1);
      }
    }
    return output;
  };
  const sanitized = visit(value);
  try {
    if (JSON.stringify(sanitized).length <= maxChars) return sanitized;
  } catch {
    return '[UNSERIALIZABLE]';
  }
  return { truncated: true, preview: redactSecrets(value, maxChars) };
}

function normalizeStringArray(value, maxItems = 50, maxChars = 1_000) {
  const seen = new Set();
  const output = [];
  for (const item of Array.isArray(value) ? value : []) {
    const text = redactSecrets(safeString(item, maxChars), maxChars).trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    output.push(text);
    if (output.length >= maxItems) break;
  }
  return output;
}

export function normalizeActivityEvent(input = {}, defaults = {}) {
  const raw = safeObject(input);
  const timestamp = safeIso(raw.timestamp || raw.createdAt) || nowIso();
  return {
    id: safeString(raw.id, 120) || makeOperationId('evt'),
    operationId: safeString(raw.operationId || defaults.operationId, 120),
    stepId: safeString(raw.stepId, 120),
    type: safeString(raw.type, 100) || 'operation_updated',
    actor: safeString(raw.actor, 120) || safeString(defaults.actor, 120) || 'system',
    message: redactSecrets(raw.message ?? '', 4_000).trim(),
    data: sanitizeStructured(raw.data ?? {}, 12_000),
    timestamp,
  };
}

export function normalizeArtifact(input = {}, defaults = {}) {
  const raw = safeObject(input);
  const createdAt = safeIso(raw.createdAt) || nowIso();
  return {
    id: safeString(raw.id, 120) || makeOperationId('artifact'),
    operationId: safeString(raw.operationId || defaults.operationId, 120),
    stepId: safeString(raw.stepId || defaults.stepId, 120),
    type: safeString(raw.type, 100) || 'artifact',
    name: redactSecrets(raw.name ?? '', 300).trim() || 'Operation artifact',
    mimeType: safeString(raw.mimeType, 120) || 'text/plain',
    content: redactSecrets(raw.content ?? '', MAX_ARTIFACT_CONTENT_CHARS),
    path: redactSecrets(raw.path ?? '', 1_000).trim(),
    url: safeHttpUrl(raw.url),
    metadata: sanitizeStructured(raw.metadata ?? {}, 12_000),
    createdAt,
  };
}

export function normalizeApproval(input = {}, defaults = {}) {
  const raw = safeObject(input);
  const status = safeEnum(raw.status, APPROVAL_STATUSES, 'pending');
  return {
    id: safeString(raw.id, 120) || makeOperationId('approval'),
    operationId: safeString(raw.operationId || defaults.operationId, 120),
    stepId: safeString(raw.stepId || defaults.stepId, 120),
    action: redactSecrets(raw.action || defaults.action || '', 300).trim() || 'execute_step',
    riskLevel: safeEnum(raw.riskLevel || defaults.riskLevel, RISK_LEVELS, 'medium'),
    reason: redactSecrets(raw.reason || defaults.reason || '', 2_000).trim(),
    requestedAt: safeIso(raw.requestedAt) || nowIso(),
    status,
    approvedAt: status === 'approved' ? (safeIso(raw.approvedAt) || nowIso()) : '',
    rejectedAt: status === 'rejected' ? (safeIso(raw.rejectedAt) || nowIso()) : '',
    approvedBy: safeString(raw.approvedBy, 200),
    approvalMessage: redactSecrets(raw.approvalMessage ?? '', 2_000).trim(),
    expiresAt: safeIso(raw.expiresAt),
    strongConfirmation: raw.strongConfirmation === true,
  };
}

export function normalizeVerificationResult(input = {}, defaults = {}) {
  const raw = safeObject(input);
  return {
    id: safeString(raw.id, 120) || makeOperationId('verify'),
    operationId: safeString(raw.operationId || defaults.operationId, 120),
    stepId: safeString(raw.stepId || defaults.stepId, 120),
    type: safeString(raw.type, 100) || 'manual_review',
    status: safeEnum(raw.status, VERIFICATION_STATUSES, 'pending'),
    required: raw.required !== false,
    command: redactSecrets(raw.command ?? '', 500).trim(),
    target: redactSecrets(raw.target ?? '', 2_000).trim(),
    startedAt: safeIso(raw.startedAt),
    completedAt: safeIso(raw.completedAt),
    output: redactSecrets(raw.output ?? '', MAX_STORED_OUTPUT_CHARS),
    error: redactSecrets(raw.error ?? '', 8_000),
    evidence: sanitizeStructured(raw.evidence ?? {}, 20_000),
    waived: raw.waived === true,
    waiverApprovalId: safeString(raw.waiverApprovalId, 120),
  };
}

export function normalizeBlocker(input = {}, defaults = {}) {
  const raw = safeObject(input);
  return {
    id: safeString(raw.id, 120) || makeOperationId('blocker'),
    operationId: safeString(raw.operationId || defaults.operationId, 120),
    stepId: safeString(raw.stepId || defaults.stepId, 120),
    type: safeString(raw.type, 100) || 'execution_blocked',
    message: redactSecrets(raw.message ?? '', 4_000).trim() || 'Operation is blocked.',
    status: safeEnum(raw.status, ['active', 'resolved'], 'active'),
    createdAt: safeIso(raw.createdAt) || nowIso(),
    resolvedAt: safeIso(raw.resolvedAt),
    resolution: redactSecrets(raw.resolution ?? '', 2_000).trim(),
  };
}

export function normalizeProviderAction(input = {}, defaults = {}) {
  const raw = safeObject(input);
  return {
    id: safeString(raw.id, 120) || makeOperationId('action'),
    operationId: safeString(raw.operationId || defaults.operationId, 120),
    stepId: safeString(raw.stepId || defaults.stepId, 120),
    provider: safeString(raw.provider || defaults.provider, 100),
    action: safeString(raw.action || defaults.action, 160),
    idempotencyKey: safeString(raw.idempotencyKey || defaults.idempotencyKey, 240),
    externalId: safeString(raw.externalId, 300),
    status: safeEnum(raw.status, PROVIDER_RESULT_STATUSES, 'unknown'),
    issuedAt: safeIso(raw.issuedAt) || nowIso(),
    updatedAt: safeIso(raw.updatedAt) || safeIso(raw.issuedAt) || nowIso(),
    completedAt: safeIso(raw.completedAt),
    cancellationConfirmed: raw.cancellationConfirmed === true,
    metadata: sanitizeStructured(raw.metadata ?? {}, 15_000),
  };
}

export function normalizeDesktopCorrelation(input = {}, defaults = {}) {
  const raw = safeObject(input);
  return {
    actionId: safeString(raw.actionId, 120),
    operationId: safeString(raw.operationId || defaults.operationId, 120),
    stepId: safeString(raw.stepId || defaults.stepId, 120),
    businessKey: safeBusinessKey(raw.businessKey || defaults.businessKey, ''),
    verificationId: safeString(raw.verificationId, 120),
    verificationType: safeString(raw.verificationType, 100),
    actionType: safeString(raw.actionType, 100),
    projectRegistryId: safeString(raw.projectRegistryId, 160),
    desktopAgentId: safeString(raw.desktopAgentId, 200),
    idempotencyKey: safeString(raw.idempotencyKey, 240),
    attemptNumber: safeInteger(raw.attemptNumber, 0, 0, 10),
    queuedAt: safeIso(raw.queuedAt) || nowIso(),
    updatedAt: safeIso(raw.updatedAt) || safeIso(raw.queuedAt) || nowIso(),
    completedAt: safeIso(raw.completedAt),
    status: safeEnum(raw.status, ['queued', 'running', 'completed', 'failed', 'recovery_required'], 'queued'),
    output: redactSecrets(raw.output ?? '', MAX_STORED_OUTPUT_CHARS),
    error: redactSecrets(raw.error ?? '', 8_000),
  };
}

export function normalizeStep(input = {}, index = 0, defaults = {}) {
  const raw = safeObject(input);
  const sequence = safeInteger(raw.sequence, index + 1, 1, MAX_STEPS);
  const status = safeEnum(raw.status, STEP_STATUSES, 'pending');
  const maxAttempts = safeInteger(raw.maxAttempts, 2, 1, 10);
  const attemptCount = safeInteger(raw.attemptCount, 0, 0, maxAttempts);
  const type = safeEnum(raw.type, STEP_TYPES, 'internal');
  const provider = safeString(raw.provider, 100) || type;
  return {
    id: safeString(raw.id, 120) || makeOperationId('step'),
    title: redactSecrets(raw.title ?? '', 300).trim() || `Step ${sequence}`,
    description: redactSecrets(raw.description ?? '', 4_000).trim(),
    type,
    status,
    sequence,
    dependsOn: normalizeStringArray(raw.dependsOn, MAX_STEPS, 120),
    riskLevel: safeEnum(raw.riskLevel || defaults.riskLevel, RISK_LEVELS, 'low'),
    approvalRequired: raw.approvalRequired === true,
    approvalId: safeString(raw.approvalId, 120),
    provider,
    toolName: safeString(raw.toolName, 160),
    input: sanitizeStructured(raw.input ?? {}, 20_000),
    output: redactSecrets(raw.output ?? '', MAX_STORED_OUTPUT_CHARS),
    error: redactSecrets(raw.error ?? '', 8_000),
    attemptCount,
    maxAttempts,
    startedAt: safeIso(raw.startedAt),
    completedAt: safeIso(raw.completedAt),
    failedAt: safeIso(raw.failedAt),
    verificationRequirements: normalizeStringArray(raw.verificationRequirements, 20, 300),
    idempotencyKey: safeString(raw.idempotencyKey, 200),
  };
}

function normalizeAcceptanceCriteria(value) {
  if (Array.isArray(value)) return normalizeStringArray(value, 30, 2_000);
  const text = redactSecrets(safeString(value, 8_000), 8_000);
  if (!text) return [];
  return text.split(/\r?\n/).map((line) => line.replace(/^[-*\d.)\s]+/, '').trim()).filter(Boolean).slice(0, 30);
}

function normalizeOperationMetadata(value) {
  const raw = safeObject(value);
  const reserved = new Set(['projectResolution', 'relevantMemory', 'currentArchitecture', 'codexJobs', 'authorizationProvenance', 'executionTarget', 'extra']);
  const extra = { ...safeObject(raw.extra) };
  for (const [key, item] of Object.entries(raw)) if (!reserved.has(key)) extra[key] = item;
  return {
    projectResolution: sanitizeStructured(raw.projectResolution ?? {}, 5_000),
    relevantMemory: normalizeStringArray(raw.relevantMemory, 20, 1_000),
    currentArchitecture: redactSecrets(raw.currentArchitecture ?? '', 30_000).trim(),
    codexJobs: sanitizeStructured(raw.codexJobs ?? {}, 15_000),
    authorizationProvenance: sanitizeStructured(raw.authorizationProvenance ?? {}, 12_000),
    executionTarget: sanitizeStructured(raw.executionTarget ?? {}, 25_000),
    extra: sanitizeStructured(extra, 12_000),
  };
}

export function normalizeOperation(input = {}, options = {}) {
  const raw = safeObject(input);
  const createdAt = safeIso(raw.createdAt) || nowIso();
  const status = safeEnum(raw.status, OPERATION_STATUSES, 'draft');
  const businessKey = safeBusinessKey(options.businessKey || raw.businessKey);
  const steps = (Array.isArray(raw.steps) ? raw.steps : [])
    .slice(0, MAX_STEPS)
    .map((step, index) => normalizeStep(step, index, { riskLevel: raw.riskLevel }))
    .sort((a, b) => a.sequence - b.sequence || a.id.localeCompare(b.id));
  const stepIds = new Set(steps.map((step) => step.id));
  for (const step of steps) step.dependsOn = step.dependsOn.filter((id) => id !== step.id && stepIds.has(id));

  const operationId = safeString(raw.id, 120) || makeOperationId();
  const normalized = {
    id: operationId,
    businessKey,
    projectId: safeString(raw.projectId, 160),
    projectName: safeString(raw.projectName, 300),
    projectRegistryId: safeString(raw.projectRegistryId, 160),
    title: redactSecrets(raw.title || raw.objective || '', 300).trim() || 'Untitled operation',
    objective: redactSecrets(raw.objective ?? '', 8_000).trim(),
    originalRequest: redactSecrets(raw.originalRequest ?? '', 12_000).trim(),
    requestedBy: safeString(raw.requestedBy, 200) || 'mark',
    source: safeString(raw.source, 100) || 'api',
    status,
    riskLevel: safeEnum(raw.riskLevel, RISK_LEVELS, 'low'),
    autonomyMode: safeEnum(raw.autonomyMode, ['manual', 'supervised', 'configured'], 'supervised'),
    acceptanceCriteria: normalizeAcceptanceCriteria(raw.acceptanceCriteria),
    plan: normalizeStringArray(raw.plan, MAX_STEPS, 2_000),
    currentStepId: stepIds.has(safeString(raw.currentStepId, 120)) ? safeString(raw.currentStepId, 120) : '',
    steps,
    artifacts: (Array.isArray(raw.artifacts) ? raw.artifacts : []).slice(-MAX_ARTIFACTS).map((item) => normalizeArtifact(item, { operationId })),
    approvals: (Array.isArray(raw.approvals) ? raw.approvals : []).slice(-MAX_APPROVALS).map((item) => normalizeApproval(item, { operationId })),
    verification: (Array.isArray(raw.verification) ? raw.verification : []).slice(-MAX_VERIFICATION_RESULTS).map((item) => normalizeVerificationResult(item, { operationId })),
    blockers: (Array.isArray(raw.blockers) ? raw.blockers : []).slice(-MAX_BLOCKERS).map((item) => normalizeBlocker(item, { operationId })),
    providerActions: (Array.isArray(raw.providerActions) ? raw.providerActions : []).slice(-200).map((item) => normalizeProviderAction(item, { operationId })),
    desktopCorrelations: (Array.isArray(raw.desktopCorrelations) ? raw.desktopCorrelations : []).slice(-200).map((item) => normalizeDesktopCorrelation(item, { operationId })),
    activityLog: (Array.isArray(raw.activityLog) ? raw.activityLog : []).slice(-MAX_ACTIVITY_EVENTS).map((item) => normalizeActivityEvent(item, { operationId })),
    createdAt,
    updatedAt: safeIso(raw.updatedAt) || createdAt,
    startedAt: safeIso(raw.startedAt),
    pausedAt: safeIso(raw.pausedAt),
    completedAt: safeIso(raw.completedAt),
    failedAt: safeIso(raw.failedAt),
    cancelledAt: safeIso(raw.cancelledAt),
    revision: safeInteger(raw.revision, 1, 1),
    metadata: normalizeOperationMetadata(raw.metadata),
  };

  if (status === 'completed' && !normalized.completedAt) normalized.completedAt = normalized.updatedAt;
  if (status === 'failed' && !normalized.failedAt) normalized.failedAt = normalized.updatedAt;
  if (status === 'cancelled' && !normalized.cancelledAt) normalized.cancelledAt = normalized.updatedAt;
  return normalized;
}

// Creation helpers deliberately discard identity and time fields. Persisted-record
// normalizers above are for rehydration and intentionally preserve them.
export function createActivityEvent(input = {}, defaults = {}) {
  return normalizeActivityEvent({ ...safeObject(input), id: makeOperationId('evt'), timestamp: nowIso(), createdAt: undefined }, defaults);
}

export function createArtifact(input = {}, defaults = {}) {
  return normalizeArtifact({ ...safeObject(input), id: makeOperationId('artifact'), createdAt: nowIso() }, defaults);
}

export function createApproval(input = {}, defaults = {}) {
  return normalizeApproval({
    ...safeObject(input), id: makeOperationId('approval'), requestedAt: nowIso(), approvedAt: undefined, rejectedAt: undefined,
  }, defaults);
}

export function createVerificationResult(input = {}, defaults = {}) {
  const raw = safeObject(input);
  const status = safeEnum(raw.status, VERIFICATION_STATUSES, 'pending');
  return normalizeVerificationResult({
    ...raw, id: makeOperationId('verify'), status,
    startedAt: status === 'running' ? nowIso() : '',
    completedAt: ['passed', 'failed', 'skipped', 'needs_manual_review'].includes(status) ? nowIso() : '',
  }, defaults);
}

export function createBlocker(input = {}, defaults = {}) {
  return normalizeBlocker({ ...safeObject(input), id: makeOperationId('blocker'), createdAt: nowIso(), resolvedAt: '' }, defaults);
}

export function createStep(input = {}, index = 0, defaults = {}) {
  return normalizeStep({
    ...safeObject(input), id: makeOperationId('step'), startedAt: '', completedAt: '', failedAt: '', attemptCount: 0, idempotencyKey: '',
  }, index, defaults);
}

export function createOperationRecord(input = {}, options = {}) {
  const raw = safeObject(input);
  const timestamp = nowIso();
  return normalizeOperation({
    ...raw,
    id: makeOperationId(),
    createdAt: timestamp,
    updatedAt: timestamp,
    startedAt: '',
    pausedAt: '',
    completedAt: '',
    failedAt: '',
    cancelledAt: '',
    revision: 1,
  }, options);
}

export function canTransitionOperation(from, to) {
  if (from === to) return true;
  return Array.isArray(OPERATION_TRANSITIONS[from]) && OPERATION_TRANSITIONS[from].includes(to);
}

export function assertOperationTransition(from, to) {
  if (!canTransitionOperation(from, to)) {
    const error = new Error(`Invalid operation transition: ${from} -> ${to}`);
    error.code = 'INVALID_TRANSITION';
    throw error;
  }
}

export function requiredVerificationPassed(operation) {
  const verification = Array.isArray(operation?.verification) ? operation.verification : [];
  const required = verification.filter((item) => item.required !== false);
  if (!required.length) return false;
  return required.every((item) => item.status === 'passed' || (item.waived === true && item.waiverApprovalId));
}

export function summarizeOperationProgress(operation) {
  const steps = Array.isArray(operation?.steps) ? operation.steps : [];
  const completed = steps.filter((step) => ['completed', 'skipped'].includes(step.status)).length;
  return {
    completed,
    total: steps.length,
    percent: steps.length ? Math.round((completed / steps.length) * 100) : 0,
  };
}
