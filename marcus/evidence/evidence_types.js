import crypto from 'node:crypto';

import {
  nowIso,
  redactSecrets,
  safeBusinessKey,
  safeHttpUrl,
  safeIso,
  safeObject,
  safeString,
  sanitizeStructured,
} from '../operations/operation_types.js';

export const EVIDENCE_SOURCES = Object.freeze([
  'github', 'codex', 'desktop', 'render', 'cloudflare', 'browser', 'operations', 'airtable', 'manual',
]);

export const EVIDENCE_TYPES = Object.freeze([
  'commit', 'branch_created', 'branch_updated', 'pull_request_opened', 'pull_request_updated',
  'pull_request_merged', 'issue_updated', 'repository_read', 'codex_handoff_created', 'codex_job_started',
  'codex_job_updated', 'codex_job_completed', 'workspace_opened', 'workspace_active', 'build_run',
  'test_run', 'lint_run', 'typecheck_run', 'deployment_started', 'deployment_completed',
  'deployment_failed', 'preview_created', 'production_published', 'browser_verified', 'browser_failed',
  'operation_created', 'operation_started', 'operation_completed', 'operation_failed', 'task_updated', 'manual_note',
]);

export const TRUSTED_EVIDENCE_SOURCES = Object.freeze([
  'github', 'codex', 'desktop', 'render', 'cloudflare', 'browser', 'operations', 'airtable',
]);

const SOURCE_SET = new Set(EVIDENCE_SOURCES);
const TYPE_SET = new Set(EVIDENCE_TYPES);
const TRUSTED_SET = new Set(TRUSTED_EVIDENCE_SOURCES);

function safeConfidence(value, fallback = 0.5) {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.round(Math.max(0, Math.min(1, number)) * 1000) / 1000;
}

function evidenceString(value, max = 2_000) {
  const text = typeof value === 'string' ? value : String(value ?? '');
  return safeString(redactSecrets(text, max), max);
}

function normalizePullRequest(value) {
  const raw = safeObject(value);
  const number = Number(raw.number);
  return {
    number: Number.isFinite(number) && number > 0 ? Math.floor(number) : null,
    title: evidenceString(raw.title, 500),
    state: safeString(raw.state, 40).toLowerCase(),
    draft: raw.draft === true,
    url: safeHttpUrl(raw.url || raw.htmlUrl),
    base: evidenceString(raw.base, 300),
    head: evidenceString(raw.head, 300),
    updatedAt: safeIso(raw.updatedAt),
    mergedAt: safeIso(raw.mergedAt),
  };
}

function normalizeDeployment(value) {
  const raw = safeObject(value);
  return {
    id: safeString(raw.id, 300),
    provider: safeString(raw.provider, 80).toLowerCase(),
    environment: safeString(raw.environment, 80).toLowerCase(),
    status: safeString(raw.status, 80).toLowerCase(),
    url: safeHttpUrl(raw.url),
    commitSha: safeString(raw.commitSha || raw.commit, 200),
    branch: evidenceString(raw.branch, 300),
  };
}

function normalizeWorkspace(value) {
  const raw = safeObject(value);
  return {
    path: evidenceString(raw.path || raw.workspacePath, 2_000),
    sessionStart: safeIso(raw.sessionStart),
    sessionEnd: safeIso(raw.sessionEnd),
    activeMinutes: Math.round(Math.max(0, Math.min(1_440, Number(raw.activeMinutes) || 0)) * 10) / 10,
    filesObserved: (Array.isArray(raw.filesObserved) ? raw.filesObserved : []).slice(0, 100).map((item) => evidenceString(item, 300)).filter(Boolean),
    commandsRun: (Array.isArray(raw.commandsRun) ? raw.commandsRun : []).slice(0, 50).map((item) => evidenceString(item, 300)).filter(Boolean),
  };
}

function createId() {
  return `evidence_${crypto.randomBytes(10).toString('base64url')}`;
}

function createDedupeKey(record) {
  const externalId = safeString(record.externalId, 500);
  if (externalId) return `${record.source}:${externalId}`.toLowerCase();
  const material = [
    record.businessKey, record.projectRegistryId, record.source, record.type, record.event,
    record.timestamp, record.repository, record.branch, record.commitSha, record.codexJobId, record.operationId,
    record.summary,
  ].join('\n');
  return `${record.source}:sha256:${crypto.createHash('sha256').update(material).digest('hex')}`;
}

export function normalizeEvidence(input = {}, options = {}) {
  const raw = safeObject(input);
  const assignedSource = safeString(options.assignedSource, 80).toLowerCase();
  const requestedSource = safeString(raw.source, 80).toLowerCase();
  const source = SOURCE_SET.has(assignedSource) ? assignedSource : requestedSource;
  if (!SOURCE_SET.has(source)) throw Object.assign(new Error('A supported evidence source is required.'), { code: 'INVALID_EVIDENCE_SOURCE' });
  const type = safeString(raw.type, 100).toLowerCase();
  if (!TYPE_SET.has(type)) throw Object.assign(new Error('A supported evidence type is required.'), { code: 'INVALID_EVIDENCE_TYPE' });
  const businessKey = safeBusinessKey(options.businessKey || raw.businessKey);
  const projectRegistryId = safeString(raw.projectRegistryId, 160);
  if (!projectRegistryId) throw Object.assign(new Error('projectRegistryId is required for project evidence.'), { code: 'EVIDENCE_PROJECT_REQUIRED' });
  const actor = safeString(raw.actor || options.actor, 200);
  if (!actor) throw Object.assign(new Error('actor is required for project evidence.'), { code: 'EVIDENCE_ACTOR_REQUIRED' });
  const provenanceInput = safeObject(raw.provenance);
  const provenanceMethod = safeString(provenanceInput.method || options.provenanceMethod, 120);
  if (!provenanceMethod) throw Object.assign(new Error('provenance.method is required for project evidence.'), { code: 'EVIDENCE_PROVENANCE_REQUIRED' });
  const observedAt = safeIso(raw.observedAt) || nowIso();
  const requestedTimestamp = safeIso(raw.timestamp);
  const timestamp = requestedTimestamp && Date.parse(requestedTimestamp) <= Date.parse(observedAt) + 5 * 60_000 ? requestedTimestamp : observedAt;
  const trusted = options.trusted === true && TRUSTED_SET.has(source);
  const record = {
    id: safeString(raw.id, 160) || createId(),
    businessKey,
    projectRegistryId,
    projectId: safeString(raw.projectId, 160),
    source,
    type,
    event: evidenceString(raw.event, 160) || type,
    summary: evidenceString(raw.summary, 2_000),
    timestamp,
    observedAt,
    actor: evidenceString(actor, 200),
    repository: evidenceString(raw.repository, 1_000),
    branch: evidenceString(raw.branch, 500),
    commitSha: safeString(raw.commitSha || raw.commit, 200),
    pullRequest: normalizePullRequest(raw.pullRequest),
    deployment: normalizeDeployment(raw.deployment),
    workspace: normalizeWorkspace(raw.workspace),
    codexJobId: evidenceString(raw.codexJobId, 300),
    operationId: evidenceString(raw.operationId, 160),
    externalId: evidenceString(raw.externalId || provenanceInput.externalId, 500),
    metadata: sanitizeStructured(raw.metadata ?? {}, 20_000),
    confidence: safeConfidence(raw.confidence, trusted ? 0.95 : 0.5),
    provenance: sanitizeStructured({
      ...provenanceInput,
      method: provenanceMethod,
      actor,
      source,
      trusted,
      observedAt,
      externalId: safeString(raw.externalId || provenanceInput.externalId, 500),
    }, 10_000),
  };
  record.dedupeKey = createDedupeKey(record);
  return record;
}

export function normalizeManualEvidence(input = {}, options = {}) {
  const raw = safeObject(input);
  if (safeString(raw.source, 80) && safeString(raw.source, 80).toLowerCase() !== 'manual') {
    throw Object.assign(new Error('Manual ingestion cannot assign a trusted evidence source.'), { code: 'EVIDENCE_SOURCE_IMPERSONATION' });
  }
  if (safeString(raw.type, 100).toLowerCase() !== 'manual_note') {
    throw Object.assign(new Error('Manual ingestion may only create manual_note evidence.'), { code: 'EVIDENCE_TYPE_IMPERSONATION' });
  }
  return normalizeEvidence({ ...raw, confidence: Math.min(0.6, Number(raw.confidence) || 0.5) }, {
    ...options,
    assignedSource: 'manual',
    trusted: false,
    provenanceMethod: safeString(raw.provenance?.method, 120) || 'authenticated_manual_ingest',
  });
}

export function isTrustedEvidenceSource(source) {
  return TRUSTED_SET.has(safeString(source, 80).toLowerCase());
}
