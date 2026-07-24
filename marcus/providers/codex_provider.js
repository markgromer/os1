import { createArtifact, makeOperationId, nowIso, PROVIDER_RESULT_STATUSES, redactSecrets, safeEnum, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';

export const CODEX_JOB_STATUSES = Object.freeze([
  'completed', 'started', 'queued', 'running', 'waiting_external', 'waiting', 'failed', 'cancelled', 'paused', 'unknown',
]);

export function normalizeProviderStatus(value, fallback = 'unknown') {
  const aliases = { success: 'completed', succeeded: 'completed', complete: 'completed', pending: 'queued', in_progress: 'running', canceled: 'cancelled' };
  const raw = safeString(value, 80).toLowerCase();
  return safeEnum(aliases[raw] || raw, PROVIDER_RESULT_STATUSES, fallback);
}

function normalizeJob(rawJob, base = {}) {
  const raw = safeObject(rawJob);
  const status = normalizeProviderStatus(raw.status, normalizeProviderStatus(base.status, 'unknown'));
  return {
    provider: safeString(raw.provider || base.provider, 100) || 'direct',
    recordId: safeString(base.recordId, 120) || makeOperationId('codexjob'),
    jobId: safeString(raw.jobId || raw.id || base.jobId, 300),
    operationId: safeString(base.operationId || raw.operationId, 120),
    stepId: safeString(base.stepId || raw.stepId, 120),
    projectRegistryId: safeString(base.projectRegistryId || raw.projectRegistryId, 160),
    repository: safeString(base.repository || raw.repository, 1_000),
    branch: safeString(raw.branch || base.branch, 500),
    status,
    idempotencyKey: safeString(base.idempotencyKey || raw.idempotencyKey, 240),
    startedAt: safeString(raw.startedAt || base.startedAt, 64) || nowIso(),
    updatedAt: nowIso(),
    completedAt: status === 'completed' ? (safeString(raw.completedAt, 64) || nowIso()) : '',
    artifacts: Array.isArray(raw.artifacts) ? sanitizeStructured(raw.artifacts.slice(0, 50), 30_000) : [],
    diffSummary: safeString(raw.diffSummary, 20_000),
    error: safeString(raw.error || raw.message, 8_000),
    rawMetadata: sanitizeStructured(raw.rawMetadata || raw.metadata || {}, 15_000),
  };
}

function formatList(values, fallback = '- None supplied.') {
  const list = (Array.isArray(values) ? values : []).map((value) => safeString(value, 2_000)).filter(Boolean);
  return list.length ? list.map((value) => `- ${value}`).join('\n') : fallback;
}

export function generateCodexHandoff({ operation, step, registryRecord, relevantMemory = [], currentArchitecture = '' }) {
  const record = safeObject(registryRecord);
  const repo = safeObject(record.repo);
  const workspace = safeObject(record.localWorkspace);
  const deployments = safeObject(record.deployments);
  const permissions = safeObject(record.permissions);
  const branchPattern = safeString(repo.workingBranchPattern, 300) || 'codex/{operationId}';
  const branch = branchPattern.replaceAll('{operationId}', operation.id).replaceAll('{projectId}', operation.projectId || operation.projectRegistryId || 'project');
  const verification = Array.isArray(step?.verificationRequirements) && step.verificationRequirements.length
    ? step.verificationRequirements
    : (Array.isArray(operation.verification) ? operation.verification.map((item) => item.type) : []);
  return redactSecrets([
    '# M.A.R.C.U.S. Durable Operation Handoff',
    '',
    '## Operation',
    `- Operation ID: ${operation.id}`,
    `- Objective: ${operation.objective}`,
    `- Original request: ${operation.originalRequest}`,
    `- Business: ${operation.businessKey}`,
    `- Project: ${operation.projectName || record.canonicalName || 'Unresolved'}`,
    `- Risk level: ${operation.riskLevel}`,
    '',
    '## Repository and branch guidance',
    `- Repository: ${repo.fullName || repo.url || 'Not registered'}`,
    `- Default branch: ${repo.defaultBranch || 'main'}`,
    `- Suggested work branch: ${branch}`,
    `- Local workspace: ${workspace.path || 'Not registered'}`,
    '- Do not push, merge, deploy, change DNS, alter credentials, or contact clients without a recorded M.A.R.C.U.S. approval.',
    '',
    '## Relevant project registry data',
    `- Canonical name: ${record.canonicalName || operation.projectName || ''}`,
    `- Description: ${record.description || ''}`,
    `- Production URL: ${deployments.productionUrl || ''}`,
    `- Preview URL: ${deployments.previewUrl || ''}`,
    `- Stack: ${(record.stack || []).join(', ') || 'Not registered'}`,
    `- Commands: ${JSON.stringify(record.commands || {})}`,
    `- Permissions: ${JSON.stringify(permissions)}`,
    '',
    '## Relevant memory',
    formatList(relevantMemory),
    '',
    '## Current architecture',
    safeString(currentArchitecture, 12_000) || 'Inspect the repository before making architectural assumptions.',
    '',
    '## Requested changes',
    step?.description || operation.objective,
    '',
    '## Constraints',
    '- Preserve existing functionality and data.',
    '- Keep the implementation scoped to this operation.',
    '- Use existing project scripts and established patterns.',
    '- Do not expose or persist secrets in output or artifacts.',
    '- Do not invent tool results or claim an action ran when it did not.',
    '- Stop and report evidence when blocked.',
    '',
    '## Acceptance criteria',
    formatList(operation.acceptanceCriteria),
    '',
    '## Verification requirements',
    formatList(verification),
    '',
    '## Expected deliverables',
    '- The scoped implementation on a nonproduction work branch or a clear evidence-backed blocker.',
    '- A changed-files summary and diff summary.',
    '- Exact build, test, lint, and typecheck results that were actually run.',
    '- Branch, commit, diff, and artifact identifiers that M.A.R.C.U.S. can attach to this operation.',
    '- A clear statement of anything that still requires manual review or approval.',
    '',
    'Completion is not accepted solely because this handoff says the work is done. M.A.R.C.U.S. will independently verify required evidence.',
  ].join('\n'), 100_000);
}

export class CodexProvider {
  constructor({ mode = 'external_handoff', directAdapter = null } = {}) {
    this.mode = mode === 'direct' && directAdapter ? 'direct' : 'external_handoff';
    this.directAdapter = directAdapter;
    this.launchesByIdempotencyKey = new Map();
  }

  async startJob({ operation, step, registryRecord, idempotencyKey }) {
    const prompt = generateCodexHandoff({
      operation,
      step,
      registryRecord,
      relevantMemory: operation.metadata?.relevantMemory || [],
      currentArchitecture: operation.metadata?.currentArchitecture || '',
    });
    const branchPattern = registryRecord?.repo?.workingBranchPattern || 'codex/{operationId}';
    const branch = branchPattern.replaceAll('{operationId}', operation.id);
    const base = {
      provider: this.mode,
      recordId: makeOperationId('codexjob'),
      jobId: '',
      operationId: operation.id,
      stepId: step.id,
      projectRegistryId: operation.projectRegistryId,
      repository: registryRecord?.repo?.fullName || registryRecord?.repo?.url || '',
      branch,
      prompt,
      status: this.mode === 'direct' ? 'queued' : 'waiting_external',
      startedAt: nowIso(),
      updatedAt: nowIso(),
      completedAt: '',
      artifacts: [],
      diffSummary: '',
      rawMetadata: {},
      idempotencyKey: safeString(idempotencyKey || step.idempotencyKey, 240),
    };
    if (this.mode === 'direct') {
      let launch = this.launchesByIdempotencyKey.get(base.idempotencyKey);
      if (!launch) {
        launch = Promise.resolve(this.directAdapter.startJob({ ...base, prompt }, { idempotencyKey: base.idempotencyKey }));
        this.launchesByIdempotencyKey.set(base.idempotencyKey, launch);
        if (this.launchesByIdempotencyKey.size > 1_000) this.launchesByIdempotencyKey.delete(this.launchesByIdempotencyKey.keys().next().value);
      }
      const launched = await launch;
      const job = normalizeJob(launched, base);
      const status = normalizeProviderStatus(launched?.status, 'started');
      job.status = status;
      return { status, job, prompt };
    }
    return {
      status: 'waiting_external',
      job: base,
      prompt,
      artifact: createArtifact({ operationId: operation.id, stepId: step.id, type: 'codex_handoff',
        name: `Codex handoff - ${operation.title}`, mimeType: 'text/markdown', content: prompt, createdAt: nowIso(),
        metadata: { providerMode: 'external_handoff', repository: base.repository, branch },
      }),
      message: 'A complete Codex handoff was generated. No direct Codex launch API is configured, so the operation is waiting for an external Codex job or result.',
    };
  }

  async getJobStatus(job) {
    if (this.mode === 'direct') return normalizeJob(await this.directAdapter.getJobStatus(job), job);
    return normalizeJob(job, job);
  }

  async sendFollowup(job, message) {
    if (this.mode === 'direct') return this.directAdapter.sendFollowup(job, message);
    return { ...job, updatedAt: nowIso(), rawMetadata: { ...safeObject(job?.rawMetadata), followups: [...(job?.rawMetadata?.followups || []), safeString(message, 8_000)].slice(-20) } };
  }

  async getArtifacts(job) {
    const artifacts = this.mode === 'direct' ? await this.directAdapter.getArtifacts(job) : job?.artifacts;
    return Array.isArray(artifacts) ? sanitizeStructured(artifacts.slice(0, 50), 50_000) : [];
  }

  async getDiff(job) {
    const diff = this.mode === 'direct' ? await this.directAdapter.getDiff(job) : { summary: job?.diffSummary };
    return sanitizeStructured({ ...safeObject(diff), summary: safeString(diff?.summary || diff?.diffSummary, 40_000) }, 50_000);
  }

  async cancelJob(job) {
    if (this.mode === 'direct') return normalizeJob(await this.directAdapter.cancelJob(job), job);
    return normalizeJob({ ...job, status: 'cancelled' }, job);
  }

  async resumeJob(job) {
    if (this.mode === 'direct') return normalizeJob(await this.directAdapter.resumeJob(job), job);
    return { ...job, status: job?.jobId ? 'running' : 'waiting_external', updatedAt: nowIso() };
  }
}
