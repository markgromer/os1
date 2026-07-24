import { makeOperationId, nowIso, redactSecrets, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';

export const CODEX_JOB_STATUSES = Object.freeze([
  'handoff_ready', 'queued', 'running', 'waiting_external', 'completed', 'failed', 'cancelled', 'paused',
]);

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
  }

  async startJob({ operation, step, registryRecord }) {
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
    };
    if (this.mode === 'direct') {
      const launched = await this.directAdapter.startJob(base);
      return { status: 'started', job: { ...base, ...sanitizeStructured(launched, 20_000) }, prompt };
    }
    return {
      status: 'waiting_external',
      job: base,
      prompt,
      artifact: {
        id: makeOperationId('artifact'), operationId: operation.id, stepId: step.id, type: 'codex_handoff',
        name: `Codex handoff - ${operation.title}`, mimeType: 'text/markdown', content: prompt, createdAt: nowIso(),
        metadata: { providerMode: 'external_handoff', repository: base.repository, branch },
      },
      message: 'A complete Codex handoff was generated. No direct Codex launch API is configured, so the operation is waiting for an external Codex job or result.',
    };
  }

  async getJobStatus(job) {
    if (this.mode === 'direct') return this.directAdapter.getJobStatus(job);
    return sanitizeStructured(job, 20_000);
  }

  async sendFollowup(job, message) {
    if (this.mode === 'direct') return this.directAdapter.sendFollowup(job, message);
    return { ...job, updatedAt: nowIso(), rawMetadata: { ...safeObject(job?.rawMetadata), followups: [...(job?.rawMetadata?.followups || []), safeString(message, 8_000)].slice(-20) } };
  }

  async getArtifacts(job) {
    if (this.mode === 'direct') return this.directAdapter.getArtifacts(job);
    return Array.isArray(job?.artifacts) ? job.artifacts : [];
  }

  async getDiff(job) {
    if (this.mode === 'direct') return this.directAdapter.getDiff(job);
    return { summary: safeString(job?.diffSummary, 20_000) };
  }

  async cancelJob(job) {
    if (this.mode === 'direct') return this.directAdapter.cancelJob(job);
    return { ...job, status: 'cancelled', updatedAt: nowIso() };
  }

  async resumeJob(job) {
    if (this.mode === 'direct') return this.directAdapter.resumeJob(job);
    return { ...job, status: job?.jobId ? 'running' : 'waiting_external', updatedAt: nowIso() };
  }
}
