import crypto from 'node:crypto';

import { safeObject, safeString } from '../operations/operation_types.js';

function parseRepoFullName(value, fallback = '') {
  const raw = safeString(value || fallback, 300).trim();
  return /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(raw) ? raw : '';
}

function normalizeTimeout(value, fallback = 30_000) {
  const n = Number(value);
  return Number.isFinite(n) && n >= 1_000 && n <= 300_000 ? n : fallback;
}

function githubHeaders(token) {
  return {
    Accept: 'application/vnd.github+json',
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
    'User-Agent': 'marcus-codex-adapter',
    'X-GitHub-Api-Version': '2022-11-28',
  };
}

async function fetchGitHubJson(url, { token, method = 'GET', body, timeoutMs = 30_000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);
  try {
    const resp = await fetch(url, {
      method,
      signal: controller.signal,
      headers: githubHeaders(token),
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (resp.status === 204) return { ok: true };
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data?.message || `GitHub API failed (${resp.status}).`);
    return data;
  } finally {
    clearTimeout(timer);
  }
}

function makeDispatchJobId({ operationId, stepId, idempotencyKey }) {
  const seed = [operationId, stepId, idempotencyKey].map((item) => safeString(item, 300)).join(':');
  return `ghdispatch_${crypto.createHash('sha256').update(seed || crypto.randomUUID()).digest('hex').slice(0, 24)}`;
}

function normalizeWorkflowRun(run = {}, fallback = {}) {
  const raw = safeObject(run);
  const conclusion = safeString(raw.conclusion, 80).toLowerCase();
  const status = safeString(raw.status, 80).toLowerCase();
  const mappedStatus = conclusion === 'success' ? 'completed'
    : ['failure', 'timed_out', 'startup_failure'].includes(conclusion) ? 'failed'
      : conclusion === 'cancelled' ? 'cancelled'
        : status === 'completed' ? 'unknown'
          : status === 'in_progress' ? 'running'
            : status === 'queued' || status === 'requested' || status === 'waiting' ? 'queued'
              : fallback.status || 'queued';
  return {
    provider: 'github_actions_codex',
    jobId: safeString(fallback.jobId, 300),
    status: mappedStatus,
    branch: safeString(fallback.branch, 500),
    rawMetadata: {
      ...(safeObject(fallback.rawMetadata)),
      workflowRunId: raw.id || '',
      workflowRunUrl: raw.html_url || '',
      workflowStatus: raw.status || '',
      workflowConclusion: raw.conclusion || '',
      workflowName: raw.name || '',
      displayTitle: raw.display_title || '',
      runNumber: raw.run_number || '',
    },
    error: mappedStatus === 'failed' ? safeString(raw.conclusion || 'GitHub Actions workflow failed.', 8_000) : '',
  };
}

export class GitHubActionsCodexAdapter {
  constructor({
    token,
    runnerRepo = 'markgromer/os1',
    eventType = 'marcus_codex_job',
    workflowFile = 'marcus-codex-runner.yml',
    timeoutMs = 30_000,
  } = {}) {
    const cleanToken = safeString(token, 2_000).trim();
    if (!cleanToken) throw new Error('GitHubActionsCodexAdapter requires token.');
    const repo = parseRepoFullName(runnerRepo);
    if (!repo) throw new Error('GitHubActionsCodexAdapter requires runnerRepo in owner/name form.');
    this.providerName = 'github_actions_codex';
    this.token = cleanToken;
    this.runnerRepo = repo;
    this.eventType = safeString(eventType, 100).trim() || 'marcus_codex_job';
    this.workflowFile = safeString(workflowFile, 200).trim() || 'marcus-codex-runner.yml';
    this.timeoutMs = normalizeTimeout(timeoutMs);
  }

  async startJob(job, { idempotencyKey = '' } = {}) {
    const jobId = makeDispatchJobId({ operationId: job.operationId, stepId: job.stepId, idempotencyKey });
    await fetchGitHubJson(`https://api.github.com/repos/${this.runnerRepo}/dispatches`, {
      token: this.token,
      method: 'POST',
      timeoutMs: this.timeoutMs,
      body: {
        event_type: this.eventType,
        client_payload: {
          jobId,
          operationId: job.operationId,
          stepId: job.stepId,
          businessKey: job.businessKey,
          projectRegistryId: job.projectRegistryId,
          repository: job.repository,
          branch: job.branch,
          prompt: job.prompt,
          idempotencyKey,
          requestedAt: new Date().toISOString(),
        },
      },
    });
    return {
      provider: 'github_actions_codex',
      jobId,
      status: 'queued',
      branch: job.branch,
      rawMetadata: {
        runnerRepo: this.runnerRepo,
        eventType: this.eventType,
        workflowFile: this.workflowFile,
        repository: job.repository,
      },
    };
  }

  async getJobStatus(job) {
    const metadata = safeObject(job?.rawMetadata);
    const existingRunId = safeString(metadata.workflowRunId, 80);
    if (existingRunId) {
      const run = await fetchGitHubJson(`https://api.github.com/repos/${this.runnerRepo}/actions/runs/${encodeURIComponent(existingRunId)}`, {
        token: this.token,
        timeoutMs: this.timeoutMs,
      });
      return normalizeWorkflowRun(run, job);
    }
    const runs = await fetchGitHubJson(`https://api.github.com/repos/${this.runnerRepo}/actions/workflows/${encodeURIComponent(this.workflowFile)}/runs?event=repository_dispatch&per_page=30`, {
      token: this.token,
      timeoutMs: this.timeoutMs,
    });
    const jobId = safeString(job?.jobId, 300);
    const operationId = safeString(job?.operationId, 120);
    const match = (Array.isArray(runs?.workflow_runs) ? runs.workflow_runs : []).find((run) => {
      const haystack = `${run?.display_title || ''} ${run?.name || ''} ${run?.head_branch || ''}`.toLowerCase();
      return (jobId && haystack.includes(jobId.toLowerCase())) || (operationId && haystack.includes(operationId.toLowerCase()));
    });
    return match ? normalizeWorkflowRun(match, job) : { ...job, provider: 'github_actions_codex', status: 'queued' };
  }

  async sendFollowup(job, message) {
    return {
      ...job,
      provider: 'github_actions_codex',
      status: job?.status || 'queued',
      rawMetadata: {
        ...safeObject(job?.rawMetadata),
        followups: [...(Array.isArray(job?.rawMetadata?.followups) ? job.rawMetadata.followups : []), safeString(message, 8_000)].slice(-20),
      },
    };
  }

  async getArtifacts() {
    return [];
  }

  async getDiff(job) {
    return { summary: safeString(job?.diffSummary, 40_000) };
  }

  async cancelJob(job) {
    return { ...job, provider: 'github_actions_codex', status: 'cancelled' };
  }
}

export function createGitHubActionsCodexAdapterFromEnv(env = process.env) {
  const enabled = safeString(env.MARCUS_CODEX_GITHUB_ACTIONS_ENABLED || env.CODEX_GITHUB_ACTIONS_ENABLED, 20).trim().toLowerCase();
  if (!['1', 'true', 'yes', 'on'].includes(enabled)) return null;
  const token = safeString(env.MARCUS_CODEX_GITHUB_TOKEN || env.CODEX_GITHUB_TOKEN || env.GITHUB_TOKEN, 2_000).trim();
  if (!token) return null;
  return new GitHubActionsCodexAdapter({
    token,
    runnerRepo: env.MARCUS_CODEX_RUNNER_REPO || env.CODEX_RUNNER_REPO || 'markgromer/os1',
    eventType: env.MARCUS_CODEX_RUNNER_EVENT_TYPE || env.CODEX_RUNNER_EVENT_TYPE || 'marcus_codex_job',
    workflowFile: env.MARCUS_CODEX_RUNNER_WORKFLOW || env.CODEX_RUNNER_WORKFLOW || 'marcus-codex-runner.yml',
    timeoutMs: env.MARCUS_CODEX_GITHUB_TIMEOUT_MS || env.CODEX_GITHUB_TIMEOUT_MS,
  });
}
