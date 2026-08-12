import { safeObject, safeString } from '../operations/operation_types.js';

function trimSlash(value) {
  return safeString(value, 2_000).replace(/\/+$/, '');
}

function normalizePath(value, fallback) {
  const raw = safeString(value, 300).trim();
  const path = raw || fallback;
  return path.startsWith('/') ? path : `/${path}`;
}

function normalizeTimeout(value, fallback = 45_000) {
  const n = Number(value);
  return Number.isFinite(n) && n >= 1_000 && n <= 300_000 ? n : fallback;
}

async function postJson(url, { token = '', timeoutMs = 45_000, body = {} } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);
  try {
    const resp = await fetch(url, {
      method: 'POST',
      signal: controller.signal,
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(body),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data?.error || data?.message || `Codex adapter HTTP ${resp.status}`);
    return data;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeAdapterJob(data = {}, fallback = {}) {
  const raw = safeObject(data);
  const session = safeObject(raw.session);
  const job = safeObject(raw.job);
  return {
    provider: safeString(raw.provider || job.provider || session.provider, 100) || 'http_codex',
    jobId: safeString(raw.jobId || raw.id || job.jobId || job.id || session.id, 300),
    status: safeString(raw.status || job.status || session.status, 100) || fallback.status || 'started',
    branch: safeString(raw.branch || job.branch || session.branch || fallback.branch, 500),
    artifacts: Array.isArray(raw.artifacts) ? raw.artifacts : (Array.isArray(job.artifacts) ? job.artifacts : []),
    diffSummary: safeString(raw.diffSummary || raw.diff?.summary || job.diffSummary, 20_000),
    error: safeString(raw.error || job.error, 8_000),
    rawMetadata: {
      adapter: 'http_codex',
      response: raw,
    },
  };
}

export class HttpCodexAdapter {
  constructor({
    baseUrl,
    token = '',
    timeoutMs = 45_000,
    startPath = '/codex/start',
    statusPath = '/codex/status',
    followupPath = '/codex/followup',
    artifactsPath = '/codex/artifacts',
    diffPath = '/codex/diff',
    cancelPath = '/codex/cancel',
  } = {}) {
    const cleanBase = trimSlash(baseUrl);
    if (!cleanBase) throw new Error('HttpCodexAdapter requires baseUrl.');
    this.providerName = 'http_codex';
    this.baseUrl = cleanBase;
    this.token = safeString(token, 2_000);
    this.timeoutMs = normalizeTimeout(timeoutMs);
    this.paths = {
      start: normalizePath(startPath, '/codex/start'),
      status: normalizePath(statusPath, '/codex/status'),
      followup: normalizePath(followupPath, '/codex/followup'),
      artifacts: normalizePath(artifactsPath, '/codex/artifacts'),
      diff: normalizePath(diffPath, '/codex/diff'),
      cancel: normalizePath(cancelPath, '/codex/cancel'),
    };
  }

  urlFor(kind) {
    return `${this.baseUrl}${this.paths[kind]}`;
  }

  async startJob(job, { idempotencyKey = '' } = {}) {
    const data = await postJson(this.urlFor('start'), {
      token: this.token,
      timeoutMs: this.timeoutMs,
      body: {
        idempotencyKey,
        operationId: job.operationId,
        stepId: job.stepId,
        businessKey: job.businessKey,
        projectRegistryId: job.projectRegistryId,
        repository: job.repository,
        branch: job.branch,
        prompt: job.prompt,
      },
    });
    return normalizeAdapterJob(data, job);
  }

  async getJobStatus(job) {
    const data = await postJson(this.urlFor('status'), {
      token: this.token,
      timeoutMs: this.timeoutMs,
      body: { jobId: job.jobId, recordId: job.recordId, operationId: job.operationId, stepId: job.stepId },
    });
    return normalizeAdapterJob(data, job);
  }

  async sendFollowup(job, message) {
    const data = await postJson(this.urlFor('followup'), {
      token: this.token,
      timeoutMs: this.timeoutMs,
      body: { jobId: job.jobId, operationId: job.operationId, stepId: job.stepId, message: safeString(message, 8_000) },
    });
    return normalizeAdapterJob(data, job);
  }

  async getArtifacts(job) {
    const data = await postJson(this.urlFor('artifacts'), {
      token: this.token,
      timeoutMs: this.timeoutMs,
      body: { jobId: job.jobId, operationId: job.operationId, stepId: job.stepId },
    });
    return Array.isArray(data?.artifacts) ? data.artifacts : [];
  }

  async getDiff(job) {
    const data = await postJson(this.urlFor('diff'), {
      token: this.token,
      timeoutMs: this.timeoutMs,
      body: { jobId: job.jobId, operationId: job.operationId, stepId: job.stepId },
    });
    return data?.diff && typeof data.diff === 'object' ? data.diff : { summary: safeString(data?.summary || data?.diffSummary, 40_000) };
  }

  async cancelJob(job) {
    const data = await postJson(this.urlFor('cancel'), {
      token: this.token,
      timeoutMs: this.timeoutMs,
      body: { jobId: job.jobId, operationId: job.operationId, stepId: job.stepId },
    });
    return normalizeAdapterJob(data, { ...job, status: 'cancelled' });
  }
}

export function createHttpCodexAdapterFromEnv(env = process.env) {
  const baseUrl = safeString(env.MARCUS_CODEX_ADAPTER_URL || env.CODEX_ADAPTER_URL, 2_000).trim();
  if (!baseUrl) return null;
  return new HttpCodexAdapter({
    baseUrl,
    token: env.MARCUS_CODEX_ADAPTER_TOKEN || env.CODEX_ADAPTER_TOKEN || '',
    timeoutMs: env.MARCUS_CODEX_ADAPTER_TIMEOUT_MS || env.CODEX_ADAPTER_TIMEOUT_MS,
    startPath: env.MARCUS_CODEX_ADAPTER_START_PATH || '/codex/start',
    statusPath: env.MARCUS_CODEX_ADAPTER_STATUS_PATH || '/codex/status',
    followupPath: env.MARCUS_CODEX_ADAPTER_FOLLOWUP_PATH || '/codex/followup',
    artifactsPath: env.MARCUS_CODEX_ADAPTER_ARTIFACTS_PATH || '/codex/artifacts',
    diffPath: env.MARCUS_CODEX_ADAPTER_DIFF_PATH || '/codex/diff',
    cancelPath: env.MARCUS_CODEX_ADAPTER_CANCEL_PATH || '/codex/cancel',
  });
}
