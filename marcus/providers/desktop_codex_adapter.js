import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { nowIso, redactSecrets, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled']);
const ACCEPTED_STATUSES = new Set(['queued', 'started', 'running', 'completed', 'failed', 'cancelled', 'paused', 'unknown']);
const MAX_EVENTS = 2_000;

function hash(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex');
}

function timingSafeEqual(left, right) {
  const a = Buffer.from(hash(left));
  const b = Buffer.from(hash(right));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function normalizeStatus(value, fallback = 'unknown') {
  const raw = safeString(value, 80).toLowerCase();
  const aliased = raw === 'success' || raw === 'succeeded' ? 'completed'
    : raw === 'in_progress' ? 'running'
      : raw === 'canceled' ? 'cancelled'
        : raw;
  return ACCEPTED_STATUSES.has(aliased) ? aliased : fallback;
}

function safeEvent(raw, sequence) {
  const event = safeObject(raw);
  const type = safeString(event.type || event.event, 160) || 'event';
  const timestamp = safeString(event.timestamp || event.occurredAt, 80) || nowIso();
  return sanitizeStructured({
    sequence: Number(event.sequence) || sequence,
    type,
    timestamp,
    data: event.data && typeof event.data === 'object' ? event.data : event,
  }, 24_000);
}

function publicJob(job, { includeEvents = false } = {}) {
  if (!job) return null;
  return {
    provider: 'desktop_codex',
    jobId: job.jobId,
    operationId: job.operationId,
    stepId: job.stepId,
    businessKey: job.businessKey,
    projectRegistryId: job.projectRegistryId,
    projectName: job.projectName,
    repository: job.repository,
    branch: job.branch,
    workspacePath: job.workspacePath,
    status: job.status,
    startedAt: job.startedAt,
    updatedAt: job.updatedAt,
    completedAt: job.completedAt,
    threadId: job.threadId,
    finalOutput: job.finalOutput,
    error: job.error,
    diffSummary: job.diffSummary,
    changedFiles: job.changedFiles,
    eventCount: job.events.length,
    followups: (job.followups || []).map(({ requestId, jobId, actionId, phase, createdAt }) => ({ requestId, jobId, actionId, phase, createdAt })),
    ...(includeEvents ? { events: job.events } : {}),
  };
}

function normalizeStoredJob(raw = {}) {
  const value = safeObject(raw);
  const events = (Array.isArray(value.events) ? value.events : []).slice(-MAX_EVENTS)
    .map((event, index) => safeEvent(event, index + 1));
  const status = normalizeStatus(value.status, 'unknown');
  return {
    jobId: safeString(value.jobId, 300),
    operationId: safeString(value.operationId, 120),
    stepId: safeString(value.stepId, 120),
    businessKey: safeString(value.businessKey, 100),
    projectRegistryId: safeString(value.projectRegistryId, 160),
    projectName: safeString(value.projectName, 300),
    repository: safeString(value.repository, 1_000),
    branch: safeString(value.branch, 500),
    workspacePath: safeString(value.workspacePath, 2_000),
    desktopAgentId: safeString(value.desktopAgentId, 200),
    idempotencyKey: safeString(value.idempotencyKey, 240),
    monitorTokenHash: safeString(value.monitorTokenHash, 100),
    status,
    startedAt: safeString(value.startedAt, 80) || nowIso(),
    updatedAt: safeString(value.updatedAt, 80) || nowIso(),
    completedAt: safeString(value.completedAt, 80),
    threadId: safeString(value.threadId, 300),
    finalOutput: redactSecrets(safeString(value.finalOutput, 40_000), 40_000),
    error: redactSecrets(safeString(value.error, 8_000), 8_000),
    diffSummary: redactSecrets(safeString(value.diffSummary, 40_000), 40_000),
    changedFiles: (Array.isArray(value.changedFiles) ? value.changedFiles : []).slice(0, 300)
      .map((item) => safeString(item, 1_000)).filter(Boolean),
    events,
    followups: Array.isArray(value.followups) ? value.followups.slice(-250) : [],
  };
}

export class DesktopCodexAdapter {
  constructor({ dataDir, queueAction, monitorBaseUrl = '' } = {}) {
    if (!dataDir) throw new Error('DesktopCodexAdapter requires dataDir.');
    if (typeof queueAction !== 'function') throw new Error('DesktopCodexAdapter requires queueAction.');
    this.providerName = 'desktop_codex';
    this.file = path.join(path.resolve(dataDir), 'desktop-codex-jobs.json');
    this.queueAction = queueAction;
    this.monitorBaseUrl = safeString(monitorBaseUrl, 2_000).replace(/\/+$/, '');
    this.jobs = new Map();
    this.loaded = false;
    this.writeQueue = Promise.resolve();
    this.followupQueue = Promise.resolve();
  }

  async ensureLoaded() {
    if (this.loaded) return;
    this.loaded = true;
    try {
      const document = JSON.parse(await fs.readFile(this.file, 'utf8'));
      for (const raw of Array.isArray(document?.jobs) ? document.jobs : []) {
        const job = normalizeStoredJob(raw);
        if (job.jobId) this.jobs.set(job.jobId, job);
      }
    } catch {
      // The store is created on the first job.
    }
  }

  async persist() {
    const write = this.writeQueue.catch(() => {}).then(async () => {
      await fs.mkdir(path.dirname(this.file), { recursive: true });
      const jobs = [...this.jobs.values()].slice(-250);
      const temporary = `${this.file}.tmp-${crypto.randomBytes(6).toString('hex')}`;
      await fs.writeFile(temporary, `${JSON.stringify({ revision: 1, updatedAt: nowIso(), jobs }, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
      await fs.rename(temporary, this.file).catch(async (error) => {
        await fs.unlink(temporary).catch(() => {});
        throw error;
      });
    });
    this.writeQueue = write;
    return write;
  }

  jobIdFor(job, idempotencyKey) {
    return `desktop_codex_${hash(`${job.operationId}:${job.stepId}:${idempotencyKey}`).slice(0, 24)}`;
  }

  monitorUrl(jobId, token) {
    if (!this.monitorBaseUrl) return '';
    return `${this.monitorBaseUrl}/codex-run.html?job=${encodeURIComponent(jobId)}&monitorToken=${encodeURIComponent(token)}`;
  }

  async startJob(job, { idempotencyKey = '' } = {}) {
    await this.ensureLoaded();
    const jobId = this.jobIdFor(job, idempotencyKey);
    const existing = this.jobs.get(jobId);
    if (existing) return this.providerJob(existing);
    const workspacePath = safeString(job.workspacePath, 2_000);
    const desktopAgentId = safeString(job.desktopAgentId, 200);
    if (!workspacePath || !desktopAgentId) {
      throw new Error('Local Codex requires an approved workspace path bound to a desktop agent.');
    }
    const monitorToken = crypto.randomBytes(24).toString('base64url');
    const record = normalizeStoredJob({
      ...job,
      jobId,
      idempotencyKey,
      workspacePath,
      desktopAgentId,
      monitorTokenHash: hash(monitorToken),
      status: 'queued',
      startedAt: nowIso(),
      updatedAt: nowIso(),
      events: [],
    });
    this.jobs.set(jobId, record);
    await this.persist();
    try {
      await this.queueAction({
        id: jobId,
        type: 'start-local-codex-job',
        idempotencyKey,
        requestedBy: `operation:${record.operationId}`,
        payload: {
          jobId,
          operationId: record.operationId,
          stepId: record.stepId,
          businessKey: record.businessKey,
          projectRegistryId: record.projectRegistryId,
          desktopAgentId,
          idempotencyKey,
          path: workspacePath,
          prompt: safeString(job.prompt, 100_000),
          branch: record.branch,
          monitorUrl: this.monitorUrl(jobId, monitorToken),
        },
      });
    } catch (error) {
      record.status = 'failed';
      record.error = redactSecrets(safeString(error?.message, 8_000), 8_000);
      record.completedAt = nowIso();
      record.updatedAt = record.completedAt;
      await this.persist();
      throw error;
    }
    return this.providerJob(record);
  }

  providerJob(job) {
    return {
      provider: 'desktop_codex',
      jobId: job.jobId,
      status: job.status,
      branch: job.branch,
      diffSummary: job.diffSummary,
      error: job.error,
      rawMetadata: {
        workspacePath: job.workspacePath,
        desktopAgentId: job.desktopAgentId,
        operationId: job.operationId,
        stepId: job.stepId,
        threadId: job.threadId,
        eventCount: job.events.length,
      },
    };
  }

  async getJobStatus(job) {
    await this.ensureLoaded();
    const record = this.jobs.get(safeString(job?.jobId, 300));
    if (!record) return { ...job, provider: 'desktop_codex', status: 'unknown', error: 'Local Codex job was not found.' };
    return this.providerJob(record);
  }

  async getArtifacts(job) {
    await this.ensureLoaded();
    const record = this.jobs.get(safeString(job?.jobId, 300));
    if (!record) return [];
    return [{
      type: 'local_codex_result',
      name: `Local Codex result - ${record.projectName || record.jobId}`,
      mimeType: 'application/json',
      content: JSON.stringify({
        jobId: record.jobId,
        threadId: record.threadId,
        status: record.status,
        finalOutput: record.finalOutput,
        changedFiles: record.changedFiles,
        eventCount: record.events.length,
      }),
      metadata: { source: 'desktop_agent', authoritative: true, workspacePath: record.workspacePath },
    }];
  }

  async getDiff(job) {
    await this.ensureLoaded();
    const record = this.jobs.get(safeString(job?.jobId, 300));
    return record ? {
      summary: record.diffSummary,
      files: record.changedFiles.map((file) => ({ path: file })),
      source: 'desktop_agent',
      authoritative: true,
    } : { summary: '' };
  }

  async queueFollowup(jobId, message, { businessKey, requestId } = {}) {
    const run = this.followupQueue.catch(() => {}).then(async () => {
      await this.ensureLoaded();
      const record = this.jobs.get(jobId);
      const reject = (message, statusCode = 409, definite = true) => Object.assign(new Error(message), { statusCode, definite });
      if (!record || record.businessKey !== businessKey) throw reject('Codex job not found in this business.', 404);
      const prior = record.followups.find((item) => item.requestId === requestId);
      const messageHash = hash(message);
      if (prior) {
        if (prior.messageHash !== messageHash) throw reject('This request id belongs to a different message.');
        if (prior.phase === 'failed') throw reject(prior.error || 'The desktop queue rejected this request.');
        if (prior.phase !== 'queued') throw reject('The earlier send has an uncertain queue result. Inspect the desktop job before resending.', 503, false);
        return { job: this.providerJob(record), receipt: { requestId, jobId, actionId: prior.actionId, phase: prior.phase, createdAt: prior.createdAt } };
      }
      if (!record.threadId) throw reject('This job has no resumable Codex thread.');
      if (!TERMINAL_STATUSES.has(record.status)) throw reject('Codex is already active or waiting. Wait for this job to finish before sending a follow-up.');
      if (record.followups.length >= 250) throw reject('This job reached its follow-up receipt limit.');
      const receipt = { requestId, jobId, actionId: `codex_followup_${hash(`${businessKey}:${jobId}:${requestId}`).slice(0, 32)}`, messageHash, phase: 'dispatching', createdAt: nowIso() };
      record.followups.push(receipt);
      try { await this.persist(); } catch (error) { record.followups.pop(); throw reject('Could not persist the send request.', 503); }
      try {
        await this.queueAction({
          id: receipt.actionId,
          idempotencyKey: receipt.actionId,
          type: 'followup-local-codex-job',
          requestedBy: 'owner:codex-compose',
          payload: {
            jobId: record.jobId, operationId: record.operationId, stepId: record.stepId,
            businessKey: record.businessKey, projectRegistryId: record.projectRegistryId,
            threadId: record.threadId, path: record.workspacePath, message,
            desktopAgentId: record.desktopAgentId,
          },
        });
      } catch (error) {
        // A persistence error is not proof that enqueue had no effect. Do not retry it as a new send.
        receipt.error = 'The desktop queue result could not be confirmed.';
        await this.persist(); throw reject(receipt.error, 503, false);
      }
      receipt.phase = 'queued';
      record.status = 'queued'; record.completedAt = ''; record.error = ''; record.updatedAt = nowIso();
      try { await this.persist(); } catch { receipt.phase = 'dispatching'; throw reject('The desktop action may be queued but its final receipt could not be saved.', 503, false); }
      return { job: this.providerJob(record), receipt: { requestId, jobId, actionId: receipt.actionId, phase: 'queued', createdAt: receipt.createdAt } };
    });
    this.followupQueue = run;
    return run;
  }

  async sendFollowup(job, message) {
    await this.ensureLoaded();
    const record = this.jobs.get(safeString(job?.jobId, 300));
    if (!record) throw new Error('Local Codex job was not found.');
    const followup = safeString(message, 8_000);
    if (!followup) throw new Error('Local Codex follow-up message is required.');
    const previous = {
      monitorTokenHash: record.monitorTokenHash,
      status: record.status,
      completedAt: record.completedAt,
      updatedAt: record.updatedAt,
      error: record.error,
    };
    const monitorToken = crypto.randomBytes(24).toString('base64url');
    record.monitorTokenHash = hash(monitorToken);
    record.status = 'queued';
    record.completedAt = '';
    record.error = '';
    record.updatedAt = nowIso();
    await this.persist();
    try {
      await this.queueAction({
        type: 'followup-local-codex-job',
        requestedBy: `operation:${record.operationId}`,
        payload: {
          jobId: record.jobId,
          operationId: record.operationId,
          stepId: record.stepId,
          businessKey: record.businessKey,
          projectRegistryId: record.projectRegistryId,
          threadId: record.threadId,
          path: record.workspacePath,
          message: followup,
          desktopAgentId: record.desktopAgentId,
          monitorUrl: this.monitorUrl(record.jobId, monitorToken),
        },
      });
    } catch (error) {
      Object.assign(record, previous);
      await this.persist();
      throw error;
    }
    return this.providerJob(record);
  }

  async cancelJob(job) {
    await this.ensureLoaded();
    const record = this.jobs.get(safeString(job?.jobId, 300));
    if (!record) return { ...job, provider: 'desktop_codex', status: 'unknown' };
    if (!TERMINAL_STATUSES.has(record.status)) {
      await this.queueAction({
        type: 'cancel-local-codex-job',
        requestedBy: `operation:${record.operationId}`,
        payload: { jobId: record.jobId, desktopAgentId: record.desktopAgentId },
      });
      record.status = 'cancelled';
      record.completedAt = nowIso();
      record.updatedAt = record.completedAt;
      await this.persist();
    }
    return this.providerJob(record);
  }

  async ingestUpdate(raw = {}) {
    await this.ensureLoaded();
    const jobId = safeString(raw.jobId, 300);
    const record = this.jobs.get(jobId);
    if (!record) throw Object.assign(new Error('Unknown local Codex job.'), { code: 'CODEX_JOB_NOT_FOUND' });
    const agentId = safeString(raw.desktopAgentId, 200);
    if (!agentId || agentId !== record.desktopAgentId) {
      throw Object.assign(new Error('Local Codex update came from the wrong desktop agent.'), { code: 'CODEX_AGENT_MISMATCH' });
    }
    const incoming = Array.isArray(raw.events) ? raw.events.slice(0, 250) : [];
    const start = record.events.reduce((highest, event) => Math.max(highest, Number(event.sequence) || 0), 0);
    record.events = [...record.events, ...incoming.map((event, index) => safeEvent(event, start + index + 1))].slice(-MAX_EVENTS);
    const incomingStatus = normalizeStatus(raw.status, record.status);
    if (!TERMINAL_STATUSES.has(record.status) || TERMINAL_STATUSES.has(incomingStatus)) record.status = incomingStatus;
    record.threadId = safeString(raw.threadId, 300) || record.threadId;
    record.finalOutput = redactSecrets(safeString(raw.finalOutput, 40_000), 40_000) || record.finalOutput;
    record.error = redactSecrets(safeString(raw.error, 8_000), 8_000) || (record.status === 'failed' ? record.error : '');
    record.diffSummary = redactSecrets(safeString(raw.diffSummary, 40_000), 40_000) || record.diffSummary;
    if (Array.isArray(raw.changedFiles)) {
      record.changedFiles = raw.changedFiles.slice(0, 300).map((item) => safeString(item, 1_000)).filter(Boolean);
    }
    record.updatedAt = nowIso();
    if (TERMINAL_STATUSES.has(record.status)) record.completedAt = safeString(raw.completedAt, 80) || nowIso();
    await this.persist();
    return publicJob(record, { includeEvents: false });
  }

  async getPublicJob(jobId, token, { after = 0 } = {}) {
    await this.ensureLoaded();
    const record = this.jobs.get(safeString(jobId, 300));
    if (!record || !token || !timingSafeEqual(record.monitorTokenHash, hash(token))) return null;
    const cursor = Math.max(0, Number(after) || 0);
    const job = publicJob(record, { includeEvents: true });
    job.events = job.events.filter((event) => Number(event.sequence) > cursor);
    return job;
  }

  async listJobs({ limit = 30, businessKey } = {}) {
    await this.ensureLoaded();
    return [...this.jobs.values()].filter((job) => businessKey === undefined || job.businessKey === businessKey).slice(-Math.max(1, Math.min(100, Number(limit) || 30))).reverse()
      .map((job) => publicJob(job, { includeEvents: false }));
  }
}
