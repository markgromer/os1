import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

const STORE_VERSION = 1;
const DEFAULT_LEASE_MS = 15 * 60_000;
const DEFAULT_MAX_ACTIONS = 2_000;

function safeString(value, maxLength = 200) {
  return typeof value === 'string' ? value.trim().slice(0, maxLength) : '';
}

function safeInteger(value, fallback = 0, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (value === null || value === undefined || (typeof value === 'string' && !value.trim())) return fallback;
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(number)));
}

function clonePayload(value) {
  const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  let encoded = '';
  try { encoded = JSON.stringify(source); } catch {
    const error = new Error('Desktop action payload must be JSON serializable.');
    error.code = 'DESKTOP_ACTION_PAYLOAD_INVALID';
    throw error;
  }
  if (encoded.length > 32_000) {
    const error = new Error('Desktop action payload exceeds the 32 KB persistence limit.');
    error.code = 'DESKTOP_ACTION_PAYLOAD_TOO_LARGE';
    throw error;
  }
  return JSON.parse(encoded);
}

function emptyDocument() {
  return {
    version: STORE_VERSION,
    revision: 1,
    updatedAt: new Date(0).toISOString(),
    actions: [],
  };
}

function normalizeAction(input = {}) {
  const id = safeString(input.id, 120);
  const type = safeString(input.type, 80);
  if (!id || !type) {
    const error = new Error('Desktop actions require an id and type.');
    error.code = 'DESKTOP_ACTION_INVALID';
    throw error;
  }
  const status = input.status === 'delivered' ? 'delivered' : 'queued';
  return {
    id,
    type,
    payload: clonePayload(input.payload),
    requestedAt: safeInteger(input.requestedAt, Date.now(), 1),
    requestedBy: safeString(input.requestedBy || 'marcus', 80),
    idempotencyKey: safeString(input.idempotencyKey, 240),
    status,
    deliveredTo: status === 'delivered' ? safeString(input.deliveredTo, 200) : '',
    deliveredAt: status === 'delivered' ? safeInteger(input.deliveredAt, 0) : 0,
    leaseExpiresAt: status === 'delivered' ? safeInteger(input.leaseExpiresAt, 0) : 0,
    deliveryAttempts: safeInteger(input.deliveryAttempts, 0, 0, 10_000),
  };
}

function normalizeDocument(input) {
  const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  return {
    version: STORE_VERSION,
    revision: safeInteger(raw.revision, 1, 1),
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : new Date(0).toISOString(),
    actions: (Array.isArray(raw.actions) ? raw.actions : []).map((action) => normalizeAction(action)),
  };
}

function targetAgentId(action) {
  return safeString(action?.payload?.desktopAgentId, 200);
}

export class DesktopActionQueue {
  constructor({ dataDir, leaseMs = DEFAULT_LEASE_MS, maxActions = DEFAULT_MAX_ACTIONS } = {}) {
    if (!dataDir) throw new Error('DesktopActionQueue requires dataDir.');
    this.file = path.join(path.resolve(dataDir), 'desktop-actions.json');
    this.leaseMs = safeInteger(leaseMs, DEFAULT_LEASE_MS, 30_000, 24 * 60 * 60_000);
    this.maxActions = safeInteger(maxActions, DEFAULT_MAX_ACTIONS, 1, 20_000);
    this.writeQueue = Promise.resolve();
  }

  async enqueue(input) {
    const action = normalizeAction({
      ...input,
      id: safeString(input?.id, 120) || crypto.randomUUID(),
      requestedAt: Date.now(),
      status: 'queued',
    });
    return this.withLock(async () => {
      const document = await this.readDocumentUnlocked();
      const duplicate = (action.idempotencyKey
        ? document.actions.find((item) => item.idempotencyKey === action.idempotencyKey)
        : null) || document.actions.find((item) => item.id === action.id);
      if (duplicate) return structuredClone(duplicate);
      if (document.actions.length >= this.maxActions) {
        const error = new Error('The durable desktop action queue is full; no pending action was discarded.');
        error.code = 'DESKTOP_ACTION_QUEUE_FULL';
        throw error;
      }
      document.actions.push(action);
      await this.writeDocumentUnlocked(document);
      return structuredClone(action);
    });
  }

  async claim(agentId, { limit = 50, now = Date.now() } = {}) {
    const claimant = safeString(agentId, 200);
    const claimLimit = safeInteger(limit, 50, 1, 200);
    const claimedAt = safeInteger(now, Date.now(), 1);
    return this.withLock(async () => {
      const document = await this.readDocumentUnlocked();
      const claimed = [];
      for (const action of document.actions) {
        if (claimed.length >= claimLimit) break;
        const target = targetAgentId(action);
        if (target && target !== claimant) continue;
        const available = action.status === 'queued'
          || (action.status === 'delivered' && action.leaseExpiresAt <= claimedAt);
        if (!available) continue;
        action.status = 'delivered';
        action.deliveredTo = claimant;
        action.deliveredAt = claimedAt;
        action.leaseExpiresAt = claimedAt + this.leaseMs;
        action.deliveryAttempts += 1;
        claimed.push(structuredClone(action));
      }
      if (claimed.length) await this.writeDocumentUnlocked(document);
      return claimed;
    });
  }

  async acknowledge({ id, agentId = '', type = '', idempotencyKey = '' } = {}) {
    const actionId = safeString(id, 120);
    if (!actionId) return false;
    const claimant = safeString(agentId, 200);
    const expectedType = safeString(type, 80);
    const expectedIdempotencyKey = safeString(idempotencyKey, 240);
    return this.withLock(async () => {
      const document = await this.readDocumentUnlocked();
      const index = document.actions.findIndex((action) => action.id === actionId);
      if (index < 0) return false;
      const action = document.actions[index];
      if (action.status !== 'delivered') return false;
      const target = targetAgentId(action);
      if (target && claimant !== target) return false;
      if (action.deliveredTo && claimant !== action.deliveredTo) return false;
      if (expectedType && action.type !== expectedType) return false;
      if (action.idempotencyKey && expectedIdempotencyKey !== action.idempotencyKey) return false;
      document.actions.splice(index, 1);
      await this.writeDocumentUnlocked(document);
      return true;
    });
  }

  async get(id) {
    const actionId = safeString(id, 120);
    if (!actionId) return null;
    return this.withLock(async () => {
      const document = await this.readDocumentUnlocked();
      const action = document.actions.find((item) => item.id === actionId);
      return action ? structuredClone(action) : null;
    });
  }

  async list() {
    return this.withLock(async () => {
      const document = await this.readDocumentUnlocked();
      return structuredClone(document.actions);
    });
  }

  async withLock(work) {
    const run = this.writeQueue.catch(() => {}).then(work);
    this.writeQueue = run;
    try { return await run; } finally {
      if (this.writeQueue === run) this.writeQueue = Promise.resolve();
    }
  }

  async readDocumentUnlocked() {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    try {
      return normalizeDocument(JSON.parse(await fs.readFile(this.file, 'utf8')));
    } catch (primaryError) {
      if (primaryError?.code === 'ENOENT') {
        const document = emptyDocument();
        await this.atomicWrite(document, { createBackup: false });
        return document;
      }
      try {
        const recovered = normalizeDocument(JSON.parse(await fs.readFile(`${this.file}.bak`, 'utf8')));
        await fs.rename(this.file, `${this.file}.corrupt-${Date.now()}`).catch(() => {});
        await this.atomicWrite(recovered, { createBackup: false });
        return recovered;
      } catch {
        const error = new Error('The durable desktop action queue is corrupt; the original file was preserved.');
        error.code = 'CORRUPT_DESKTOP_ACTION_QUEUE';
        error.cause = primaryError;
        throw error;
      }
    }
  }

  async writeDocumentUnlocked(document) {
    const normalized = normalizeDocument({
      ...document,
      revision: safeInteger(document.revision, 1, 1) + 1,
      updatedAt: new Date().toISOString(),
    });
    await this.atomicWrite(normalized);
  }

  async atomicWrite(value, { createBackup = true } = {}) {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    const temporary = `${this.file}.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    if (createBackup) await fs.copyFile(this.file, `${this.file}.bak`).catch(() => {});
    await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    try { await fs.rename(temporary, this.file); } catch (error) {
      await fs.unlink(temporary).catch(() => {});
      throw error;
    }
  }
}

export { normalizeAction as normalizeDesktopAction, normalizeDocument as normalizeDesktopActionDocument };
