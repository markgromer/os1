import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { nowIso, redactSecrets, safeBusinessKey, safeIso, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';

export const ATTENTION_STATUSES = Object.freeze(['open', 'acknowledged', 'deferred', 'resolved', 'superseded']);
export const ATTENTION_OWNERS = Object.freeze(['marcus', 'mark', 'shared', 'external']);

function normalizeItem(input = {}, businessKey = '') {
  const raw = safeObject(input);
  const status = safeString(raw.status, 40).toLowerCase();
  const owner = safeString(raw.owner, 40).toLowerCase();
  const createdAt = safeIso(raw.createdAt) || nowIso();
  return {
    id: safeString(raw.id, 160) || `attention_${crypto.randomBytes(10).toString('base64url')}`,
    businessKey: safeBusinessKey(businessKey || raw.businessKey),
    fingerprint: safeString(raw.fingerprint, 300),
    signalId: safeString(raw.signalId, 160),
    signalType: safeString(raw.signalType, 160),
    subject: sanitizeStructured(raw.subject ?? {}, 2_000),
    title: redactSecrets(safeString(raw.title, 300), 300) || 'MARCUS needs attention',
    reason: redactSecrets(safeString(raw.reason, 4_000), 4_000),
    severity: safeString(raw.severity, 40) || 'notice',
    confidence: Math.max(0, Math.min(1, Number.isFinite(Number(raw.confidence)) ? Number(raw.confidence) : 1)),
    owner: ATTENTION_OWNERS.includes(owner) ? owner : 'marcus',
    status: ATTENTION_STATUSES.includes(status) ? status : 'open',
    occurrences: Math.max(1, Number(raw.occurrences) || 1),
    firstObservedAt: safeIso(raw.firstObservedAt) || createdAt,
    lastObservedAt: safeIso(raw.lastObservedAt) || createdAt,
    deferUntil: safeIso(raw.deferUntil),
    resolution: redactSecrets(safeString(raw.resolution, 4_000), 4_000),
    evidence: (Array.isArray(raw.evidence) ? raw.evidence : []).slice(0, 30).map((item) => sanitizeStructured(item, 4_000)),
    createdAt,
    updatedAt: safeIso(raw.updatedAt) || createdAt,
  };
}

export class AttentionStore {
  constructor({ dataDir, maxItems = 1_000 } = {}) {
    if (!dataDir) throw new Error('AttentionStore requires dataDir.');
    this.dataDir = path.resolve(String(dataDir));
    this.maxItems = Math.max(100, Number(maxItems) || 1_000);
    this.queues = new Map();
  }

  fileForBusiness(businessKey) { return path.join(this.dataDir, 'businesses', safeBusinessKey(businessKey), 'marcus-attention.json'); }

  async read(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = this.fileForBusiness(key);
    try {
      const parsed = JSON.parse(await fs.readFile(file, 'utf8'));
      return { version: 1, businessKey: key, revision: Math.max(1, Number(parsed.revision) || 1), items: (Array.isArray(parsed.items) ? parsed.items : []).map((item) => normalizeItem(item, key)) };
    } catch (error) {
      if (error?.code === 'ENOENT') return { version: 1, businessKey: key, revision: 1, items: [] };
      try {
        const parsed = JSON.parse(await fs.readFile(`${file}.bak`, 'utf8'));
        return { version: 1, businessKey: key, revision: Math.max(1, Number(parsed.revision) || 1), items: (Array.isArray(parsed.items) ? parsed.items : []).map((item) => normalizeItem(item, key)) };
      } catch { throw Object.assign(new Error(`MARCUS attention store is unreadable for ${key}.`), { code: 'CORRUPT_ATTENTION_STORE', cause: error }); }
    }
  }

  async mutate(businessKey, mutation) {
    const key = safeBusinessKey(businessKey);
    const previous = this.queues.get(key) || Promise.resolve();
    let output;
    const queued = previous.catch(() => {}).then(async () => {
      const document = await this.read(key);
      output = await mutation(document);
      document.items = document.items.slice(-this.maxItems);
      document.revision += 1;
      const file = this.fileForBusiness(key);
      await fs.mkdir(path.dirname(file), { recursive: true });
      const temporary = `${file}.tmp-${process.pid}-${Date.now()}`;
      await fs.writeFile(temporary, `${JSON.stringify(document, null, 2)}\n`, 'utf8');
      await fs.copyFile(file, `${file}.bak`).catch((error) => { if (error?.code !== 'ENOENT') throw error; });
      await fs.rename(temporary, file);
    });
    this.queues.set(key, queued);
    try { await queued; return output; } finally { if (this.queues.get(key) === queued) this.queues.delete(key); }
  }

  async raise(businessKey, input = {}) {
    return this.mutate(businessKey, (document) => {
      const candidate = normalizeItem(input, businessKey);
      const fingerprint = candidate.fingerprint || `${candidate.signalType}:${candidate.subject?.type || ''}:${candidate.subject?.id || ''}:${candidate.owner}`;
      const index = document.items.findIndex((item) => item.fingerprint === fingerprint && ['open', 'acknowledged', 'deferred'].includes(item.status));
      if (index >= 0) {
        const current = document.items[index];
        document.items[index] = normalizeItem({ ...current, ...candidate, id: current.id, fingerprint, status: current.status, occurrences: current.occurrences + 1, firstObservedAt: current.firstObservedAt, lastObservedAt: nowIso(), updatedAt: nowIso() }, businessKey);
        return structuredClone(document.items[index]);
      }
      candidate.fingerprint = fingerprint;
      document.items.push(candidate);
      return structuredClone(candidate);
    });
  }

  async list(businessKey, { status = '', owner = '', limit = 100 } = {}) {
    const items = (await this.read(businessKey)).items.filter((item) => (!status || item.status === status) && (!owner || item.owner === owner));
    return items.slice(-Math.max(1, Number(limit) || 100)).reverse();
  }

  async transition(businessKey, id, status, { resolution = '', deferUntil = '' } = {}) {
    if (!ATTENTION_STATUSES.includes(status)) throw Object.assign(new Error(`Unsupported attention status: ${status}.`), { code: 'ATTENTION_STATUS_INVALID' });
    return this.mutate(businessKey, (document) => {
      const index = document.items.findIndex((item) => item.id === id);
      if (index < 0) throw Object.assign(new Error('Attention item not found.'), { code: 'ATTENTION_NOT_FOUND' });
      document.items[index] = normalizeItem({ ...document.items[index], status, resolution, deferUntil, updatedAt: nowIso() }, businessKey);
      return structuredClone(document.items[index]);
    });
  }

  async reopenDue(businessKey, now = new Date()) {
    const nowMs = now instanceof Date ? now.getTime() : Date.parse(now);
    const current = await this.read(businessKey);
    if (!current.items.some((item) => item.status === 'deferred' && item.deferUntil && Date.parse(item.deferUntil) <= nowMs)) return [];
    return this.mutate(businessKey, (document) => {
      const reopened = [];
      document.items = document.items.map((item) => {
        if (item.status !== 'deferred' || !item.deferUntil || Date.parse(item.deferUntil) > nowMs) return item;
        const next = normalizeItem({ ...item, status: 'open', deferUntil: '', updatedAt: nowIso() }, businessKey);
        reopened.push(next); return next;
      });
      return structuredClone(reopened);
    });
  }
}
