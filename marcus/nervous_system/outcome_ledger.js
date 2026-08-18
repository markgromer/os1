import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { nowIso, redactSecrets, safeBusinessKey, safeIso, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';

function normalizeOutcome(input = {}, businessKey = '') {
  const raw = safeObject(input);
  const recordedAt = safeIso(raw.recordedAt) || nowIso();
  return {
    id: safeString(raw.id, 160) || `outcome_${crypto.randomBytes(10).toString('base64url')}`,
    businessKey: safeBusinessKey(businessKey || raw.businessKey),
    traceId: safeString(raw.traceId, 160),
    signalId: safeString(raw.signalId, 160),
    signalType: safeString(raw.signalType, 160),
    pathway: safeString(raw.pathway, 160),
    response: redactSecrets(safeString(raw.response, 2_000), 2_000),
    status: ['succeeded', 'failed', 'partial', 'deferred', 'unknown'].includes(raw.status) ? raw.status : 'unknown',
    evidence: (Array.isArray(raw.evidence) ? raw.evidence : []).slice(0, 30).map((item) => sanitizeStructured(item, 4_000)),
    correction: redactSecrets(safeString(raw.correction, 4_000), 4_000),
    reusable: raw.reusable === true,
    recordedAt,
    updatedAt: safeIso(raw.updatedAt) || recordedAt,
  };
}

export class OutcomeLedger {
  constructor({ dataDir, maxEntries = 2_000 } = {}) {
    if (!dataDir) throw new Error('OutcomeLedger requires dataDir.');
    this.dataDir = path.resolve(String(dataDir));
    this.maxEntries = Math.max(100, Number(maxEntries) || 2_000);
    this.queues = new Map();
  }
  fileForBusiness(businessKey) { return path.join(this.dataDir, 'businesses', safeBusinessKey(businessKey), 'marcus-outcomes.json'); }
  async read(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = this.fileForBusiness(key);
    try { const value = JSON.parse(await fs.readFile(file, 'utf8')); return { version: 1, businessKey: key, entries: (Array.isArray(value.entries) ? value.entries : []).map((item) => normalizeOutcome(item, key)) }; }
    catch (error) {
      if (error?.code === 'ENOENT') return { version: 1, businessKey: key, entries: [] };
      try { const value = JSON.parse(await fs.readFile(`${file}.bak`, 'utf8')); return { version: 1, businessKey: key, entries: (Array.isArray(value.entries) ? value.entries : []).map((item) => normalizeOutcome(item, key)) }; }
      catch { throw Object.assign(new Error(`MARCUS outcome ledger is unreadable for ${key}.`), { code: 'CORRUPT_OUTCOME_LEDGER', cause: error }); }
    }
  }
  async write(businessKey, mutation) {
    const key = safeBusinessKey(businessKey);
    const previous = this.queues.get(key) || Promise.resolve(); let output;
    const queued = previous.catch(() => {}).then(async () => {
      const document = await this.read(key); output = await mutation(document); document.entries = document.entries.slice(-this.maxEntries);
      const file = this.fileForBusiness(key); await fs.mkdir(path.dirname(file), { recursive: true }); const temporary = `${file}.tmp-${process.pid}-${Date.now()}`;
      await fs.writeFile(temporary, `${JSON.stringify(document, null, 2)}\n`, 'utf8'); await fs.copyFile(file, `${file}.bak`).catch((error) => { if (error?.code !== 'ENOENT') throw error; }); await fs.rename(temporary, file);
    });
    this.queues.set(key, queued); try { await queued; return output; } finally { if (this.queues.get(key) === queued) this.queues.delete(key); }
  }
  async record(businessKey, input) { return this.write(businessKey, (document) => { const entry = normalizeOutcome(input, businessKey); document.entries.push(entry); return structuredClone(entry); }); }
  async list(businessKey, { traceId = '', status = '', limit = 100 } = {}) { return (await this.read(businessKey)).entries.filter((entry) => (!traceId || entry.traceId === traceId) && (!status || entry.status === status)).slice(-Math.max(1, Number(limit) || 100)).reverse(); }
  async correct(businessKey, id, correction, { reusable = false } = {}) { return this.write(businessKey, (document) => { const index = document.entries.findIndex((entry) => entry.id === id); if (index < 0) throw Object.assign(new Error('Outcome not found.'), { code: 'OUTCOME_NOT_FOUND' }); document.entries[index] = normalizeOutcome({ ...document.entries[index], correction, reusable, updatedAt: nowIso() }, businessKey); return structuredClone(document.entries[index]); }); }
}
