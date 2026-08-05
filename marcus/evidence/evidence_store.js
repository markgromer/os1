import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { nowIso, safeBusinessKey, safeInteger, safeObject, sanitizeStructured } from '../operations/operation_types.js';
import { normalizeEvidence } from './evidence_types.js';

const STORE_VERSION = 1;

function emptyDocument(businessKey) {
  return {
    version: STORE_VERSION,
    businessKey: safeBusinessKey(businessKey),
    revision: 1,
    updatedAt: new Date(0).toISOString(),
    evidence: [],
    sourceState: {},
    analysis: { calculatedAt: '', snapshots: [], currentFocus: null },
  };
}

function normalizeDocument(input, businessKey, maxHistory) {
  const raw = safeObject(input);
  const key = safeBusinessKey(businessKey || raw.businessKey);
  const evidence = [];
  const seen = new Set();
  for (const item of (Array.isArray(raw.evidence) ? raw.evidence : []).slice(-maxHistory)) {
    try {
      const normalized = normalizeEvidence(item, {
        businessKey: key,
        assignedSource: item?.source,
        trusted: item?.provenance?.trusted === true,
        actor: item?.actor,
        provenanceMethod: item?.provenance?.method || 'store_recovery',
      });
      if (seen.has(normalized.dedupeKey)) continue;
      seen.add(normalized.dedupeKey);
      evidence.push(normalized);
    } catch {
      // Invalid historical entries are omitted during bounded normalization.
    }
  }
  evidence.sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.id.localeCompare(b.id));
  return {
    version: STORE_VERSION,
    businessKey: key,
    revision: safeInteger(raw.revision, 1, 1),
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : new Date(0).toISOString(),
    evidence,
    sourceState: sanitizeStructured(raw.sourceState ?? {}, 200_000),
    analysis: sanitizeStructured(raw.analysis ?? { calculatedAt: '', snapshots: [], currentFocus: null }, 2_000_000),
  };
}

export class ProjectEvidenceStore {
  constructor({ dataDir, maxHistory = 20_000 } = {}) {
    if (!dataDir) throw new Error('ProjectEvidenceStore requires dataDir.');
    this.dataDir = path.resolve(dataDir);
    this.businessDataDir = path.join(this.dataDir, 'businesses');
    this.maxHistory = safeInteger(maxHistory, 20_000, 500, 100_000);
    this.writeQueues = new Map();
  }

  fileForBusiness(businessKey) {
    return path.join(this.businessDataDir, safeBusinessKey(businessKey), 'project-evidence.json');
  }

  async ensureBusiness(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = this.fileForBusiness(key);
    await fs.mkdir(path.dirname(file), { recursive: true });
    try { await fs.access(file); } catch { await this.atomicWrite(file, emptyDocument(key), { createBackup: false }); }
    return file;
  }

  async readDocument(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = await this.ensureBusiness(key);
    try {
      return normalizeDocument(JSON.parse(await fs.readFile(file, 'utf8')), key, this.maxHistory);
    } catch (primaryError) {
      try {
        const recovered = normalizeDocument(JSON.parse(await fs.readFile(`${file}.bak`, 'utf8')), key, this.maxHistory);
        await fs.rename(file, `${file}.corrupt-${Date.now()}`).catch(() => {});
        await this.atomicWrite(file, recovered, { createBackup: false });
        return recovered;
      } catch {
        throw Object.assign(new Error(`Project evidence store is corrupt for business ${key}; the original file was preserved.`), {
          code: 'CORRUPT_PROJECT_EVIDENCE_STORE', cause: primaryError,
        });
      }
    }
  }

  async list(businessKey, filters = {}) {
    const document = await this.readDocument(businessKey);
    let items = document.evidence.slice();
    if (filters.projectRegistryId) items = items.filter((item) => item.projectRegistryId === filters.projectRegistryId);
    if (filters.source) {
      const sources = new Set(String(filters.source).split(',').map((item) => item.trim().toLowerCase()).filter(Boolean));
      items = items.filter((item) => sources.has(item.source));
    }
    if (filters.type) {
      const types = new Set(String(filters.type).split(',').map((item) => item.trim().toLowerCase()).filter(Boolean));
      items = items.filter((item) => types.has(item.type));
    }
    if (filters.since) {
      const since = Date.parse(filters.since);
      if (Number.isFinite(since)) items = items.filter((item) => Date.parse(item.timestamp) >= since);
    }
    items.sort((a, b) => b.timestamp.localeCompare(a.timestamp) || b.observedAt.localeCompare(a.observedAt));
    return items.slice(0, safeInteger(filters.limit, 500, 1, 5_000));
  }

  async append(businessKey, inputs, options = {}) {
    const key = safeBusinessKey(businessKey);
    const list = Array.isArray(inputs) ? inputs : [inputs];
    return this.mutate(key, (document) => {
      const dedupe = new Set(document.evidence.map((item) => item.dedupeKey));
      const ids = new Set(document.evidence.map((item) => item.id));
      const accepted = [];
      const duplicates = [];
      for (const input of list.slice(0, 5_000)) {
        const item = normalizeEvidence(input, { ...options, businessKey: key });
        if (dedupe.has(item.dedupeKey) || ids.has(item.id)) {
          duplicates.push(item.dedupeKey);
          continue;
        }
        document.evidence.push(item);
        dedupe.add(item.dedupeKey);
        ids.add(item.id);
        accepted.push(item);
      }
      document.evidence.sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.id.localeCompare(b.id));
      if (document.evidence.length > this.maxHistory) document.evidence = document.evidence.slice(-this.maxHistory);
      return { accepted, duplicateCount: duplicates.length, duplicates: duplicates.slice(0, 100) };
    });
  }

  async reconcile(businessKey, evidenceId, updater) {
    return this.mutate(businessKey, (document) => {
      const index = document.evidence.findIndex((item) => item.id === evidenceId);
      if (index < 0) throw Object.assign(new Error('Evidence not found for reconciliation.'), { code: 'EVIDENCE_NOT_FOUND' });
      const current = structuredClone(document.evidence[index]);
      const candidate = typeof updater === 'function' ? updater(current) : { ...current, ...safeObject(updater) };
      document.evidence[index] = normalizeEvidence({
        ...candidate,
        id: current.id,
        businessKey: current.businessKey,
        projectRegistryId: current.projectRegistryId,
        source: current.source,
        type: current.type,
        timestamp: current.timestamp,
        externalId: current.externalId,
        provenance: { ...safeObject(candidate.provenance), reconciledAt: nowIso() },
      }, {
        businessKey: current.businessKey,
        assignedSource: current.source,
        trusted: current.provenance?.trusted === true,
        actor: current.actor,
        provenanceMethod: current.provenance?.method || 'explicit_reconciliation',
      });
      return document.evidence[index];
    });
  }

  async getSourceState(businessKey, sourceKey) {
    const document = await this.readDocument(businessKey);
    return structuredClone(safeObject(document.sourceState)[String(sourceKey || '')] || {});
  }

  async setSourceState(businessKey, sourceKey, value) {
    const key = String(sourceKey || '').trim().slice(0, 500);
    if (!key) throw new Error('sourceKey is required.');
    return this.mutate(businessKey, (document) => {
      document.sourceState = { ...safeObject(document.sourceState), [key]: sanitizeStructured(value ?? {}, 100_000) };
      return structuredClone(document.sourceState[key]);
    });
  }

  async getAnalysis(businessKey) {
    const document = await this.readDocument(businessKey);
    return structuredClone(safeObject(document.analysis));
  }

  async setAnalysis(businessKey, analysis) {
    return this.mutate(businessKey, (document) => {
      document.analysis = sanitizeStructured(analysis ?? {}, 2_000_000);
      return structuredClone(document.analysis);
    });
  }

  async mutate(businessKey, mutator) {
    const key = safeBusinessKey(businessKey);
    const previous = this.writeQueues.get(key) || Promise.resolve();
    const run = previous.catch(() => {}).then(async () => {
      const document = await this.readDocument(key);
      const result = await mutator(document);
      document.revision += 1;
      document.updatedAt = nowIso();
      await this.atomicWrite(this.fileForBusiness(key), normalizeDocument(document, key, this.maxHistory));
      return result;
    });
    this.writeQueues.set(key, run);
    try { return await run; } finally { if (this.writeQueues.get(key) === run) this.writeQueues.delete(key); }
  }

  async atomicWrite(file, value, { createBackup = true } = {}) {
    await fs.mkdir(path.dirname(file), { recursive: true });
    const temporary = `${file}.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    if (createBackup) await fs.copyFile(file, `${file}.bak`).catch(() => {});
    await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    try { await fs.rename(temporary, file); } catch (error) { await fs.unlink(temporary).catch(() => {}); throw error; }
  }
}

export { emptyDocument as createEmptyProjectEvidenceDocument, normalizeDocument as normalizeProjectEvidenceDocument };
