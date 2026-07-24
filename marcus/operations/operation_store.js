import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { normalizeOperation, nowIso, safeBusinessKey, safeInteger } from './operation_types.js';

const STORE_VERSION = 1;

function emptyDocument(businessKey) {
  return {
    version: STORE_VERSION,
    businessKey: safeBusinessKey(businessKey),
    revision: 1,
    updatedAt: new Date(0).toISOString(),
    operations: [],
  };
}

function normalizeDocument(input, businessKey) {
  const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const key = safeBusinessKey(businessKey || raw.businessKey);
  return {
    version: STORE_VERSION,
    businessKey: key,
    revision: safeInteger(raw.revision, 1, 1),
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : new Date(0).toISOString(),
    operations: (Array.isArray(raw.operations) ? raw.operations : []).map((operation) => normalizeOperation(operation, { businessKey: key })),
  };
}

export class OperationStore {
  constructor({ dataDir }) {
    if (!dataDir) throw new Error('OperationStore requires dataDir.');
    this.dataDir = path.resolve(dataDir);
    this.businessDataDir = path.join(this.dataDir, 'businesses');
    this.writeQueues = new Map();
  }

  fileForBusiness(businessKey) {
    const key = safeBusinessKey(businessKey);
    return path.join(this.businessDataDir, key, 'operations.json');
  }

  async discoverBusinessKeys() {
    let entries = [];
    try { entries = await fs.readdir(this.businessDataDir, { withFileTypes: true }); } catch { return []; }
    const keys = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const key = safeBusinessKey(entry.name, '');
      if (!key) continue;
      try {
        await fs.access(this.fileForBusiness(key));
        keys.push(key);
      } catch {
        // This business has no operation store yet.
      }
    }
    return keys;
  }

  async ensureBusiness(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = this.fileForBusiness(key);
    await fs.mkdir(path.dirname(file), { recursive: true });
    try {
      await fs.access(file);
    } catch {
      await this.atomicWrite(file, emptyDocument(key), { createBackup: false });
    }
    return file;
  }

  async readDocument(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = await this.ensureBusiness(key);
    try {
      const parsed = JSON.parse(await fs.readFile(file, 'utf8'));
      return normalizeDocument(parsed, key);
    } catch (primaryError) {
      const backupFile = `${file}.bak`;
      try {
        const parsedBackup = JSON.parse(await fs.readFile(backupFile, 'utf8'));
        const recovered = normalizeDocument(parsedBackup, key);
        const corruptFile = `${file}.corrupt-${Date.now()}`;
        await fs.rename(file, corruptFile).catch(() => {});
        await this.atomicWrite(file, recovered, { createBackup: false });
        return recovered;
      } catch {
        const error = new Error(`Operations store is corrupt for business ${key}; the original file was preserved.`);
        error.code = 'CORRUPT_OPERATIONS_STORE';
        error.cause = primaryError;
        throw error;
      }
    }
  }

  async list(businessKey, filters = {}) {
    const document = await this.readDocument(businessKey);
    let operations = document.operations.slice();
    if (filters.status) {
      const statuses = new Set((Array.isArray(filters.status) ? filters.status : String(filters.status).split(','))
        .map((item) => String(item || '').trim().toLowerCase()).filter(Boolean));
      operations = operations.filter((operation) => statuses.has(operation.status));
    }
    if (filters.projectRegistryId) operations = operations.filter((operation) => operation.projectRegistryId === filters.projectRegistryId);
    if (filters.projectId) operations = operations.filter((operation) => operation.projectId === filters.projectId);
    operations.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
    const limit = safeInteger(filters.limit, 200, 1, 500);
    return operations.slice(0, limit);
  }

  async listAll(businessKey, filters = {}) {
    const document = await this.readDocument(businessKey);
    let operations = document.operations.slice();
    if (filters.nonterminal === true) operations = operations.filter((operation) => !['completed', 'failed', 'cancelled'].includes(operation.status));
    operations.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
    return operations;
  }

  async get(businessKey, operationId) {
    const id = String(operationId || '').trim();
    if (!id) return null;
    const document = await this.readDocument(businessKey);
    return document.operations.find((operation) => operation.id === id) || null;
  }

  async create(businessKey, input) {
    const key = safeBusinessKey(businessKey);
    return this.mutate(key, (document) => {
      const operation = normalizeOperation(input, { businessKey: key });
      if (document.operations.some((item) => item.id === operation.id)) {
        const error = new Error(`Operation already exists: ${operation.id}`);
        error.code = 'OPERATION_EXISTS';
        throw error;
      }
      document.operations.push(operation);
      return operation;
    });
  }

  async update(businessKey, operationId, updater, options = {}) {
    const key = safeBusinessKey(businessKey);
    const id = String(operationId || '').trim();
    return this.mutate(key, (document) => {
      const index = document.operations.findIndex((operation) => operation.id === id);
      if (index < 0) {
        const error = new Error('Operation not found.');
        error.code = 'OPERATION_NOT_FOUND';
        throw error;
      }
      const current = document.operations[index];
      if (Number.isFinite(Number(options.expectedOperationRevision))
        && Number(options.expectedOperationRevision) !== current.revision) {
        const error = new Error('Operation revision mismatch. Reload and try again.');
        error.code = 'REVISION_MISMATCH';
        error.currentRevision = current.revision;
        throw error;
      }
      const candidate = typeof updater === 'function' ? updater(structuredClone(current)) : { ...current, ...updater };
      const timestamp = nowIso();
      const normalized = normalizeOperation({
        ...candidate,
        id: current.id,
        businessKey: key,
        createdAt: current.createdAt,
        updatedAt: timestamp,
        revision: current.revision + 1,
      }, { businessKey: key });
      document.operations[index] = normalized;
      return normalized;
    }, options);
  }

  async mutate(businessKey, mutator, options = {}) {
    const key = safeBusinessKey(businessKey);
    const previous = this.writeQueues.get(key) || Promise.resolve();
    const run = previous.catch(() => {}).then(async () => {
      const document = await this.readDocument(key);
      if (Number.isFinite(Number(options.expectedStoreRevision))
        && Number(options.expectedStoreRevision) !== document.revision) {
        const error = new Error('Operations store revision mismatch.');
        error.code = 'STORE_REVISION_MISMATCH';
        error.currentRevision = document.revision;
        throw error;
      }
      const result = await mutator(document);
      document.revision += 1;
      document.updatedAt = nowIso();
      await this.atomicWrite(this.fileForBusiness(key), normalizeDocument(document, key));
      return result;
    });
    this.writeQueues.set(key, run);
    try {
      return await run;
    } finally {
      if (this.writeQueues.get(key) === run) this.writeQueues.delete(key);
    }
  }

  async atomicWrite(file, value, { createBackup = true } = {}) {
    await fs.mkdir(path.dirname(file), { recursive: true });
    const temporary = `${file}.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    const json = `${JSON.stringify(value, null, 2)}\n`;
    if (createBackup) {
      try {
        await fs.copyFile(file, `${file}.bak`);
      } catch {
        // No prior file is a normal first-write condition.
      }
    }
    await fs.writeFile(temporary, json, { encoding: 'utf8', flag: 'wx' });
    try {
      await fs.rename(temporary, file);
    } catch (error) {
      await fs.unlink(temporary).catch(() => {});
      throw error;
    }
  }
}

export { emptyDocument as createEmptyOperationDocument, normalizeDocument as normalizeOperationDocument };
