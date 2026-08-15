import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { nowIso, safeBusinessKey, safeIso, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';
import { AWARENESS_LIFECYCLES, lifecycleFromProjectStatus } from '../projects/project_lifecycle.js';

const LIFECYCLES = new Set(AWARENESS_LIFECYCLES);

function awarenessId(projectRegistryId) {
  return `awareness_${crypto.createHash('sha256').update(String(projectRegistryId)).digest('hex').slice(0, 20)}`;
}

function normalizeMemory(value = {}) {
  const raw = safeObject(value);
  return {
    status: ['indexed', 'fresh', 'stale', 'incomplete', 'unavailable'].includes(safeString(raw.status, 40)) ? safeString(raw.status, 40) : 'unavailable',
    summary: safeString(raw.summary, 4_000),
    sources: (Array.isArray(raw.sources) ? raw.sources : []).slice(0, 50).map((source) => ({
      type: safeString(source?.type, 80),
      path: safeString(source?.path, 2_000),
      hash: safeString(source?.hash, 128),
      modifiedAt: safeIso(source?.modifiedAt),
      indexedAt: safeIso(source?.indexedAt),
      excerpt: safeString(source?.excerpt, 4_000),
    })).filter((source) => source.type && source.path),
    repositoryManifest: sanitizeStructured(raw.repositoryManifest ?? {}, 20_000),
    contentHash: safeString(raw.contentHash, 128),
    lastIndexedAt: safeIso(raw.lastIndexedAt),
    error: safeString(raw.error, 1_000),
  };
}

function normalizeRecord(input = {}, businessKey = '') {
  const raw = safeObject(input);
  const projectRegistryId = safeString(raw.projectRegistryId, 160);
  if (!projectRegistryId) throw Object.assign(new Error('Awareness projectRegistryId is required.'), { code: 'AWARENESS_PROJECT_INVALID' });
  const createdAt = safeIso(raw.createdAt) || nowIso();
  const lifecycle = safeString(raw.lifecycle, 40).toLowerCase();
  return {
    id: safeString(raw.id, 120) || awarenessId(projectRegistryId),
    businessKey: safeBusinessKey(businessKey || raw.businessKey),
    projectRegistryId,
    canonicalName: safeString(raw.canonicalName, 300),
    aliases: [...new Set((Array.isArray(raw.aliases) ? raw.aliases : []).map((item) => safeString(item, 300)).filter(Boolean))].slice(0, 50),
    lifecycle: LIFECYCLES.has(lifecycle) ? lifecycle : 'active',
    lifecycleSource: safeString(raw.lifecycleSource, 100) || 'project_registry',
    objectiveBelief: safeString(raw.objectiveBelief, 4_000),
    latestMeaningfulChange: safeString(raw.latestMeaningfulChange, 2_000),
    currentActivity: safeString(raw.currentActivity, 2_000),
    blockerOrDependency: safeString(raw.blockerOrDependency, 2_000),
    likelyNextStep: safeString(raw.likelyNextStep, 2_000),
    uncertaintyNotes: (Array.isArray(raw.uncertaintyNotes) ? raw.uncertaintyNotes : []).map((item) => safeString(item, 1_000)).filter(Boolean).slice(0, 20),
    confidence: Math.max(0, Math.min(1, Number(raw.confidence) || 0.5)),
    memory: normalizeMemory(raw.memory),
    workHistory: (Array.isArray(raw.workHistory) ? raw.workHistory : []).slice(-200).map((item) => ({
      eventId: safeString(item?.eventId, 200),
      type: safeString(item?.type, 80),
      summary: safeString(item?.summary, 2_000),
      status: safeString(item?.status, 80),
      recordedAt: safeIso(item?.recordedAt),
    })).filter((item) => item.eventId),
    lifecycleHistory: (Array.isArray(raw.lifecycleHistory) ? raw.lifecycleHistory : []).slice(-100).map((item) => sanitizeStructured(item, 4_000)),
    createdAt,
    updatedAt: safeIso(raw.updatedAt) || createdAt,
  };
}

function emptyDocument(businessKey) {
  return { version: 1, businessKey: safeBusinessKey(businessKey), revision: 1, updatedAt: new Date(0).toISOString(), projects: [] };
}

function normalizeDocument(input, businessKey) {
  const raw = safeObject(input);
  const key = safeBusinessKey(businessKey || raw.businessKey);
  return {
    version: 1,
    businessKey: key,
    revision: Math.max(1, Math.floor(Number(raw.revision) || 1)),
    updatedAt: safeIso(raw.updatedAt) || new Date(0).toISOString(),
    projects: (Array.isArray(raw.projects) ? raw.projects : []).map((item) => normalizeRecord(item, key)),
  };
}

export class AwarenessStore {
  constructor({ dataDir } = {}) {
    if (!dataDir) throw new Error('AwarenessStore requires dataDir.');
    this.dataDir = path.resolve(String(dataDir));
    this.queues = new Map();
  }

  fileForBusiness(businessKey) {
    return path.join(this.dataDir, 'businesses', safeBusinessKey(businessKey), 'marcus-awareness.json');
  }

  async read(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = this.fileForBusiness(key);
    try {
      return normalizeDocument(JSON.parse(await fs.readFile(file, 'utf8')), key);
    } catch (error) {
      if (error?.code === 'ENOENT') return emptyDocument(key);
      try {
        const recovered = normalizeDocument(JSON.parse(await fs.readFile(`${file}.bak`, 'utf8')), key);
        await fs.rename(file, `${file}.corrupt-${Date.now()}`).catch(() => {});
        await this.writeFile(file, recovered, false);
        return recovered;
      } catch {
        throw Object.assign(new Error(`Marcus awareness store is corrupt for business ${key}; the original file was preserved.`), { code: 'CORRUPT_AWARENESS_STORE', cause: error });
      }
    }
  }

  async list(businessKey) {
    return (await this.read(businessKey)).projects;
  }

  async get(businessKey, id) {
    return (await this.list(businessKey)).find((item) => item.id === id || item.projectRegistryId === id) || null;
  }

  async synchronize(businessKey, registryProjects = []) {
    return this.mutate(businessKey, (document) => {
      const timestamp = nowIso();
      for (const project of Array.isArray(registryProjects) ? registryProjects : []) {
        const projectRegistryId = safeString(project?.id, 160);
        if (!projectRegistryId) continue;
        const index = document.projects.findIndex((item) => item.projectRegistryId === projectRegistryId);
        const registryLifecycle = lifecycleFromProjectStatus(project?.status);
        if (index < 0) {
          document.projects.push(normalizeRecord({
            projectRegistryId,
            canonicalName: project?.canonicalName,
            aliases: project?.aliases,
            lifecycle: registryLifecycle,
            lifecycleSource: 'project_registry',
            objectiveBelief: project?.currentObjective?.desiredOutcome || project?.description,
            confidence: 0.65,
            createdAt: timestamp,
            updatedAt: timestamp,
          }, document.businessKey));
          continue;
        }
        const current = document.projects[index];
        const registryIsTerminal = ['archived', 'completed'].includes(registryLifecycle);
        const lifecycle = registryIsTerminal || current.lifecycleSource === 'project_registry' ? registryLifecycle : current.lifecycle;
        const candidate = normalizeRecord({
          ...current,
          canonicalName: project?.canonicalName || current.canonicalName,
          aliases: project?.aliases || current.aliases,
          lifecycle,
          lifecycleSource: lifecycle !== current.lifecycle ? 'project_registry' : current.lifecycleSource,
          objectiveBelief: project?.currentObjective?.desiredOutcome || current.objectiveBelief || project?.description,
        }, document.businessKey);
        if (JSON.stringify(candidate) !== JSON.stringify(current)) {
          document.projects[index] = normalizeRecord({ ...candidate, updatedAt: timestamp }, document.businessKey);
        }
      }
      return document.projects.map((item) => structuredClone(item));
    });
  }

  async setLifecycle(businessKey, id, lifecycle, { actor = 'mark', reason = '', source = 'awareness_api' } = {}) {
    const next = safeString(lifecycle, 40).toLowerCase();
    if (!LIFECYCLES.has(next)) throw Object.assign(new Error(`Unsupported awareness lifecycle: ${next}.`), { code: 'AWARENESS_LIFECYCLE_INVALID' });
    return this.mutate(businessKey, (document) => {
      const index = document.projects.findIndex((item) => item.id === id || item.projectRegistryId === id);
      if (index < 0) throw Object.assign(new Error('Awareness project not found.'), { code: 'AWARENESS_PROJECT_NOT_FOUND' });
      const current = document.projects[index];
      if (current.lifecycle === next) return structuredClone(current);
      const timestamp = nowIso();
      const updated = normalizeRecord({
        ...current,
        lifecycle: next,
        lifecycleSource: source,
        lifecycleHistory: [...current.lifecycleHistory, {
          from: current.lifecycle,
          to: next,
          actor: safeString(actor, 120),
          reason: safeString(reason, 1_000),
          source: safeString(source, 100),
          changedAt: timestamp,
        }],
        updatedAt: timestamp,
      }, document.businessKey);
      document.projects[index] = updated;
      return structuredClone(updated);
    });
  }

  async updateMemory(businessKey, id, memory) {
    return this.mutate(businessKey, (document) => {
      const index = document.projects.findIndex((item) => item.id === id || item.projectRegistryId === id);
      if (index < 0) throw Object.assign(new Error('Awareness project not found.'), { code: 'AWARENESS_PROJECT_NOT_FOUND' });
      document.projects[index] = normalizeRecord({ ...document.projects[index], memory, updatedAt: nowIso() }, document.businessKey);
      return structuredClone(document.projects[index]);
    });
  }

  async appendWorkEvent(businessKey, id, event = {}) {
    return this.mutate(businessKey, (document) => {
      const index = document.projects.findIndex((item) => item.id === id || item.projectRegistryId === id);
      if (index < 0) throw Object.assign(new Error('Awareness project not found.'), { code: 'AWARENESS_PROJECT_NOT_FOUND' });
      const eventId = safeString(event.eventId || event.id, 200);
      if (!eventId) throw Object.assign(new Error('Awareness work event id is required.'), { code: 'AWARENESS_EVENT_INVALID' });
      const current = document.projects[index];
      if (current.workHistory.some((item) => item.eventId === eventId)) return { created: false, record: structuredClone(current) };
      const recordedAt = safeIso(event.recordedAt) || nowIso();
      const updated = normalizeRecord({
        ...current,
        workHistory: [...current.workHistory, {
          eventId,
          type: safeString(event.type, 80) || 'work_update',
          summary: safeString(event.summary, 2_000),
          status: safeString(event.status, 80),
          recordedAt,
        }],
        latestMeaningfulChange: safeString(event.summary, 2_000) || current.latestMeaningfulChange,
        updatedAt: recordedAt,
      }, document.businessKey);
      document.projects[index] = updated;
      return { created: true, record: structuredClone(updated) };
    });
  }

  async mutate(businessKey, mutator) {
    const key = safeBusinessKey(businessKey);
    const previous = this.queues.get(key) || Promise.resolve();
    const run = previous.catch(() => {}).then(async () => {
      const document = await this.read(key);
      const before = JSON.stringify(document.projects);
      const result = await mutator(document);
      if (JSON.stringify(document.projects) === before) return result;
      document.revision += 1;
      document.updatedAt = nowIso();
      await this.writeFile(this.fileForBusiness(key), document, true);
      return result;
    });
    this.queues.set(key, run);
    try { return await run; } finally { if (this.queues.get(key) === run) this.queues.delete(key); }
  }

  async writeFile(file, value, backup = true) {
    await fs.mkdir(path.dirname(file), { recursive: true });
    if (backup) await fs.copyFile(file, `${file}.bak`).catch(() => {});
    const temporary = `${file}.${process.pid}.${Date.now()}.tmp`;
    await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
    await fs.rename(temporary, file);
  }
}

export { awarenessId as awarenessIdForProject };
