import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import {
  nowIso,
  redactSecrets,
  safeBusinessKey,
  safeInteger,
  safeObject,
  safeString,
} from '../operations/operation_types.js';

const STORE_VERSION = 1;
const MAX_ITEMS = 500;
const MEMORY_KINDS = new Set(['mission', 'standing_instruction', 'preference', 'decision', 'fact']);
const MEMORY_STATUSES = new Set(['active', 'superseded', 'archived']);

export const DEFAULT_MISSION_MEMORIES = Object.freeze([
  Object.freeze({
    seedKey: 'marcus-core-mission-v1',
    kind: 'mission',
    title: 'Marcus core mission',
    priority: 5,
    content: "Act as Mark's durable, trusted operator: preserve mission and project context; deeply audit real systems; prepare and launch strong Codex work; use GitHub, Cloudflare, OpenAI, messaging, and Obsidian; protect credentials; require explicit approval for external communication and production mutations; independently verify outcomes; provide reliable mobile and voice access; and never claim work is complete without evidence.",
  }),
  Object.freeze({
    seedKey: 'marcus-assistance-standard-v1',
    kind: 'standing_instruction',
    title: 'Trusted operator standard',
    priority: 5,
    content: "Reduce how often Mark must repeat context. Investigate before answering, distinguish what is known from what is inferred, take approved work through execution and verification, and remain direct about blockers or missing evidence.",
  }),
  Object.freeze({
    seedKey: 'marcus-voice-preference-v1',
    kind: 'preference',
    title: 'Voice implementation preference',
    priority: 4,
    content: 'Use the best maintained prebuilt voice interface that fits Marcus instead of spending substantial time iterating on a custom voice bot.',
  }),
  Object.freeze({
    seedKey: 'marcus-relationship-voice-v1',
    kind: 'preference',
    title: 'Lived-in Marcus relationship',
    priority: 5,
    content: 'Marcus and Mark should sound like longtime teammates: shared shorthand, earned riffing and pointed jabs, mutual ability to take a joke, honest pushback, and fierce protection of Mark and private context.',
  }),
  Object.freeze({
    seedKey: 'marcus-locked-voice-authority-v1',
    kind: 'decision',
    title: '[Locked] Voice authority remains narrow',
    priority: 5,
    content: '[LOCKED] Realtime voice may converse and call the durable Marcus operator, but it does not receive direct GitHub, Cloudflare, deployment, messaging, billing, or production mutation authority.',
  }),
  Object.freeze({
    seedKey: 'marcus-locked-external-approval-v1',
    kind: 'decision',
    title: '[Locked] External actions require approval',
    priority: 5,
    content: '[LOCKED] External messages, publishing, deployments, DNS changes, merges, billing, and other consequential actions require the existing explicit approval path.',
  }),
]);

function normalizeText(value, maxChars) {
  return redactSecrets(safeString(value, maxChars), maxChars).replace(/\s+/g, ' ').trim();
}

function containsSecret(value) {
  const raw = safeString(value, 8_000);
  if (!raw) return false;
  return redactSecrets(raw, 8_000) !== raw
    || /-----BEGIN [^-]*(?:PRIVATE KEY|CERTIFICATE)-----/i.test(raw);
}

function normalizeKind(value) {
  const kind = safeString(value, 80).toLowerCase();
  return MEMORY_KINDS.has(kind) ? kind : 'fact';
}

function normalizeStatus(value) {
  const status = safeString(value, 80).toLowerCase();
  return MEMORY_STATUSES.has(status) ? status : 'active';
}

function memoryId(value = '') {
  const candidate = safeString(value, 120);
  return /^mem_[A-Za-z0-9_-]{8,}$/.test(candidate)
    ? candidate
    : `mem_${crypto.randomBytes(10).toString('base64url')}`;
}

function normalizeMemory(input, businessKey, { rejectSecrets = false, defaults = {} } = {}) {
  const raw = safeObject(input);
  const requestedKind = safeString(raw.kind || defaults.kind, 80).toLowerCase();
  const requestedStatus = safeString(raw.status || defaults.status, 80).toLowerCase();
  if (rejectSecrets && requestedKind && !MEMORY_KINDS.has(requestedKind)) {
    throw Object.assign(new Error(`Unsupported mission memory kind: ${requestedKind}.`), { code: 'MEMORY_KIND_INVALID' });
  }
  if (rejectSecrets && requestedStatus && !MEMORY_STATUSES.has(requestedStatus)) {
    throw Object.assign(new Error(`Unsupported mission memory status: ${requestedStatus}.`), { code: 'MEMORY_STATUS_INVALID' });
  }
  if (rejectSecrets && (containsSecret(raw.title) || containsSecret(raw.content))) {
    throw Object.assign(new Error('Mission memory cannot store credentials or secret-like values.'), { code: 'MEMORY_SECRET_REJECTED' });
  }
  const timestamp = safeString(raw.createdAt || defaults.createdAt, 64) || nowIso();
  const content = normalizeText(raw.content ?? defaults.content, 4_000);
  if (!content) throw Object.assign(new Error('Mission memory content is required.'), { code: 'MEMORY_CONTENT_REQUIRED' });
  const title = normalizeText(raw.title ?? defaults.title, 240) || content.slice(0, 120);
  return {
    id: memoryId(raw.id || defaults.id),
    businessKey: safeBusinessKey(businessKey),
    kind: normalizeKind(raw.kind || defaults.kind),
    status: normalizeStatus(raw.status || defaults.status),
    scope: raw.projectId ? 'project' : 'global',
    projectId: safeString(raw.projectId || defaults.projectId, 160),
    revision: safeInteger(raw.revision ?? defaults.revision, 1, 1),
    supersedesId: safeString(raw.supersedesId, 160),
    supersededBy: safeString(raw.supersededBy, 160),
    sourceRefs: (Array.isArray(raw.sourceRefs) ? raw.sourceRefs : []).slice(0, 20).map((entry) => normalizeText(entry, 500)),
    confidence: Math.max(0, Math.min(1, Number.isFinite(raw.confidence) ? raw.confidence : 1)),
    reviewAfter: Number.isFinite(Date.parse(raw.reviewAfter)) ? new Date(raw.reviewAfter).toISOString() : '',
    title,
    content,
    priority: safeInteger(raw.priority ?? defaults.priority, 3, 1, 5),
    seedKey: safeString(raw.seedKey || defaults.seedKey, 160),
    source: safeString(raw.source || defaults.source, 120) || 'authenticated_operator',
    createdBy: safeString(raw.createdBy || defaults.createdBy, 120) || 'mark',
    createdAt: timestamp,
    updatedAt: safeString(raw.updatedAt || defaults.updatedAt, 64) || timestamp,
    lastConfirmedAt: safeString(raw.lastConfirmedAt || defaults.lastConfirmedAt, 64) || timestamp,
  };
}

function emptyDocument(businessKey) {
  return {
    version: STORE_VERSION,
    businessKey: safeBusinessKey(businessKey),
    revision: 1,
    updatedAt: new Date(0).toISOString(),
    memories: [],
  };
}

function normalizeDocument(input, businessKey) {
  const raw = safeObject(input);
  const key = safeBusinessKey(businessKey || raw.businessKey);
  const memories = [];
  const ids = new Set();
  for (const item of (Array.isArray(raw.memories) ? raw.memories : []).slice(-MAX_ITEMS)) {
    try {
      const normalized = normalizeMemory(item, key);
      if (ids.has(normalized.id)) continue;
      ids.add(normalized.id);
      memories.push(normalized);
    } catch {
      // Invalid historical entries are omitted while preserving the rest of the store.
    }
  }
  return {
    version: STORE_VERSION,
    businessKey: key,
    revision: safeInteger(raw.revision, 1, 1),
    updatedAt: safeString(raw.updatedAt, 64) || new Date(0).toISOString(),
    memories,
  };
}

function searchTokens(value) {
  return [...new Set(String(value || '').toLowerCase().match(/[a-z0-9]{3,}/g) || [])].slice(0, 40);
}

function memorySearchScore(memory, query) {
  const tokens = searchTokens(query);
  const text = `${memory.title} ${memory.content}`.toLowerCase();
  const matches = tokens.filter((token) => text.includes(token)).length;
  const kindWeight = memory.kind === 'mission' ? 80 : memory.kind === 'standing_instruction' ? 60 : 20;
  return (memory.priority * 100) + kindWeight + (matches * 50);
}

export function formatMissionMemoryForPrompt(memories, { maxChars = 6_000 } = {}) {
  const lines = (Array.isArray(memories) ? memories : [])
    .filter((memory) => memory?.status === 'active')
    .map((memory) => `- [${memory.kind}; priority ${memory.priority}] ${memory.title}: ${memory.content}`);
  return lines.join('\n').slice(0, Math.max(500, Math.min(20_000, Number(maxChars) || 6_000)));
}

export class MissionMemoryStore {
  constructor({ dataDir } = {}) {
    if (!dataDir) throw new Error('MissionMemoryStore requires dataDir.');
    this.dataDir = path.resolve(String(dataDir));
    this.writeQueues = new Map();
  }

  fileForBusiness(businessKey) {
    return path.join(this.dataDir, 'businesses', safeBusinessKey(businessKey), 'marcus-mission-memory.json');
  }

  async readDocument(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = this.fileForBusiness(key);
    try {
      return normalizeDocument(JSON.parse(await fs.readFile(file, 'utf8')), key);
    } catch (primaryError) {
      if (primaryError?.code === 'ENOENT') return emptyDocument(key);
      try {
        const recovered = normalizeDocument(JSON.parse(await fs.readFile(`${file}.bak`, 'utf8')), key);
        await fs.rename(file, `${file}.corrupt-${Date.now()}`).catch(() => {});
        await this.atomicWrite(file, recovered, { createBackup: false });
        return recovered;
      } catch {
        throw Object.assign(new Error(`Mission memory store is corrupt for business ${key}; the original file was preserved.`), {
          code: 'CORRUPT_MISSION_MEMORY_STORE',
          cause: primaryError,
        });
      }
    }
  }

  async ensureDefaults(businessKey) {
    const key = safeBusinessKey(businessKey);
    const current = await this.readDocument(key);
    const existingSeedKeys = new Set(current.memories.map((item) => item.seedKey).filter(Boolean));
    const missing = DEFAULT_MISSION_MEMORIES.filter((item) => !existingSeedKeys.has(item.seedKey));
    if (!missing.length) return current;
    await this.mutate(key, (document) => {
      const timestamp = nowIso();
      const seen = new Set(document.memories.map((item) => item.seedKey).filter(Boolean));
      for (const item of missing) {
        if (seen.has(item.seedKey)) continue;
        document.memories.push(normalizeMemory({
          ...item,
          source: 'system_seed',
          createdBy: 'mark_instruction',
          createdAt: timestamp,
          updatedAt: timestamp,
          lastConfirmedAt: timestamp,
        }, key));
        seen.add(item.seedKey);
      }
      return null;
    });
    return this.readDocument(key);
  }

  async list(businessKey, filters = {}) {
    const document = await this.ensureDefaults(businessKey);
    let memories = document.memories.slice();
    if (filters.status) memories = memories.filter((item) => item.status === normalizeStatus(filters.status));
    if (filters.projectId !== undefined) memories = memories.filter((item) => !item.projectId || item.projectId === filters.projectId);
    if (filters.kind && MEMORY_KINDS.has(String(filters.kind).toLowerCase())) {
      memories = memories.filter((item) => item.kind === String(filters.kind).toLowerCase());
    }
    if (filters.query) {
      const tokens = searchTokens(filters.query);
      if (tokens.length) memories = memories.filter((item) => tokens.some((token) => `${item.title} ${item.content}`.toLowerCase().includes(token)));
    }
    memories.sort((a, b) => b.priority - a.priority || b.updatedAt.localeCompare(a.updatedAt) || a.id.localeCompare(b.id));
    return {
      version: document.version,
      businessKey: document.businessKey,
      revision: document.revision,
      updatedAt: document.updatedAt,
      memories: memories.slice(0, safeInteger(filters.limit, 100, 1, MAX_ITEMS)),
    };
  }

  async relevant(businessKey, query = '', { limit = 12, projectId = '' } = {}) {
    const { memories } = await this.list(businessKey, { status: 'active', limit: MAX_ITEMS, projectId });
    return memories
      .map((memory) => ({ memory, score: memorySearchScore(memory, query) }))
      .sort((a, b) => b.score - a.score || b.memory.updatedAt.localeCompare(a.memory.updatedAt))
      .slice(0, safeInteger(limit, 12, 1, 30))
      .map(({ memory }) => memory);
  }

  async add(businessKey, input = {}, options = {}) {
    const key = safeBusinessKey(businessKey);
    const candidate = normalizeMemory({
      ...safeObject(input),
      source: options.source || input.source,
      createdBy: options.actor || input.createdBy,
    }, key, { rejectSecrets: true });
    return this.mutate(key, (document) => {
      const contentKey = candidate.content.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
      const duplicate = document.memories.find((item) => item.status === 'active'
        && item.projectId === candidate.projectId
        && item.kind === candidate.kind
        && item.content.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim() === contentKey);
      if (duplicate) {
        duplicate.lastConfirmedAt = nowIso();
        duplicate.updatedAt = duplicate.lastConfirmedAt;
        duplicate.priority = Math.max(duplicate.priority, candidate.priority);
        return { memory: structuredClone(duplicate), created: false };
      }
      document.memories.push(candidate);
      if (document.memories.length > MAX_ITEMS) {
        const removable = document.memories.findIndex((item) => item.status !== 'active' && !item.seedKey && item.kind !== 'decision');
        if (removable < 0) throw Object.assign(new Error('Memory capacity reached; decisions and active records cannot be silently discarded.'), { code: 'MEMORY_CAPACITY' });
        document.memories.splice(removable, 1);
      }
      return { memory: structuredClone(candidate), created: true };
    });
  }

  async update(businessKey, memoryIdValue, patch = {}, options = {}) {
    const key = safeBusinessKey(businessKey);
    const id = safeString(memoryIdValue, 120);
    return this.mutate(key, (document) => {
      const index = document.memories.findIndex((item) => item.id === id);
      if (index < 0) throw Object.assign(new Error('Mission memory not found.'), { code: 'MISSION_MEMORY_NOT_FOUND' });
      const current = document.memories[index];
      const rawPatch = safeObject(patch);
      if (current.kind === 'decision' && ['content', 'title', 'kind', 'status'].some((field) => Object.hasOwn(rawPatch, field))) {
        throw Object.assign(new Error('Decisions require explicit supersession; existing decision history is immutable.'), { code: 'DECISION_REQUIRES_SUPERSESSION' });
      }
      const candidate = normalizeMemory({
        ...current,
        ...(Object.hasOwn(rawPatch, 'title') ? { title: rawPatch.title } : {}),
        ...(Object.hasOwn(rawPatch, 'content') ? { content: rawPatch.content } : {}),
        ...(Object.hasOwn(rawPatch, 'kind') ? { kind: rawPatch.kind } : {}),
        ...(Object.hasOwn(rawPatch, 'status') ? { status: rawPatch.status } : {}),
        ...(Object.hasOwn(rawPatch, 'priority') ? { priority: rawPatch.priority } : {}),
        id: current.id,
        revision: current.revision + 1,
        seedKey: current.seedKey,
        createdAt: current.createdAt,
        createdBy: current.createdBy,
        source: safeString(options.source, 120) || current.source,
        updatedAt: nowIso(),
        lastConfirmedAt: current.lastConfirmedAt,
      }, key, { rejectSecrets: true });
      document.memories[index] = candidate;
      return structuredClone(candidate);
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
    if (createBackup) await fs.copyFile(file, `${file}.bak`).catch(() => {});
    await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    const attempts = process.platform === 'win32' ? 6 : 1;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        await fs.rename(temporary, file);
        return;
      } catch (error) {
        const transient = ['EACCES', 'EBUSY', 'EPERM'].includes(error?.code);
        if (attempt < attempts && transient) {
          await new Promise((resolve) => setTimeout(resolve, 20 * attempt));
          continue;
        }
        await fs.unlink(temporary).catch(() => {});
        throw error;
      }
    }
  }

  async supersedeDecision(businessKey, id, replacement, { actor = 'mark', expectedRevision } = {}) {
    return this.mutate(businessKey, (document) => {
      const current = document.memories.find((item) => item.id === id);
      if (!current || current.kind !== 'decision') throw Object.assign(new Error('Decision not found.'), { code: 'DECISION_NOT_FOUND' });
      if (current.status !== 'active' || current.revision !== expectedRevision) throw Object.assign(new Error('Decision revision changed; reload before superseding.'), { code: 'REVISION_MISMATCH' });
      if (!Array.isArray(replacement.sourceRefs) || !replacement.sourceRefs.length) throw Object.assign(new Error('Decision supersession requires source references.'), { code: 'DECISION_SOURCE_REQUIRED' });
      if (document.memories.length >= MAX_ITEMS) throw Object.assign(new Error('Decision ledger capacity reached.'), { code: 'MEMORY_CAPACITY' });
      const next = normalizeMemory({ ...replacement, id: '', kind: 'decision', status: 'active', projectId: current.projectId,
        supersedesId: current.id, createdBy: actor, source: 'authenticated_decision', revision: 1 }, businessKey, { rejectSecrets: true });
      current.status = 'superseded'; current.supersededBy = next.id; current.revision++; current.updatedAt = nowIso();
      document.memories.push(next);
      return { previous: structuredClone(current), decision: structuredClone(next) };
    });
  }
}

export {
  emptyDocument as createEmptyMissionMemoryDocument,
  normalizeDocument as normalizeMissionMemoryDocument,
};
