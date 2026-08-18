import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

const STORE_VERSION = 1;
const ACTIVE_STATUSES = new Set(['active', 'recovering', 'waiting_for_approval']);

function clean(value, limit = 2_000) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, limit);
}

function normalizeMission(input = {}) {
  const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const now = new Date().toISOString();
  return {
    id: clean(raw.id, 120) || `browser_mission_${crypto.randomBytes(9).toString('hex')}`,
    businessKey: clean(raw.businessKey, 100) || 'personal',
    status: ['active', 'recovering', 'waiting_for_approval', 'completed', 'blocked', 'cancelled'].includes(raw.status)
      ? raw.status : 'active',
    platform: clean(raw.platform, 60),
    objective: clean(raw.objective, 2_000),
    currentInstruction: clean(raw.currentInstruction, 2_000),
    currentSkill: clean(raw.currentSkill, 120),
    currentStep: clean(raw.currentStep, 240),
    instructions: (Array.isArray(raw.instructions) ? raw.instructions : []).map((item) => clean(item, 2_000)).filter(Boolean).slice(-80),
    skillHistory: (Array.isArray(raw.skillHistory) ? raw.skillHistory : []).map((item) => ({
      skill: clean(item?.skill, 120), status: clean(item?.status, 60), at: clean(item?.at, 40),
    })).filter((item) => item.skill).slice(-120),
    evidence: (Array.isArray(raw.evidence) ? raw.evidence : []).map((item) => ({
      type: clean(item?.type, 100), summary: clean(item?.summary, 1_000), at: clean(item?.at, 40),
    })).filter((item) => item.type || item.summary).slice(-160),
    attempts: Math.max(0, Math.min(1_000, Number(raw.attempts) || 0)),
    error: clean(raw.error, 1_000),
    createdAt: clean(raw.createdAt, 40) || now,
    updatedAt: clean(raw.updatedAt, 40) || now,
    completedAt: clean(raw.completedAt, 40),
  };
}

export class BrowserMissionStore {
  constructor({ dataDir } = {}) {
    if (!dataDir) throw new Error('BrowserMissionStore requires dataDir.');
    this.file = path.join(path.resolve(String(dataDir)), 'marcus-browser-missions.json');
    this.writeQueue = Promise.resolve();
  }

  async list() {
    return this.withLock(async () => structuredClone((await this.readDocumentUnlocked()).missions));
  }

  async active(businessKey) {
    const key = clean(businessKey, 100) || 'personal';
    const missions = await this.list();
    return missions.filter((mission) => mission.businessKey === key && ACTIVE_STATUSES.has(mission.status))
      .sort((left, right) => String(right.updatedAt).localeCompare(String(left.updatedAt)))[0] || null;
  }

  async startOrResume({ businessKey, platform, instruction, skill } = {}) {
    const key = clean(businessKey, 100) || 'personal';
    const currentPlatform = clean(platform, 60);
    const currentInstruction = clean(instruction, 2_000);
    return this.withLock(async () => {
      const document = await this.readDocumentUnlocked();
      const existing = document.missions
        .filter((mission) => mission.businessKey === key && ACTIVE_STATUSES.has(mission.status)
          && (!currentPlatform || !mission.platform || mission.platform === currentPlatform))
        .sort((left, right) => String(right.updatedAt).localeCompare(String(left.updatedAt)))[0];
      const timestamp = new Date().toISOString();
      let mission;
      if (existing) {
        mission = normalizeMission({
          ...existing,
          status: existing.status === 'waiting_for_approval' ? existing.status : 'active',
          platform: currentPlatform || existing.platform,
          currentInstruction,
          currentSkill: clean(skill, 120) || existing.currentSkill,
          currentStep: clean(skill, 120) || existing.currentStep,
          instructions: [...existing.instructions, currentInstruction],
          attempts: existing.attempts + 1,
          error: '',
          updatedAt: timestamp,
        });
      } else {
        mission = normalizeMission({
          businessKey: key,
          platform: currentPlatform,
          objective: currentInstruction,
          currentInstruction,
          currentSkill: skill,
          currentStep: skill,
          instructions: [currentInstruction],
          attempts: 1,
          createdAt: timestamp,
          updatedAt: timestamp,
        });
      }
      const next = document.missions.filter((item) => item.id !== mission.id);
      next.push(mission);
      await this.atomicWrite({ ...document, revision: document.revision + 1, updatedAt: timestamp, missions: next.slice(-200) });
      return structuredClone(mission);
    });
  }

  async recordResult(id, { skill, ok, waitingForApproval = false, completed = false, evidence, error } = {}) {
    const missionId = clean(id, 120);
    return this.withLock(async () => {
      const document = await this.readDocumentUnlocked();
      const index = document.missions.findIndex((mission) => mission.id === missionId);
      if (index < 0) return null;
      const previous = document.missions[index];
      const timestamp = new Date().toISOString();
      const status = completed ? 'completed' : waitingForApproval ? 'waiting_for_approval' : ok ? 'active' : 'recovering';
      const mission = normalizeMission({
        ...previous,
        status,
        currentSkill: clean(skill, 120) || previous.currentSkill,
        currentStep: completed ? 'complete' : waitingForApproval ? 'waiting for Mark approval' : ok ? 'continue mission' : 'recover last skill',
        skillHistory: [...previous.skillHistory, { skill, status: ok ? 'verified' : 'failed', at: timestamp }],
        evidence: evidence ? [...previous.evidence, { ...evidence, at: timestamp }] : previous.evidence,
        error: ok ? '' : error,
        updatedAt: timestamp,
        completedAt: completed ? timestamp : '',
      });
      document.missions[index] = mission;
      await this.atomicWrite({ ...document, revision: document.revision + 1, updatedAt: timestamp });
      return structuredClone(mission);
    });
  }

  async withLock(work) {
    const run = this.writeQueue.catch(() => {}).then(work);
    this.writeQueue = run;
    try { return await run; } finally {
      if (this.writeQueue === run) this.writeQueue = Promise.resolve();
    }
  }

  normalizeDocument(input = {}) {
    const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
    return {
      version: STORE_VERSION,
      revision: Number.isSafeInteger(Number(raw.revision)) && Number(raw.revision) > 0 ? Number(raw.revision) : 1,
      updatedAt: clean(raw.updatedAt, 40) || new Date(0).toISOString(),
      missions: (Array.isArray(raw.missions) ? raw.missions : []).map(normalizeMission).slice(-200),
    };
  }

  async readDocumentUnlocked() {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    try {
      return this.normalizeDocument(JSON.parse(await fs.readFile(this.file, 'utf8')));
    } catch (primaryError) {
      if (primaryError?.code === 'ENOENT') {
        const document = this.normalizeDocument();
        await this.atomicWrite(document, { createBackup: false });
        return document;
      }
      try {
        const recovered = this.normalizeDocument(JSON.parse(await fs.readFile(`${this.file}.bak`, 'utf8')));
        await fs.rename(this.file, `${this.file}.corrupt-${Date.now()}`).catch(() => {});
        await this.atomicWrite(recovered, { createBackup: false });
        return recovered;
      } catch {
        const error = new Error('The durable MARCUS browser mission store is corrupt; the original file was preserved.');
        error.code = 'CORRUPT_BROWSER_MISSION_STORE';
        error.cause = primaryError;
        throw error;
      }
    }
  }

  async atomicWrite(value, { createBackup = true } = {}) {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    const temporary = `${this.file}.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    if (createBackup) await fs.copyFile(this.file, `${this.file}.bak`).catch(() => {});
    await fs.writeFile(temporary, `${JSON.stringify(this.normalizeDocument(value), null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    try { await fs.rename(temporary, this.file); } catch (error) {
      await fs.unlink(temporary).catch(() => {});
      throw error;
    }
  }
}
