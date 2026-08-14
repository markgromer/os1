import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { makeOperationId, nowIso, safeBusinessKey, safeIso, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';
import { validateAllowedWorkspaceRoots, validateTrustedWorkspace } from './workspace_trust.js';

function safeUrl(value) {
  const text = safeString(value, 2_000);
  try {
    const parsed = new URL(text);
    if (!['http:', 'https:'].includes(parsed.protocol) || parsed.username || parsed.password) return '';
    return parsed.toString();
  } catch {
    return '';
  }
}

function stringArray(value, limit = 50, max = 500) {
  const seen = new Set();
  const output = [];
  for (const item of Array.isArray(value) ? value : []) {
    const text = safeString(item, max);
    if (!text || seen.has(text.toLowerCase())) continue;
    seen.add(text.toLowerCase());
    output.push(text);
    if (output.length >= limit) break;
  }
  return output;
}

export function repositoryFromUrl(repoUrl) {
  const url = safeString(repoUrl, 2_000);
  const match = url.match(/github\.com[/:]([^/\s]+)\/([^/#\s]+?)(?:\.git)?$/i);
  if (!match) return { provider: '', owner: '', name: '', fullName: '', url: safeUrl(url), defaultBranch: '', workingBranchPattern: '' };
  const owner = match[1];
  const name = match[2].replace(/\.git$/i, '');
  return {
    provider: 'github',
    owner,
    name,
    fullName: `${owner}/${name}`,
    url: safeUrl(url),
    defaultBranch: '',
    workingBranchPattern: 'codex/{operationId}',
  };
}

function normalizeRepository(value = {}) {
  const raw = safeObject(value);
  const fromUrl = repositoryFromUrl(raw.url || raw.repoUrl);
  const owner = safeString(raw.owner, 200) || fromUrl.owner;
  const name = safeString(raw.name, 300) || fromUrl.name;
  return {
    provider: safeString(raw.provider, 100).toLowerCase() || fromUrl.provider,
    owner,
    name,
    fullName: safeString(raw.fullName, 500) || (owner && name ? `${owner}/${name}` : fromUrl.fullName),
    url: safeUrl(raw.url || raw.repoUrl) || fromUrl.url,
    defaultBranch: safeString(raw.defaultBranch, 200) || 'main',
    workingBranchPattern: safeString(raw.workingBranchPattern, 300) || 'codex/{operationId}',
  };
}

function normalizeWorkspace(value = {}) {
  const raw = safeObject(value);
  const challenge = safeObject(raw.approvalChallenge);
  const proof = safeObject(raw.validationProof);
  const normalizedProof = {
    method: ['same_machine', 'desktop_agent_attestation'].includes(safeString(proof.method, 80)) ? safeString(proof.method, 80) : '',
    challengeId: safeString(proof.challengeId, 120),
    businessKey: safeBusinessKey(proof.businessKey, ''),
    projectRegistryId: safeString(proof.projectRegistryId, 160),
    desktopAgentId: safeString(proof.desktopAgentId, 200),
    registeredPath: safeString(proof.registeredPath, 2_000),
    canonicalPath: safeString(proof.canonicalPath, 2_000),
    validatedAt: safeIso(proof.validatedAt),
  };
  const proofComplete = Boolean(normalizedProof.method && normalizedProof.challengeId && normalizedProof.businessKey
    && normalizedProof.projectRegistryId && normalizedProof.desktopAgentId && normalizedProof.registeredPath
    && normalizedProof.canonicalPath && normalizedProof.validatedAt);
  const requestedTrust = safeString(raw.trustStatus, 40);
  return {
    path: safeString(raw.path || raw.workspacePath, 2_000),
    platform: safeString(raw.platform, 100),
    desktopAgentId: safeString(raw.desktopAgentId, 200),
    trustStatus: requestedTrust === 'approved' && proofComplete ? 'approved' : (requestedTrust === 'rejected' ? 'rejected' : 'pending'),
    trustSource: proofComplete ? safeString(raw.trustSource, 100) : '',
    approvedAt: proofComplete ? safeIso(raw.approvedAt) : '',
    canonicalPath: proofComplete ? normalizedProof.canonicalPath : '',
    operatorApproval: {
      approvedBy: safeString(raw.operatorApproval?.approvedBy, 120),
      approvedAt: safeIso(raw.operatorApproval?.approvedAt),
      message: safeString(raw.operatorApproval?.message, 2_000),
    },
    approvalChallenge: {
      id: safeString(challenge.id, 120),
      status: ['pending', 'validated', 'failed'].includes(safeString(challenge.status, 40)) ? safeString(challenge.status, 40) : '',
      businessKey: safeBusinessKey(challenge.businessKey, ''),
      projectRegistryId: safeString(challenge.projectRegistryId, 160),
      desktopAgentId: safeString(challenge.desktopAgentId, 200),
      registeredPath: safeString(challenge.registeredPath, 2_000),
      createdAt: safeIso(challenge.createdAt),
      completedAt: safeIso(challenge.completedAt),
      error: safeString(challenge.error, 2_000),
    },
    validationProof: normalizedProof,
  };
}

function normalizeDeployments(value = {}, legacy = {}) {
  const raw = safeObject(value);
  const source = safeObject(legacy);
  return {
    productionUrl: safeUrl(raw.productionUrl || source.productionUrl || source.liveUrl || source.websiteUrl),
    previewUrl: safeUrl(raw.previewUrl || source.previewUrl || source.stagingUrl),
    renderServiceId: safeString(raw.renderServiceId || source.renderServiceId, 300),
    renderServiceName: safeString(raw.renderServiceName || source.renderServiceName, 300),
    cloudflareProject: safeString(raw.cloudflareProject || source.cloudflareProject, 300),
    cloudflareAccountId: safeString(raw.cloudflareAccountId || source.cloudflareAccountId, 300),
    cloudflareZoneId: safeString(raw.cloudflareZoneId || source.cloudflareZoneId, 300),
    cloudflareZoneName: safeString(raw.cloudflareZoneName || source.cloudflareZoneName, 300),
  };
}

function normalizeReferences(value = {}) {
  const raw = safeObject(value);
  const output = {};
  for (const [key, item] of Object.entries(raw).slice(0, 50)) {
    const safeKey = safeString(key, 100);
    const url = safeUrl(item);
    if (safeKey && url) output[safeKey] = url;
  }
  return output;
}

function normalizeTextList(value, limit = 20, max = 1_000) {
  return stringArray(value, limit, max);
}

function normalizeObjective(value = {}) {
  const raw = safeObject(value);
  const desiredOutcome = safeString(raw.desiredOutcome || raw.outcome || raw.objective || raw.title, 2_000);
  const definitionOfDone = safeString(raw.definitionOfDone || raw.doneDefinition || raw.acceptanceCriteria, 4_000);
  if (!desiredOutcome && !definitionOfDone) return {};
  return {
    desiredOutcome,
    whyItMatters: safeString(raw.whyItMatters || raw.why || raw.businessReason, 2_000),
    definitionOfDone,
    successEvidence: normalizeTextList(raw.successEvidence || raw.evidence || raw.verification, 20, 1_000),
    owner: safeString(raw.owner, 300),
    cadence: safeString(raw.cadence || raw.expectedCadence || raw.targetCadence, 300),
    risks: normalizeTextList(raw.risks, 20, 1_000),
    dependencies: normalizeTextList(raw.dependencies, 20, 1_000),
    status: safeString(raw.status, 100) || 'active',
    confidence: Math.max(0, Math.min(1, Number(raw.confidence) || 0.5)),
    updatedAt: safeIso(raw.updatedAt),
  };
}

function normalizeProjectMemory(value = {}) {
  const raw = safeObject(value);
  return {
    facts: normalizeTextList(raw.facts, 50, 1_000),
    preferences: normalizeTextList(raw.preferences, 50, 1_000),
    constraints: normalizeTextList(raw.constraints, 50, 1_000),
    decisions: normalizeTextList(raw.decisions, 50, 1_000),
    rejectedApproaches: normalizeTextList(raw.rejectedApproaches, 50, 1_000),
    lessons: normalizeTextList(raw.lessons, 50, 1_000),
    openQuestions: normalizeTextList(raw.openQuestions, 50, 1_000),
    followUps: normalizeTextList(raw.followUps, 50, 1_000),
  };
}

function normalizeCommands(value = {}) {
  const raw = safeObject(value);
  const allowed = ['install', 'dev', 'build', 'test', 'lint', 'typecheck'];
  return Object.fromEntries(allowed.map((name) => [name, safeString(raw[name], 500)]).filter(([, command]) => command));
}

export function normalizeProjectRegistryRecord(input = {}, options = {}) {
  const raw = safeObject(input);
  const createdAt = safeIso(raw.createdAt) || nowIso();
  const businessKey = safeBusinessKey(options.businessKey || raw.businessKey);
  const canonicalName = safeString(raw.canonicalName || raw.name || raw.projectName, 300);
  if (!canonicalName) {
    const error = new Error('Project registry canonicalName is required.');
    error.code = 'INVALID_PROJECT_REGISTRY_RECORD';
    throw error;
  }
  const aliases = stringArray(raw.aliases, 50, 300).filter((alias) => alias.toLowerCase() !== canonicalName.toLowerCase());
  const recordId = safeString(raw.id, 160) || makeOperationId('registry');
  const localWorkspace = normalizeWorkspace(raw.localWorkspace || { path: raw.workspacePath });
  const proof = safeObject(localWorkspace.validationProof);
  if (localWorkspace.trustStatus === 'approved' && (
    proof.businessKey !== businessKey
    || proof.projectRegistryId !== recordId
    || proof.desktopAgentId !== localWorkspace.desktopAgentId
    || proof.registeredPath !== localWorkspace.path
    || proof.canonicalPath !== localWorkspace.canonicalPath
  )) {
    localWorkspace.trustStatus = 'pending';
    localWorkspace.trustSource = '';
    localWorkspace.approvedAt = '';
    localWorkspace.canonicalPath = '';
    localWorkspace.validationProof = {};
  }
  return {
    id: recordId,
    businessKey,
    projectId: safeString(raw.projectId, 160),
    canonicalName,
    aliases,
    description: safeString(raw.description, 8_000),
    businessArea: safeString(raw.businessArea || raw.productArea || raw.area, 300),
    currentObjective: normalizeObjective(raw.currentObjective || {
      desiredOutcome: raw.objective || raw.currentObjectiveText,
      whyItMatters: raw.whyItMatters,
      definitionOfDone: raw.definitionOfDone,
      successEvidence: raw.successEvidence,
      owner: raw.objectiveOwner || raw.owner,
      cadence: raw.expectedCadence || raw.cadence,
      risks: raw.risks,
      dependencies: raw.dependencies,
      status: raw.objectiveStatus,
      confidence: raw.objectiveConfidence,
    }),
    definitionOfDone: safeString(raw.definitionOfDone || raw.currentObjective?.definitionOfDone, 4_000),
    status: safeString(raw.status, 100) || 'active',
    owner: safeString(raw.owner, 300),
    teamMembers: stringArray(raw.teamMembers, 100, 300),
    repo: normalizeRepository(raw.repo || { url: raw.repoUrl }),
    localWorkspace,
    deployments: normalizeDeployments(raw.deployments, raw),
    services: sanitizeStructured(raw.services ?? {}, 20_000),
    communication: {
      slackChannels: stringArray(raw.communication?.slackChannels || raw.slackChannels, 50, 300),
      emailAliases: stringArray(raw.communication?.emailAliases || raw.emailAliases, 50, 300),
    },
    documentation: {
      primaryUrl: safeUrl(raw.documentation?.primaryUrl || raw.docsUrl || raw.driveFolderUrl),
      references: normalizeReferences(raw.documentation?.references || raw.documentationLinks),
      airtableUrl: safeUrl(raw.documentation?.airtableUrl || raw.airtableUrl),
    },
    stack: stringArray(raw.stack, 50, 200),
    commands: normalizeCommands(raw.commands || raw.scripts),
    permissions: sanitizeStructured(raw.permissions ?? {}, 10_000),
    durableMemory: normalizeProjectMemory(raw.durableMemory || raw.memory),
    archiveHistory: Array.isArray(raw.archiveHistory) ? raw.archiveHistory.slice(-50).map((item) => sanitizeStructured(item, 5_000)) : [],
    metadata: sanitizeStructured(raw.metadata ?? {}, 20_000),
    createdAt,
    updatedAt: safeIso(raw.updatedAt) || createdAt,
  };
}

export function createProjectRegistryRecord(input = {}, options = {}) {
  const raw = safeObject(input);
  const timestamp = nowIso();
  const localWorkspace = normalizeWorkspace(raw.localWorkspace || { path: raw.workspacePath });
  if (localWorkspace.path) {
    localWorkspace.trustStatus = 'pending';
    localWorkspace.trustSource = '';
    localWorkspace.approvedAt = '';
    localWorkspace.desktopAgentId = '';
    localWorkspace.canonicalPath = '';
  }
  return normalizeProjectRegistryRecord({
    ...raw, id: makeOperationId('registry'), businessKey: options.businessKey,
    localWorkspace, createdAt: timestamp, updatedAt: timestamp,
  }, options);
}

function emptyRegistry(businessKey) {
  return { version: 1, businessKey: safeBusinessKey(businessKey), revision: 1, updatedAt: new Date(0).toISOString(), projects: [] };
}

function normalizeRegistryDocument(input, businessKey) {
  const raw = safeObject(input);
  const key = safeBusinessKey(businessKey || raw.businessKey);
  return {
    version: 1,
    businessKey: key,
    revision: Math.max(1, Math.floor(Number(raw.revision) || 1)),
    updatedAt: safeIso(raw.updatedAt) || new Date(0).toISOString(),
    projects: (Array.isArray(raw.projects) ? raw.projects : []).map((record) => normalizeProjectRegistryRecord(record, { businessKey: key })),
  };
}

function isEmpty(value) {
  if (value === null || value === undefined || value === '') return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === 'object') return Object.values(value).every(isEmpty);
  return false;
}

function fillMissing(existing, incoming) {
  if (Array.isArray(existing)) return existing.length ? existing : incoming;
  if (!existing || typeof existing !== 'object') return isEmpty(existing) ? incoming : existing;
  const output = { ...existing };
  for (const [key, value] of Object.entries(incoming || {})) {
    if (isEmpty(output[key])) output[key] = value;
    else if (output[key] && typeof output[key] === 'object' && !Array.isArray(output[key]) && value && typeof value === 'object' && !Array.isArray(value)) {
      output[key] = fillMissing(output[key], value);
    }
  }
  return output;
}

export class ProjectRegistry {
  constructor({ dataDir, allowedWorkspaceRoots = [] }) {
    if (!dataDir) throw new Error('ProjectRegistry requires dataDir.');
    this.dataDir = path.resolve(dataDir);
    this.queues = new Map();
    this.allowedWorkspaceRoots = Array.isArray(allowedWorkspaceRoots) && allowedWorkspaceRoots.length
      ? validateAllowedWorkspaceRoots(allowedWorkspaceRoots)
      : [];
  }

  fileForBusiness(businessKey) {
    return path.join(this.dataDir, 'businesses', safeBusinessKey(businessKey), 'project-registry.json');
  }

  async discoverBusinessKeys() {
    let entries = [];
    try { entries = await fs.readdir(path.join(this.dataDir, 'businesses'), { withFileTypes: true }); } catch { return []; }
    const keys = [];
    for (const entry of entries) {
      const key = entry.isDirectory() ? safeBusinessKey(entry.name, '') : '';
      if (!key) continue;
      try { await fs.access(this.fileForBusiness(key)); keys.push(key); } catch { /* no registry */ }
    }
    return keys;
  }

  async ensure(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = this.fileForBusiness(key);
    await fs.mkdir(path.dirname(file), { recursive: true });
    try { await fs.access(file); } catch { await this.writeFile(file, emptyRegistry(key), false); }
    return file;
  }

  async read(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = await this.ensure(key);
    try {
      return normalizeRegistryDocument(JSON.parse(await fs.readFile(file, 'utf8')), key);
    } catch (error) {
      try {
        const backup = normalizeRegistryDocument(JSON.parse(await fs.readFile(`${file}.bak`, 'utf8')), key);
        await fs.rename(file, `${file}.corrupt-${Date.now()}`).catch(() => {});
        await this.writeFile(file, backup, false);
        return backup;
      } catch {
        const failure = new Error(`Project registry is corrupt for business ${key}; the original file was preserved.`);
        failure.code = 'CORRUPT_PROJECT_REGISTRY';
        failure.cause = error;
        throw failure;
      }
    }
  }

  async list(businessKey) {
    return (await this.read(businessKey)).projects;
  }

  async get(businessKey, id) {
    return (await this.list(businessKey)).find((record) => record.id === id) || null;
  }

  async create(businessKey, input) {
    return this.mutate(businessKey, (document) => {
      const record = createProjectRegistryRecord(input, { businessKey });
      if (document.projects.some((item) => item.id === record.id)) {
        const error = new Error('Project registry record already exists.');
        error.code = 'PROJECT_REGISTRY_EXISTS';
        throw error;
      }
      document.projects.push(record);
      return record;
    });
  }

  async update(businessKey, id, patch) {
    return this.mutate(businessKey, (document) => {
      const index = document.projects.findIndex((record) => record.id === id);
      if (index < 0) {
        const error = new Error('Project registry record not found.');
        error.code = 'PROJECT_REGISTRY_NOT_FOUND';
        throw error;
      }
      const current = document.projects[index];
      const rawPatch = safeObject(patch);
      const merged = { ...current, ...rawPatch };
      for (const key of ['repo', 'deployments', 'services', 'communication', 'documentation', 'commands', 'permissions', 'metadata']) {
        if (rawPatch[key] && typeof rawPatch[key] === 'object' && !Array.isArray(rawPatch[key])) merged[key] = { ...safeObject(current[key]), ...rawPatch[key] };
      }
      if (rawPatch.localWorkspace && typeof rawPatch.localWorkspace === 'object' && !Array.isArray(rawPatch.localWorkspace)) {
        merged.localWorkspace = { ...safeObject(current.localWorkspace) };
        if (Object.prototype.hasOwnProperty.call(rawPatch.localWorkspace, 'path')) merged.localWorkspace.path = safeString(rawPatch.localWorkspace.path, 2_000);
        if (Object.prototype.hasOwnProperty.call(rawPatch.localWorkspace, 'platform')) merged.localWorkspace.platform = safeString(rawPatch.localWorkspace.platform, 100);
      }
      const updated = normalizeProjectRegistryRecord({ ...merged, id: current.id, businessKey: current.businessKey, createdAt: current.createdAt, updatedAt: nowIso() }, { businessKey });
      if (safeObject(rawPatch.localWorkspace).path && safeObject(rawPatch.localWorkspace).path !== current.localWorkspace.path) {
        updated.localWorkspace = {
          ...updated.localWorkspace, trustStatus: 'pending', trustSource: '', approvedAt: '', desktopAgentId: '', canonicalPath: '',
          operatorApproval: {}, approvalChallenge: {}, validationProof: {},
        };
      }
      document.projects[index] = updated;
      return updated;
    });
  }

  async approveWorkspace(businessKey, id, input = {}) {
    const raw = safeObject(input);
    const desktopAgentId = safeString(raw.desktopAgentId, 200);
    const actor = safeString(raw.approvedBy || raw.actor, 120) || 'authenticated_operator';
    if (!desktopAgentId) throw Object.assign(new Error('desktopAgentId is required to request workspace approval.'), { code: 'WORKSPACE_APPROVAL_INVALID' });
    const snapshot = await this.get(businessKey, id);
    if (!snapshot) throw Object.assign(new Error('Project registry record not found.'), { code: 'PROJECT_REGISTRY_NOT_FOUND' });
    const registeredPath = safeString(snapshot.localWorkspace?.path, 2_000);
    if (!registeredPath) throw Object.assign(new Error('The project has no workspace path to approve.'), { code: 'WORKSPACE_APPROVAL_INVALID' });
    const suppliedPath = safeString(raw.path || raw.canonicalPath, 2_000);
    if (suppliedPath && suppliedPath !== registeredPath) {
      throw Object.assign(new Error('Workspace approval must validate the registered project path; a replacement browser/API path is not accepted.'), { code: 'WORKSPACE_REGISTRY_MISMATCH' });
    }
    const challengeId = makeOperationId('workspace');
    let canonicalPath = '';
    if (this.allowedWorkspaceRoots.length && raw.remoteValidation !== true) {
      canonicalPath = validateTrustedWorkspace({
        workspacePath: registeredPath, registeredPath, allowedRoots: this.allowedWorkspaceRoots,
      });
    }
    return this.mutate(businessKey, (document) => {
      const record = document.projects.find((item) => item.id === id);
      if (!record) throw Object.assign(new Error('Project registry record not found.'), { code: 'PROJECT_REGISTRY_NOT_FOUND' });
      if (record.localWorkspace.path !== registeredPath) throw Object.assign(new Error('The registered workspace changed before approval.'), { code: 'WORKSPACE_REGISTRY_MISMATCH' });
      const timestamp = nowIso();
      const challenge = {
        id: challengeId, status: canonicalPath ? 'validated' : 'pending', businessKey: safeBusinessKey(businessKey),
        projectRegistryId: id, desktopAgentId, registeredPath, createdAt: timestamp,
        completedAt: canonicalPath ? timestamp : '', error: '',
      };
      const proof = canonicalPath ? {
        method: 'same_machine', challengeId, businessKey: safeBusinessKey(businessKey), projectRegistryId: id,
        desktopAgentId, registeredPath, canonicalPath, validatedAt: timestamp,
      } : {};
      record.localWorkspace = {
        ...record.localWorkspace,
        desktopAgentId,
        trustStatus: canonicalPath ? 'approved' : 'pending',
        trustSource: canonicalPath ? 'operator_and_same_machine_validation' : '',
        approvedAt: canonicalPath ? timestamp : '',
        canonicalPath,
        operatorApproval: { approvedBy: actor, approvedAt: timestamp, message: safeString(raw.message, 2_000) },
        approvalChallenge: challenge,
        validationProof: proof,
      };
      record.updatedAt = timestamp;
      return record;
    });
  }

  async attestWorkspace(businessKey, id, input = {}) {
    const raw = safeObject(input);
    return this.mutate(businessKey, (document) => {
      const record = document.projects.find((item) => item.id === id);
      if (!record) throw Object.assign(new Error('Project registry record not found.'), { code: 'PROJECT_REGISTRY_NOT_FOUND' });
      const workspace = record.localWorkspace;
      const challenge = safeObject(workspace.approvalChallenge);
      const challengeId = safeString(raw.challengeId || raw.actionId || raw.id, 120);
      const desktopAgentId = safeString(raw.desktopAgentId, 200);
      const registeredPath = safeString(raw.registeredPath, 2_000);
      const canonicalPath = safeString(raw.canonicalPath, 2_000);
      const identityMatches = Boolean(challenge.id && challenge.id === challengeId
        && challenge.businessKey === safeBusinessKey(businessKey)
        && challenge.projectRegistryId === id
        && challenge.desktopAgentId === desktopAgentId
        && challenge.registeredPath === registeredPath
        && workspace.path === registeredPath
        && workspace.desktopAgentId === desktopAgentId
        && workspace.operatorApproval?.approvedAt);
      if (identityMatches && challenge.status === 'validated' && raw.ok === true
        && canonicalPath && workspace.canonicalPath === canonicalPath) return record;
      if (identityMatches && challenge.status === 'failed' && raw.ok !== true) return record;
      if (!identityMatches || challenge.status !== 'pending') {
        throw Object.assign(new Error('Workspace validation result does not match its pending approval challenge.'), { code: 'WORKSPACE_ATTESTATION_MISMATCH' });
      }
      const timestamp = nowIso();
      if (raw.ok !== true) {
        workspace.trustStatus = 'rejected'; workspace.trustSource = ''; workspace.approvedAt = ''; workspace.canonicalPath = '';
        workspace.validationProof = {};
        workspace.approvalChallenge = { ...challenge, status: 'failed', completedAt: timestamp, error: safeString(raw.error, 2_000) || 'Desktop workspace validation failed.' };
        record.updatedAt = timestamp;
        return record;
      }
      if (!canonicalPath) throw Object.assign(new Error('A successful workspace attestation requires the canonical path.'), { code: 'WORKSPACE_ATTESTATION_INVALID' });
      workspace.trustStatus = 'approved';
      workspace.trustSource = 'operator_and_desktop_agent_attestation';
      workspace.approvedAt = timestamp;
      workspace.canonicalPath = canonicalPath;
      workspace.approvalChallenge = { ...challenge, status: 'validated', completedAt: timestamp, error: '' };
      workspace.validationProof = {
        method: 'desktop_agent_attestation', challengeId, businessKey: safeBusinessKey(businessKey), projectRegistryId: id,
        desktopAgentId, registeredPath, canonicalPath, validatedAt: timestamp,
      };
      record.updatedAt = timestamp;
      return record;
    });
  }

  async synchronizeLegacyProjects(businessKey, legacyProjects = []) {
    const list = Array.isArray(legacyProjects) ? legacyProjects : [];
    return this.mutate(businessKey, (document) => {
      const created = [];
      const enriched = [];
      for (const legacy of list) {
        const raw = safeObject(legacy);
        const canonicalName = safeString(raw.name, 300);
        if (!canonicalName) continue;
        const candidate = normalizeProjectRegistryRecord({
          businessKey,
          projectId: raw.id,
          canonicalName,
          aliases: raw.aliases,
          description: raw.agentBrief || raw.description,
          status: raw.status,
          owner: raw.owner || raw.accountManagerName,
          teamMembers: raw.teamMembers,
          repoUrl: raw.repoUrl,
          workspacePath: raw.workspacePath,
          localWorkspace: raw.workspacePath ? {
            path: raw.workspacePath,
            desktopAgentId: safeString(raw.desktopAgentId, 200),
            trustStatus: 'pending',
            trustSource: '',
            approvedAt: '',
            canonicalPath: '',
          } : {},
          productionUrl: raw.productionUrl || raw.websiteUrl || raw.liveUrl,
          previewUrl: raw.previewUrl || raw.stagingUrl,
          renderServiceId: raw.renderServiceId,
          renderServiceName: raw.renderServiceName,
          cloudflareProject: raw.cloudflareProject,
          cloudflareAccountId: raw.cloudflareAccountId,
          cloudflareZoneId: raw.cloudflareZoneId,
          cloudflareZoneName: raw.cloudflareZoneName,
          services: raw.services,
          airtableUrl: raw.airtableUrl,
          docsUrl: raw.docsUrl || raw.driveFolderUrl,
          slackChannels: raw.slackChannels,
          stack: raw.stack,
          scripts: raw.scripts || raw.commands,
          permissions: raw.permissions,
          metadata: { ...safeObject(raw.metadata), migratedFromLegacyProject: true },
        }, { businessKey });
        const index = document.projects.findIndex((record) => (candidate.projectId && record.projectId === candidate.projectId)
          || record.canonicalName.toLowerCase() === candidate.canonicalName.toLowerCase());
        if (index < 0) {
          document.projects.push(candidate);
          created.push(candidate.id);
          continue;
        }
        const current = document.projects[index];
        const filled = fillMissing(current, candidate);
        const aliases = stringArray([...(current.aliases || []), ...(candidate.aliases || []), candidate.canonicalName], 50, 300)
          .filter((alias) => alias.toLowerCase() !== current.canonicalName.toLowerCase());
        const comparable = normalizeProjectRegistryRecord({ ...filled, aliases, id: current.id, canonicalName: current.canonicalName, businessKey: current.businessKey, createdAt: current.createdAt, updatedAt: current.updatedAt }, { businessKey });
        if (JSON.stringify(comparable) !== JSON.stringify(current)) {
          document.projects[index] = { ...comparable, updatedAt: nowIso() };
          enriched.push(current.id);
        }
      }
      if (!created.length && !enriched.length) document.__skipWrite = true;
      return { created, enriched, total: document.projects.length };
    });
  }

  async mutate(businessKey, mutator) {
    const key = safeBusinessKey(businessKey);
    const previous = this.queues.get(key) || Promise.resolve();
    const run = previous.catch(() => {}).then(async () => {
      const document = await this.read(key);
      const result = await mutator(document);
      if (document.__skipWrite === true) return result;
      document.revision += 1;
      document.updatedAt = nowIso();
      await this.writeFile(this.fileForBusiness(key), normalizeRegistryDocument(document, key));
      return result;
    });
    this.queues.set(key, run);
    try { return await run; } finally { if (this.queues.get(key) === run) this.queues.delete(key); }
  }

  async writeFile(file, value, backup = true) {
    await fs.mkdir(path.dirname(file), { recursive: true });
    if (backup) await fs.copyFile(file, `${file}.bak`).catch(() => {});
    const temporary = `${file}.tmp-${crypto.randomBytes(6).toString('hex')}`;
    await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    await fs.rename(temporary, file).catch(async (error) => {
      await fs.unlink(temporary).catch(() => {});
      throw error;
    });
  }
}
