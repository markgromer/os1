import { safeBusinessKey, safeObject, safeString } from '../operations/operation_types.js';

export const PROJECT_RESOLUTION_THRESHOLDS = Object.freeze({ high: 75, medium: 45 });

function normalizeText(value) {
  return safeString(value, 20_000)
    .toLowerCase()
    .replace(/https?:\/\//g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function tokens(value) {
  return new Set(normalizeText(value).split(' ').filter((token) => token.length >= 2));
}

function includesPhrase(haystack, needle) {
  const source = ` ${normalizeText(haystack)} `;
  const target = normalizeText(needle);
  return Boolean(target) && source.includes(` ${target} `);
}

function tokenOverlap(a, b) {
  const left = tokens(a);
  const right = tokens(b);
  if (!left.size || !right.size) return 0;
  let shared = 0;
  for (const token of left) if (right.has(token)) shared += 1;
  return shared / Math.max(left.size, right.size);
}

function domainFromUrl(value) {
  try { return new URL(value).hostname.toLowerCase().replace(/^www\./, ''); } catch { return ''; }
}

function scoreRecord({ request, record, legacyProject, context }) {
  const reasons = [];
  let score = 0;
  const requestNormalized = normalizeText(request);
  const explicitRegistryId = safeString(context.registryId, 160);
  const explicitProjectId = safeString(context.projectId, 160);
  if (explicitRegistryId && explicitRegistryId === record.id) {
    score = 100;
    reasons.push('explicit registry id');
  }
  if (explicitProjectId && (explicitProjectId === record.projectId || explicitProjectId === legacyProject?.id)) {
    score = Math.max(score, 100);
    reasons.push('explicit project id');
  }

  const names = [record.canonicalName, ...(Array.isArray(record.aliases) ? record.aliases : [])].filter(Boolean);
  for (const [index, name] of names.entries()) {
    if (normalizeText(request) === normalizeText(name)) {
      score = Math.max(score, index === 0 ? 94 : 91);
      reasons.push(index === 0 ? 'exact canonical name' : `exact alias: ${name}`);
    } else if (includesPhrase(request, name)) {
      score = Math.max(score, index === 0 ? 84 : 81);
      reasons.push(index === 0 ? 'canonical name mentioned' : `alias mentioned: ${name}`);
    } else {
      const overlap = tokenOverlap(request, name);
      if (overlap >= 0.66) {
        score += Math.round(overlap * (index === 0 ? 32 : 26));
        reasons.push(`name token overlap ${Math.round(overlap * 100)}%`);
      }
    }
  }

  const repoNames = [record.repo?.name, record.repo?.fullName].filter(Boolean);
  for (const repoName of repoNames) {
    if (includesPhrase(request, repoName)) {
      score += 58;
      reasons.push(`repository mentioned: ${repoName}`);
      break;
    }
  }

  const domains = [record.deployments?.productionUrl, record.deployments?.previewUrl]
    .map(domainFromUrl).filter(Boolean);
  for (const domain of domains) {
    if (request.toLowerCase().includes(domain) || includesPhrase(request, domain.replace(/\./g, ' '))) {
      score += 52;
      reasons.push(`deployment domain mentioned: ${domain}`);
      break;
    }
  }

  if (safeString(context.currentProjectId, 160)
    && [record.id, record.projectId, legacyProject?.id].includes(context.currentProjectId)) {
    score += 22;
    reasons.push('current project context');
  }

  const desktop = safeObject(context.desktop);
  const desktopWorkspace = safeObject(desktop.workspace);
  const desktopText = [
    desktop.projectName,
    desktop.workspacePath,
    desktop.windowTitle,
    desktop.activeFile,
    desktopWorkspace.folderName,
    desktopWorkspace.workspacePath,
    desktopWorkspace.activeFile,
  ].filter(Boolean).join(' ');
  const workspace = safeString(record.localWorkspace?.path, 2_000);
  if (desktopText && (includesPhrase(desktopText, record.canonicalName) || (workspace && normalizeText(desktopText).includes(normalizeText(workspace))))) {
    score += 20;
    reasons.push('current desktop activity');
  }

  const notes = [legacyProject?.agentBrief, legacyProject?.description, record.description].filter(Boolean).join(' ');
  const notesOverlap = tokenOverlap(requestNormalized, notes);
  if (notesOverlap >= 0.15) {
    score += Math.min(16, Math.round(notesOverlap * 40));
    reasons.push('project notes overlap');
  }

  const recent = (Array.isArray(context.recentOperations) ? context.recentOperations : [])
    .find((operation) => operation.projectRegistryId === record.id || (record.projectId && operation.projectId === record.projectId));
  if (recent) {
    score += 10;
    reasons.push('recent operation context');
  }

  if (safeBusinessKey(context.activeBusinessKey) === record.businessKey) score += 3;
  return { score: Math.min(100, score), reasons };
}

export class ProjectResolver {
  constructor({ registry }) {
    this.registry = registry;
  }

  async resolve({ businessKey, request, legacyProjects = [], context = {} }) {
    const key = safeBusinessKey(businessKey);
    const records = (await this.registry.list(key))
      .filter((record) => safeString(record?.status, 100).toLowerCase() !== 'archived');
    const legacy = Array.isArray(legacyProjects) ? legacyProjects : [];
    const ctx = { ...safeObject(context), activeBusinessKey: key };
    const scored = records.map((record) => {
      const legacyProject = legacy.find((project) => (record.projectId && project?.id === record.projectId)
        || normalizeText(project?.name) === normalizeText(record.canonicalName));
      const result = scoreRecord({ request: safeString(request, 20_000), record, legacyProject, context: ctx });
      return { registryRecord: record, project: legacyProject || null, score: result.score, reasons: result.reasons };
    }).sort((a, b) => b.score - a.score || a.registryRecord.canonicalName.localeCompare(b.registryRecord.canonicalName));

    const best = scored[0] || null;
    const second = scored[1] || null;
    const margin = best ? best.score - (second?.score || 0) : 0;
    let confidence = 'low';
    if (best && best.score >= PROJECT_RESOLUTION_THRESHOLDS.high && (margin >= 8 || best.score >= 94)) confidence = 'high';
    else if (best && best.score >= PROJECT_RESOLUTION_THRESHOLDS.medium) confidence = 'medium';
    const resolved = confidence !== 'low';
    const reason = best
      ? `${best.registryRecord.canonicalName} scored ${best.score}; ${best.reasons.join(', ') || 'weak contextual match'}${second ? `; next alternative ${second.registryRecord.canonicalName} scored ${second.score}` : ''}.`
      : 'No project registry records exist for this business.';
    return {
      resolved,
      confidence,
      score: best?.score || 0,
      project: resolved ? best.project : null,
      registryRecord: resolved ? best.registryRecord : null,
      alternatives: scored.slice(resolved ? 1 : 0, 5).map((item) => ({
        project: item.project,
        registryRecord: item.registryRecord,
        score: item.score,
        reason: item.reasons.join(', '),
      })),
      reason,
      thresholds: PROJECT_RESOLUTION_THRESHOLDS,
    };
  }
}

export { normalizeText as normalizeProjectResolutionText, scoreRecord as scoreProjectRegistryRecord };
