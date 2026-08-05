import { safeObject, safeString } from '../operations/operation_types.js';

const TOOL_NAMES = new Set([
  'get_project_activity', 'list_project_activity', 'get_current_focus', 'list_stale_projects',
  'list_project_bottlenecks', 'refresh_project_evidence', 'explain_project_state', 'compare_project_activity',
]);

export function getMarcusProjectActivityToolDefinitions() {
  return [
    {
      type: 'function', function: {
        name: 'get_project_activity', description: 'Get deterministic observed activity, state, confidence, evidence counts, and risks for one registered project.',
        parameters: { type: 'object', properties: { projectRegistryId: { type: 'string' } }, required: ['projectRegistryId'] },
      },
    },
    {
      type: 'function', function: {
        name: 'list_project_activity', description: 'List evidence-derived activity snapshots for registered projects. Airtable is only a supporting signal.',
        parameters: { type: 'object', properties: { state: { type: 'string' }, limit: { type: 'number' } } },
      },
    },
    {
      type: 'function', function: {
        name: 'get_current_focus', description: 'Get the deterministic current-focus project, prior focus, focus-shift time, score, and supporting evidence.',
        parameters: { type: 'object', properties: {} },
      },
    },
    {
      type: 'function', function: {
        name: 'list_stale_projects', description: 'List stale, dormant, and abandoned-candidate projects with evidence-backed age thresholds.',
        parameters: { type: 'object', properties: {} },
      },
    },
    {
      type: 'function', function: {
        name: 'list_project_bottlenecks', description: 'List deterministic project bottlenecks and the exact evidence and thresholds behind each conclusion.',
        parameters: { type: 'object', properties: { code: { type: 'string' } } },
      },
    },
    {
      type: 'function', function: {
        name: 'refresh_project_evidence', description: 'Refresh trusted GitHub, operation, Codex, Airtable, Render, and Cloudflare evidence, then recalculate activity.',
        parameters: { type: 'object', properties: { force: { type: 'boolean' }, sources: { type: 'array', items: { type: 'string' } } } },
      },
    },
    {
      type: 'function', function: {
        name: 'explain_project_state', description: 'Explain a calculated project state using recorded evidence, source weights, decay, missing signals, and thresholds.',
        parameters: { type: 'object', properties: { projectRegistryId: { type: 'string' } }, required: ['projectRegistryId'] },
      },
    },
    {
      type: 'function', function: {
        name: 'compare_project_activity', description: 'Compare deterministic activity snapshots and supporting signals for two or more registered projects.',
        parameters: { type: 'object', properties: { projectRegistryIds: { type: 'array', items: { type: 'string' }, minItems: 2, maxItems: 10 } }, required: ['projectRegistryIds'] },
      },
    },
  ];
}
export function isMarcusProjectActivityTool(name) {
  return TOOL_NAMES.has(name);
}

export async function executeMarcusProjectActivityTool({ name, args, service, businessKey }) {
  const input = safeObject(args);
  if (name === 'get_project_activity' || name === 'explain_project_state') {
    const activity = await service.getProjectActivity(businessKey, safeString(input.projectRegistryId, 160));
    return { ok: true, activity };
  }
  const analysis = name === 'refresh_project_evidence'
    ? (await service.refresh(businessKey, { force: input.force === true, sources: Array.isArray(input.sources) ? input.sources : null })).activity
    : await service.getActivity(businessKey);
  if (name === 'list_project_activity') {
    let snapshots = Array.isArray(analysis.snapshots) ? analysis.snapshots : [];
    const state = safeString(input.state, 100).toLowerCase();
    if (state) snapshots = snapshots.filter((item) => item.state === state);
    return { ok: true, projects: snapshots.slice(0, Math.max(1, Math.min(100, Number(input.limit) || 50))) };
  }
  if (name === 'get_current_focus') return { ok: true, currentFocus: analysis.currentFocus };
  if (name === 'list_stale_projects') return { ok: true, projects: analysis.stale || [], thresholds: analysis.rules };
  if (name === 'list_project_bottlenecks') {
    const code = safeString(input.code, 100).toLowerCase();
    const projects = (analysis.bottlenecks || []).map((item) => code ? { ...item, risks: item.risks.filter((risk) => risk.code === code) } : item)
      .filter((item) => item.risks.length);
    return { ok: true, projects, thresholds: analysis.rules };
  }
  if (name === 'refresh_project_evidence') return { ok: true, activity: analysis };
  if (name === 'compare_project_activity') {
    const ids = new Set((Array.isArray(input.projectRegistryIds) ? input.projectRegistryIds : []).map((item) => safeString(item, 160)).filter(Boolean));
    if (ids.size < 2) return { ok: false, error: 'At least two projectRegistryIds are required.' };
    return { ok: true, projects: (analysis.snapshots || []).filter((item) => ids.has(item.projectRegistryId)) };
  }
  return { ok: false, error: `Unknown project activity tool: ${name}` };
}
