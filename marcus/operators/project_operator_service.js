import { safeBusinessKey, safeObject, safeString } from '../operations/operation_types.js';

const CODEX_OPERATOR_RE = /\b(codex|audit|repo|repository|fix|build|implement|worker|cloudflare|deploy|mobile|broken|get .* working|start .* session)\b/i;

function preview(value, max = 240) {
  return safeString(value, max).replace(/\s+/g, ' ').trim();
}

function repoParts(repo = {}) {
  const fullName = safeString(repo.fullName, 300);
  if (fullName.includes('/')) return { fullName };
  const url = safeString(repo.url, 1_000);
  const match = url.match(/github\.com[:/]+([^/\s]+)\/([^/\s.git#?]+)/i);
  return match ? { fullName: `${match[1]}/${match[2]}` } : { fullName: '' };
}

function summarizeProject(project = {}) {
  const repo = repoParts(project.repo);
  return {
    id: safeString(project.id, 160),
    projectId: safeString(project.projectId, 160),
    name: safeString(project.canonicalName || project.name, 300),
    aliases: Array.isArray(project.aliases) ? project.aliases.slice(0, 12).map((item) => safeString(item, 120)).filter(Boolean) : [],
    repo: repo.fullName,
    workspacePath: safeString(project.localWorkspace?.path || project.localWorkspace?.canonicalPath, 2_000),
    workspaceTrust: safeString(project.localWorkspace?.trustStatus, 80) || 'unregistered',
    productionUrl: safeString(project.deployments?.productionUrl, 1_000),
    previewUrl: safeString(project.deployments?.previewUrl, 1_000),
    cloudflareProject: safeString(project.deployments?.cloudflareProject, 300),
    renderServiceId: safeString(project.deployments?.renderServiceId, 300),
    stack: Array.isArray(project.stack) ? project.stack.slice(0, 12).map((item) => safeString(item, 80)).filter(Boolean) : [],
    commands: safeObject(project.commands),
  };
}

function selectLegacyRows(store = {}, project = {}) {
  const name = safeString(project.canonicalName || project.name, 300).toLowerCase();
  const projectId = safeString(project.projectId || project.id, 160);
  const matchesProject = (item) => {
    const values = [item?.projectId, item?.project, item?.projectName, item?.name, item?.title]
      .map((value) => safeString(value, 300).toLowerCase()).filter(Boolean);
    return values.some((value) => value === projectId.toLowerCase() || (name && value.includes(name)));
  };
  return {
    projects: (Array.isArray(store.projects) ? store.projects : []).filter(matchesProject).slice(0, 5),
    tasks: (Array.isArray(store.tasks) ? store.tasks : []).filter(matchesProject).slice(0, 12),
    inboxItems: (Array.isArray(store.inboxItems) ? store.inboxItems : []).filter(matchesProject).slice(0, 8),
    notes: safeObject(store.projectNotes)[projectId] || safeObject(store.marcusNotes)[projectId] || '',
  };
}

function formatContextBrief({ request, project, resolution, legacyRows, evidence, activity, desktopContext, repoFiles }) {
  const summary = summarizeProject(project);
  const lines = [
    `Original request: ${preview(request, 1000)}`,
    `Resolved project: ${summary.name || 'unresolved'} (${resolution?.confidence || 'unknown'} confidence).`,
    summary.repo ? `GitHub repo: ${summary.repo}.` : 'GitHub repo: not registered.',
    summary.productionUrl ? `Production URL: ${summary.productionUrl}.` : '',
    summary.cloudflareProject ? `Cloudflare project: ${summary.cloudflareProject}.` : '',
    summary.workspacePath ? `Local workspace: ${summary.workspacePath} (${summary.workspaceTrust}).` : 'Local workspace: not registered.',
    summary.stack.length ? `Stack: ${summary.stack.join(', ')}.` : '',
    Object.keys(summary.commands).length ? `Commands: ${JSON.stringify(summary.commands).slice(0, 1000)}.` : '',
  ].filter(Boolean);

  const taskLines = (legacyRows.tasks || []).slice(0, 8).map((task) =>
    `- ${preview(task.title || task.name, 180)}${task.status ? ` (${task.status})` : ''}${task.dueDate ? ` due ${task.dueDate}` : ''}`);
  const inboxLines = (legacyRows.inboxItems || []).slice(0, 6).map((item) =>
    `- ${preview(item.contactName || item.fromName || item.sender || item.source || 'Message', 120)}: ${preview(item.subject || item.text || item.body || item.summary, 220)}`);
  const evidenceLines = (Array.isArray(evidence) ? evidence : []).slice(0, 10).map((item) =>
    `- [${preview(item.source, 40)}/${preview(item.type, 40)}] ${preview(item.summary || item.event, 220)}`);
  const repoFileLines = (Array.isArray(repoFiles) ? repoFiles : []).map((file) =>
    `- ${file.path}: ${preview(file.summary || file.content, 260)}`);
  const desktop = safeObject(desktopContext);
  const ws = safeObject(desktop.workspace);
  const desktopLines = [
    ws.workspacePath ? `Active desktop workspace: ${ws.folderName || ws.workspacePath}.` : '',
    ws.gitBranch ? `Active branch: ${ws.gitBranch}.` : '',
    Array.isArray(ws.gitStatus) && ws.gitStatus.length ? `Uncommitted files: ${ws.gitStatus.slice(0, 12).map((s) => `${s.status} ${s.file}`).join(', ')}.` : '',
  ].filter(Boolean);

  return [
    '# Marcus Project Execution Brief',
    '',
    '## Request And Resolution',
    ...lines.map((line) => `- ${line}`),
    '',
    '## Current Project Signals',
    taskLines.length ? taskLines.join('\n') : '- No matching open tasks were found in the legacy store.',
    '',
    '## Recent Communication',
    inboxLines.length ? inboxLines.join('\n') : '- No matching inbox items were found.',
    '',
    '## Project Evidence',
    evidenceLines.length ? evidenceLines.join('\n') : '- No project evidence has been collected yet.',
    '',
    '## Activity Snapshot',
    activity?.currentFocusProject ? `- Current focus: ${activity.currentFocusProject}.` : '- No calculated project activity snapshot was available.',
    activity?.status ? `- Activity status: ${activity.status}.` : '',
    activity?.reason ? `- Reason: ${preview(activity.reason, 300)}.` : '',
    '',
    '## Desktop Context',
    desktopLines.length ? desktopLines.map((line) => `- ${line}`).join('\n') : '- No live desktop workspace context was available.',
    '',
    '## Repository Files Sample',
    repoFileLines.length ? repoFileLines.join('\n') : '- Repository file sampling was unavailable or not configured.',
  ].filter((line) => line !== '').join('\n');
}

function composeCodexPrompt({ request, project, executionBrief }) {
  const summary = summarizeProject(project);
  return [
    '# Goal for Codex',
    '',
    '## Objective',
    preview(request, 1600) || 'Audit the resolved project and implement the requested improvement.',
    '',
    '## Project',
    `- Name: ${summary.name || 'Unknown'}`,
    `- Registry ID: ${summary.id || 'unregistered'}`,
    `- Repository: ${summary.repo || 'not registered'}`,
    `- Local workspace: ${summary.workspacePath || 'not registered'} (${summary.workspaceTrust})`,
    `- Production URL: ${summary.productionUrl || 'not registered'}`,
    `- Cloudflare project: ${summary.cloudflareProject || 'not registered'}`,
    '',
    '## Marcus Audit Brief',
    executionBrief.slice(0, 12_000),
    '',
    '## Instructions',
    '- Inspect the repository before changing code.',
    '- Make the smallest coherent change that satisfies the objective.',
    '- Preserve existing behavior and data outside the requested scope.',
    '- Do not deploy, publish, merge, change DNS, bill, text, email, or contact customers.',
    '- If a production-affecting action is needed, stop and report the exact approval needed.',
    '- Do not claim success without verification evidence.',
    '',
    '## Verification',
    '- Run the most relevant project checks from package scripts or documented commands.',
    '- For UI work, verify at desktop and mobile widths and report what was checked.',
    '- Return changed files, verification output, remaining risks, and any manual approval needed.',
  ].join('\n');
}

function replyForResult(result) {
  if (result.status === 'needs_project') {
    const choices = result.alternatives.map((item) => `- ${item.name || item.id}`).join('\n');
    return `I need one project clarified before I start Codex.\n${choices || 'No confident project match was found.'}`;
  }
  const handoff = result.codexPrompt ? 'I prepared the Codex prompt and saved it in the durable operation.' : 'I created the durable operation.';
  const waiting = result.operation?.status === 'blocked'
    ? 'It is waiting for a real Codex session/result; I am not pretending the handoff has executed.'
    : `Operation status is ${result.operation?.status || 'unknown'}.`;
  return [
    `I resolved this to ${result.project?.name || result.operation?.projectName || 'the project'} and audited the available context.`,
    `${handoff} ${waiting}`,
    `Operation: ${result.operation?.id || 'not created'}.`,
  ].join('\n');
}

export class ProjectOperatorService {
  constructor({
    operationsEngine,
    projectEvidenceService,
    getLegacyStore,
    getDesktopContext = async () => ({}),
    githubApi = null,
  } = {}) {
    if (!operationsEngine) throw new Error('ProjectOperatorService requires operationsEngine.');
    this.operationsEngine = operationsEngine;
    this.projectEvidenceService = projectEvidenceService;
    this.getLegacyStore = typeof getLegacyStore === 'function' ? getLegacyStore : async () => ({});
    this.getDesktopContext = typeof getDesktopContext === 'function' ? getDesktopContext : async () => ({});
    this.githubApi = typeof githubApi === 'function' ? githubApi : null;
  }

  shouldHandle(message) {
    return CODEX_OPERATOR_RE.test(safeString(message, 4_000));
  }

  async sampleRepoFiles(project) {
    if (!this.githubApi) return [];
    const { fullName } = repoParts(project?.repo);
    if (!fullName) return [];
    const [owner, repo] = fullName.split('/');
    const candidates = ['README.md', 'package.json', 'wrangler.jsonc', 'wrangler.toml'];
    const out = [];
    for (const filePath of candidates) {
      try {
        const data = await this.githubApi(`/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/contents/${filePath}`);
        const encoded = safeString(data?.content, 100_000).replace(/\s+/g, '');
        const content = encoded ? Buffer.from(encoded, 'base64').toString('utf8') : '';
        if (content) out.push({ path: filePath, content: content.slice(0, 2_000) });
      } catch {
        // Missing sample files are normal across projects.
      }
    }
    return out;
  }

  async buildExecutionBrief(businessKey, message, resolution) {
    const key = safeBusinessKey(businessKey);
    const project = resolution?.registryRecord || {};
    const [legacyStore, desktopContext, evidence, activity, repoFiles] = await Promise.all([
      this.getLegacyStore(key).catch(() => ({})),
      this.getDesktopContext().catch(() => ({})),
      this.projectEvidenceService && project.id
        ? this.projectEvidenceService.getProjectEvidence(key, project.id, { limit: 20 }).catch(() => [])
        : [],
      this.projectEvidenceService && project.id
        ? this.projectEvidenceService.getProjectActivity(key, project.id).catch(() => null)
        : null,
      this.sampleRepoFiles(project),
    ]);
    const legacyRows = selectLegacyRows(legacyStore, project);
    const text = formatContextBrief({ request: message, project, resolution, legacyRows, evidence, activity, desktopContext, repoFiles });
    return { text, legacyRows, evidence, activity, desktopContext, repoFiles };
  }

  async prepareCodexOperation(businessKey, { message, projectId = '', source = 'project_operator' } = {}) {
    const key = safeBusinessKey(businessKey);
    const request = safeString(message, 12_000);
    if (!request) throw new Error('message is required.');
    const resolution = await this.operationsEngine.resolveProject(key, request, { projectId });
    if (resolution.confidence === 'low' || !resolution.registryRecord) {
      const alternatives = (resolution.alternatives || []).map((item) => ({
        id: item.registryRecord?.id || '',
        name: item.registryRecord?.canonicalName || '',
        score: item.score,
      })).slice(0, 8);
      return {
        ok: true,
        status: 'needs_project',
        resolution,
        alternatives,
        reply: replyForResult({ status: 'needs_project', alternatives }),
      };
    }
    const brief = await this.buildExecutionBrief(key, request, resolution);
    const codexPrompt = composeCodexPrompt({ request, project: resolution.registryRecord, executionBrief: brief.text });
    const acceptanceCriteria = [
      request,
      'Marcus gathered project context before creating the Codex handoff.',
      'Codex receives the audit brief, constraints, approval boundaries, and verification requirements.',
      'Completion is not accepted without registered implementation and verification evidence.',
    ];
    const created = await this.operationsEngine.createFromRequest(key, {
      originalRequest: request,
      projectRegistryId: resolution.registryRecord.id,
      projectId: resolution.registryRecord.projectId,
      projectName: resolution.registryRecord.canonicalName,
      requestedBy: 'mark',
      source,
      autoPlan: true,
      autoStart: true,
      acceptanceCriteria,
      currentArchitecture: brief.text,
      relevantMemory: [
        `Marcus execution brief prepared at ${new Date().toISOString()}.`,
        ...brief.text.split('\n').filter((line) => line.startsWith('- ')).slice(0, 24),
      ],
      metadata: {
        projectOperator: {
          promptVersion: 1,
          executionBrief: brief.text,
          codexPrompt,
        },
      },
    });
    const handoff = created.operation.artifacts.find((artifact) => artifact.type === 'codex_handoff');
    const result = {
      ok: true,
      status: 'codex_prepared',
      resolution,
      project: summarizeProject(resolution.registryRecord),
      auditBrief: brief.text,
      codexPrompt,
      handoff: handoff ? { id: handoff.id, name: handoff.name, content: handoff.content } : null,
      operation: created.operation,
      reused: created.reused === true,
    };
    result.reply = replyForResult(result);
    return result;
  }
}

export { composeCodexPrompt, formatContextBrief, summarizeProject };
