import { safeBusinessKey, safeObject, safeString } from '../operations/operation_types.js';

const CODEX_OPERATOR_RE = /\b(codex|audit|repo|repository|fix|build|implement|install|replace|migrate|upgrade|website|site|worker|cloudflare|deploy|mobile|broken|get .* working|start .* session)\b/i;

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

function normalizeRepoFullName(value) {
  const raw = safeString(value, 300).trim();
  if (/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(raw)) return raw;
  const match = raw.match(/github\.com[:/]+([^/\s]+)\/([^/\s.git#?]+)/i);
  return match ? `${match[1]}/${match[2]}` : '';
}

function repoNameTokens(value) {
  return safeString(value, 300)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .split(/\s+/)
    .filter((token) => token.length >= 3 && !new Set(['the', 'new', 'old', 'and', 'hub', 'site', 'app', 'repo', 'github', 'website', 'system', 'legacy', 'installed', 'install', 'replace']).has(token));
}

function extractRepoSearchTerms(request) {
  const text = safeString(request, 2_000);
  const terms = new Set();
  for (const match of text.matchAll(/github\.com[:/]+([^/\s]+)\/([^/\s.git#?]+)/gi)) terms.add(`${match[1]}/${match[2]}`);
  for (const match of text.matchAll(/\b([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)\b/g)) terms.add(match[1]);
  for (const match of text.matchAll(/\b([a-z0-9.-]+\.[a-z]{2,})(?:\/[^\s]*)?/gi)) terms.add(match[1]);

  const phrases = [
    ...text.matchAll(/\b(new|legacy|old|current)\s+([A-Za-z][A-Za-z0-9 -]{2,40}?)(?=\s+(?:hub|system|project|repo|repository|installed|install|replace|in github|$))/gi),
    ...text.matchAll(/\b([A-Za-z][A-Za-z0-9 -]{2,40}?\s+hub)\b/gi),
  ];
  for (const match of phrases) terms.add((match[2] || match[1] || '').trim());
  if (/\breggie\b/i.test(text)) terms.add('Reggie');
  if (/\breggie hub\b/i.test(text)) terms.add('Reggie hub');
  return [...terms].map((term) => term.trim()).filter(Boolean).slice(0, 8);
}

function scoreRepoForTerms(repo, terms) {
  const fullName = safeString(repo.full_name || repo.fullName, 300);
  const name = safeString(repo.name, 200);
  const description = safeString(repo.description, 500);
  const haystack = `${fullName} ${name} ${description}`.toLowerCase();
  let score = 0;
  for (const term of terms) {
    const clean = safeString(term, 300).toLowerCase();
    const full = normalizeRepoFullName(clean);
    if (full && fullName.toLowerCase() === full.toLowerCase()) score += 100;
    if (clean && haystack.includes(clean)) score += 60;
    const tokens = repoNameTokens(clean);
    for (const token of tokens) if (haystack.includes(token)) score += 12;
  }
  return score;
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

function formatContextBrief({ request, project, resolution, legacyRows, evidence, activity, desktopContext, repoFiles, audit = {} }) {
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
    `- ${file.repo ? `${file.repo}:` : ''}${file.path}: ${preview(file.summary || file.content, 260)}`);
  const auditRepos = Array.isArray(audit.repos) ? audit.repos : [];
  const auditRepoLines = auditRepos.map((repo) =>
    `- ${repo.fullName}${repo.description ? `: ${preview(repo.description, 180)}` : ''}${repo.defaultBranch ? ` (default ${repo.defaultBranch})` : ''}`);
  const auditFindingLines = (Array.isArray(audit.findings) ? audit.findings : []).map((finding) => `- ${finding}`);
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
    '## GitHub Audit',
    auditRepoLines.length ? auditRepoLines.join('\n') : '- No related GitHub repositories were inspected.',
    auditFindingLines.length ? auditFindingLines.join('\n') : '',
    '',
    '## Activity Snapshot',
    activity?.currentFocusProject ? `- Current focus: ${activity.currentFocusProject}.` : '- No calculated project activity snapshot was available.',
    activity?.status ? `- Activity status: ${activity.status}.` : '',
    activity?.reason ? `- Reason: ${preview(activity.reason, 300)}.` : '',
    '',
    '## Desktop Context',
    desktopLines.length ? desktopLines.map((line) => `- ${line}`).join('\n') : '- No live desktop workspace context was available.',
    '',
    '## Repository Files Inspected',
    repoFileLines.length ? repoFileLines.join('\n') : '- Repository file inspection was unavailable or not configured.',
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
  const inspected = result.auditSummary ? `Inspected: ${result.auditSummary}.` : '';
  const waiting = result.operation?.status === 'blocked'
    ? 'It is waiting for a real Codex session/result; I am not pretending the handoff has executed.'
    : `Operation status is ${result.operation?.status || 'unknown'}.`;
  return [
    `I resolved this to ${result.project?.name || result.operation?.projectName || 'the project'} and audited the available context.`,
    inspected,
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

  async discoverRelatedRepos(request, project) {
    const primary = repoParts(project?.repo).fullName;
    const terms = extractRepoSearchTerms(request);
    const byName = new Map();
    const addRepo = (repo = {}, score = 0, source = 'unknown') => {
      const fullName = normalizeRepoFullName(repo.full_name || repo.fullName || repo.html_url || primary);
      if (!fullName) return;
      const existing = byName.get(fullName) || {};
      byName.set(fullName, {
        fullName,
        name: safeString(repo.name || fullName.split('/')[1], 200),
        description: safeString(repo.description || existing.description, 500),
        defaultBranch: safeString(repo.default_branch || repo.defaultBranch || existing.defaultBranch, 120),
        htmlUrl: safeString(repo.html_url || existing.htmlUrl, 1_000),
        private: repo.private === true || existing.private === true,
        score: Math.max(Number(existing.score) || 0, score),
        source: existing.source ? `${existing.source},${source}` : source,
      });
    };
    if (primary) addRepo({ fullName: primary }, 150, 'project_registry');
    if (!this.githubApi) return [...byName.values()];

    for (const term of terms) {
      const full = normalizeRepoFullName(term);
      if (full) addRepo({ fullName: full }, 130, 'request');
    }

    try {
      const repos = await this.githubApi('/user/repos?per_page=100&sort=updated&affiliation=owner,collaborator,organization_member');
      for (const repo of Array.isArray(repos) ? repos : []) {
        const score = scoreRepoForTerms(repo, terms);
        if (score >= 24) addRepo(repo, score, 'github_user_repos');
      }
    } catch {
      // Repo listing may be unavailable with narrower tokens; direct full-name repos still work.
    }

    return [...byName.values()]
      .sort((a, b) => b.score - a.score || a.fullName.localeCompare(b.fullName))
      .slice(0, 8);
  }

  async sampleRepoFiles(project, request = '') {
    if (!this.githubApi) return [];
    const repos = await this.discoverRelatedRepos(request, project);
    if (!repos.length) return [];
    const candidates = [
      'README.md',
      'package.json',
      'wrangler.jsonc',
      'wrangler.toml',
      'render.yaml',
      '.github/workflows/marcus-codex-runner.yml',
      '.github/workflows/deploy.yml',
      '.github/workflows/pages.yml',
      'src/index.ts',
      'src/index.js',
    ];
    const out = [];
    for (const repoInfo of repos) {
      const [owner, repo] = repoInfo.fullName.split('/');
      for (const filePath of candidates) {
        try {
          const data = await this.githubApi(`/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/contents/${filePath}`);
          const encoded = safeString(data?.content, 100_000).replace(/\s+/g, '');
          const content = encoded ? Buffer.from(encoded, 'base64').toString('utf8') : '';
          if (content) out.push({ repo: repoInfo.fullName, path: filePath, content: content.slice(0, 4_000) });
        } catch {
          // Missing sample files are normal across projects.
        }
      }
    }
    return out.slice(0, 32);
  }

  async buildGithubAudit(request, project) {
    if (!this.githubApi) return { repos: [], files: [], findings: ['GitHub API was not configured for repository inspection.'] };
    const repos = await this.discoverRelatedRepos(request, project);
    const files = await this.sampleRepoFiles(project, request);
    const findings = [];
    if (!repos.length) {
      findings.push('No related repositories were discovered from the request or project registry.');
    } else {
      findings.push(`Discovered ${repos.length} related repositor${repos.length === 1 ? 'y' : 'ies'} from the request, project registry, and GitHub repo list.`);
    }
    if (!files.length) {
      findings.push('No key repository files were readable from the discovered repositories.');
    } else {
      const byRepo = files.reduce((acc, file) => ({ ...acc, [file.repo]: (acc[file.repo] || 0) + 1 }), {});
      findings.push(`Read ${files.length} key file sample${files.length === 1 ? '' : 's'} across ${Object.keys(byRepo).length} repositor${Object.keys(byRepo).length === 1 ? 'y' : 'ies'}.`);
    }
    return { repos, files, findings };
  }

  async buildExecutionBrief(businessKey, message, resolution) {
    const key = safeBusinessKey(businessKey);
    const project = resolution?.registryRecord || {};
    const [legacyStore, desktopContext, evidence, activity, audit] = await Promise.all([
      this.getLegacyStore(key).catch(() => ({})),
      this.getDesktopContext().catch(() => ({})),
      this.projectEvidenceService && project.id
        ? this.projectEvidenceService.getProjectEvidence(key, project.id, { limit: 20 }).catch(() => [])
        : [],
      this.projectEvidenceService && project.id
        ? this.projectEvidenceService.getProjectActivity(key, project.id).catch(() => null)
        : null,
      this.buildGithubAudit(message, project),
    ]);
    const repoFiles = audit.files || [];
    const legacyRows = selectLegacyRows(legacyStore, project);
    const text = formatContextBrief({ request: message, project, resolution, legacyRows, evidence, activity, desktopContext, repoFiles, audit });
    return { text, legacyRows, evidence, activity, desktopContext, repoFiles, audit };
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
          promptVersion: 2,
          executionBrief: brief.text,
          codexPrompt,
          githubAudit: {
            repos: (brief.audit?.repos || []).map((repo) => ({ fullName: repo.fullName, source: repo.source, score: repo.score })),
            files: (brief.repoFiles || []).map((file) => ({ repo: file.repo, path: file.path })),
            findings: brief.audit?.findings || [],
          },
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
      auditSummary: `${brief.audit?.repos?.length || 0} repos, ${brief.repoFiles?.length || 0} files`,
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
