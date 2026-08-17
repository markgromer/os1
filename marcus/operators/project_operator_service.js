import { redactSecrets, safeBusinessKey, safeObject, safeString } from '../operations/operation_types.js';
import { explicitlyDefersProjectAudit, withoutExplicitlyNegatedClauses } from '../core/request_intent.js';
import { formatJobPrimingManifest, selectJobPriming } from '../jobs/job_priming.js';
import { assessLockedDecisionConflict } from '../memory/locked_decisions.js';

const PROJECT_OPERATOR_ACTION_RE = /\b(audit|inspect|review|check|fix|build|implement|install(?:ed|ing)?|replace|migrate|upgrade|deploy|publish|modify|change|create|add|prepare|write|set\s*up|get [^.!?\n]{0,80} (?:working|going)|start [^.!?\n]{0,80} session|(?:send|submit|put|pass|feed|run) [^.!?\n]{0,160} (?:into|to|through|with) codex|prompt codex)\b/i;
const MAX_AUDIT_REPOSITORIES = 6;
const MAX_AUDIT_FILES_PER_REPOSITORY = 10;
const MAX_AUDIT_FILES_TOTAL = 36;
const MAX_AUDIT_FILE_CHARS = 6_000;
const MAX_EXECUTION_BRIEF_CHARS = 30_000;
const AUDIT_GITHUB_TIMEOUT_MS = 6_000;
const AUDIT_TEXT_EXTENSIONS = new Set([
  '', '.c', '.cc', '.conf', '.cpp', '.cs', '.css', '.go', '.graphql', '.h', '.html', '.java', '.js', '.json', '.jsx',
  '.kt', '.md', '.mjs', '.php', '.prisma', '.properties', '.py', '.rb', '.rs', '.scss', '.sh', '.sql', '.svelte',
  '.toml', '.ts', '.tsx', '.txt', '.vue', '.xml', '.yaml', '.yml',
]);
const AUDIT_MANIFEST_NAMES = new Set([
  'package.json', 'pyproject.toml', 'requirements.txt', 'go.mod', 'cargo.toml', 'composer.json', 'gemfile',
  'wrangler.jsonc', 'wrangler.toml', 'render.yaml', 'render.yml', 'tsconfig.json', 'vite.config.js', 'vite.config.ts',
  'next.config.js', 'next.config.mjs', 'next.config.ts', 'astro.config.mjs', 'dockerfile', 'docker-compose.yml',
]);

function mapWithConcurrency(values, limit, mapper) {
  const input = Array.isArray(values) ? values : [];
  const results = new Array(input.length);
  let cursor = 0;
  const workers = Array.from({ length: Math.min(Math.max(1, limit), input.length) }, async () => {
    while (cursor < input.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await mapper(input[index], index);
    }
  });
  return Promise.all(workers).then(() => results);
}

function encodeRepoPath(value) {
  return safeString(value, 2_000).split('/').map((part) => encodeURIComponent(part)).join('/');
}

function isSafeAuditFilePath(value, size = 0) {
  const filePath = safeString(value, 2_000).replace(/\\/g, '/');
  if (!filePath || filePath.startsWith('/') || filePath.includes('../')) return false;
  if (Number(size) > 400_000) return false;
  if (/(^|\/)(node_modules|vendor|dist|build|coverage|\.next|\.git|\.wrangler)(\/|$)/i.test(filePath)) return false;
  if (/(^|\/)(\.env(?:\..*)?|id_rsa|id_ed25519|credentials\.json|service-account[^/]*\.json|secrets?\.(?:json|ya?ml|toml))$/i.test(filePath)) return false;
  const base = filePath.split('/').pop().toLowerCase();
  const extensionIndex = base.lastIndexOf('.');
  const extension = extensionIndex > 0 ? base.slice(extensionIndex) : '';
  return AUDIT_TEXT_EXTENSIONS.has(extension) || AUDIT_MANIFEST_NAMES.has(base) || /^readme(?:\.|$)/i.test(base);
}

function auditRequestTokens(request) {
  return [...new Set([
    ...repoNameTokens(request),
    ...extractRepoSearchTerms(request).flatMap((term) => repoNameTokens(term)),
  ])].filter((token) => !['audit', 'check', 'review', 'start', 'codex', 'project', 'repository'].includes(token)).slice(0, 24);
}

function auditPathScore(filePath, requestTokens = []) {
  const normalized = safeString(filePath, 2_000).replace(/\\/g, '/');
  const lower = normalized.toLowerCase();
  const parts = lower.split('/');
  const base = parts.at(-1) || '';
  const depth = Math.max(0, parts.length - 1);
  let score = Math.max(0, 40 - (depth * 5));
  const reasons = [];

  if (/^readme(?:\.|$)/i.test(base)) { score += 1_000; reasons.push('repository overview'); }
  if (AUDIT_MANIFEST_NAMES.has(base)) { score += 900; reasons.push('manifest or runtime configuration'); }
  if (/^\.github\/workflows\/.+\.ya?ml$/i.test(lower)) { score += 720; reasons.push('automation workflow'); }
  if (/(^|\/)(src|app|server|worker|api|lib|components|pages|routes)(\/|$)/i.test(lower)) score += 90;
  if (/^(index|main|app|server|worker|route|page)\.(?:[cm]?[jt]sx?|py|go|rs|php)$/i.test(base)) {
    score += 260;
    reasons.push('runtime entry point');
  }
  if (/(^|\/)(test|tests|__tests__|spec)(\/|\.)/i.test(lower)) score += 80;
  if (/(^|\/)(architecture|docs?|adr)(\/|\.|$)/i.test(lower)) score += 110;

  const matches = requestTokens.filter((token) => lower.includes(token));
  if (matches.length) {
    score += 1_200 + (matches.length * 100);
    reasons.push(`request terms: ${matches.slice(0, 5).join(', ')}`);
  }
  return { score, reason: reasons.join('; ') || 'representative source file' };
}

function selectAuditTreeFiles(treeEntries, request, limit = MAX_AUDIT_FILES_PER_REPOSITORY) {
  const tokens = auditRequestTokens(request);
  return (Array.isArray(treeEntries) ? treeEntries : [])
    .filter((entry) => entry?.type === 'blob' && isSafeAuditFilePath(entry.path, entry.size))
    .map((entry) => ({ ...entry, ...auditPathScore(entry.path, tokens) }))
    .sort((a, b) => b.score - a.score || Number(a.size || 0) - Number(b.size || 0) || a.path.localeCompare(b.path))
    .slice(0, limit);
}

function summarizeRepositoryTree(treeEntries = []) {
  const blobs = treeEntries.filter((entry) => entry?.type === 'blob');
  const directoryCounts = new Map();
  const extensionCounts = new Map();
  for (const entry of blobs) {
    const filePath = safeString(entry.path, 2_000).replace(/\\/g, '/');
    const parts = filePath.split('/');
    if (parts.length > 1) directoryCounts.set(parts[0], (directoryCounts.get(parts[0]) || 0) + 1);
    const base = parts.at(-1) || '';
    const dot = base.lastIndexOf('.');
    const extension = dot > 0 ? base.slice(dot).toLowerCase() : '[none]';
    extensionCounts.set(extension, (extensionCounts.get(extension) || 0) + 1);
  }
  const top = (map, limit) => [...map.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, limit)
    .map(([name, count]) => ({ name, count }));
  return {
    fileCount: blobs.length,
    directoryCount: treeEntries.filter((entry) => entry?.type === 'tree').length,
    topDirectories: top(directoryCounts, 8),
    topExtensions: top(extensionCounts, 8),
  };
}

function auditCodeFence(filePath) {
  const extension = (safeString(filePath, 2_000).split('.').pop() || '').toLowerCase();
  return ({ js: 'javascript', mjs: 'javascript', jsx: 'jsx', ts: 'typescript', tsx: 'tsx', py: 'python', rb: 'ruby', rs: 'rust', yml: 'yaml', md: 'markdown' })[extension] || extension;
}

function preview(value, max = 240) {
  return safeString(value, max).replace(/\s+/g, ' ').trim();
}

function repoParts(repo = {}) {
  const fullName = safeString(repo.fullName, 300);
  if (fullName.includes('/')) return { fullName };
  const url = safeString(repo.url, 1_000);
  const match = url.match(/github\.com[:/]+([^/\s]+)\/([^/\s#?]+)/i);
  return match ? { fullName: `${match[1]}/${match[2].replace(/\.git$/i, '')}` } : { fullName: '' };
}

function normalizeRepoFullName(value) {
  const raw = safeString(value, 300).trim().replace(/[?#].*$/, '').replace(/\/+$/, '').replace(/\.git$/i, '');
  if (/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(raw)) return raw;
  const match = raw.match(/github\.com[:/]+([^/\s]+)\/([^/\s#?]+)/i);
  return match ? `${match[1]}/${match[2].replace(/\.git$/i, '')}` : '';
}

function normalizeProjectName(value) {
  return safeString(value, 2_000).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().replace(/\s+/g, ' ');
}

function compactProjectName(value) {
  return normalizeProjectName(value).replace(/\s+/g, '');
}

function humanizeWorkspaceName(value) {
  return safeString(value, 300)
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function codexWorkspaceMatchesRequest(request, workspace = {}) {
  const requestNormalized = normalizeProjectName(request);
  const requestCompact = compactProjectName(request);
  const names = [workspace.projectName, humanizeWorkspaceName(workspace.folderName), workspace.folderName]
    .map((value) => safeString(value, 300))
    .filter(Boolean);
  return names.some((name) => {
    const normalized = normalizeProjectName(name);
    const compact = compactProjectName(name);
    return normalized.length >= 4 && (` ${requestNormalized} `.includes(` ${normalized} `)
      || (compact.length >= 5 && requestCompact.includes(compact)));
  });
}

function codexWorkspaceMatchesProject(workspace = {}, project = {}) {
  const projectPath = safeString(project.localWorkspace?.canonicalPath || project.localWorkspace?.path, 2_000).toLowerCase();
  const candidatePath = safeString(workspace.workspacePath, 2_000).toLowerCase();
  if (projectPath && candidatePath && projectPath === candidatePath) return true;
  const projectRepo = repoParts(project.repo).fullName.toLowerCase();
  const candidateRepo = normalizeRepoFullName(workspace.gitRemote).toLowerCase();
  if (projectRepo && candidateRepo && projectRepo === candidateRepo) return true;
  const candidateNames = [workspace.projectName, workspace.folderName].map(compactProjectName).filter(Boolean);
  return [project.canonicalName, ...(Array.isArray(project.aliases) ? project.aliases : [])]
    .map(compactProjectName)
    .some((name) => name && candidateNames.includes(name));
}

function relativeSessionDescription(modifiedAt) {
  const ageMs = Date.now() - Date.parse(safeString(modifiedAt, 64));
  if (!Number.isFinite(ageMs) || ageMs < 0) return 'recent';
  const minutes = Math.floor(ageMs / 60_000);
  if (minutes < 2) return 'active now';
  if (minutes < 60) return `active ${minutes} minutes ago`;
  const hours = Math.floor(minutes / 60);
  return hours === 1 ? 'active an hour ago' : `active ${hours} hours ago`;
}

export function extractExplicitGitHubRepositories(request) {
  const text = safeString(request, 2_000);
  const repositories = new Set();
  const add = (value) => {
    const normalized = normalizeRepoFullName(value);
    if (normalized) repositories.add(normalized);
  };
  for (const match of text.matchAll(/github\.com[:/]+([^/\s]+)\/([^/\s#?]+)/gi)) add(`${match[1]}/${match[2]}`);
  for (const match of text.matchAll(/\b([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\.git)\b/g)) add(match[1]);
  for (const match of text.matchAll(/\b(?:github(?:\s+(?:project|repo|repository))?|repo(?:sitory)?|at|from|use)\s+(?:the\s+)?([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+(?:\.git)?)\b/gi)) add(match[1]);
  if (/^\s*[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+(?:\.git)?\s*$/.test(text)) add(text);
  return [...repositories].slice(0, 8);
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
  const terms = new Set(extractExplicitGitHubRepositories(text));
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

function formatContextBrief({ request, project, resolution, legacyRows, evidence, activity, desktopContext, repoFiles, audit = {}, missionMemory = [] }) {
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
  const repoFileBlocks = (Array.isArray(repoFiles) ? repoFiles : []).slice(0, 18).map((file) => {
    const excerpt = redactSecrets(safeString(file.content || file.summary, 1_200), 1_200).replaceAll('```', '`` `').trim();
    return [
      `### ${file.repo ? `${file.repo}:` : ''}${file.path}`,
      `- Selection reason: ${preview(file.reason, 300) || 'representative repository evidence'}`,
      `- Blob size: ${Number(file.size || 0)} bytes; excerpt: ${excerpt.length} characters.`,
      excerpt ? `\`\`\`${auditCodeFence(file.path)}\n${excerpt}\n\`\`\`` : '- Content was unavailable.',
    ].join('\n');
  });
  const auditRepos = Array.isArray(audit.repos) ? audit.repos : [];
  const auditRepoLines = auditRepos.map((repo) => {
    const head = repo.headCommit?.sha
      ? `; head ${safeString(repo.headCommit.sha, 40).slice(0, 12)} ${preview(repo.headCommit.message, 120)}`
      : '';
    const pulls = Array.isArray(repo.openPullRequests) ? repo.openPullRequests : [];
    const directories = (repo.topDirectories || []).map((item) => `${item.name} (${item.count})`).join(', ');
    const extensions = (repo.topExtensions || []).map((item) => `${item.name} (${item.count})`).join(', ');
    return [
      `- ${repo.fullName}${repo.description ? `: ${preview(repo.description, 180)}` : ''}`,
      `  - Default branch: ${repo.defaultBranch || 'unknown'}${head}`,
      `  - Tree: ${Number(repo.fileCount || 0)} files, ${Number(repo.directoryCount || 0)} directories${repo.treeTruncated ? ', GitHub reported a truncated tree' : ''}.`,
      `  - Top directories: ${directories || 'root-only or unavailable'}.`,
      `  - Dominant extensions: ${extensions || 'unavailable'}.`,
      `  - Open pull requests: ${pulls.length}${pulls.length ? ` (${pulls.slice(0, 5).map((pull) => `#${pull.number} ${preview(pull.title, 80)}`).join('; ')})` : ''}.`,
      `  - Selected files: ${(repo.selectedPaths || []).join(', ') || 'none'}.`,
    ].join('\n');
  });
  const auditFindingLines = (Array.isArray(audit.findings) ? audit.findings : []).map((finding) => `- ${finding}`);
  const coverage = safeObject(audit.coverage);
  const desktop = safeObject(desktopContext);
  const ws = safeObject(desktop.workspace);
  const desktopLines = [
    ws.workspacePath ? `Active desktop workspace: ${ws.folderName || ws.workspacePath}.` : '',
    ws.gitBranch ? `Active branch: ${ws.gitBranch}.` : '',
    Array.isArray(ws.gitStatus) && ws.gitStatus.length ? `Uncommitted files: ${ws.gitStatus.slice(0, 12).map((s) => `${s.status} ${s.file}`).join(', ')}.` : '',
  ].filter(Boolean);
  const missionMemoryLines = (Array.isArray(missionMemory) ? missionMemory : []).slice(0, 12).map((memory) =>
    `- [${preview(memory.kind, 60)}; priority ${Number(memory.priority || 0)}] ${preview(memory.title, 180)}: ${preview(memory.content, 700)}`);

  return [
    '# Marcus Project Execution Brief',
    '',
    '## Request And Resolution',
    ...lines.map((line) => `- ${line}`),
    '',
    '## Operator Mission Memory',
    missionMemoryLines.length ? missionMemoryLines.join('\n') : '- No durable mission memory was available.',
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
    `- Audit coverage: ${Number(coverage.repositoriesInspected || 0)} repositories, ${Number(coverage.treesIndexed || 0)} recursive trees, ${Number(coverage.pathsIndexed || 0)} paths indexed, ${Number(coverage.filesRead || 0)} files read, ${Number(coverage.apiCalls || 0)} GitHub API calls, ${Number(coverage.durationMs || 0)} ms.`,
    '',
    '## Repository Evidence Excerpts',
    repoFileBlocks.length ? repoFileBlocks.join('\n\n') : '- Repository file inspection was unavailable or not configured.',
    '',
    '## Activity Snapshot',
    activity?.currentFocusProject ? `- Current focus: ${activity.currentFocusProject}.` : '- No calculated project activity snapshot was available.',
    activity?.status ? `- Activity status: ${activity.status}.` : '',
    activity?.reason ? `- Reason: ${preview(activity.reason, 300)}.` : '',
    '',
    '## Desktop Context',
    desktopLines.length ? desktopLines.map((line) => `- ${line}`).join('\n') : '- No live desktop workspace context was available.',
  ].filter((line) => line !== '').join('\n');
}

function composeCodexPrompt({ request, project, executionBrief }) {
  const summary = summarizeProject(project);
  return [
    '# Goal for Codex',
    '',
    '## Objective',
    redactSecrets(safeString(request, 8_000), 8_000).trim() || 'Audit the resolved project and implement the requested improvement.',
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
    executionBrief.slice(0, MAX_EXECUTION_BRIEF_CHARS),
    '',
    '## Instructions',
    '- Inspect the repository before changing code.',
    '- Treat Marcus audit evidence as a starting point, not permission to skip opening the relevant files and their dependents.',
    '- If the objective references related repositories, determine each repository role and do not silently reduce the requested scope to the primary repository.',
    '- Make the smallest coherent change that satisfies the objective.',
    '- Preserve existing behavior and data outside the requested scope.',
    '- Do not deploy, publish, merge, change DNS, bill, text, email, or contact customers.',
    '- If a production-affecting action is needed, stop and report the exact approval needed.',
    '- Do not claim success without verification evidence.',
    '',
    '## Verification',
    '- Run the most relevant project checks from package scripts or documented commands.',
    '- For UI work, verify at desktop and mobile widths and report what was checked.',
    '- Reconcile the final implementation against every requirement in the original request and the Marcus audit brief.',
    '- Return changed files, verification output, remaining risks, and any manual approval needed.',
  ].join('\n');
}

function replyForResult(result) {
  if (result.status === 'needs_project') {
    const choices = result.alternatives.map((item) => `- ${item.name || item.id}`).join('\n');
    return `I need one project clarified before I start Codex.\n${choices || 'No confident project match was found.'}`;
  }
  const project = result.project?.name || result.operation?.projectName || 'the project';
  const status = result.operation?.status || 'unknown';
  const inspected = result.auditSummary ? ` Inspected: ${result.auditSummary}.` : '';
  if (status === 'waiting_for_approval') return `I resolved this to ${project} and audited the available context.${inspected} I need approval before I start the Codex implementation.`;
  if (status === 'awaiting_provider') return `I resolved this to ${project} and audited the available context.${inspected} The Codex job is queued with the provider now.`;
  if (status === 'blocked') return `I resolved this to ${project} and audited the available context.${inspected} The Codex handoff is ready, but no real session or result is attached yet.`;
  return `I resolved this to ${project} and audited the available context.${inspected} Status is ${status}.`;
}

export class ProjectOperatorService {
  constructor({
    operationsEngine,
    projectEvidenceService,
    getLegacyStore,
    getDesktopContext = async () => ({}),
    getMissionMemory = async () => [],
    githubApi = null,
  } = {}) {
    if (!operationsEngine) throw new Error('ProjectOperatorService requires operationsEngine.');
    this.operationsEngine = operationsEngine;
    this.projectEvidenceService = projectEvidenceService;
    this.getLegacyStore = typeof getLegacyStore === 'function' ? getLegacyStore : async () => ({});
    this.getDesktopContext = typeof getDesktopContext === 'function' ? getDesktopContext : async () => ({});
    this.getMissionMemory = typeof getMissionMemory === 'function' ? getMissionMemory : async () => [];
    this.githubApi = typeof githubApi === 'function' ? githubApi : null;
  }

  shouldHandle(message) {
    const text = safeString(message, 4_000);
    if (explicitlyDefersProjectAudit(text)) return false;
    return PROJECT_OPERATOR_ACTION_RE.test(withoutExplicitlyNegatedClauses(text));
  }

  shouldHandleStatus(message) {
    const text = safeString(message, 4_000);
    const asksForStatus = /\b(status|progress|state|standing|where (?:is|are|does)|how (?:is|are).{0,60}(?:going|doing))\b/i.test(text);
    const namesProjectSurface = /\b(project|repo|repository|site|website|app|codex)\b/i.test(text);
    return asksForStatus && namesProjectSurface;
  }

  async applyDesktopAuthorization(businessKey, project, desktop) {
    const authorization = safeObject(desktop?.desktopAuthorization);
    const agentId = safeString(authorization.agentId, 200);
    if (!project?.id || project.localWorkspace?.trustStatus === 'approved'
      || authorization.scope !== 'full_pc' || authorization.broadWorkspaceRootsAllowed !== true || !agentId) return project;
    try {
      return await this.operationsEngine.approveProjectWorkspace(businessKey, project.id, {
        desktopAgentId: agentId,
        remoteValidation: true,
        actor: 'mark_full_pc_authorization',
        message: 'Mark explicitly authorized Marcus to use the full PC. Exact workspace access remains bound to desktop attestation.',
      });
    } catch {
      return project;
    }
  }

  async ensureExplicitGithubProject(businessKey, request) {
    const key = safeBusinessKey(businessKey);
    const explicit = extractExplicitGitHubRepositories(request)[0] || '';
    if (!explicit) return null;

    const records = await this.operationsEngine.listProjectRegistry(key);
    const existing = records.find((record) => repoParts(record.repo).fullName.toLowerCase() === explicit.toLowerCase());
    if (existing) return existing;

    const [owner, name] = explicit.split('/');
    let repository = null;
    if (this.githubApi) {
      try {
        repository = await this.githubApi(`/repos/${encodeURIComponent(owner)}/${encodeURIComponent(name)}`);
      } catch {
        // The authenticated user supplied an explicit target; preserve it even if
        // metadata lookup is temporarily unavailable.
      }
    }
    const canonicalName = safeString(repository?.name || name, 300);
    try {
      return await this.operationsEngine.createProjectRegistryRecord(key, {
        canonicalName,
        aliases: [...new Set([explicit, `${canonicalName} repo`, `${canonicalName} project`])],
        description: safeString(repository?.description, 8_000),
        repo: {
          provider: 'github',
          owner,
          name: canonicalName,
          fullName: explicit,
          url: safeString(repository?.html_url, 1_000) || `https://github.com/${explicit}`,
          defaultBranch: safeString(repository?.default_branch, 200) || 'main',
        },
        metadata: { discoveredBy: 'marcus_project_operator', discoveredFromExplicitRequest: true },
      });
    } catch (error) {
      const refreshed = await this.operationsEngine.listProjectRegistry(key);
      const raced = refreshed.find((record) => repoParts(record.repo).fullName.toLowerCase() === explicit.toLowerCase());
      if (raced) return raced;
      throw error;
    }
  }

  async ensureExplicitDesktopProject(businessKey, request) {
    const key = safeBusinessKey(businessKey);
    const desktop = await this.getDesktopContext().catch(() => ({}));
    const candidates = (Array.isArray(desktop?.codexWorkspaces) ? desktop.codexWorkspaces : [])
      .filter((workspace) => codexWorkspaceMatchesRequest(request, workspace));
    const candidate = candidates[0];
    if (!candidate) return null;

    const records = await this.operationsEngine.listProjectRegistry(key);
    const candidatePath = safeString(candidate.workspacePath, 2_000).toLowerCase();
    const candidateRepo = normalizeRepoFullName(candidate.gitRemote);
    const existing = records.find((record) => {
      const recordPath = safeString(record.localWorkspace?.canonicalPath || record.localWorkspace?.path, 2_000).toLowerCase();
      const recordRepo = repoParts(record.repo).fullName;
      return (candidatePath && recordPath === candidatePath)
        || (candidateRepo && recordRepo.toLowerCase() === candidateRepo.toLowerCase());
    });
    if (existing) return this.applyDesktopAuthorization(key, existing, desktop);

    const canonicalName = safeString(candidate.projectName, 300)
      || humanizeWorkspaceName(candidate.folderName)
      || safeString(candidate.folderName, 300);
    const repo = candidateRepo ? {
      provider: 'github',
      owner: candidateRepo.split('/')[0],
      name: candidateRepo.split('/')[1],
      fullName: candidateRepo,
      url: `https://github.com/${candidateRepo}`,
      defaultBranch: safeString(candidate.gitBranch, 200) || 'main',
    } : {};
    try {
      const created = await this.operationsEngine.createProjectRegistryRecord(key, {
        canonicalName,
        aliases: [...new Set([
          safeString(candidate.folderName, 300),
          `${canonicalName} project`,
          candidateRepo,
        ].filter(Boolean))],
        repo,
        localWorkspace: { path: safeString(candidate.workspacePath, 2_000), platform: 'win32' },
        metadata: {
          discoveredBy: 'marcus_codex_workspace',
          discoveredFromExplicitRequest: true,
          codexSessionId: safeString(candidate.sessionId, 160),
          codexSessionModifiedAt: safeString(candidate.modifiedAt, 64),
        },
      });
      return this.applyDesktopAuthorization(key, created, desktop);
    } catch (error) {
      const refreshed = await this.operationsEngine.listProjectRegistry(key);
      const raced = refreshed.find((record) => {
        const recordPath = safeString(record.localWorkspace?.path, 2_000).toLowerCase();
        return candidatePath && recordPath === candidatePath;
      });
      if (raced) return this.applyDesktopAuthorization(key, raced, desktop);
      throw error;
    }
  }

  async resolveProjectContext(businessKey, { message, projectId = '', projectRegistryId = '', currentProjectId = '' } = {}) {
    const key = safeBusinessKey(businessKey);
    const request = safeString(message, 12_000);
    const explicit = await this.ensureExplicitGithubProject(key, request)
      || await this.ensureExplicitDesktopProject(key, request);
    const resolution = await this.operationsEngine.resolveProject(key, request, {
      projectId,
      registryId: explicit?.id || projectRegistryId,
      currentProjectId,
    });
    return { resolution, project: resolution.registryRecord ? summarizeProject(resolution.registryRecord) : null, registered: Boolean(explicit) };
  }

  async readProjectStatus(businessKey, { message, projectId = '', projectRegistryId = '', currentProjectId = '' } = {}) {
    const key = safeBusinessKey(businessKey);
    const context = await this.resolveProjectContext(key, { message, projectId, projectRegistryId, currentProjectId });
    const { resolution } = context;
    if (resolution.confidence === 'low' || !resolution.registryRecord) {
      const alternatives = (resolution.alternatives || []).map((item) => ({
        id: item.registryRecord?.id || '',
        name: item.registryRecord?.canonicalName || '',
        score: item.score,
      })).slice(0, 8);
      return { ok: true, status: 'needs_project', resolution, alternatives, reply: replyForResult({ status: 'needs_project', alternatives }) };
    }

    const project = resolution.registryRecord;
    const [desktop, operations, activity, audit] = await Promise.all([
      this.getDesktopContext().catch(() => ({})),
      this.operationsEngine.listOperations(key, { limit: 100 }).catch(() => []),
      this.projectEvidenceService?.getProjectActivity(key, project.id).catch(() => null) || null,
      this.buildGithubAudit(message, project).catch(() => ({ repos: [], files: [], findings: [], coverage: {} })),
    ]);
    const codexWorkspace = (Array.isArray(desktop?.codexWorkspaces) ? desktop.codexWorkspaces : [])
      .find((workspace) => codexWorkspaceMatchesProject(workspace, project));
    const projectOperations = (Array.isArray(operations) ? operations : [])
      .filter((operation) => operation.projectRegistryId === project.id || (project.projectId && operation.projectId === project.projectId))
      .sort((left, right) => String(right.updatedAt || '').localeCompare(String(left.updatedAt || '')));
    const latestOperation = projectOperations[0] || null;
    const primaryRepo = (audit.repos || []).find((repo) => repo.fullName.toLowerCase() === repoParts(project.repo).fullName.toLowerCase())
      || audit.repos?.[0]
      || null;

    const lines = [];
    if (codexWorkspace) {
      const branch = safeString(codexWorkspace.gitBranch, 120) || 'an unknown branch';
      lines.push(`${project.canonicalName} has a Codex workspace ${relativeSessionDescription(codexWorkspace.modifiedAt)} on ${branch}.`);
      const changed = Number(codexWorkspace.gitStatusCount || codexWorkspace.gitStatus?.length || 0);
      lines.push(changed
        ? `That checkout has ${changed} changed or untracked file${changed === 1 ? '' : 's'}, so the current work is still local and in progress.`
        : 'The checkout is clean, with no local changes reported by the desktop relay.');
    } else {
      lines.push(`${project.canonicalName} is registered, but the desktop relay does not currently report a matching Codex workspace.`);
    }
    if (primaryRepo?.headCommit) {
      lines.push(`GitHub ${primaryRepo.defaultBranch || 'main'} is at ${primaryRepo.headCommit.sha.slice(0, 7)} (${preview(primaryRepo.headCommit.message, 120)}), with ${primaryRepo.openPullRequests?.length || 0} open pull request${primaryRepo.openPullRequests?.length === 1 ? '' : 's'}.`);
    }
    if (latestOperation) {
      lines.push(`Marcus's latest durable operation for it is ${safeString(latestOperation.status, 80).replaceAll('_', ' ')}: ${preview(latestOperation.title || latestOperation.objective, 140)}.`);
    } else {
      lines.push('Marcus has no durable operation tied to this project yet; the visible work is happening directly in Codex.');
    }

    return {
      ok: true,
      status: 'project_status',
      resolution,
      project: summarizeProject(project),
      codexWorkspace: codexWorkspace ? {
        workspacePath: safeString(codexWorkspace.workspacePath, 2_000),
        folderName: safeString(codexWorkspace.folderName, 300),
        modifiedAt: safeString(codexWorkspace.modifiedAt, 64),
        gitBranch: safeString(codexWorkspace.gitBranch, 120),
        gitStatusCount: Number(codexWorkspace.gitStatusCount || codexWorkspace.gitStatus?.length || 0),
        gitRecentCommits: Array.isArray(codexWorkspace.gitRecentCommits)
          ? codexWorkspace.gitRecentCommits.slice(0, 3).map((item) => safeString(item, 240))
          : [],
      } : null,
      activity: activity ? {
        activityStatus: safeString(activity.activityStatus || activity.status, 100),
        lastActivityAt: safeString(activity.lastActivityAt || activity.updatedAt, 64),
        nextAction: safeString(activity.nextAction, 500),
        reason: safeString(activity.reason, 500),
      } : null,
      audit: {
        coverage: safeObject(audit.coverage),
        findings: Array.isArray(audit.findings) ? audit.findings.slice(0, 12).map((item) => safeString(item, 500)) : [],
        repositories: (Array.isArray(audit.repos) ? audit.repos : []).slice(0, 6).map((repo) => ({
          fullName: safeString(repo.fullName, 300),
          defaultBranch: safeString(repo.defaultBranch, 120),
          pushedAt: safeString(repo.pushedAt, 64),
          headCommit: repo.headCommit ? {
            sha: safeString(repo.headCommit.sha, 100),
            message: safeString(repo.headCommit.message, 300),
            authoredAt: safeString(repo.headCommit.authoredAt, 64),
          } : null,
          openPullRequestCount: Array.isArray(repo.openPullRequests) ? repo.openPullRequests.length : 0,
        })),
      },
      latestOperation: latestOperation ? {
        id: safeString(latestOperation.id, 160),
        title: safeString(latestOperation.title, 300),
        status: safeString(latestOperation.status, 100),
        currentStepId: safeString(latestOperation.currentStepId, 160),
        updatedAt: safeString(latestOperation.updatedAt, 64),
      } : null,
      reply: lines.join(' '),
    };
  }

  async discoverRelatedRepos(request, project, githubApi = this.githubApi) {
    const primary = repoParts(project?.repo).fullName;
    const terms = extractRepoSearchTerms(request);
    const byName = new Map();
    const addRepo = (repo = {}, score = 0, source = 'unknown') => {
      const fullName = normalizeRepoFullName(repo.full_name || repo.fullName || repo.html_url || primary);
      if (!fullName) return;
      const key = fullName.toLowerCase();
      const existing = byName.get(key) || {};
      const sources = new Set(safeString(existing.source, 500).split(',').filter(Boolean));
      sources.add(source);
      byName.set(key, {
        fullName: safeString(repo.full_name || repo.fullName, 300) || existing.fullName || fullName,
        name: safeString(repo.name || fullName.split('/')[1], 200),
        description: safeString(repo.description || existing.description, 500),
        defaultBranch: safeString(repo.default_branch || repo.defaultBranch || existing.defaultBranch, 120),
        htmlUrl: safeString(repo.html_url || existing.htmlUrl, 1_000),
        private: repo.private === true || existing.private === true,
        score: Math.max(Number(existing.score) || 0, score),
        source: [...sources].join(','),
      });
    };
    if (primary) addRepo({ fullName: primary }, 150, 'project_registry');
    if (!githubApi) return [...byName.values()];

    for (const term of terms) {
      const full = normalizeRepoFullName(term);
      if (full) addRepo({ fullName: full }, 130, 'request');
    }

    try {
      const repos = await githubApi('/user/repos?per_page=100&sort=updated&affiliation=owner,collaborator,organization_member');
      for (const repo of Array.isArray(repos) ? repos : []) {
        const score = scoreRepoForTerms(repo, terms);
        if (score >= 24) addRepo(repo, score, 'github_user_repos');
      }
    } catch {
      // Repo listing may be unavailable with narrower tokens; direct full-name repos still work.
    }

    return [...byName.values()]
      .sort((a, b) => b.score - a.score || a.fullName.localeCompare(b.fullName))
      .slice(0, MAX_AUDIT_REPOSITORIES);
  }

  async inspectRepository(repoInfo, request, githubApi) {
    const [owner, repo] = safeString(repoInfo.fullName, 300).split('/');
    const base = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`;
    const errors = [];
    let metadata = {};
    try {
      metadata = await githubApi(base);
    } catch (error) {
      errors.push(`metadata: ${preview(error?.message || error, 200)}`);
    }
    const fullName = safeString(metadata?.full_name || repoInfo.fullName, 300);
    const defaultBranch = safeString(metadata?.default_branch || repoInfo.defaultBranch, 120) || 'main';
    const [treeResult, commitsResult, pullsResult] = await Promise.allSettled([
      githubApi(`${base}/git/trees/${encodeURIComponent(defaultBranch)}?recursive=1`),
      githubApi(`${base}/commits?sha=${encodeURIComponent(defaultBranch)}&per_page=5`),
      githubApi(`${base}/pulls?state=open&sort=updated&direction=desc&per_page=10`),
    ]);
    if (treeResult.status === 'rejected') errors.push(`tree: ${preview(treeResult.reason?.message || treeResult.reason, 200)}`);
    if (commitsResult.status === 'rejected') errors.push(`commits: ${preview(commitsResult.reason?.message || commitsResult.reason, 200)}`);
    if (pullsResult.status === 'rejected') errors.push(`pull requests: ${preview(pullsResult.reason?.message || pullsResult.reason, 200)}`);

    const treeEntries = treeResult.status === 'fulfilled' && Array.isArray(treeResult.value?.tree) ? treeResult.value.tree : [];
    const fallbackPaths = [
      'README.md', 'package.json', 'pyproject.toml', 'requirements.txt', 'go.mod', 'Cargo.toml',
      'wrangler.jsonc', 'wrangler.toml', 'render.yaml', 'vite.config.ts', 'next.config.js',
      '.github/workflows/deploy.yml', '.github/workflows/pages.yml', 'src/index.ts', 'src/index.js', 'server.js',
    ].map((filePath) => ({ type: 'blob', path: filePath, size: 0 }));
    const selected = selectAuditTreeFiles(
      treeEntries.length ? treeEntries : fallbackPaths,
      request,
      treeEntries.length ? MAX_AUDIT_FILES_PER_REPOSITORY : 4,
    );
    const fileResults = await mapWithConcurrency(selected, 4, async (entry) => {
      try {
        const data = await githubApi(`${base}/contents/${encodeRepoPath(entry.path)}?ref=${encodeURIComponent(defaultBranch)}`);
        const encoded = safeString(data?.content, 600_000).replace(/\s+/g, '');
        const decoded = encoded ? Buffer.from(encoded, 'base64').toString('utf8') : '';
        const content = redactSecrets(decoded, MAX_AUDIT_FILE_CHARS).trim();
        return content ? {
          repo: fullName,
          path: entry.path,
          size: Number(data?.size || entry.size || Buffer.byteLength(decoded, 'utf8')),
          sha: safeString(data?.sha || entry.sha, 100),
          score: Number(entry.score || 0),
          reason: entry.reason,
          content,
        } : null;
      } catch (error) {
        errors.push(`${entry.path}: ${preview(error?.message || error, 160)}`);
        return null;
      }
    });
    const commits = commitsResult.status === 'fulfilled' && Array.isArray(commitsResult.value) ? commitsResult.value : [];
    const pulls = pullsResult.status === 'fulfilled' && Array.isArray(pullsResult.value) ? pullsResult.value : [];
    const treeSummary = summarizeRepositoryTree(treeEntries);
    return {
      repo: {
        ...repoInfo,
        fullName,
        name: safeString(metadata?.name || repoInfo.name, 200),
        description: safeString(metadata?.description || repoInfo.description, 500),
        defaultBranch,
        htmlUrl: safeString(metadata?.html_url || repoInfo.htmlUrl, 1_000),
        private: metadata?.private === true || repoInfo.private === true,
        archived: metadata?.archived === true,
        pushedAt: safeString(metadata?.pushed_at, 64),
        treeIndexed: treeEntries.length > 0,
        treeTruncated: treeResult.status === 'fulfilled' && treeResult.value?.truncated === true,
        ...treeSummary,
        selectedPaths: fileResults.filter(Boolean).map((file) => file.path),
        headCommit: commits[0] ? {
          sha: safeString(commits[0]?.sha, 100),
          message: safeString(commits[0]?.commit?.message, 500),
          authoredAt: safeString(commits[0]?.commit?.author?.date, 64),
        } : null,
        recentCommits: commits.slice(0, 5).map((commit) => ({
          sha: safeString(commit?.sha, 100),
          message: safeString(commit?.commit?.message, 500),
          authoredAt: safeString(commit?.commit?.author?.date, 64),
        })),
        openPullRequests: pulls.slice(0, 10).map((pull) => ({
          number: Number(pull?.number || 0),
          title: safeString(pull?.title, 300),
          draft: pull?.draft === true,
          head: safeString(pull?.head?.ref, 200),
          base: safeString(pull?.base?.ref, 200),
          updatedAt: safeString(pull?.updated_at, 64),
        })),
        errors: errors.slice(0, 20),
      },
      files: fileResults.filter(Boolean),
    };
  }

  async sampleRepoFiles(project, request = '') {
    return (await this.buildGithubAudit(request, project)).files;
  }

  async buildGithubAudit(request, project) {
    if (!this.githubApi) return { repos: [], files: [], findings: ['GitHub API was not configured for repository inspection.'] };
    const startedAt = Date.now();
    let apiCalls = 0;
    const githubApi = async (pathPart) => {
      apiCalls += 1;
      return this.githubApi(pathPart, { timeoutMs: AUDIT_GITHUB_TIMEOUT_MS });
    };
    const discovered = await this.discoverRelatedRepos(request, project, githubApi);
    const inspections = await mapWithConcurrency(discovered, 2, (repoInfo) => this.inspectRepository(repoInfo, request, githubApi));
    const repos = inspections.map((inspection) => inspection.repo);
    const files = [];
    for (let index = 0; index < MAX_AUDIT_FILES_PER_REPOSITORY && files.length < MAX_AUDIT_FILES_TOTAL; index += 1) {
      for (const inspection of inspections) {
        if (inspection.files[index]) files.push(inspection.files[index]);
        if (files.length >= MAX_AUDIT_FILES_TOTAL) break;
      }
    }
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
      findings.push(`Read ${files.length} request-ranked file${files.length === 1 ? '' : 's'} across ${Object.keys(byRepo).length} repositor${Object.keys(byRepo).length === 1 ? 'y' : 'ies'}.`);
    }
    const partialRepos = repos.filter((repo) => !repo.treeIndexed || repo.treeTruncated || repo.errors?.length);
    if (partialRepos.length) findings.push(`Partial GitHub evidence for: ${partialRepos.map((repo) => repo.fullName).join(', ')}.`);
    const minimumExpectedFiles = repos.reduce((sum, repo) => sum + Math.min(4, Math.max(1, Number(repo.fileCount || 0))), 0);
    const failedChecks = repos.reduce((sum, repo) => sum + (Array.isArray(repo.errors) ? repo.errors.length : 0), 0);
    const coverage = {
      mode: repos.length
        && repos.every((repo) => repo.treeIndexed && !repo.treeTruncated && !repo.errors?.length)
        && files.length >= minimumExpectedFiles ? 'deep' : 'partial',
      repositoriesInspected: repos.length,
      treesIndexed: repos.filter((repo) => repo.treeIndexed).length,
      pathsIndexed: repos.reduce((sum, repo) => sum + Number(repo.fileCount || 0), 0),
      filesRead: files.length,
      requestRelevantFiles: files.filter((file) => safeString(file.reason, 500).includes('request terms:')).length,
      failedChecks,
      apiCalls,
      durationMs: Date.now() - startedAt,
    };
    findings.push(`Audit mode ${coverage.mode}: indexed ${coverage.pathsIndexed} paths and read ${coverage.filesRead} files in ${coverage.durationMs} ms.`);
    return { repos, files, findings, coverage };
  }

  async buildExecutionBrief(businessKey, message, resolution) {
    const key = safeBusinessKey(businessKey);
    const project = resolution?.registryRecord || {};
    const [legacyStore, desktopContext, evidence, activity, audit, missionMemory] = await Promise.all([
      this.getLegacyStore(key).catch(() => ({})),
      this.getDesktopContext().catch(() => ({})),
      this.projectEvidenceService && project.id
        ? this.projectEvidenceService.getProjectEvidence(key, project.id, { limit: 20 }).catch(() => [])
        : [],
      this.projectEvidenceService && project.id
        ? this.projectEvidenceService.getProjectActivity(key, project.id).catch(() => null)
        : null,
      this.buildGithubAudit(message, project),
      this.getMissionMemory(key, message).catch(() => []),
    ]);
    const repoFiles = audit.files || [];
    const legacyRows = selectLegacyRows(legacyStore, project);
    const priming = selectJobPriming(message);
    const text = `${formatJobPrimingManifest(message)}\n\n${formatContextBrief({ request: message, project, resolution, legacyRows, evidence, activity, desktopContext, repoFiles, audit, missionMemory })}`;
    return { text, legacyRows, evidence, activity, desktopContext, repoFiles, audit, missionMemory, priming };
  }

  async prepareCodexOperation(businessKey, { message, projectId = '', projectRegistryId = '', currentProjectId = '', resolutionRequest = '', source = 'project_operator', autoStart = true } = {}) {
    const key = safeBusinessKey(businessKey);
    const request = safeString(message, 12_000);
    if (!request) throw new Error('message is required.');
    const resolverText = safeString(resolutionRequest, 12_000) || request;
    const explicit = await this.ensureExplicitGithubProject(key, resolverText)
      || await this.ensureExplicitDesktopProject(key, resolverText);
    const resolution = await this.operationsEngine.resolveProject(key, resolverText, {
      projectId,
      registryId: explicit?.id || projectRegistryId,
      currentProjectId,
    });
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
    const lockedConflict = assessLockedDecisionConflict(request, brief.missionMemory);
    if (lockedConflict) return { ok: true, ...lockedConflict, resolution, project: summarizeProject(resolution.registryRecord) };
    const codexPrompt = composeCodexPrompt({ request, project: resolution.registryRecord, executionBrief: brief.text });
    const coverage = safeObject(brief.audit?.coverage);
    const auditSummary = [
      `${Number(coverage.repositoriesInspected || 0)} repos`,
      `${Number(coverage.pathsIndexed || 0)} paths indexed`,
      `${Number(coverage.filesRead || 0)} files read`,
      `${Number(coverage.durationMs || 0)} ms`,
    ].join(', ');
    const acceptanceCriteria = [
      request,
      `Marcus gathered project context before creating the Codex handoff (${auditSummary}).`,
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
      autoStart: autoStart !== false,
      acceptanceCriteria,
      currentArchitecture: brief.text.slice(0, MAX_EXECUTION_BRIEF_CHARS),
      relevantMemory: [
        `Marcus execution brief prepared at ${new Date().toISOString()}.`,
        ...brief.text.split('\n').filter((line) => line.startsWith('- ')).slice(0, 24),
      ],
      metadata: {
        projectOperator: {
          promptVersion: 3,
          promptLength: codexPrompt.length,
          executionBriefLength: brief.text.length,
          jobPriming: brief.priming,
          missionMemory: (brief.missionMemory || []).slice(0, 20).map((memory) => ({
            id: memory.id,
            kind: memory.kind,
            title: memory.title,
            priority: memory.priority,
            source: memory.source,
            updatedAt: memory.updatedAt,
            lastConfirmedAt: memory.lastConfirmedAt,
          })),
          githubAudit: {
            coverage,
            repos: (brief.audit?.repos || []).map((repo) => ({
              fullName: repo.fullName,
              source: repo.source,
              score: repo.score,
              defaultBranch: repo.defaultBranch,
              fileCount: repo.fileCount,
              treeIndexed: repo.treeIndexed,
              treeTruncated: repo.treeTruncated,
              headSha: repo.headCommit?.sha || '',
              openPullRequestCount: repo.openPullRequests?.length || 0,
            })),
            files: (brief.repoFiles || []).map((file) => ({
              repo: file.repo,
              path: file.path,
              sha: file.sha,
              size: file.size,
              score: file.score,
              reason: file.reason,
            })),
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
      auditSummary,
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
