import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { nowIso, safeString } from '../operations/operation_types.js';

const IGNORED_DIRECTORIES = new Set([
  '.git', '.next', '.nuxt', '.output', '.turbo', '.vercel',
  'build', 'coverage', 'dist', 'node_modules', 'out', 'tmp', 'vendor',
]);
const SECRET_FILE_PATTERN = /(^|[\\/])(?:\.env(?:\..*)?|id_rsa|id_ed25519|credentials?\.json|secrets?\.(?:json|ya?ml|txt)|.*\.(?:pem|key|p12|pfx))$/i;
const MAX_MANIFEST_FILES = 5_000;
const MAX_SOURCE_CHARS = 24_000;

function slug(value) {
  return String(value || '').normalize('NFKD').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 120);
}

function hash(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex');
}

function redactSecretLikeText(value) {
  return String(value || '')
    .replace(/\b(?:sk|ghp|github_pat|xox[baprs]|AIza)[-_A-Za-z0-9]{12,}\b/g, '[REDACTED]')
    .replace(/((?:api[_ -]?key|token|password|secret|private[_ -]?key)\s*[:=]\s*)[^\s,;]+/gi, '$1[REDACTED]');
}

function summaryFromSources(sources, project) {
  const preferred = [...sources].sort((left, right) => {
    const rank = { marcus_root: 0, obsidian_project: 1, readme: 2, package: 3 };
    return (rank[left.type] ?? 9) - (rank[right.type] ?? 9);
  });
  const lines = [];
  for (const source of preferred) {
    for (const rawLine of String(source.excerpt || '').split(/\r?\n/)) {
      const line = rawLine.replace(/^\s*(?:[-*#]+|\d+[.)])\s*/, '').trim();
      if (!line || /^(?:append log|current project map|working rule|links|tags|status)$/i.test(line)) continue;
      if (!lines.some((item) => item.toLowerCase() === line.toLowerCase())) lines.push(line);
      if (lines.join(' ').length >= 2_400) break;
    }
    if (lines.join(' ').length >= 2_400) break;
  }
  return safeString(lines.join(' ') || project?.currentObjective?.desiredOutcome || project?.description || `Known project: ${project?.canonicalName || 'unnamed project'}.`, 3_000);
}

async function readableFile(file) {
  if (!file || SECRET_FILE_PATTERN.test(file)) return null;
  try {
    const stat = await fs.stat(file);
    if (!stat.isFile() || stat.size > 1_000_000) return null;
    const content = redactSecretLikeText((await fs.readFile(file, 'utf8')).slice(0, MAX_SOURCE_CHARS));
    return { content, stat };
  } catch {
    return null;
  }
}

async function scanRepository(root) {
  const files = [];
  const directoryCounts = new Map();
  const extensionCounts = new Map();
  let truncated = false;
  const walk = async (directory, depth) => {
    if (depth > 12 || files.length >= MAX_MANIFEST_FILES) { truncated = true; return; }
    let entries = [];
    try { entries = await fs.readdir(directory, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      if (files.length >= MAX_MANIFEST_FILES) { truncated = true; break; }
      if (entry.isSymbolicLink()) continue;
      const full = path.join(directory, entry.name);
      const relative = path.relative(root, full).replaceAll('\\', '/');
      if (entry.isDirectory()) {
        if (IGNORED_DIRECTORIES.has(entry.name.toLowerCase())) continue;
        const top = relative.split('/')[0];
        directoryCounts.set(top, (directoryCounts.get(top) || 0) + 1);
        await walk(full, depth + 1);
      } else if (entry.isFile()) {
        if (SECRET_FILE_PATTERN.test(relative)) continue;
        let stat = null;
        try { stat = await fs.stat(full); } catch { continue; }
        files.push({ path: relative, size: stat.size, modifiedAt: stat.mtime.toISOString() });
        const extension = path.extname(entry.name).toLowerCase() || '[none]';
        extensionCounts.set(extension, (extensionCounts.get(extension) || 0) + 1);
      }
    }
  };
  await walk(root, 0);
  const ranked = (map) => [...map.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 20).map(([name, count]) => ({ name, count }));
  return {
    root,
    fileCount: files.length,
    truncated,
    topDirectories: ranked(directoryCounts),
    topExtensions: ranked(extensionCounts),
    keyFiles: files.filter((item) => /^(?:readme(?:\.[^/]+)?|package\.json|pyproject\.toml|requirements\.txt|go\.mod|cargo\.toml|composer\.json|wrangler\.(?:toml|jsonc)|render\.yaml|server\.[cm]?js|marcus\.txt)$/i.test(item.path)).slice(0, 50),
    contentHash: hash(files.map((item) => `${item.path}:${item.size}:${item.modifiedAt}`).join('\n')),
  };
}

export class ProjectMemoryIndexer {
  constructor({ vaultDir = path.join(process.cwd(), 'docs', 'marcus'), currentWorkspace = process.cwd() } = {}) {
    this.vaultDir = path.resolve(vaultDir);
    this.currentWorkspace = path.resolve(currentWorkspace);
  }

  canAccessWorkspace(project) {
    const workspace = safeString(project?.localWorkspace?.canonicalPath || project?.localWorkspace?.path, 2_000);
    if (!workspace) return false;
    const resolved = path.resolve(workspace);
    return resolved === this.currentWorkspace || project?.localWorkspace?.trustStatus === 'approved';
  }

  async ensureRootNote(project) {
    if (!this.canAccessWorkspace(project)) return { created: false, reason: 'workspace_unavailable_or_untrusted' };
    const workspace = path.resolve(project.localWorkspace?.canonicalPath || project.localWorkspace?.path);
    const file = path.join(workspace, 'marcus.txt');
    try { await fs.access(file); return { created: false, path: file }; } catch {}
    const timestamp = nowIso().slice(0, 10);
    const text = [
      'MARCUS PROJECT NOTE',
      '',
      `Project: ${safeString(project.canonicalName, 300)}`,
      `Workspace: ${workspace}`,
      `Status: ${safeString(project.status, 100) || 'active'}`,
      `Last reviewed: ${timestamp}`,
      '',
      'Purpose:',
      safeString(project.currentObjective?.desiredOutcome || project.description || 'Project purpose has not been summarized yet.', 2_000),
      '',
      'Standing instruction:',
      'Read this repository and this note before reporting. Append concise dated context after meaningful work. Keep credentials and raw secrets out.',
      '',
      'Append Log:',
      '',
      timestamp,
      '- MARCUS created this project-memory anchor during project onboarding or memory refresh.',
      '',
    ].join('\n');
    await fs.writeFile(file, text, { encoding: 'utf8', flag: 'wx' }).catch((error) => {
      if (error?.code !== 'EEXIST') throw error;
    });
    return { created: true, path: file };
  }

  async indexProject(project, { createRootNote = true } = {}) {
    const indexedAt = nowIso();
    const workspaceAccessible = this.canAccessWorkspace(project);
    const workspace = workspaceAccessible ? path.resolve(project.localWorkspace?.canonicalPath || project.localWorkspace?.path) : '';
    if (createRootNote && workspaceAccessible) await this.ensureRootNote(project);

    const candidates = [];
    if (workspace) {
      candidates.push({ type: 'marcus_root', file: path.join(workspace, 'marcus.txt') });
      candidates.push({ type: 'readme', file: path.join(workspace, 'README.md') });
      candidates.push({ type: 'package', file: path.join(workspace, 'package.json') });
    }
    const projectSlugs = [...new Set([project?.canonicalName, ...(Array.isArray(project?.aliases) ? project.aliases : [])].map(slug).filter(Boolean))];
    for (const projectSlug of projectSlugs) candidates.push({ type: 'obsidian_project', file: path.join(this.vaultDir, 'projects', `${projectSlug}.md`) });

    const sources = [];
    const seen = new Set();
    for (const candidate of candidates) {
      const key = path.resolve(candidate.file).toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      const loaded = await readableFile(candidate.file);
      if (!loaded) continue;
      sources.push({
        type: candidate.type,
        path: candidate.file,
        hash: hash(loaded.content),
        modifiedAt: loaded.stat.mtime.toISOString(),
        indexedAt,
        excerpt: loaded.content.slice(0, 4_000),
      });
    }

    const repositoryManifest = workspace ? await scanRepository(workspace) : {};
    const contentHash = hash(JSON.stringify({ sources: sources.map((item) => [item.type, item.path, item.hash]), repository: repositoryManifest.contentHash || '' }));
    return {
      status: sources.length || repositoryManifest.fileCount ? 'fresh' : 'unavailable',
      summary: summaryFromSources(sources, project),
      sources,
      repositoryManifest,
      contentHash,
      lastIndexedAt: indexedAt,
      error: sources.length || repositoryManifest.fileCount ? '' : 'No trusted local workspace or matching project note was readable.',
    };
  }

  async appendWorkNote(project, event = {}) {
    if (!this.canAccessWorkspace(project)) return { appended: false, reason: 'workspace_unavailable_or_untrusted' };
    const ensured = await this.ensureRootNote(project);
    const file = ensured.path;
    const eventId = safeString(event.eventId || event.id, 200);
    if (!eventId) return { appended: false, reason: 'event_id_required' };
    const existing = await fs.readFile(file, 'utf8');
    const marker = `[MARCUS event: ${eventId}]`;
    if (existing.includes(marker)) return { appended: false, duplicate: true, path: file };
    const date = safeString(event.recordedAt, 64).slice(0, 10) || nowIso().slice(0, 10);
    const summary = redactSecretLikeText(safeString(event.summary, 2_000));
    const verification = redactSecretLikeText(safeString(event.verification, 1_500));
    const blockers = redactSecretLikeText(safeString(event.blockers, 1_500));
    const lines = [
      '',
      date,
      `- ${summary || 'MARCUS recorded a meaningful project update.'}`,
      event.status ? `- Status: ${safeString(event.status, 80)}` : '',
      verification ? `- Verification: ${verification}` : '',
      blockers ? `- Unresolved: ${blockers}` : '',
      `- ${marker}`,
      '',
    ].filter((line) => line !== '');
    await fs.appendFile(file, `${lines.join('\n')}\n`, 'utf8');
    return { appended: true, path: file };
  }
}

export { scanRepository as scanProjectRepositoryManifest };
