import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repositoryRoot = path.resolve(fileURLToPath(new URL('..', import.meta.url)));
const defaultVaultRoot = path.join(repositoryRoot, 'docs', 'marcus');

const FOLDER_TAGS = Object.freeze({
  daily: '#daily',
  projects: '#project',
  people: '#person',
  clients: '#client',
  money: '#money',
  decisions: '#decision',
  workflows: '#workflow',
  inbox: '#inbox',
  schedule: '#schedule',
  conversations: '#conversation',
  workload: '#workload',
  status: '#status',
  sources: '#source',
});

const SECRET_PATTERNS = [
  /\b(?:sk|ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{16,}\b/i,
  /-----BEGIN [^-]*(?:PRIVATE KEY|CERTIFICATE)-----/i,
  /\b(?:api[_-]?key|token|password|secret)\s*[:=]\s*["']?[^"'\s]{8,}/i,
];

async function collectMarkdownFiles(directory) {
  const output = [];
  for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) output.push(...await collectMarkdownFiles(full));
    else if (entry.isFile() && entry.name.toLowerCase().endsWith('.md')) output.push(full);
  }
  return output;
}

function relativePath(file) {
  return path.relative(repositoryRoot, file).replaceAll(path.sep, '/');
}

function noteSlug(file) {
  return path.basename(file, '.md').toLowerCase();
}

function extractTags(markdown) {
  const line = markdown.split(/\r?\n/).find((item) => /^tags:\s*/i.test(item.trim()));
  if (!line) return [];
  return [...new Set((line.match(/#[A-Za-z0-9][A-Za-z0-9/_-]*/g) || []).map((tag) => tag.toLowerCase()))];
}

function wikiTargets(markdown) {
  const targets = [];
  const pattern = /\[\[([^\]]+)\]\]/g;
  let match;
  while ((match = pattern.exec(markdown)) !== null) {
    const raw = String(match[1] || '').split('|')[0].split('#')[0].replaceAll('\\', '/').trim();
    const target = raw.split('/').filter(Boolean).at(-1)?.replace(/\.md$/i, '').toLowerCase();
    if (target) targets.push(target);
  }
  return new Set(targets);
}

function isTemplate(file) {
  return relativePath(file).includes('/templates/');
}

function isIndex(file) {
  return /(?:^|\/)[a-z-]+-index\.md$/i.test(relativePath(file));
}

function folderKey(file, vaultRoot) {
  const relative = path.relative(vaultRoot, file).replaceAll(path.sep, '/');
  const first = relative.split('/')[0];
  return relative.includes('/') ? first : '';
}

function hasSecretLikeText(markdown) {
  const withoutCodeBlocks = markdown.replace(/```[\s\S]*?```/g, '');
  return SECRET_PATTERNS.some((pattern) => pattern.test(withoutCodeBlocks));
}

export async function checkMarcusNotes({ vaultRoot = defaultVaultRoot } = {}) {
  const files = await collectMarkdownFiles(vaultRoot);
  const issues = [];

  for (const file of files) {
    const markdown = await fs.readFile(file, 'utf8');
    const rel = relativePath(file);
    const tags = extractTags(markdown);
    const tagSet = new Set(tags);
    const targets = wikiTargets(markdown);
    const folder = folderKey(file, vaultRoot);
    const slug = noteSlug(file);

    if (hasSecretLikeText(markdown)) {
      issues.push({ file: rel, code: 'secret_like_text', message: 'Note contains credential-like text.' });
    }

    if (isTemplate(file)) continue;

    const requiresStatus = rel !== 'docs/marcus/README.md';
    if (requiresStatus && !/^status:\s*\S+/im.test(markdown)) {
      issues.push({ file: rel, code: 'missing_status', message: 'Operational notes need a Status line.' });
    }

    const requiredTag = FOLDER_TAGS[folder];
    if (requiredTag && !isIndex(file) && !tagSet.has(requiredTag)) {
      issues.push({ file: rel, code: 'missing_type_tag', message: `Expected ${requiredTag} for notes in ${folder}/.` });
    }

    if (folder === 'projects' && !isIndex(file)) {
      const projectTag = `#project/${slug}`;
      if (!tagSet.has(projectTag)) {
        issues.push({ file: rel, code: 'missing_project_slug_tag', message: `Expected stable project tag ${projectTag}.` });
      }
    }

    for (const tag of tags) {
      const match = tag.match(/^#project\/([a-z0-9][a-z0-9_-]*)$/);
      if (!match) continue;
      const projectSlug = match[1];
      if (folder === 'projects' && slug === projectSlug) continue;
      if (!targets.has(projectSlug)) {
        issues.push({
          file: rel,
          code: 'project_tag_without_link',
          message: `Project tag ${tag} needs a matching wiki link [[${projectSlug}]].`,
        });
      }
    }
  }

  return {
    vaultRoot,
    filesChecked: files.length,
    issues,
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = await checkMarcusNotes();
  if (result.issues.length) {
    console.error(`Marcus note check failed: ${result.issues.length} issue(s).`);
    for (const item of result.issues) {
      console.error(`- ${item.file}: ${item.code}: ${item.message}`);
    }
    process.exitCode = 1;
  } else {
    console.log(`Marcus note check passed across ${result.filesChecked} Markdown file(s).`);
  }
}
