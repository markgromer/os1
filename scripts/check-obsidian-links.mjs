import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repositoryRoot = path.resolve(fileURLToPath(new URL('..', import.meta.url)));
const defaultVaultRoot = path.join(repositoryRoot, 'docs', 'marcus');

// Intentionally unresolved future-facing notes must be listed here with a reason.
// Prefer creating a concise placeholder/index note instead of adding to this list.
export const ALLOWED_UNRESOLVED_LINKS = new Map([
]);

async function collectMarkdownFiles(directory) {
  const output = [];
  for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      output.push(...await collectMarkdownFiles(full));
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith('.md')) {
      output.push(full);
    }
  }
  return output;
}

export function normalizeWikiTarget(rawTarget) {
  const withoutAlias = String(rawTarget || '').split('|')[0];
  const withoutAnchor = withoutAlias.split('#')[0];
  const normalizedSlashes = withoutAnchor.replaceAll('\\', '/').trim();
  const lastSegment = normalizedSlashes.split('/').filter(Boolean).at(-1) || '';
  return lastSegment.replace(/\.md$/i, '').trim();
}

export function extractWikiLinks(markdown) {
  const links = [];
  const pattern = /\[\[([^\]]+)\]\]/g;
  let match;
  while ((match = pattern.exec(markdown)) !== null) {
    const target = normalizeWikiTarget(match[1]);
    if (target) links.push({ raw: match[0], target });
  }
  return links;
}

export async function checkObsidianLinks({
  vaultRoot = defaultVaultRoot,
  allowlist = ALLOWED_UNRESOLVED_LINKS,
} = {}) {
  const files = await collectMarkdownFiles(vaultRoot);
  const basenames = new Set(files.map((file) => path.basename(file, '.md')));
  const unresolved = [];

  for (const file of files) {
    const markdown = await fs.readFile(file, 'utf8');
    const relativeFile = path.relative(repositoryRoot, file).replaceAll(path.sep, '/');
    for (const link of extractWikiLinks(markdown)) {
      if (basenames.has(link.target)) continue;
      const reason = allowlist.get(link.target);
      if (reason) continue;
      unresolved.push({
        file: relativeFile,
        target: link.target,
        raw: link.raw,
      });
    }
  }

  return {
    vaultRoot,
    filesChecked: files.length,
    linksChecked: files.length
      ? (await Promise.all(files.map(async (file) => extractWikiLinks(await fs.readFile(file, 'utf8')).length)))
        .reduce((sum, count) => sum + count, 0)
      : 0,
    unresolved,
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = await checkObsidianLinks();
  if (result.unresolved.length) {
    console.error(`Obsidian wiki-link check failed: ${result.unresolved.length} unresolved link(s).`);
    for (const item of result.unresolved) {
      console.error(`- ${item.file}: ${item.raw} -> ${item.target}`);
    }
    process.exitCode = 1;
  } else {
    console.log(`Obsidian wiki-link check passed: ${result.linksChecked} link(s) across ${result.filesChecked} Markdown file(s).`);
  }
}
