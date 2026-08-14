import { execFile } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import { promisify } from 'node:util';
import { checkObsidianLinks } from './check-obsidian-links.mjs';

const execFileAsync = promisify(execFile);
const root = process.cwd();
const files = ['server.js', 'mcpClient.js', 'desktop-agent.cjs', 'public/app.js'];

async function collect(directory) {
  const output = [];
  for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) output.push(...await collect(full));
    else if (/\.(?:js|cjs|mjs)$/.test(entry.name)) output.push(path.relative(root, full));
  }
  return output;
}

files.push(...await collect(path.join(root, 'marcus')));
files.push(...await collect(path.join(root, 'test')));

for (const file of [...new Set(files)].sort()) {
  await execFileAsync(process.execPath, ['--check', file], { cwd: root });
}

const obsidianResult = await checkObsidianLinks();
if (obsidianResult.unresolved.length) {
  for (const item of obsidianResult.unresolved) {
    console.error(`Unresolved Obsidian link in ${item.file}: ${item.raw} -> ${item.target}`);
  }
  process.exitCode = 1;
  throw new Error(`Obsidian wiki-link check failed with ${obsidianResult.unresolved.length} unresolved link(s).`);
}

console.log(`Syntax lint passed for ${new Set(files).size} JavaScript files.`);
console.log(`Obsidian wiki-link check passed for ${obsidianResult.linksChecked} link(s) across ${obsidianResult.filesChecked} Markdown file(s).`);
