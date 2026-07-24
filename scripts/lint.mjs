import { execFile } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import { promisify } from 'node:util';

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

console.log(`Syntax lint passed for ${new Set(files).size} JavaScript files.`);
