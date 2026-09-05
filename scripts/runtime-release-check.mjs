import fs from 'node:fs/promises';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = path.resolve(fileURLToPath(new URL('..', import.meta.url)));

export function privateRuntimePaths(files) {
  const patterns = [
    /^(?:constitution|AGENTS\.md)$/i,
    /^marcus\/constitution\//i,
    /^docs\/marcus\/(?:constitution-[^/]+|gpt-6-(?:qualification|readiness))\.md$/i,
    /^scripts\/constitution-[^/]+\.mjs$/i,
    /^test\/constitution-program\.test\.js$/i,
    /^output\/(?:model-evals|constitution-acceptance|constitution-checks|worktrees)\//i,
    /(?:^|\/)(?:data|node_modules)\//i,
    /(?:^|\/)\.env(?:$|\.(?!example$|sample$))/i,
  ];
  return files.filter((file) => patterns.some((pattern) => pattern.test(String(file).replaceAll('\\', '/').replace(/^\.\//, ''))));
}

export async function inspectRuntimePackage(directory = root) {
  // Check the actual Git index, not just ignore rules: forced additions must fail CI.
  const tracked = execFileSync('git', ['ls-files', '--cached', '-z'], { cwd: directory, encoding: 'utf8', windowsHide: true }).split('\0').filter(Boolean);
  const errors = privateRuntimePaths(tracked).map((file) => `Private/local artifact is tracked: ${file}`);
  const manifest = JSON.parse(await fs.readFile(path.join(directory, 'package.json'), 'utf8'));
  for (const [name, command] of Object.entries(manifest.scripts || {})) {
    if (name.startsWith('constitution:') || /scripts[\\/]constitution-/.test(command)) errors.push(`Runtime script depends on the private framework: ${name}`);
  }
  const profiles = JSON.parse(await fs.readFile(path.join(directory, 'marcus/models/model_profiles.json'), 'utf8'));
  for (const profile of profiles.profiles || []) {
    for (const evidence of profile.qualification?.evidence || []) {
      if (!String(evidence).startsWith('private:')) errors.push('Runtime qualification must reference the private ledger without bundling its archive.');
    }
  }
  return { trackedFiles: tracked.length, errors };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const report = await inspectRuntimePackage();
  if (report.errors.length) {
    console.error(report.errors.join('\n'));
    process.exitCode = 1;
  } else console.log(`Runtime packaging passed: ${report.trackedFiles} tracked files; no private design/evaluation archive.`);
}
