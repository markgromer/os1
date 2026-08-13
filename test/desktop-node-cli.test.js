import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { localPackageBinInvocation, npmCliInvocation } = require('../desktop-node-cli.cjs');

test('npm scripts invoke the JavaScript CLI through node instead of npm.cmd', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-node-cli-'));
  const execPath = path.join(root, 'node.exe');
  const cliPath = path.join(root, 'node_modules', 'npm', 'bin', 'npm-cli.js');
  await fs.mkdir(path.dirname(cliPath), { recursive: true });
  await fs.writeFile(cliPath, '');
  try {
    const invocation = npmCliInvocation(['run', 'test'], { execPath });
    assert.equal(invocation.ok, true);
    assert.equal(invocation.command, execPath);
    assert.deepEqual(invocation.args, [cliPath, 'run', 'test']);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('Cloudflare deploy resolves the reviewed local Wrangler binary without a shell', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-wrangler-cli-'));
  const packageRoot = path.join(root, 'node_modules', 'wrangler');
  const binaryPath = path.join(packageRoot, 'bin', 'wrangler.js');
  await fs.mkdir(path.dirname(binaryPath), { recursive: true });
  await fs.writeFile(path.join(packageRoot, 'package.json'), JSON.stringify({ name: 'wrangler', bin: { wrangler: 'bin/wrangler.js' } }));
  await fs.writeFile(binaryPath, '');
  try {
    const invocation = localPackageBinInvocation(root, 'wrangler', 'wrangler', ['deploy'], { execPath: 'C:\\Node\\node.exe' });
    assert.equal(invocation.ok, true);
    assert.equal(invocation.command, 'C:\\Node\\node.exe');
    assert.deepEqual(invocation.args, [await fs.realpath(binaryPath), 'deploy']);

    await fs.writeFile(path.join(packageRoot, 'package.json'), JSON.stringify({ name: 'wrangler', bin: { wrangler: '../../outside.js' } }));
    const escaped = localPackageBinInvocation(root, 'wrangler', 'wrangler', ['deploy']);
    assert.equal(escaped.ok, false);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
