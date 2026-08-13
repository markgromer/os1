import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const {
  createPcAccessPolicy,
  isSensitivePath,
  listPcDirectory,
  readPcTextFile,
  searchPcFiles,
  toDesktopActionOutcome,
  validatePcPath,
} = require('../desktop-pc-operator.cjs');

async function withFixture(callback) {
  const base = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-pc-operator-'));
  const root = path.join(base, 'authorized');
  const sibling = path.join(base, 'unauthorized');
  await fs.mkdir(path.join(root, 'Projects', 'Scoop Fairies'), { recursive: true });
  await fs.mkdir(sibling, { recursive: true });
  await fs.writeFile(path.join(root, 'Projects', 'Scoop Fairies', 'README.md'), '# Scoop Fairies\n');
  await fs.writeFile(path.join(root, '.env'), 'OPENAI_API_KEY=never-relay\n');
  await fs.writeFile(path.join(sibling, 'outside.txt'), 'outside\n');
  try { await callback({ base, root, sibling }); }
  finally { await fs.rm(base, { recursive: true, force: true }); }
}

test('PC policy searches bounded roots and refuses path-prefix escape', async () => {
  await withFixture(async ({ root, sibling }) => {
    const policy = createPcAccessPolicy({ workspaceRoots: [root] });
    assert.equal(policy.fullPcAccess, false);
    assert.deepEqual(policy.roots, [await fs.realpath(root)]);

    const found = searchPcFiles({ query: 'scoop fairies', limit: 10 }, policy);
    assert.equal(found.ok, true);
    assert.equal(found.results.length, 1);
    assert.equal(found.results[0].type, 'directory');
    assert.match(found.results[0].path, /Scoop Fairies$/);

    const escaped = validatePcPath(path.join(sibling, 'outside.txt'), policy, { kind: 'file' });
    assert.equal(escaped.ok, false);
    assert.match(escaped.error, /outside the authorized PC roots/i);
  });
});

test('PC text reads are bounded and credential-bearing paths are not relayed', async () => {
  await withFixture(async ({ root }) => {
    const policy = createPcAccessPolicy({ fullPcAccess: true, pcAccessRoots: [root] });
    const readme = readPcTextFile({ path: path.join(root, 'Projects', 'Scoop Fairies', 'README.md') }, policy);
    assert.equal(readme.ok, true);
    assert.equal(readme.content, '# Scoop Fairies\n');

    const secretPath = path.join(root, '.env');
    assert.equal(isSensitivePath(secretPath), true);
    const secret = readPcTextFile({ path: secretPath }, policy);
    assert.equal(secret.ok, false);
    assert.equal(secret.sensitive, true);
    assert.equal(secret.approvalRequired, true);
    assert.doesNotMatch(JSON.stringify(secret), /never-relay/);
  });
});

test('PC directory listing reports type and sensitivity without reading content', async () => {
  await withFixture(async ({ root }) => {
    const policy = createPcAccessPolicy({ fullPcAccess: true, pcAccessRoots: [root] });
    const listing = listPcDirectory({ path: root }, policy);
    assert.equal(listing.ok, true);
    assert.equal(listing.entries.some((entry) => entry.name === 'Projects' && entry.type === 'directory'), true);
    assert.equal(listing.entries.some((entry) => entry.name === '.env' && entry.sensitive === true), true);
  });
});

test('PC operator outcomes preserve bounded evidence in the desktop result envelope', async () => {
  await withFixture(async ({ root }) => {
    const policy = createPcAccessPolicy({ fullPcAccess: true, pcAccessRoots: [root] });
    const search = toDesktopActionOutcome(searchPcFiles({ query: 'scoop fairies', limit: 10 }, policy));
    assert.equal(search.ok, true);
    assert.equal(search.details.results[0].name, 'Scoop Fairies');

    const read = toDesktopActionOutcome(readPcTextFile({ path: path.join(root, 'Projects', 'Scoop Fairies', 'README.md') }, policy));
    assert.equal(read.ok, true);
    assert.equal(read.details.content, '# Scoop Fairies\n');

    const refused = toDesktopActionOutcome(readPcTextFile({ path: path.join(root, '.env') }, policy));
    assert.equal(refused.ok, false);
    assert.equal(refused.details.sensitive, true);
    assert.equal(refused.details.approvalRequired, true);
    assert.doesNotMatch(JSON.stringify(refused), /never-relay/);
  });
});
