import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';

test('live MARCUS prompt keeps the social persona distinct from Mark', async () => {
  const source = await fs.readFile(path.join(process.cwd(), 'server.js'), 'utf8');

  assert.match(source, /adopt MARCUS's public persona exclusively/i);
  assert.match(source, /own first-hand experience working with Mark/i);
  assert.match(source, /Never impersonate Mark/i);
  assert.match(source, /gives the person a way to reach their own conclusion/i);
  assert.match(source, /Skip automatic validation, hype, motivational filler/i);
});
