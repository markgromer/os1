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
  assert.match(source, /earn attention rather than ask for engagement/i);
  assert.match(source, /Google\+, generic founder content, or ChatGPT thought-leadership/i);
  assert.match(source, /Before drafting a new community post, call it in the current conversation/i);
  assert.match(source, /Default to no poll/i);
  assert.match(source, /rewrite and retry immediately in the same turn/i);
  assert.match(source, /names the person, tool, number, decision, or consequence/i);
  assert.match(source, /browserIntent === 'marcus_browser_prepare_post' \? 9 : 4/i);
  assert.match(source, /forcedLiveTool = 'marcus_browser_prepare_post'/i);
});
