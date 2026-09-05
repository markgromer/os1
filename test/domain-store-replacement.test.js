import test from 'node:test';
import assert from 'node:assert/strict';
import { replaceDomainFile } from '../marcus/operations/domain_store.js';

test('domain replacement retries transient Windows locks without repeating the transaction', async () => {
  let attempts = 0; const delays = [];
  await replaceDomainFile('temporary', 'primary', { platform: 'win32', delay: async (ms) => delays.push(ms), rename: async (from, to) => {
    assert.equal(from, 'temporary'); assert.equal(to, 'primary');
    if (++attempts < 3) throw Object.assign(new Error('Locked'), { code: 'EPERM' });
  } });
  assert.equal(attempts, 3); assert.deepEqual(delays, [20, 40]);
});

test('domain replacement preserves bounded failure and does not retry permanent or non-Windows errors', async () => {
  for (const [platform, code, expected] of [['win32', 'EACCES', 6], ['win32', 'ENOSPC', 1], ['linux', 'EPERM', 1]]) {
    let attempts = 0; const error = Object.assign(new Error(code), { code });
    await assert.rejects(replaceDomainFile('temporary', 'primary', { platform, delay: async () => {}, rename: async () => { attempts++; throw error; } }), (actual) => actual === error);
    assert.equal(attempts, expected);
  }
});
