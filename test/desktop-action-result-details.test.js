import test from 'node:test';
import assert from 'node:assert/strict';

import { normalizeDesktopActionDetails } from '../marcus/desktop/action_result_details.js';

test('community observation results preserve bounded structured evidence above the default limit', () => {
  const observations = Array.from({ length: 30 }, (_, index) => ({
    sourceKey: `source-${index}`,
    member: { displayName: `Member ${index}` },
    contentSummary: `Specific visible community evidence ${index}. ${'detail '.repeat(180)}`,
  }));
  const details = normalizeDesktopActionDetails('marcus-browser-command', {
    command: 'observe-community',
    result: { observations },
  });

  assert.equal(details.truncated, undefined);
  assert.equal(details.result.observations.length, 30);
});

test('unrelated desktop action results retain the existing conservative limit', () => {
  const details = normalizeDesktopActionDetails('pc-read-text-file', {
    content: 'x'.repeat(25_000),
  });

  assert.equal(details.truncated, true);
  assert.equal(details.preview.length, 20_000);
});

