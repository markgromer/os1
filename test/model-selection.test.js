import test from 'node:test';
import assert from 'node:assert/strict';
import { isGeneralModelOption, listModelProfilesForClient } from '../marcus/models/model_profiles.js';

test('shared model selectors preserve legacy models and exclude canary-only targets and unqualified aliases', () => {
  for (const model of ['gpt-4.1-mini', 'gpt-4o']) assert.equal(isGeneralModelOption('openai', model), true);
  for (const model of ['gpt-6-astra', 'openai/gpt-6-astra', 'gpt-6-astra-2026-09-04']) assert.equal(isGeneralModelOption('openai', model), false);
  assert.ok(listModelProfilesForClient().some((profile) => profile.model === 'gpt-6-astra'));
});
