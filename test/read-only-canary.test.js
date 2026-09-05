import test from 'node:test';
import assert from 'node:assert/strict';
import { chooseReadOnlyRoute, keyFingerprint, runReadOnlyCanary } from '../marcus/models/read_only_canary.js';
import { dashboardPreviewMessages, validateDashboardPreview } from '../marcus/models/dashboard_preview.js';

const baseline = { provider: 'openai', model: 'gpt-4.1-mini', apiKey: 'fixture' };
const profile = { model: 'gpt-6-astra', qualification: { accessStatus: 'verified', evaluationStatus: 'passed', qualifiedWorkloads: ['dashboardPreview'], shadowPassedWorkloads: ['dashboardPreview'] },
  rollout: { status: 'canary', trafficPercent: 10, enabledWorkloads: ['dashboardPreview'], credentialFingerprint: keyFingerprint('fixture') } };
const sampledId = Array.from({ length: 100 }, (_, i) => String(i)).find((requestId) => chooseReadOnlyRoute({ baseline, workload: 'dashboardPreview', requestId, profile }).canary);

test('a manually configured GPT-6 baseline cannot bypass sampling, credential qualification or the kill switch', async () => {
  for (const model of ['gpt-6-astra', 'openai/gpt-6-astra', 'gpt-6-astra-2026-09-04']) {
    for (const disabled of [false, true]) {
      let calls = 0;
      const result = await runReadOnlyCanary({ baseline: { ...baseline, model }, workload: 'dashboardPreview',
        requestId: sampledId, profile, disabled, messages: [], validate: () => ({ ok: true }),
        complete: async () => { calls++; return { ok: true }; } });
      assert.equal(result.ok, false);
      assert.equal(calls, 0);
    }
  }
});

test('canary is credential, route, and sample bound; kill switch restores the baseline', () => {
  assert.ok(sampledId);
  for (const patch of [{ disabled: true }, { workload: 'marcusChat' }, { baseline: { ...baseline, apiKey: 'different' } }]) {
    assert.equal(chooseReadOnlyRoute({ baseline, workload: 'dashboardPreview', requestId: sampledId, profile, ...patch }).canary, false);
  }
  const count = Array.from({ length: 1000 }, (_, requestId) => chooseReadOnlyRoute({ baseline, workload: 'dashboardPreview', requestId, profile }).canary).filter(Boolean).length;
  assert.ok(count > 50 && count < 150);
});

test('canary failure reuses the exact baseline without changing configuration or sending tools', async () => {
  const models = [];
  const result = await runReadOnlyCanary({ baseline, workload: 'dashboardPreview', requestId: sampledId, profile,
    messages: dashboardPreviewMessages([], []), validate: (value) => validateDashboardPreview(value, [], []),
    complete: async (request) => {
      assert.equal(request.tools, undefined);
      assert.equal(request.deadlineMs, 20_000);
      models.push(request.route.model);
      return request.route.model === 'gpt-6-astra' ? { ok: false } : { ok: true, message: { content: '{"tasks":{},"inbox":{}}' } };
    } });
  assert.equal(result.ok, true);
  assert.deepEqual(models, ['gpt-6-astra', baseline.model]);
  assert.equal(baseline.model, 'gpt-4.1-mini');
});

test('preview validation rejects invented IDs, tool calls, and malformed output', () => {
  const tasks = [{ id: 't1' }];
  for (const message of [
    { content: '{"tasks":{},"inbox":{}}' },
    { content: '{"tasks":{"t1":{"title":"Title","summary":"Summary"}},"inbox":{},"extra":1}' },
    { content: 'PWNED' },
    { content: '{}', tool_calls: [{ id: 'call' }] },
  ]) assert.equal(validateDashboardPreview({ ok: true, message }, tasks, []).ok, false);
});
