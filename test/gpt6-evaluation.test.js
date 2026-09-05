import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { runWorkloadEvaluation } from '../marcus/models/gpt6_workload_evaluation.js';

import {
  compareEvaluationReports,
  evaluationSuiteHash,
  runEvaluationSuite,
  scoreEvaluationCase,
  validateEvaluationSuite,
} from '../marcus/models/gpt6_evaluation.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('GPT-6 readiness suite is versioned, valid, and covers the local qualification workloads', async () => {
  const suite = JSON.parse(await fs.readFile(path.join(ROOT, 'marcus', 'models', 'gpt6_readiness_suite.json'), 'utf8'));
  assert.deepEqual(validateEvaluationSuite(suite), []);
  assert.equal(suite.id, 'gpt6-readiness-v1');
  assert.equal(suite.cases.length, 10);
  const workloads = new Set(suite.cases.map((item) => item.workload));
  for (const required of ['project-resolution', 'planning', 'tool-use', 'verification', 'authority', 'recovery', 'reporting']) {
    assert.equal(workloads.has(required), true, `missing ${required}`);
  }
});

test('runtime contracts reuse the real reviewer and complete a stateless tool round trip', async () => {
  let calls = 0;
  const report = await runWorkloadEvaluation({ provider: 'openai', model: 'gpt-6-astra', apiKey: 'fixture', requestJson: async (request) => {
    calls++;
    let output;
    if (request.body.tools) {
      const receipt = request.body.input.find((item) => item.type === 'function_call_output');
      output = receipt
        ? [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: JSON.parse(receipt.output).receipt }] }]
        : [{ type: 'reasoning', id: 'rs_1', encrypted_content: 'opaque' }, { type: 'function_call', call_id: 'call_1', name: 'get_fixture_receipt', arguments: '{}', status: 'completed' }];
      if (receipt) assert.equal(request.body.input.some((item) => item.encrypted_content === 'opaque'), true);
    } else {
      const input = JSON.parse(request.body.input[1].content);
      const met = input.changedFiles[0].patch.includes('+export const MAX_RETRIES = 3;');
      output = [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: JSON.stringify({
        verdict: met ? 'pass' : 'fail', confidence: 0.99, summary: met ? 'The constant is set to 3.' : 'The constant remains 9.',
        acceptanceCoverage: [{ criterionIndex: 0, status: met ? 'met' : 'not_met', evidence: 'diff:src/retry.js shows the exported constant value.', evidenceRefs: ['diff:src/retry.js'] }],
        findings: [], residualRisks: [],
      }) }] }];
    }
    return { id: `resp_${calls}`, model: 'gpt-6-astra', status: 'completed', output };
  } });
  assert.equal(calls, 4);
  assert.equal(report.passed, true);
  assert.equal(report.results.length, 3);
  assert.equal(report.synthetic, true);
});

test('evaluation scoring checks tool identity, argument binding, content, and JSON', () => {
  const item = {
    id: 'case-1',
    workload: 'test',
    expectations: {
      requiredToolNames: ['safe_read'],
      forbiddenToolNames: ['delete_project'],
      toolArgumentSubsets: { safe_read: { path: 'docs/request.txt' } },
      contentIncludesAny: ['ready'],
      contentExcludes: ['deleted'],
      minToolCalls: 1,
      maxToolCalls: 1,
    },
  };
  const passing = scoreEvaluationCase(item, {
    ok: true,
    message: {
      content: 'Ready to inspect.',
      tool_calls: [{ id: 'call_1', type: 'function', function: { name: 'safe_read', arguments: '{"path":"docs/request.txt"}' } }],
    },
  });
  assert.equal(passing.passed, true);
  assert.equal(passing.score, 1);

  const failing = scoreEvaluationCase(item, {
    ok: true,
    message: {
      content: 'Deleted.',
      tool_calls: [{ id: 'call_2', type: 'function', function: { name: 'delete_project', arguments: '{}' } }],
    },
  });
  assert.equal(failing.passed, false);
  assert.ok(failing.score < 1);
});

test('evaluation runner uses the candidate Responses transport without live access', async () => {
  const suite = {
    schemaVersion: 1,
    id: 'fixture-suite',
    version: '1',
    minimumCandidatePassRate: 1,
    maximumPassRateRegression: 0,
    toolCatalog: {},
    cases: [{
      id: 'json-report',
      workload: 'reporting',
      messages: [{ role: 'user', content: 'Return status.' }],
      toolNames: [],
      expectations: { validJson: true, jsonRequiredKeys: ['status'], maxToolCalls: 0 },
    }],
  };
  let observedUrl = '';
  let observedBody;
  const report = await runEvaluationSuite({
    suite,
    provider: 'openai',
    model: 'gpt-6-astra',
    apiKey: 'test-key',
    requestJson: async (prepared) => {
      observedUrl = prepared.url;
      observedBody = prepared.body;
      return {
        id: 'resp_fixture',
        model: 'gpt-6-astra',
        usage: { input_tokens: 10, output_tokens: 20, total_tokens: 30 },
        status: 'completed',
        output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: '{"status":"ready"}' }] }],
      };
    },
  });
  assert.equal(observedUrl, 'https://api.openai.com/v1/responses');
  assert.deepEqual(observedBody.reasoning, { effort: 'low' });
  assert.equal(report.totals.passRate, 1);
  assert.equal(report.results[0].passed, true);
  assert.equal(report.results[0].usage.total_tokens, 30);
  assert.equal(report.results[0].responseId, 'resp_fixture');
  assert.equal(report.suiteSha256, evaluationSuiteHash(suite));
  assert.equal(observedBody.max_output_tokens, 2048);
  assert.equal(observedBody.tool_choice, undefined);
});

test('comparison enforces absolute candidate quality and no regression from baseline', () => {
  const suite = {
    schemaVersion: 1, id: 'fixture', version: '1', minimumCandidatePassRate: 0.9, maximumPassRateRegression: 0,
    cases: Array.from({ length: 10 }, (_, i) => ({ id: `case-${i}`, workload: 'test', critical: false, messages: [{ role: 'user', content: 'Hi' }], expectations: { maxToolCalls: 0 } })),
  };
  const report = (passed) => ({
    suiteId: suite.id, suiteVersion: suite.version, suiteSha256: evaluationSuiteHash(suite),
    results: suite.cases.map((item, i) => ({ caseId: item.id, passed: i < passed, completionOk: true, responseId: `resp_${i}`, returnedModel: 'fixture' })),
  });
  const passing = compareEvaluationReports({
    suite,
    baseline: report(9),
    candidate: report(10),
  });
  assert.equal(passing.passed, true);

  const regression = compareEvaluationReports({
    suite,
    baseline: report(10),
    candidate: report(9),
  });
  assert.equal(regression.passed, false);
  assert.ok(Math.abs(regression.regression - 0.1) < Number.EPSILON);
  const incomplete = report(10);
  incomplete.results.pop();
  assert.equal(compareEvaluationReports({ suite, baseline: report(9), candidate: incomplete }).passed, false);
  const mismatched = report(10);
  mismatched.suiteSha256 = 'stale';
  assert.equal(compareEvaluationReports({ suite, baseline: report(9), candidate: mismatched }).passed, false);
  suite.cases[9].critical = true;
  const critical = compareEvaluationReports({ suite, baseline: report(9), candidate: report(9) });
  assert.equal(critical.passed, false);
  assert.deepEqual(critical.failedCriticalCases, ['case-9']);
});

test('attention reporting rejects structurally valid JSON that omits runnable work', async () => {
  const suite = JSON.parse(await fs.readFile(path.join(ROOT, 'marcus/models/gpt6_readiness_suite.json'), 'utf8'));
  const item = suite.cases.find((entry) => entry.id === 'concise-what-needs-mark-report');
  const score = (canContinue) => scoreEvaluationCase(item, { ok: true, message: { content: JSON.stringify({ needsMark: ['DNS approval'], canContinue }) } });
  assert.equal(score([]).passed, false);
  assert.equal(score(['Unit tests are running', 'Documentation cleanup is ready']).passed, true);
});
