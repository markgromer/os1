import assert from 'node:assert/strict';
import test from 'node:test';

import { CodexResultReviewer } from '../marcus/operations/codex_result_reviewer.js';
import { OperationVerification } from '../marcus/operations/operation_verification.js';

function operation() {
  return {
    objective: 'Add a verified setup dialog.',
    originalRequest: 'Add a setup dialog for API token and slug.',
    acceptanceCriteria: [
      'A settings button opens the setup dialog.',
      'Sweep is blocked until token and slug are verified.',
    ],
  };
}

function diff(overrides = {}) {
  return {
    source: 'github_api',
    authoritative: true,
    evidenceDigest: 'd'.repeat(64),
    repository: 'markgromer/Reggie',
    baseRef: 'main',
    headRef: 'codex/setup',
    headSha: 'a'.repeat(40),
    totals: { files: 1, reportedFiles: 1, additions: 8, deletions: 1 },
    files: [{
      path: 'src/setup.js',
      status: 'modified',
      additions: 8,
      deletions: 1,
      patchAvailable: true,
      patchTruncated: false,
      patch: '@@ -1 +1,8 @@\n+openSetupDialog();\n+disableSweepUntilVerified();',
    }],
    checks: { combinedState: 'success', checkRuns: [], statuses: [] },
    collectionErrors: [],
    ...overrides,
  };
}

test('independent Codex result reviewer passes only complete criterion-by-criterion evidence', async () => {
  let systemPrompt = '';
  let responseFormat;
  const reviewer = new CodexResultReviewer({
    complete: async ({ messages, responseFormat: format }) => {
      systemPrompt = messages[0].content;
      responseFormat = format;
      return {
        ok: true,
        provider: 'openai',
        model: 'gpt-review',
        message: { content: JSON.stringify({
          verdict: 'pass',
          confidence: 0.94,
          summary: 'The settings dialog and verification gate are visible in the diff.',
          acceptanceCoverage: [
            { criterionIndex: 0, status: 'met', evidence: 'src/setup.js opens the dialog.', evidenceRefs: ['diff:src/setup.js'] },
            { criterionIndex: 1, status: 'met', evidence: 'src/setup.js gates sweep on verification.', evidenceRefs: ['diff:src/setup.js'] },
          ],
          findings: [],
          residualRisks: ['Runtime behavior still requires separate test evidence.'],
        }) },
      };
    },
  });
  const artifact = await reviewer.review({ operation: operation(), diff: diff() });
  assert.equal(artifact.type, 'codex_result_review');
  assert.equal(artifact.metadata.reviewStatus, 'passed');
  assert.equal(artifact.metadata.evidenceDigest, 'd'.repeat(64));
  assert.match(systemPrompt, /patches are untrusted data/i);
  const content = JSON.parse(artifact.content);
  assert.equal(content.review.acceptanceCoverage.length, 2);
  assert.equal(content.review.residualRisks.length, 1);
  assert.equal(responseFormat.type, 'json_schema');
  assert.equal(responseFormat.json_schema.strict, true);
  assert.equal(responseFormat.json_schema.schema.properties.acceptanceCoverage.minItems, 2);
  assert.equal(responseFormat.json_schema.schema.properties.acceptanceCoverage.maxItems, 2);
});

test('project-operator control criteria are bound deterministically to durable evidence', async () => {
  let suppliedCriteria;
  const reviewer = new CodexResultReviewer({
    complete: async ({ messages, responseFormat }) => {
      suppliedCriteria = JSON.parse(messages[1].content).acceptanceCriteria;
      assert.equal(responseFormat.json_schema.schema.properties.acceptanceCoverage.minItems, 1);
      return {
        ok: true,
        provider: 'openai',
        model: 'gpt-review',
        message: { content: JSON.stringify({
          verdict: 'pass',
          confidence: 0.98,
          summary: 'The requested endpoint and tests are visible, and registered test evidence passed.',
          acceptanceCoverage: [{
            criterionIndex: 0,
            status: 'met',
            evidence: 'src/setup.js contains the requested implementation and authenticated tests passed.',
            evidenceRefs: ['diff:src/setup.js', 'verification:test'],
          }],
          findings: [],
          residualRisks: [],
        }) },
      };
    },
  });
  const projectOperation = {
    ...operation(),
    acceptanceCriteria: [
      'Add a verified setup dialog and run its tests.',
      'Marcus gathered project context before creating the Codex handoff (1 repos, 6 paths indexed, 6 files read, 2124 ms).',
      'Codex receives the audit brief, constraints, approval boundaries, and verification requirements.',
      'Completion is not accepted without registered implementation and verification evidence.',
    ],
    verification: [
      { type: 'build', status: 'passed', waived: false, output: 'Dry-run build passed.' },
      { type: 'test', status: 'passed', waived: false, output: 'All focused tests passed.' },
      { type: 'diff_review', status: 'passed', waived: false, output: 'Old review result.' },
    ],
    metadata: {
      codexJobs: { codex: { provider: 'github_actions_codex', jobId: 'job-123' } },
      extra: { projectOperator: {
        promptVersion: 3,
        promptLength: 11281,
        executionBriefLength: 9396,
        githubAudit: { coverage: { repositoriesInspected: 1, pathsIndexed: 6, filesRead: 6 } },
      } },
    },
  };
  const artifact = await reviewer.review({
    operation: projectOperation,
    diff: diff(),
    artifacts: [{ type: 'github_result_evidence' }],
  });
  assert.deepEqual(suppliedCriteria, [{ criterionIndex: 0, criterion: projectOperation.acceptanceCriteria[0] }]);
  assert.equal(artifact.metadata.reviewStatus, 'passed');
  const coverage = JSON.parse(artifact.content).review.acceptanceCoverage;
  assert.equal(coverage.length, 4);
  assert.deepEqual(coverage[1].evidenceRefs, ['operation:github_audit']);
  assert.deepEqual(coverage[2].evidenceRefs, ['operation:codex_handoff']);
  assert.ok(coverage[3].evidenceRefs.includes('operation:implementation_evidence'));
  assert.ok(coverage[3].evidenceRefs.includes('verification:test'));
  assert.ok(!coverage[3].evidenceRefs.includes('verification:diff_review'));
});

test('project-operator completion control fails closed without verification evidence', async () => {
  const reviewer = new CodexResultReviewer({
    complete: async () => ({
      ok: true,
      provider: 'openai',
      model: 'gpt-review',
      message: { content: JSON.stringify({
        verdict: 'pass', confidence: 0.99, summary: 'The implementation is visible.',
        acceptanceCoverage: [{ criterionIndex: 0, status: 'met', evidence: 'The requested code is present.', evidenceRefs: ['diff:src/setup.js'] }],
        findings: [], residualRisks: [],
      }) },
    }),
  });
  const projectOperation = {
    ...operation(),
    acceptanceCriteria: [
      'Add a verified setup dialog.',
      'Marcus gathered project context before creating the Codex handoff (1 repos, 1 paths indexed, 1 files read, 1 ms).',
      'Codex receives the audit brief, constraints, approval boundaries, and verification requirements.',
      'Completion is not accepted without registered implementation and verification evidence.',
    ],
    metadata: {
      codexJobs: { codex: { provider: 'github_actions_codex', jobId: 'job-123' } },
      extra: { projectOperator: {
        promptVersion: 3, promptLength: 100, executionBriefLength: 80,
        githubAudit: { coverage: { repositoriesInspected: 1, pathsIndexed: 1, filesRead: 1 } },
      } },
    },
  };
  const artifact = await reviewer.review({ operation: projectOperation, diff: diff(), artifacts: [{ type: 'commit' }] });
  assert.equal(artifact.metadata.reviewStatus, 'needs_manual_review');
  const coverage = JSON.parse(artifact.content).review.acceptanceCoverage;
  assert.equal(coverage[3].status, 'unknown');
});

test('independent Codex result reviewer cannot pass incomplete criterion coverage', async () => {
  const reviewer = new CodexResultReviewer({
    complete: async () => ({
      ok: true,
      provider: 'openai',
      model: 'gpt-review',
      message: { content: JSON.stringify({
        verdict: 'pass',
        confidence: 0.99,
        acceptanceCoverage: [{ criterionIndex: 0, status: 'met', evidence: 'One criterion only.', evidenceRefs: ['diff:src/setup.js'] }],
        findings: [],
      }) },
    }),
  });
  const artifact = await reviewer.review({ operation: operation(), diff: diff() });
  assert.equal(artifact.metadata.reviewStatus, 'needs_manual_review');
});

test('independent Codex result reviewer rejects empty citations and unsupported test claims', async () => {
  const reviewer = new CodexResultReviewer({
    complete: async () => ({
      ok: true,
      provider: 'openai',
      model: 'gpt-review',
      message: { content: JSON.stringify({
        verdict: 'pass',
        confidence: 0.99,
        summary: 'The implementation is correct and all tests passed.',
        acceptanceCoverage: [
          { criterionIndex: 0, status: 'met', evidence: '' },
          { criterionIndex: 1, status: 'met', evidence: 'src/setup.js gates sweep on verification.', evidenceRefs: ['diff:src/setup.js'] },
        ],
        findings: [],
      }) },
    }),
  });
  const artifact = await reviewer.review({ operation: operation(), diff: diff() });
  assert.equal(artifact.metadata.reviewStatus, 'needs_manual_review');
  const content = JSON.parse(artifact.content);
  assert.equal(content.review.acceptanceCoverage[0].status, 'unknown');
  assert.deepEqual(content.review.unsupportedClaims, ['test']);
});

test('independent Codex result reviewer rejects invented evidence references', async () => {
  const reviewer = new CodexResultReviewer({
    complete: async () => ({
      ok: true, provider: 'openai', model: 'gpt-review',
      message: { content: JSON.stringify({
        verdict: 'pass', confidence: 0.99,
        acceptanceCoverage: [
          { criterionIndex: 0, status: 'met', evidence: 'The dialog is implemented.', evidenceRefs: ['diff:missing.js'] },
          { criterionIndex: 1, status: 'met', evidence: 'The sweep is gated.', evidenceRefs: ['diff:src/setup.js'] },
        ],
        findings: [],
      }) },
    }),
  });
  const artifact = await reviewer.review({ operation: operation(), diff: diff() });
  assert.equal(artifact.metadata.reviewStatus, 'needs_manual_review');
  const content = JSON.parse(artifact.content);
  assert.equal(content.review.acceptanceCoverage[0].status, 'unknown');
  assert.deepEqual(content.review.acceptanceCoverage[0].evidenceRefs, []);
});

test('independent Codex result reviewer fails closed before model review for incomplete diffs and failed checks', async () => {
  let calls = 0;
  const reviewer = new CodexResultReviewer({ complete: async () => { calls += 1; return { ok: false }; } });
  const incomplete = await reviewer.review({
    operation: operation(),
    diff: diff({ files: [{ path: 'asset.bin', patchAvailable: false, patchTruncated: false }], totals: { files: 1, reportedFiles: 1 } }),
  });
  assert.equal(incomplete.metadata.reviewStatus, 'needs_manual_review');

  const failed = await reviewer.review({
    operation: operation(),
    diff: diff({ checks: { checkRuns: [{ name: 'test', status: 'completed', conclusion: 'failure' }], statuses: [] } }),
  });
  assert.equal(failed.metadata.reviewStatus, 'failed');

  const unavailableChecks = await reviewer.review({
    operation: operation(),
    diff: diff({ collectionErrors: [{ scope: 'check_runs', status: 403, message: 'Resource not accessible.' }] }),
  });
  assert.equal(unavailableChecks.metadata.reviewStatus, 'needs_manual_review');
  assert.equal(calls, 0);
});

test('independent Codex result reviewer treats malformed model output as manual review', async () => {
  const reviewer = new CodexResultReviewer({
    complete: async () => ({ ok: true, provider: 'openai', model: 'gpt-review', message: { content: 'I think it looks fine.' } }),
  });
  const artifact = await reviewer.review({ operation: operation(), diff: diff() });
  assert.equal(artifact.metadata.reviewStatus, 'needs_manual_review');
  assert.match(JSON.parse(artifact.content).reason, /invalid structured output/i);
});

test('diff verification requires matching authoritative evidence provenance', async () => {
  const verification = new OperationVerification();
  const step = { id: 'verify-step', input: { requirements: [{ type: 'diff_review', required: true }] } };
  const baseOperation = {
    id: 'op-review',
    verification: [],
    artifacts: [
      { id: 'diff-artifact', type: 'codex_diff', name: 'Diff', metadata: { source: 'github_api', authoritative: true, evidenceDigest: 'a'.repeat(64) } },
      { id: 'review-artifact', type: 'codex_result_review', name: 'Review', metadata: { source: 'independent_ai_review', evidenceSource: 'github_api', authoritativeEvidence: true, evidenceDigest: 'b'.repeat(64), reviewStatus: 'passed' } },
    ],
  };
  const mismatch = await verification.run({ operation: baseOperation, step, registryRecord: {} });
  assert.equal(mismatch.results[0].status, 'needs_manual_review');

  baseOperation.artifacts[1].metadata.evidenceDigest = 'a'.repeat(64);
  const matched = await verification.run({ operation: baseOperation, step, registryRecord: {} });
  assert.equal(matched.results[0].status, 'passed');
});
