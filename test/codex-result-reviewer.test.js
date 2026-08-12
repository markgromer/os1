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
  const reviewer = new CodexResultReviewer({
    complete: async ({ messages }) => {
      systemPrompt = messages[0].content;
      return {
        ok: true,
        provider: 'openai',
        model: 'gpt-review',
        message: { content: JSON.stringify({
          verdict: 'pass',
          confidence: 0.94,
          summary: 'The settings dialog and verification gate are visible in the diff.',
          acceptanceCoverage: [
            { criterionIndex: 0, status: 'met', evidence: 'src/setup.js opens the dialog.' },
            { criterionIndex: 1, status: 'met', evidence: 'src/setup.js gates sweep on verification.' },
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
        acceptanceCoverage: [{ criterionIndex: 0, status: 'met', evidence: 'One criterion only.' }],
        findings: [],
      }) },
    }),
  });
  const artifact = await reviewer.review({ operation: operation(), diff: diff() });
  assert.equal(artifact.metadata.reviewStatus, 'needs_manual_review');
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
