import test from 'node:test';
import assert from 'node:assert/strict';
import { privateRuntimePaths, inspectRuntimePackage } from '../scripts/runtime-release-check.mjs';

test('runtime packaging rejects private framework, evidence, secrets and generated worktrees', () => {
  const privateFiles = ['constitution', 'AGENTS.md', 'marcus/constitution/program.json', 'docs/marcus/constitution-kernel.md', 'docs/marcus/gpt-6-qualification.md', 'scripts/constitution-program.mjs', 'test/constitution-program.test.js', 'output/model-evals/result.json', 'output/constitution-acceptance/receipt.json', 'output/constitution-checks/report.json', 'output/worktrees/private/file.js', 'data/settings.json', '.env', '.env.production', 'node_modules/package/index.js'];
  assert.deepEqual(privateRuntimePaths(privateFiles), privateFiles);
  assert.deepEqual(privateRuntimePaths(['.\\docs\\marcus\\constitution-audit.md']), ['.\\docs\\marcus\\constitution-audit.md']);
  assert.deepEqual(privateRuntimePaths(['server.js', 'marcus/work/work_graph.js', 'marcus/models/model_profiles.json', 'test/constitution-backups.test.js', 'docs/marcus/README.md', '.env.example']), []);
});

test('the deployable package has no private framework dependency or tracked archive', async () => {
  const report = await inspectRuntimePackage();
  assert.deepEqual(report.errors, []);
  assert.ok(report.trackedFiles > 0);
});
