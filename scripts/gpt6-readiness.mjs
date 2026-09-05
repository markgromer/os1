import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';

import { normalizeAiHttpResponse, prepareAiHttpRequest } from '../marcus/models/ai_transport.js';
import { compareEvaluationReports, runEvaluationSuite, validateEvaluationSuite } from '../marcus/models/gpt6_evaluation.js';
import { getModelProfile, MODEL_PROFILES, validateModelProfiles } from '../marcus/models/model_profiles.js';
import { runWorkloadEvaluation } from '../marcus/models/gpt6_workload_evaluation.js';

const ROOT = path.resolve(fileURLToPath(new URL('..', import.meta.url)));
const SUITE_FILE = path.join(ROOT, 'marcus', 'models', 'gpt6_readiness_suite.json');
const TARGET_MODEL = 'gpt-6-astra';

function argument(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '').trim() : '';
}

async function loadSuite() {
  return JSON.parse(await fs.readFile(SUITE_FILE, 'utf8'));
}

async function writeReport(report, prefix) {
  const outputDirectory = path.join(ROOT, 'output', 'model-evals');
  await fs.mkdir(outputDirectory, { recursive: true });
  const filename = `${prefix}-${report.createdAt.replace(/[:.]/g, '-')}.json`;
  const outputFile = path.join(outputDirectory, filename);
  await fs.writeFile(outputFile, `${JSON.stringify(report, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
  console.log(`Evidence: ${path.relative(ROOT, outputFile).replaceAll(path.sep, '/')}`);
  return outputFile;
}

export async function requestJson(prepared) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(new Error('timeout')), prepared.timeoutMs);
  try {
    const response = await fetch(prepared.url, {
      method: 'POST',
      headers: prepared.headers,
      body: JSON.stringify(prepared.body),
      signal: controller.signal,
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = String(data?.error?.message || data?.error || `HTTP ${response.status}`);
      throw new Error(`${response.status} ${detail}`.slice(0, 700));
    }
    return data;
  } finally {
    clearTimeout(timeoutId);
  }
}

function printCheck(suite) {
  const profileErrors = validateModelProfiles(MODEL_PROFILES);
  const suiteErrors = validateEvaluationSuite(suite);
  const profile = getModelProfile('openai', TARGET_MODEL);
  const errors = [...profileErrors, ...suiteErrors];
  if (!profile) errors.push(`Missing model profile for ${TARGET_MODEL}.`);
  if (profile?.endpoint !== 'responses') errors.push(`${TARGET_MODEL} must use the Responses endpoint.`);
  if (!profile?.reasoning?.supported?.includes('low')) errors.push(`${TARGET_MODEL} must support the low evaluation baseline.`);

  console.log(`GPT-6 readiness assets: ${errors.length ? 'INVALID' : 'READY'}`);
  console.log(`Profile version: ${MODEL_PROFILES.version}`);
  console.log(`Evaluation suite: ${suite.id} ${suite.version} (${suite.cases.length} cases)`);
  console.log(`Target: ${TARGET_MODEL} via ${profile?.endpoint || '(missing endpoint)'}`);
  console.log(`Account access: ${profile?.qualification?.accessStatus || 'unknown'}`);
  console.log(`Evaluation: ${profile?.qualification?.evaluationStatus || 'unknown'}`);
  console.log(`Runtime rollout: ${profile?.rollout?.status || 'unknown'}`);
  if (errors.length) console.log(errors.map((error) => `- ${error}`).join('\n'));
  return errors;
}

async function probeAccess(apiKey) {
  const report = {
    schemaVersion: 1, type: 'marcus_model_access_probe', createdAt: new Date().toISOString(),
    requestedModel: TARGET_MODEL, provider: 'openai', endpoint: 'responses',
    credentialFingerprint: crypto.createHash('sha256').update(apiKey).digest('hex').slice(0, 16),
    passed: false,
  };
  const prepared = prepareAiHttpRequest({
    route: { provider: 'openai', model: TARGET_MODEL, apiKey },
    workload: 'access-probe',
    purpose: 'probe',
    messages: [
      { role: 'system', content: 'Reply with exactly READY.' },
      { role: 'user', content: 'GPT-6 access probe.' },
    ],
    timeoutMs: 180_000,
    maxOutputTokens: 1024,
  });
  if (!prepared.ok) throw new Error(prepared.error);
  const startedAt = Date.now();
  try {
    const data = await requestJson(prepared);
    const completion = normalizeAiHttpResponse({
      transport: prepared.transport, data, provider: 'openai', model: TARGET_MODEL,
    });
    if (!completion.ok) throw new Error(completion.error);
    report.returnedModel = completion.returnedModel;
    report.responseId = completion.responseId;
    report.usage = completion.usage;
    report.output = String(completion.message?.content || '');
    report.passed = (report.returnedModel === TARGET_MODEL || report.returnedModel.startsWith(`${TARGET_MODEL}-`)) && report.output.trim() === 'READY';
    if (!report.passed) throw new Error('Probe returned an unexpected model or output.');
    console.log(`Access probe passed for ${TARGET_MODEL} through ${prepared.transport}.`);
    console.log(`Response id: ${completion.responseId || '(not returned)'}`);
    console.log(`Output: ${String(completion.message?.content || '').slice(0, 200)}`);
  } catch (error) {
    // Provider errors can echo credentials. Redact before persistence or logging.
    report.error = String(error?.message || error).replaceAll(apiKey, '[REDACTED]').slice(0, 700);
    throw new Error(report.error);
  } finally {
    report.elapsedMs = Date.now() - startedAt;
    await writeReport(report, 'gpt6-access');
  }
}

async function evaluate(suite, candidateApiKey, baselineModel, baselineProvider, baselineApiKey) {
  if (!baselineModel) throw new Error('A baseline is required. Run with --baseline <current route model> or set OPENAI_BASELINE_MODEL.');
  if (baselineModel.toLowerCase() === TARGET_MODEL) throw new Error('The baseline must differ from gpt-6-astra.');
  if (!['openai', 'openrouter'].includes(baselineProvider)) throw new Error('Baseline provider must be openai or openrouter.');
  if (!baselineApiKey) throw new Error(`${baselineProvider === 'openrouter' ? 'OPENROUTER_API_KEY' : 'OPENAI_API_KEY'} is required for the selected baseline provider.`);
  console.log(`Running ${suite.cases.length} synthetic, no-side-effect cases against baseline ${baselineProvider}:${baselineModel}...`);
  const onProgress = ({ caseId, passed, elapsedMs }) => console.log(`${passed ? 'PASS' : 'FAIL'} ${caseId} (${elapsedMs}ms)`);
  const baseline = await runEvaluationSuite({ suite, provider: baselineProvider, model: baselineModel, apiKey: baselineApiKey, requestJson, onProgress });
  console.log(`Running the same cases against candidate ${TARGET_MODEL}...`);
  const candidate = await runEvaluationSuite({ suite, provider: 'openai', model: TARGET_MODEL, apiKey: candidateApiKey, requestJson, onProgress });
  const comparison = compareEvaluationReports({ suite, baseline, candidate });
  const report = {
    schemaVersion: 1,
    type: 'marcus_model_qualification',
    createdAt: new Date().toISOString(),
    suite: { id: suite.id, version: suite.version },
    credentialFingerprint: crypto.createHash('sha256').update(candidateApiKey).digest('hex').slice(0, 16),
    baseline,
    candidate,
    comparison,
  };
  await writeReport(report, 'gpt6-readiness');
  console.log(`Baseline pass rate: ${(comparison.baselineRate * 100).toFixed(1)}%`);
  console.log(`Candidate pass rate: ${(comparison.candidateRate * 100).toFixed(1)}%`);
  console.log(`Qualification result: ${comparison.passed ? 'PASS' : 'FAIL'}`);
  if (!comparison.passed) process.exitCode = 1;
}

async function exercise(candidateApiKey, baselineModel, baselineProvider, baselineApiKey) {
  if (!baselineModel || baselineModel === TARGET_MODEL || !['openai', 'openrouter'].includes(baselineProvider) || !baselineApiKey) {
    throw new Error('An explicitly configured, distinct baseline model and provider key are required.');
  }
  const report = { schemaVersion: 1, type: 'marcus_runtime_contract_comparison', createdAt: new Date().toISOString(),
    synthetic: true, credentialFingerprint: crypto.createHash('sha256').update(candidateApiKey).digest('hex').slice(0, 16) };
  const onProgress = (item) => console.log(`${item.passed ? 'PASS' : 'FAIL'} ${item.caseId}`);
  console.log(`Exercising real reviewer and tool-continuation contracts with ${baselineProvider}:${baselineModel}...`);
  report.baseline = await runWorkloadEvaluation({ provider: baselineProvider, model: baselineModel, apiKey: baselineApiKey, requestJson, onProgress });
  console.log(`Exercising the same contracts with ${TARGET_MODEL}...`);
  report.candidate = await runWorkloadEvaluation({ provider: 'openai', model: TARGET_MODEL, apiKey: candidateApiKey, requestJson, onProgress });
  report.passed = report.baseline.passed && report.candidate.passed;
  await writeReport(report, 'gpt6-runtime-contracts');
  if (!report.passed) process.exitCode = 1;
}

async function main() {
  const command = String(process.argv[2] || 'check').trim().toLowerCase();
  const suite = await loadSuite();
  const errors = printCheck(suite);
  if (errors.length) {
    process.exitCode = 1;
    return;
  }
  if (command === 'check' || command === 'status') return;
  if (!['probe', 'evaluate', 'exercise'].includes(command)) {
    throw new Error('Usage: node scripts/gpt6-readiness.mjs [check|probe|evaluate|exercise] [--settings-file <file>] [--baseline <model>] [--baseline-provider openai|openrouter]');
  }
  // Explicit read-only settings selection; never search unrelated credential stores.
  const settingsFile = argument('--settings-file');
  const saved = settingsFile ? JSON.parse(await fs.readFile(path.resolve(settingsFile), 'utf8')) : {};
  const apiKey = String(process.env.OPENAI_API_KEY || saved.openaiApiKey || '').trim();
  if (!apiKey) throw new Error('OPENAI_API_KEY or --settings-file containing the configured OpenAI key is required.');
  if (command === 'probe') {
    await probeAccess(apiKey);
    return;
  }
  const baselineModel = argument('--baseline') || String(process.env.OPENAI_BASELINE_MODEL || '').trim();
  const baselineProvider = (argument('--baseline-provider') || String(process.env.OPENAI_BASELINE_PROVIDER || 'openai')).trim().toLowerCase();
  const baselineApiKey = baselineProvider === 'openrouter'
    ? String(process.env.OPENROUTER_API_KEY || process.env.OPENROUTER_API_TOKEN || saved.openrouterApiKey || '').trim()
    : apiKey;
  if (command === 'exercise') await exercise(apiKey, baselineModel, baselineProvider, baselineApiKey);
  else await evaluate(suite, apiKey, baselineModel, baselineProvider, baselineApiKey);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main().catch((error) => {
  console.error(`GPT-6 readiness failed: ${String(error?.message || error)}`);
  process.exitCode = 1;
});
