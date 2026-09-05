import { normalizeAiHttpResponse, prepareAiHttpRequest } from './ai_transport.js';
import crypto from 'node:crypto';

export function evaluationSuiteHash(suite) {
  return crypto.createHash('sha256').update(JSON.stringify(suite)).digest('hex');
}

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function isObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function objectContains(actual, expected) {
  if (!isObject(actual) || !isObject(expected)) return false;
  return Object.entries(expected).every(([key, value]) => {
    if (isObject(value)) return objectContains(actual[key], value);
    return actual[key] === value;
  });
}

function parseToolArguments(call) {
  try {
    const parsed = JSON.parse(String(call?.function?.arguments || '{}'));
    return isObject(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

export function validateEvaluationSuite(suite) {
  const errors = [];
  if (!isObject(suite)) return ['Evaluation suite must be a JSON object.'];
  if (suite.schemaVersion !== 1) errors.push('evaluation suite schemaVersion must be 1.');
  if (!text(suite.id) || !text(suite.version)) errors.push('evaluation suite id and version are required.');
  const minimumPassRate = Number(suite.minimumCandidatePassRate);
  if (!Number.isFinite(minimumPassRate) || minimumPassRate < 0 || minimumPassRate > 1) {
    errors.push('minimumCandidatePassRate must be between 0 and 1.');
  }
  const maximumRegression = Number(suite.maximumPassRateRegression);
  if (!Number.isFinite(maximumRegression) || maximumRegression < 0 || maximumRegression > 1) {
    errors.push('maximumPassRateRegression must be between 0 and 1.');
  }
  const catalog = isObject(suite.toolCatalog) ? suite.toolCatalog : {};
  const cases = Array.isArray(suite.cases) ? suite.cases : [];
  if (!cases.length) errors.push('At least one evaluation case is required.');
  const ids = new Set();
  for (const item of cases) {
    const id = text(item?.id);
    if (!id) errors.push('Every evaluation case requires an id.');
    else if (ids.has(id)) errors.push(`Duplicate evaluation case id: ${id}.`);
    ids.add(id);
    if (!text(item?.workload)) errors.push(`${id || 'Evaluation case'} requires a workload.`);
    if (!Array.isArray(item?.messages) || !item.messages.length) errors.push(`${id || 'Evaluation case'} requires messages.`);
    if (!isObject(item?.expectations) || !Object.keys(item.expectations).length) errors.push(`${id || 'Evaluation case'} requires expectations.`);
    for (const toolName of Array.isArray(item?.toolNames) ? item.toolNames : []) {
      if (!catalog[toolName]) errors.push(`${id || 'Evaluation case'} references missing tool ${toolName}.`);
    }
  }
  return [...new Set(errors)];
}

export function toolsForEvaluationCase(suite, item) {
  const catalog = isObject(suite?.toolCatalog) ? suite.toolCatalog : {};
  return (Array.isArray(item?.toolNames) ? item.toolNames : []).map((name) => ({
    type: 'function',
    function: {
      name,
      description: text(catalog[name]?.description),
      parameters: isObject(catalog[name]?.parameters) ? catalog[name].parameters : { type: 'object', properties: {} },
    },
  }));
}

export function scoreEvaluationCase(item, completion) {
  const expectations = isObject(item?.expectations) ? item.expectations : {};
  const message = completion?.message && typeof completion.message === 'object' ? completion.message : {};
  const content = String(message.content || '');
  const contentLower = content.toLowerCase();
  const calls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
  const callNames = calls.map((call) => text(call?.function?.name));
  const checks = [];
  const add = (name, passed, detail) => checks.push({ name, passed: Boolean(passed), detail });
  if (Array.isArray(item?.toolNames)) {
    add('only offered tools', callNames.every((name) => item.toolNames.includes(name)), callNames.join(', ') || 'none');
  }
  for (const call of calls) {
    let args;
    try { args = JSON.parse(call?.function?.arguments); } catch { /* rejected below */ }
    add('valid tool arguments', isObject(args), 'Tool arguments must be a JSON object.');
  }

  for (const name of Array.isArray(expectations.requiredToolNames) ? expectations.requiredToolNames : []) {
    add(`required tool ${name}`, callNames.includes(name), callNames.includes(name) ? 'present' : `observed: ${callNames.join(', ') || 'none'}`);
  }
  for (const name of Array.isArray(expectations.forbiddenToolNames) ? expectations.forbiddenToolNames : []) {
    add(`forbidden tool ${name}`, !callNames.includes(name), callNames.includes(name) ? 'called' : 'not called');
  }
  for (const [name, subset] of Object.entries(isObject(expectations.toolArgumentSubsets) ? expectations.toolArgumentSubsets : {})) {
    const matchingCall = calls.find((call) => text(call?.function?.name) === name && objectContains(parseToolArguments(call), subset));
    add(`arguments for ${name}`, Boolean(matchingCall), matchingCall ? 'matched required subset' : 'required argument subset was not observed');
  }
  if (Number.isFinite(Number(expectations.minToolCalls))) {
    add('minimum tool calls', calls.length >= Number(expectations.minToolCalls), `observed ${calls.length}`);
  }
  if (Number.isFinite(Number(expectations.maxToolCalls))) {
    add('maximum tool calls', calls.length <= Number(expectations.maxToolCalls), `observed ${calls.length}`);
  }
  const includesAny = Array.isArray(expectations.contentIncludesAny) ? expectations.contentIncludesAny.map(text).filter(Boolean) : [];
  if (includesAny.length) {
    add('content includes any', includesAny.some((value) => contentLower.includes(value.toLowerCase())), `expected one of: ${includesAny.join(', ')}`);
  }
  const includesAll = Array.isArray(expectations.contentIncludesAll) ? expectations.contentIncludesAll.map(text).filter(Boolean) : [];
  if (includesAll.length) {
    add('content includes all', includesAll.every((value) => contentLower.includes(value.toLowerCase())), `expected: ${includesAll.join(', ')}`);
  }
  const excludes = Array.isArray(expectations.contentExcludes) ? expectations.contentExcludes.map(text).filter(Boolean) : [];
  for (const value of excludes) add(`content excludes ${value}`, !contentLower.includes(value.toLowerCase()), contentLower.includes(value.toLowerCase()) ? 'present' : 'absent');

  let parsedJson = null;
  if (expectations.validJson === true || Array.isArray(expectations.jsonRequiredKeys)) {
    try { parsedJson = JSON.parse(content); } catch {}
  }
  if (expectations.validJson === true) add('valid JSON', isObject(parsedJson), isObject(parsedJson) ? 'valid object' : 'not a valid JSON object');
  for (const key of Array.isArray(expectations.jsonRequiredKeys) ? expectations.jsonRequiredKeys : []) {
    add(`JSON key ${key}`, isObject(parsedJson) && Object.hasOwn(parsedJson, key), isObject(parsedJson) && Object.hasOwn(parsedJson, key) ? 'present' : 'missing');
  }
  for (const [key, terms] of Object.entries(expectations.jsonArrayIncludesAll || {})) {
    const values = parsedJson?.[key];
    const combined = Array.isArray(values) ? values.join(' ').toLowerCase() : '';
    add(`JSON array ${key} covers required work`, Array.isArray(values) && values.every((value) => typeof value === 'string')
      && terms.every((term) => combined.includes(term.toLowerCase())), `required: ${terms.join(', ')}`);
  }

  if (!completion?.ok) add('completion succeeded', false, text(completion?.error) || 'completion failed');
  const passedChecks = checks.filter((check) => check.passed).length;
  return {
    caseId: text(item?.id),
    workload: text(item?.workload),
    passed: checks.length > 0 && passedChecks === checks.length,
    score: checks.length ? passedChecks / checks.length : 0,
    checks,
    observed: {
      content,
      toolCalls: calls.map((call) => ({ name: text(call?.function?.name), arguments: parseToolArguments(call) })),
    },
  };
}

export async function runEvaluationSuite({ suite, provider = 'openai', model, apiKey, requestJson, onProgress = () => {} } = {}) {
  const errors = validateEvaluationSuite(suite);
  if (errors.length) throw new Error(`Invalid evaluation suite: ${errors.join(' ')}`);
  if (!text(model)) throw new Error('Evaluation model is required.');
  if (!text(apiKey)) throw new Error('Evaluation API key is required.');
  if (typeof requestJson !== 'function') throw new Error('requestJson adapter is required.');

  const startedAt = new Date().toISOString();
  const results = [];
  for (const item of suite.cases) {
    const caseStart = Date.now();
    const prepared = prepareAiHttpRequest({
      route: { provider, model, apiKey },
      workload: item.workload,
      purpose: 'evaluation',
      messages: item.messages,
      tools: toolsForEvaluationCase(suite, item),
      toolChoice: item.toolChoice || 'auto',
      responseFormat: item.responseFormat,
      timeoutMs: item.timeoutMs || 180_000,
      maxOutputTokens: 2048,
    });
    let completion;
    if (!prepared.ok) {
      completion = prepared;
    } else {
      try {
        const data = await requestJson(prepared);
        completion = normalizeAiHttpResponse({ transport: prepared.transport, data, provider, model });
      } catch (error) {
        completion = { ok: false, error: String(error?.message || error).replaceAll(apiKey, '[REDACTED]').slice(0, 700) };
      }
    }
    const result = scoreEvaluationCase(item, completion);
    result.elapsedMs = Date.now() - caseStart;
    result.usage = completion.usage || null;
    result.responseId = completion.responseId || '';
    result.returnedModel = completion.returnedModel || '';
    result.completionOk = completion.ok === true;
    results.push(result);
    onProgress({ caseId: item.id, passed: result.passed, elapsedMs: result.elapsedMs });
  }
  const passedCases = results.filter((result) => result.passed).length;
  return {
    schemaVersion: 1,
    suiteId: suite.id,
    suiteVersion: suite.version,
    suiteSha256: evaluationSuiteHash(suite),
    provider,
    model,
    startedAt,
    completedAt: new Date().toISOString(),
    totals: {
      cases: results.length,
      passed: passedCases,
      failed: results.length - passedCases,
      passRate: results.length ? passedCases / results.length : 0,
    },
    results,
  };
}

export function compareEvaluationReports({ suite, baseline, candidate } = {}) {
  const integrityErrors = validateEvaluationSuite(suite);
  const caseIds = (suite?.cases || []).map((item) => item.id);
  for (const [label, report] of Object.entries({ baseline, candidate })) {
    const ids = (report?.results || []).map((item) => item.caseId);
    if (report?.suiteId !== suite?.id || report?.suiteVersion !== suite?.version || report?.suiteSha256 !== evaluationSuiteHash(suite)) {
      integrityErrors.push(`${label}: suite identity mismatch.`);
    }
    if (!caseIds.length || ids.length !== caseIds.length || new Set(ids).size !== ids.length || caseIds.some((id) => !ids.includes(id))) {
      integrityErrors.push(`${label}: missing, duplicate, or unexpected cases.`);
    }
    if ((report?.results || []).some((item) => !item.completionOk || !item.responseId || !item.returnedModel)) {
      integrityErrors.push(`${label}: provider completion evidence missing or failed.`);
    }
    if (report?.provider === 'openai' && (report?.results || []).some((item) => item.returnedModel !== report.model && !item.returnedModel?.startsWith(`${report.model}-`))) {
      integrityErrors.push(`${label}: returned model does not match the requested model.`);
    }
  }
  const rate = (report) => (report?.results || []).filter((item) => item.passed).length / (caseIds.length || 1);
  const baselineRate = rate(baseline);
  const candidateRate = rate(candidate);
  const minimumCandidatePassRate = Number(suite?.minimumCandidatePassRate) || 0;
  const maximumPassRateRegression = Number(suite?.maximumPassRateRegression) || 0;
  const regression = Math.max(0, baselineRate - candidateRate);
  // Boundary failures are never averaged away by unrelated successes.
  const requiredCases = (suite?.cases || []).filter((item) => item.critical !== false).map((item) => item.id);
  const failedCriticalCases = requiredCases.filter((id) => !candidate?.results?.find((item) => item.caseId === id)?.passed);
  return {
    passed: !integrityErrors.length && !failedCriticalCases.length && candidateRate >= minimumCandidatePassRate && regression <= maximumPassRateRegression,
    integrityErrors,
    failedCriticalCases,
    baselineRate,
    candidateRate,
    regression,
    minimumCandidatePassRate,
    maximumPassRateRegression,
  };
}
