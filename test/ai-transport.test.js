import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  AI_TRANSPORT_CHAT_COMPLETIONS,
  AI_TRANSPORT_RESPONSES,
  normalizeAiHttpResponse,
  prepareAiHttpRequest,
  responsesInputFromChatMessages,
} from '../marcus/models/ai_transport.js';
import {
  getModelProfile,
  MODEL_PROFILES,
  resolveModelDeployment,
  validateModelProfiles,
} from '../marcus/models/model_profiles.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function functionTool(name = 'lookup_status') {
  return {
    type: 'function',
    function: {
      name,
      description: 'Read status.',
      parameters: {
        type: 'object',
        properties: { id: { type: 'string' } },
        required: ['id'],
        additionalProperties: false,
      },
    },
  };
}

test('the HTTP transport cannot directly dispatch the preview candidate outside its sampled account boundary', () => {
  for (const route of [
    { provider: 'openai', model: 'gpt-6-astra', apiKey: 'test-key' },
    { provider: 'openai', model: 'gpt-6-astra', apiKey: 'unqualified-key', canary: true },
  ]) {
    const request = prepareAiHttpRequest({ route, workload: 'dashboardPreview', maxOutputTokens: 2048, messages: [] });
    assert.equal(request.ok, false);
    assert.match(request.error, /sampled preview canary/);
  }
});

test('GPT-6 profile is valid, Responses-based, and excludes the broad main-agent route', async () => {
  const diskDocument = JSON.parse(await fs.readFile(path.join(ROOT, 'marcus', 'models', 'model_profiles.json'), 'utf8'));
  assert.deepEqual(validateModelProfiles(diskDocument), []);
  assert.deepEqual(validateModelProfiles(MODEL_PROFILES), []);
  const profile = getModelProfile('openai', 'gpt-6-astra');
  assert.equal(profile.endpoint, 'responses');
  assert.equal(profile.reasoning.default, 'low');
  assert.equal(profile.reasoning.supported.includes('none'), false);
  assert.equal(profile.rollout.status, 'canary');
  assert.equal(profile.qualification.accessStatus, 'verified');

  const runtime = resolveModelDeployment({ provider: 'openai', model: 'gpt-6-astra', workload: 'marcusChat' });
  assert.equal(runtime.allowed, false);
  assert.match(runtime.reason, /not enabled/i);
  const blockedRequest = prepareAiHttpRequest({
    route: { provider: 'openai', model: 'gpt-6-astra', apiKey: 'test-key' },
    workload: 'marcusChat',
    messages: [{ role: 'user', content: 'Hello' }],
  });
  assert.equal(blockedRequest.ok, false);
  assert.match(blockedRequest.error, /not enabled/i);
  const evaluation = resolveModelDeployment({ provider: 'openai', model: 'gpt-6-astra', workload: 'planning', purpose: 'evaluation' });
  assert.equal(evaluation.allowed, true);
  assert.equal(evaluation.endpoint, 'responses');
});

test('unqualified snapshots, provider aliases, shadow calls, and premature canaries are blocked', () => {
  for (const [provider, model] of [['openrouter', 'openai/gpt-6-astra'], ['openai', 'gpt-6-astra-2026-09-04']]) {
    assert.equal(resolveModelDeployment({ provider, model, workload: 'marcusChat' }).allowed, false);
  }
  assert.equal(resolveModelDeployment({ provider: 'openai', model: 'gpt-6-astra', purpose: 'shadow', workload: 'marcusChat' }).allowed, false);
  const document = JSON.parse(JSON.stringify(MODEL_PROFILES));
  document.profiles[0].rollout.status = 'canary';
  document.profiles[0].rollout.enabledWorkloads = ['marcusChat'];
  assert.match(validateModelProfiles(document).join(' '), /passing workload evaluation and shadow evidence/);
});

test('GPT-6 evaluation request converts messages, tools, forced choice, and structured output to Responses', () => {
  const prepared = prepareAiHttpRequest({
    route: { provider: 'openai', model: 'gpt-6-astra', apiKey: 'test-key' },
    workload: 'planning',
    purpose: 'evaluation',
    messages: [
      { role: 'system', content: 'Stay bounded.' },
      { role: 'user', content: 'Check op-1.' },
    ],
    tools: [functionTool()],
    toolChoice: { type: 'function', function: { name: 'lookup_status' } },
    responseFormat: {
      type: 'json_schema',
      json_schema: {
        name: 'status_result',
        strict: true,
        schema: { type: 'object', properties: { ok: { type: 'boolean' } }, required: ['ok'], additionalProperties: false },
      },
    },
    timeoutMs: 10_000,
  });

  assert.equal(prepared.ok, true);
  assert.equal(prepared.transport, AI_TRANSPORT_RESPONSES);
  assert.equal(prepared.url, 'https://api.openai.com/v1/responses');
  assert.equal(prepared.timeoutMs, 180_000);
  assert.deepEqual(prepared.body.reasoning, { effort: 'low' });
  assert.equal(prepared.body.store, false);
  assert.deepEqual(prepared.body.include, ['reasoning.encrypted_content']);
  assert.deepEqual(prepared.body.tools[0], {
    type: 'function',
    name: 'lookup_status',
    description: 'Read status.',
    parameters: functionTool().function.parameters,
    strict: false,
  });
  assert.deepEqual(prepared.body.tool_choice, { type: 'function', name: 'lookup_status' });
  assert.equal(prepared.body.text.format.type, 'json_schema');
  assert.equal(prepared.body.text.format.name, 'status_result');
  for (const unsupported of ['temperature', 'top_p', 'top_logprobs', 'logprobs', 'response_format', 'messages']) {
    assert.equal(Object.hasOwn(prepared.body, unsupported), false);
  }
});

test('Responses output normalizes to the existing chat tool-call contract and preserves stateless reasoning items', () => {
  const responseData = {
    id: 'resp_123',
    status: 'completed',
    output: [
      { type: 'reasoning', id: 'rs_1', encrypted_content: 'opaque' },
      { type: 'function_call', id: 'fc_1', call_id: 'call_1', name: 'lookup_status', arguments: '{"id":"op-1"}', status: 'completed' },
      { type: 'function_call', id: 'fc_2', call_id: 'call_2', name: 'lookup_status', arguments: '{"id":"op-2"}', status: 'completed' },
    ],
    usage: { input_tokens: 10, output_tokens: 5, total_tokens: 15 },
  };
  const completion = normalizeAiHttpResponse({
    transport: AI_TRANSPORT_RESPONSES,
    data: responseData,
    provider: 'openai',
    model: 'gpt-6-astra',
  });
  assert.equal(completion.ok, true);
  assert.equal(completion.message.tool_calls.length, 2);
  assert.equal(completion.message.tool_calls[0].id, 'call_1');
  assert.equal(completion.message.tool_calls[0].function.name, 'lookup_status');
  assert.equal(completion.message.response_id, 'resp_123');
  assert.deepEqual(completion.message.response_output, responseData.output);

  const continuation = responsesInputFromChatMessages([
    completion.message,
    { role: 'tool', tool_call_id: 'call_1', content: '{"ok":true}' },
    { role: 'tool', tool_call_id: 'call_2', content: '{"ok":false}' },
  ]);
  assert.deepEqual(continuation.slice(0, 3), responseData.output);
  assert.deepEqual(continuation[3], { type: 'function_call_output', call_id: 'call_1', output: '{"ok":true}' });
  assert.deepEqual(continuation[4], { type: 'function_call_output', call_id: 'call_2', output: '{"ok":false}' });
});

test('Responses text and errors normalize without changing callers', () => {
  const textCompletion = normalizeAiHttpResponse({
    transport: AI_TRANSPORT_RESPONSES,
    provider: 'openai',
    model: 'gpt-6-astra',
    data: {
      id: 'resp_text',
      status: 'completed',
      output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'Ready.' }] }],
    },
  });
  assert.equal(textCompletion.ok, true);
  assert.equal(textCompletion.message.content, 'Ready.');
  assert.deepEqual(textCompletion.message.tool_calls, undefined);

  const incomplete = normalizeAiHttpResponse({
    transport: AI_TRANSPORT_RESPONSES,
    provider: 'openai',
    model: 'gpt-6-astra',
    data: { status: 'incomplete', incomplete_details: { reason: 'max_output_tokens' }, output: [] },
  });
  assert.equal(incomplete.ok, false);
  assert.match(incomplete.error, /incomplete.*max_output_tokens/i);
});

test('existing OpenRouter and OpenAI models preserve Chat Completions transport', () => {
  const openrouter = prepareAiHttpRequest({
    route: { provider: 'openrouter', model: 'openai/gpt-4o-mini', apiKey: 'router-key' },
    workload: 'marcusChat',
    messages: [{ role: 'user', content: 'Hello' }],
    tools: [functionTool()],
    toolChoice: 'auto',
    openrouterReferer: 'https://example.test',
    openrouterTitle: 'MARCUS Test',
  });
  assert.equal(openrouter.ok, true);
  assert.equal(openrouter.transport, AI_TRANSPORT_CHAT_COMPLETIONS);
  assert.equal(openrouter.url, 'https://openrouter.ai/api/v1/chat/completions');
  assert.equal(openrouter.headers['HTTP-Referer'], 'https://example.test');
  assert.equal(openrouter.headers['X-Title'], 'MARCUS Test');
  assert.deepEqual(openrouter.body.messages, [{ role: 'user', content: 'Hello' }]);
  assert.deepEqual(openrouter.body.tools, [functionTool()]);

  const openai = prepareAiHttpRequest({
    route: { provider: 'openai', model: 'gpt-4o-mini', apiKey: 'openai-key' },
    workload: 'dashboardPreview',
    messages: [{ role: 'user', content: 'Hello' }],
  });
  assert.equal(openai.transport, AI_TRANSPORT_CHAT_COMPLETIONS);
  assert.equal(openai.url, 'https://api.openai.com/v1/chat/completions');
});

test('Responses rejects partial, duplicate, and malformed function calls before dispatch', () => {
  const call = { type: 'function_call', id: 'fc_1', call_id: 'call_1', name: 'safe_read', arguments: '{}' };
  for (const data of [
    { status: 'queued', output: [call] },
    { status: 'in_progress', output: [call] },
    { status: 'completed', output: [{ ...call, status: 'in_progress' }] },
    { status: 'completed', output: [{ ...call, call_id: '' }] },
    { status: 'completed', output: [{ ...call, arguments: '{' }] },
    { status: 'completed', output: [{ ...call, arguments: '[]' }] },
    { status: 'completed', output: [call, call] },
  ]) {
    assert.equal(normalizeAiHttpResponse({ transport: AI_TRANSPORT_RESPONSES, data }).ok, false);
  }
});

test('rollback strips Responses state and tool-less requests omit tool_choice', () => {
  const request = prepareAiHttpRequest({
    route: { provider: 'openai', model: 'gpt-4o', apiKey: 'fixture' },
    messages: [{ role: 'assistant', content: 'Ready', response_id: 'resp_1', response_output: [] }],
    toolChoice: 'auto', maxOutputTokens: 2048,
  });
  assert.deepEqual(request.body.messages, [{ role: 'assistant', content: 'Ready' }]);
  assert.equal(request.body.tool_choice, undefined);
  assert.equal(request.body.max_completion_tokens, 2048);
});
