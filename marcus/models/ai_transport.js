import { getModelProfile, resolveModelDeployment } from './model_profiles.js';
import { keyFingerprint } from './read_only_canary.js';

export const AI_TRANSPORT_CHAT_COMPLETIONS = 'chat_completions';
export const AI_TRANSPORT_RESPONSES = 'responses';

function asText(value) {
  if (typeof value === 'string') return value;
  if (value === null || value === undefined) return '';
  return JSON.stringify(value);
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

export function responsesToolsFromChatTools(tools) {
  return (Array.isArray(tools) ? tools : []).map((tool) => {
    if (tool?.type !== 'function' || !tool.function) return cloneJson(tool);
    return {
      type: 'function',
      name: String(tool.function.name || ''),
      description: String(tool.function.description || ''),
      parameters: tool.function.parameters && typeof tool.function.parameters === 'object'
        ? cloneJson(tool.function.parameters)
        : { type: 'object', properties: {} },
      // Responses otherwise normalizes legacy optional fields into strict schemas.
      strict: tool.function.strict === true,
    };
  });
}

export function responsesToolChoiceFromChatChoice(choice) {
  if (!choice || typeof choice === 'string') return choice || undefined;
  if (choice.type === 'function' && choice.function?.name) {
    return { type: 'function', name: String(choice.function.name) };
  }
  return cloneJson(choice);
}

export function responsesTextFormatFromChatFormat(format) {
  if (!format || typeof format !== 'object') return undefined;
  if (format.type === 'json_schema' && format.json_schema && typeof format.json_schema === 'object') {
    return { type: 'json_schema', ...cloneJson(format.json_schema) };
  }
  return cloneJson(format);
}

export function responsesInputFromChatMessages(messages) {
  const input = [];
  for (const rawMessage of Array.isArray(messages) ? messages : []) {
    const message = rawMessage && typeof rawMessage === 'object' ? rawMessage : {};
    const role = String(message.role || '').trim();

    if (role === 'assistant' && Array.isArray(message.response_output) && message.response_output.length) {
      input.push(...cloneJson(message.response_output));
      continue;
    }
    if (role === 'tool') {
      const callId = String(message.tool_call_id || '').trim();
      if (callId) input.push({ type: 'function_call_output', call_id: callId, output: asText(message.content) });
      continue;
    }
    if (!['system', 'developer', 'user', 'assistant'].includes(role)) continue;

    const content = asText(message.content);
    if (content) input.push({ role, content });
    if (role === 'assistant' && Array.isArray(message.tool_calls)) {
      for (const call of message.tool_calls) {
        const callId = String(call?.id || '').trim();
        const name = String(call?.function?.name || '').trim();
        if (!callId || !name) continue;
        input.push({
          type: 'function_call',
          call_id: callId,
          name,
          arguments: asText(call?.function?.arguments || '{}'),
        });
      }
    }
  }
  return input;
}

function timeoutFor({ model, requestedTimeoutMs, profileTimeoutMs }) {
  const requested = Number.isFinite(Number(requestedTimeoutMs)) ? Math.max(5_000, Number(requestedTimeoutMs)) : 30_000;
  if (Number(profileTimeoutMs) >= 5_000) return Math.max(requested, Number(profileTimeoutMs));
  const modelLower = String(model || '').trim().toLowerCase();
  if (modelLower.startsWith('gpt-5')) return Math.max(requested, 90_000);
  if (modelLower.includes('gpt-4.1') || modelLower.includes('gpt-4o')) return Math.max(requested, 45_000);
  return requested;
}

export function prepareAiHttpRequest({
  route,
  workload,
  purpose = 'runtime',
  messages,
  tools,
  toolChoice,
  responseFormat,
  timeoutMs,
  maxOutputTokens,
  openrouterReferer = '',
  openrouterTitle = 'M.A.R.C.U.S.',
} = {}) {
  const provider = String(route?.provider || '').trim().toLowerCase();
  const model = String(route?.model || '').trim();
  const apiKey = String(route?.apiKey || '').trim();
  if (!apiKey) return { ok: false, error: `AI is not enabled (missing API key for ${provider || 'provider'})` };
  if (!model) return { ok: false, error: 'AI model is not configured.' };
  if (!['openai', 'openrouter'].includes(provider)) return { ok: false, error: 'Unsupported AI provider.' };
  if (maxOutputTokens !== undefined && (!Number.isInteger(maxOutputTokens) || maxOutputTokens < 16)) {
    return { ok: false, error: 'maxOutputTokens must be an integer of at least 16.' };
  }

  const deployment = resolveModelDeployment({ provider, model, workload, purpose });
  if (!deployment.allowed) {
    return { ok: false, error: `${deployment.reason} Run the GPT-6 readiness evaluation before enabling this route.`, deployment };
  }
  if (purpose === 'runtime' && deployment.rolloutStatus === 'canary') {
    const profile = getModelProfile(provider, model);
    if (route.canary !== true || workload !== 'dashboardPreview'
      || keyFingerprint(apiKey) !== profile?.rollout?.credentialFingerprint
      || process.env.MARCUS_DISABLE_GPT6_CANARY === 'true'
      || (Array.isArray(tools) && tools.length) || !Number.isInteger(maxOutputTokens) || maxOutputTokens > 2048) {
      return { ok: false, error: 'GPT-6 runtime use is limited to the credential-bound, tool-free sampled preview canary.', deployment };
    }
  }
  const transport = deployment.endpoint === AI_TRANSPORT_RESPONSES
    ? AI_TRANSPORT_RESPONSES
    : AI_TRANSPORT_CHAT_COMPLETIONS;
  const baseUrl = provider === 'openrouter' ? 'https://openrouter.ai/api/v1' : 'https://api.openai.com/v1';
  const headers = {
    Authorization: `Bearer ${apiKey}`,
    'Content-Type': 'application/json',
  };
  if (provider === 'openrouter') {
    const referer = String(openrouterReferer || '').trim();
    if (referer) headers['HTTP-Referer'] = referer;
    headers['X-Title'] = String(openrouterTitle || '').trim() || 'M.A.R.C.U.S.';
  }

  let body;
  let url;
  if (transport === AI_TRANSPORT_RESPONSES) {
    url = `${baseUrl}/responses`;
    body = {
      model,
      input: responsesInputFromChatMessages(messages),
      store: false,
      include: ['reasoning.encrypted_content'],
    };
    if (deployment.reasoningEffort) body.reasoning = { effort: deployment.reasoningEffort };
    const responseTools = responsesToolsFromChatTools(tools);
    if (responseTools.length) body.tools = responseTools;
    const responseToolChoice = responsesToolChoiceFromChatChoice(toolChoice);
    if (responseTools.length && responseToolChoice) body.tool_choice = responseToolChoice;
    if (maxOutputTokens !== undefined) body.max_output_tokens = maxOutputTokens;
    const textFormat = responsesTextFormatFromChatFormat(responseFormat);
    if (textFormat) body.text = { format: textFormat };
  } else {
    url = `${baseUrl}/chat/completions`;
    body = { model, messages: (Array.isArray(messages) ? messages : []).map(({ response_output, response_id, ...message }) => message) };
    if (Array.isArray(tools) && tools.length) body.tools = tools;
    if (Array.isArray(tools) && tools.length && toolChoice) body.tool_choice = toolChoice;
    if (maxOutputTokens !== undefined) body.max_completion_tokens = maxOutputTokens;
    if (responseFormat) body.response_format = responseFormat;
  }

  return {
    ok: true,
    provider,
    model,
    transport,
    deployment,
    url,
    headers,
    body,
    timeoutMs: timeoutFor({ model, requestedTimeoutMs: timeoutMs, profileTimeoutMs: deployment.timeoutMs }),
  };
}

function responseText(output) {
  const parts = [];
  for (const item of Array.isArray(output) ? output : []) {
    if (item?.type !== 'message') continue;
    for (const content of Array.isArray(item.content) ? item.content : []) {
      if (content?.type === 'output_text' && typeof content.text === 'string') parts.push(content.text);
      if (content?.type === 'refusal' && typeof content.refusal === 'string') parts.push(content.refusal);
    }
  }
  return parts.join('\n').trim();
}

export function normalizeAiHttpResponse({ transport, data, provider, model } = {}) {
  if (transport === AI_TRANSPORT_RESPONSES) {
    const status = String(data?.status || '').trim().toLowerCase();
    if (data?.error || status === 'failed' || status === 'cancelled') {
      const detail = String(data?.error?.message || data?.incomplete_details?.reason || status || 'unknown error');
      return { ok: false, error: `AI Responses request failed. provider=${provider}. model=${model}. ${detail}`.slice(0, 700) };
    }
    if (status === 'incomplete') {
      const detail = String(data?.incomplete_details?.reason || 'response was incomplete');
      return { ok: false, error: `AI Responses request was incomplete. provider=${provider}. model=${model}. ${detail}`.slice(0, 700) };
    }
    if (status !== 'completed') return { ok: false, error: `AI Responses request is not completed (${status || 'missing status'}).` };
    const output = Array.isArray(data?.output) ? data.output : [];
    const callIds = new Set();
    for (const item of output) {
      if (item?.status && item.status !== 'completed') return { ok: false, error: 'AI returned an unfinished output item.' };
      if (item?.type !== 'function_call') continue;
      let args;
      try { args = JSON.parse(item.arguments); } catch { /* rejected below */ }
      if (!item.call_id || !item.name || callIds.has(item.call_id) || !args || typeof args !== 'object' || Array.isArray(args)) {
        return { ok: false, error: 'AI returned a malformed or duplicate function call.' };
      }
      callIds.add(item.call_id);
    }
    const toolCalls = output
      .filter((item) => item?.type === 'function_call' && item?.name)
      .map((item) => ({
        id: String(item.call_id),
        type: 'function',
        function: {
          name: String(item.name),
          arguments: asText(item.arguments || '{}'),
        },
      }))
      .filter((call) => call.id);
    const content = responseText(output);
    if (!content && !toolCalls.length) return { ok: false, error: 'AI returned no message or function call.' };
    return {
      ok: true,
      provider,
      model,
      returnedModel: String(data?.model || ''),
      transport,
      responseId: String(data?.id || ''),
      usage: data?.usage && typeof data.usage === 'object' ? cloneJson(data.usage) : null,
      message: {
        role: 'assistant',
        content,
        ...(toolCalls.length ? { tool_calls: toolCalls } : {}),
        response_id: String(data?.id || ''),
        response_output: cloneJson(output),
      },
    };
  }

  const message = data?.choices?.[0]?.message;
  if (!message) return { ok: false, error: 'AI returned no message.' };
  return {
    ok: true,
    provider,
    model,
    returnedModel: String(data?.model || ''),
    responseId: String(data?.id || ''),
    transport: AI_TRANSPORT_CHAT_COMPLETIONS,
    usage: data?.usage && typeof data.usage === 'object' ? cloneJson(data.usage) : null,
    message,
  };
}
