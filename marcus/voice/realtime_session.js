export const DEFAULT_MARCUS_REALTIME_MODEL = 'gpt-realtime-2.1';
export const DEFAULT_MARCUS_REALTIME_VOICE = 'marin';

function normalizeId(value, fallback) {
  const normalized = String(value || '').trim();
  return /^[A-Za-z0-9._-]{1,120}$/.test(normalized) ? normalized : fallback;
}

export function buildMarcusRealtimeClientSecretRequest({ model, voice } = {}) {
  const realtimeModel = normalizeId(model, DEFAULT_MARCUS_REALTIME_MODEL);
  const realtimeVoice = normalizeId(voice, DEFAULT_MARCUS_REALTIME_VOICE);

  return {
    session: {
      type: 'realtime',
      model: realtimeModel,
      instructions: [
        "You are Marcus, Mark's trusted project operator, speaking live.",
        'Keep the conversation natural, calm, concise, and spoken-friendly.',
        'For every substantive question, project discussion, request, decision, or follow-up, call marcus_operator exactly once with the complete user intent. Preserve project names and important details. Short approval follow-ups such as "do it" must also go through the tool.',
        'Do not answer substantive requests from your own knowledge and do not claim that work was executed independently. The operator tool is your durable execution layer for project context, operations, Codex work, approvals, and completion evidence.',
        'After marcus_operator returns, summarize the result in one or two spoken sentences unless Mark asks for detail. Preserve approval requests, blockers, and uncertainty; include operation IDs only when needed to disambiguate.',
        'Do not say you are handing the request to Marcus or waiting on Marcus. You are Marcus.',
        'Never bypass Marcus approval requirements for external messages, publishing, deployment, DNS, merges, billing, or other consequential actions.',
        'You may respond without a tool only to a brief greeting, a request to repeat yourself, or a voice-session control question.',
      ].join('\n'),
      audio: {
        input: {
          noise_reduction: { type: 'near_field' },
          transcription: {
            model: 'gpt-live-transcribe',
            delay: 'low',
            prompt: 'A project operations conversation with Marcus. Preserve project, repository, domain, company, and product names exactly.',
            keywords: ['Marcus', 'Codex', 'GitHub', 'Cloudflare', 'Reggie', 'Sweep and Go'],
            languages: ['en'],
          },
          turn_detection: {
            type: 'semantic_vad',
            eagerness: 'medium',
            create_response: true,
            interrupt_response: true,
          },
        },
        output: {
          voice: realtimeVoice,
        },
      },
      reasoning: { effort: 'low' },
      parallel_tool_calls: false,
      tools: [
        {
          type: 'function',
          name: 'marcus_operator',
          description: 'Send Mark\'s complete spoken request to the durable Marcus operator. Use this for all substantive conversation, project work, Codex work, status, decisions, and approvals.',
          parameters: {
            type: 'object',
            properties: {
              message: {
                type: 'string',
                description: 'Mark\'s complete request or follow-up, preserving project names, constraints, and approval language.',
              },
            },
            required: ['message'],
            additionalProperties: false,
          },
        },
      ],
      tool_choice: 'auto',
      max_output_tokens: 1200,
    },
  };
}
