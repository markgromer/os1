import {
  buildMarcusRealtimeInstructions,
  DEFAULT_MARCUS_PERSONALITY_MODE,
  normalizeMarcusPersonalityMode,
} from './personality_modes.js';

export const DEFAULT_MARCUS_REALTIME_MODEL = 'gpt-realtime-2.1';
export const DEFAULT_MARCUS_REALTIME_VOICE = 'cedar';
export { DEFAULT_MARCUS_PERSONALITY_MODE, normalizeMarcusPersonalityMode };

function normalizeId(value, fallback) {
  const normalized = String(value || '').trim();
  return /^[A-Za-z0-9._-]{1,120}$/.test(normalized) ? normalized : fallback;
}

export function buildMarcusRealtimeClientSecretRequest({ model, voice, personalityMode } = {}) {
  const realtimeModel = normalizeId(model, DEFAULT_MARCUS_REALTIME_MODEL);
  const realtimeVoice = normalizeId(voice, DEFAULT_MARCUS_REALTIME_VOICE);
  const mode = normalizeMarcusPersonalityMode(personalityMode);

  return {
    session: {
      type: 'realtime',
      model: realtimeModel,
      instructions: buildMarcusRealtimeInstructions({ personalityMode: mode }),
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
          description: 'Send Mark\'s complete spoken request to the durable Marcus operator for project work, live status, Codex work, audits, approvals, consequential actions, durable memory, and verified completion evidence.',
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
        {
          type: 'function',
          name: 'set_marcus_personality_mode',
          description: 'Change Marcus voice personality mode for this Realtime voice session when Mark explicitly asks for a mode change.',
          parameters: {
            type: 'object',
            properties: {
              mode: {
                type: 'string',
                enum: ['operator', 'dry', 'no_bullshit', 'meeting_shadow', 'public_assistant', 'demo', 'roast_light'],
                description: 'The requested Marcus personality mode.',
              },
            },
            required: ['mode'],
            additionalProperties: false,
          },
        },
      ],
      tool_choice: 'auto',
      max_output_tokens: 480,
    },
  };
}
