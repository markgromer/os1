import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildMarcusRealtimeClientSecretRequest,
  DEFAULT_MARCUS_PERSONALITY_MODE,
  DEFAULT_MARCUS_REALTIME_MODEL,
  DEFAULT_MARCUS_REALTIME_VOICE,
  normalizeMarcusPersonalityMode,
} from '../marcus/voice/realtime_session.js';

test('Marcus realtime voice is Marcus and delegates durable work to the operator', () => {
  const request = buildMarcusRealtimeClientSecretRequest();
  const session = request.session;

  assert.equal(session.type, 'realtime');
  assert.equal(session.model, DEFAULT_MARCUS_REALTIME_MODEL);
  assert.equal(session.audio.output.voice, DEFAULT_MARCUS_REALTIME_VOICE);
  assert.equal(session.audio.input.turn_detection.type, 'semantic_vad');
  assert.equal(session.audio.input.turn_detection.interrupt_response, true);
  assert.equal(session.audio.input.turn_detection.create_response, true);
  assert.equal(session.audio.input.transcription.model, 'gpt-live-transcribe');
  assert.equal(session.reasoning.effort, 'low');
  assert.equal(session.parallel_tool_calls, false);
  assert.match(session.instructions, /You are Marcus/i);
  assert.match(session.instructions, /one or two spoken sentences/i);
  assert.match(session.instructions, /Smart dry humor/i);
  assert.match(session.instructions, /Protect Mark's time/i);
  assert.match(session.instructions, /Do not recap/i);
  assert.match(session.instructions, /let me know if you need anything else/i);
  assert.match(session.instructions, /ordinary conversation/i);
  assert.match(session.instructions, /longtime operating partner/i);
  assert.match(session.instructions, /riff, tease each other/i);
  assert.match(session.instructions, /Reggie font pull request/i);
  assert.match(session.instructions, /Never read file IDs/i);
  assert.match(session.instructions, /call it immediately and silently/i);
  assert.match(session.instructions, /governor removed/i);
  assert.match(session.instructions, /Repo is clean\. Miraculously/i);
  assert.match(session.instructions, /holding a knife/i);
  assert.match(session.instructions, /Never bypass Marcus approval requirements/i);
  assert.match(session.instructions, /Mode: Operator/i);
  assert.equal(Object.hasOwn(session, 'metadata'), false);
  assert.equal(session.max_output_tokens, 480);
  assert.equal(session.tool_choice, 'auto');
  assert.equal(session.tools.length, 2);
  assert.equal(session.tools[0].name, 'marcus_operator');
  assert.deepEqual(session.tools[0].parameters.required, ['message']);
  assert.equal(session.tools[0].parameters.additionalProperties, false);
  assert.equal(session.tools[1].name, 'set_marcus_personality_mode');
  assert.deepEqual(session.tools[1].parameters.required, ['mode']);
  assert.deepEqual(session.tools[1].parameters.properties.mode.enum, [
    'operator',
    'dry',
    'no_bullshit',
    'meeting_shadow',
    'public_assistant',
    'demo',
    'roast_light',
  ]);
});

test('Marcus realtime voice normalizes unsafe model and voice overrides', () => {
  const request = buildMarcusRealtimeClientSecretRequest({
    model: 'bad model id',
    voice: '../../secret',
  });

  assert.equal(request.session.model, DEFAULT_MARCUS_REALTIME_MODEL);
  assert.equal(request.session.audio.output.voice, DEFAULT_MARCUS_REALTIME_VOICE);
  assert.match(request.session.instructions, /Mode: Operator/i);
  assert.equal(Object.hasOwn(request.session, 'metadata'), false);
});

test('Marcus realtime voice includes explicit mode fragments without weakening authority', () => {
  const demo = buildMarcusRealtimeClientSecretRequest({ personalityMode: 'demo' }).session;
  assert.equal(Object.hasOwn(demo, 'metadata'), false);
  assert.match(demo.instructions, /Mode: Demo/i);
  assert.match(demo.instructions, /not a serious client-call default/i);
  assert.match(demo.instructions, /set_marcus_personality_mode/i);
  assert.match(demo.instructions, /Never bypass Marcus approval requirements/i);

  const publicAssistant = buildMarcusRealtimeClientSecretRequest({ personalityMode: 'public-assistant' }).session;
  assert.equal(Object.hasOwn(publicAssistant, 'metadata'), false);
  assert.match(publicAssistant.instructions, /Mode: Public Assistant/i);
  assert.match(publicAssistant.instructions, /No snark about clients/i);
  assert.doesNotMatch(publicAssistant.instructions, /Mode: Demo/i);
  assert.doesNotMatch(publicAssistant.instructions, /Mode: Roast Light/i);
});

test('Marcus realtime voice normalizes personality mode aliases safely', () => {
  assert.equal(normalizeMarcusPersonalityMode('public-assistant'), 'public_assistant');
  assert.equal(normalizeMarcusPersonalityMode('Roast Light'), 'roast_light');
  assert.equal(normalizeMarcusPersonalityMode('../../demo'), DEFAULT_MARCUS_PERSONALITY_MODE);
});
