import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildMarcusRealtimeClientSecretRequest,
  DEFAULT_MARCUS_REALTIME_MODEL,
  DEFAULT_MARCUS_REALTIME_VOICE,
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
  assert.match(session.instructions, /ordinary conversation/i);
  assert.match(session.instructions, /Do not read long PR numbers/i);
  assert.match(session.instructions, /Never bypass Marcus approval requirements/i);
  assert.equal(session.tool_choice, 'auto');
  assert.equal(session.tools.length, 1);
  assert.equal(session.tools[0].name, 'marcus_operator');
  assert.deepEqual(session.tools[0].parameters.required, ['message']);
  assert.equal(session.tools[0].parameters.additionalProperties, false);
});

test('Marcus realtime voice normalizes unsafe model and voice overrides', () => {
  const request = buildMarcusRealtimeClientSecretRequest({
    model: 'bad model id',
    voice: '../../secret',
  });

  assert.equal(request.session.model, DEFAULT_MARCUS_REALTIME_MODEL);
  assert.equal(request.session.audio.output.voice, DEFAULT_MARCUS_REALTIME_VOICE);
});
