import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repositoryRoot = path.resolve(fileURLToPath(new URL('..', import.meta.url)));

async function freePort() {
  const server = net.createServer();
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function spawnServer({ startupCheck = false, adminToken = 'test-admin-token', production = false, extraEnv = {}, testRoot = '' } = {}) {
  const root = testRoot || await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-server-smoke-'));
  const workspaceRoot = path.join(root, 'workspaces');
  await fs.mkdir(workspaceRoot, { recursive: true });
  const port = await freePort();
  const child = spawn(process.execPath, ['server.js'], {
    cwd: repositoryRoot,
    windowsHide: true,
    env: {
      ...process.env,
      PORT: String(port),
      MARCUS_HOST: '127.0.0.1',
      TASK_TRACKER_DATA_DIR: path.join(root, 'data'),
      TASK_TRACKER_SETTINGS_DIR: path.join(root, 'settings'),
      TASK_TRACKER_BACKUP_DIR: path.join(root, 'backups'),
      MARCUS_ALLOW_UNAUTHENTICATED_LOCAL: adminToken ? 'false' : 'true',
      MARCUS_STARTUP_CHECK: startupCheck ? 'true' : 'false',
      NODE_ENV: production ? 'production' : 'test',
      ADMIN_TOKEN: adminToken,
      MARCUS_ALLOWED_WORKSPACE_ROOTS: workspaceRoot,
      RENDER: '', RENDER_SERVICE_ID: '', RENDER_EXTERNAL_URL: '',
      ...extraEnv,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (chunk) => { output += String(chunk); });
  child.stderr.on('data', (chunk) => { output += String(chunk); });
  const hasExited = () => child.exitCode !== null || child.signalCode !== null;
  const waitForExit = () => hasExited()
    ? Promise.resolve({ code: child.exitCode, signal: child.signalCode, output })
    : new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal, output })));
  const waitForReady = async () => {
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      if (output.includes('M.A.R.C.U.S. running on')) return;
      if (child.exitCode !== null) throw new Error(`Server exited before ready (${child.exitCode}).\n${output}`);
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error(`Timed out waiting for server.\n${output}`);
  };
  const close = async ({ preserveRoot = false } = {}) => {
    if (!hasExited()) child.kill();
    if (!hasExited()) await waitForExit();
    if (!preserveRoot) await fs.rm(root, { recursive: true, force: true });
  };
  return { child, port, root, workspaceRoot, get output() { return output; }, waitForExit, waitForReady, close };
}

async function withMockCodexAdapter(callback) {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ provider: 'mock_http_codex', jobId: 'mock-job-1', status: 'started' }));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  try {
    return await callback(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

async function withMockQuoApi(callback) {
  const received = [];
  const server = http.createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/v1/phone-numbers') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ data: [{ id: 'PN_TEST', formattedNumber: '(555) 000-1111', number: '+15550001111', users: [{ id: 'US_TEST' }] }] }));
      return;
    }
    if (req.method === 'POST' && req.url === '/v1/messages') {
      let body = '';
      req.on('data', (chunk) => { body += String(chunk); });
      req.on('end', () => {
        received.push({ authorization: req.headers.authorization, body: JSON.parse(body || '{}') });
        res.writeHead(202, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ data: { id: `MSG_${received.length}`, status: 'queued' } }));
      });
      return;
    }
    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'not found' }));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  try {
    return await callback({ baseUrl: `http://127.0.0.1:${port}`, received });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

async function withMockSmtp(callback) {
  const received = [];
  let rejectedMessages = 0;
  const server = net.createServer((socket) => {
    let buffer = '';
    let dataLines = null;
    socket.setEncoding('utf8');
    socket.write('220 mock-smtp ESMTP\r\n');
    socket.on('data', (chunk) => {
      buffer += chunk;
      while (buffer.includes('\r\n')) {
        const separator = buffer.indexOf('\r\n');
        const line = buffer.slice(0, separator);
        buffer = buffer.slice(separator + 2);
        if (dataLines) {
          if (line === '.') {
            const message = dataLines.join('\r\n');
            dataLines = null;
            if (rejectedMessages > 0) {
              rejectedMessages -= 1;
              socket.write('550 5.7.1 sender is not authorized\r\n');
            } else {
              received.push(message);
              socket.write(`250 2.0.0 queued as TEST_${received.length}\r\n`);
            }
          } else {
            dataLines.push(line);
          }
          continue;
        }
        if (/^(EHLO|HELO)\b/i.test(line)) socket.write('250-mock-smtp\r\n250-AUTH PLAIN\r\n250 PIPELINING\r\n');
        else if (/^AUTH PLAIN\b/i.test(line)) socket.write('235 2.7.0 authenticated\r\n');
        else if (/^MAIL FROM:/i.test(line) || /^RCPT TO:/i.test(line)) socket.write('250 2.1.0 ok\r\n');
        else if (/^DATA$/i.test(line)) { dataLines = []; socket.write('354 End data with <CR><LF>.<CR><LF>\r\n'); }
        else if (/^QUIT$/i.test(line)) { socket.write('221 2.0.0 bye\r\n'); socket.end(); }
        else if (/^(RSET|NOOP)\b/i.test(line)) socket.write('250 2.0.0 ok\r\n');
        else socket.write('250 2.0.0 ok\r\n');
      }
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  try {
    return await callback({
      port,
      received,
      rejectNextMessage() { rejectedMessages += 1; },
    });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

test('isolated server startup succeeds only for explicit loopback development or configured admin auth', async () => {
  const local = await spawnServer({ startupCheck: true, adminToken: '' });
  try {
    const exited = await local.waitForExit();
    assert.equal(exited.code, 0, exited.output);
    assert.match(exited.output, /startup validation completed/i);
  } finally { await local.close(); }

  const hosted = await spawnServer({ startupCheck: true, adminToken: '', production: true });
  try {
    const exited = await hosted.waitForExit();
    assert.notEqual(exited.code, 0);
    assert.match(exited.output, /ADMIN_TOKEN is required/i);
  } finally { await hosted.close(); }
});

test('server auth, business scope, existing reads, Marcus routing, and Live operation summary regressions', async () => {
  const server = await spawnServer({ extraEnv: { OPENAI_API_KEY: '', OPENROUTER_API_KEY: '' } });
  const base = `http://127.0.0.1:${server.port}`;
  const adminHeaders = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
  try {
    await server.waitForReady();
    assert.equal((await fetch(`${base}/api/health`)).status, 200);
    const livePage = await fetch(`${base}/live.html`);
    const liveHtml = await livePage.text();
    assert.equal(livePage.status, 200);
    assert.match(liveHtml, /<body class="live-focus">/);
    assert.match(liveHtml, /class="live-focus-hero"/);
    assert.match(liveHtml, /body\.live-focus \.command-stage/);
    const visualizerPage = await fetch(`${base}/visualizer.html`);
    const visualizerHtml = await visualizerPage.text();
    assert.equal(visualizerPage.status, 200);
    assert.match(visualizerHtml, /body: JSON\.stringify\(\{\s*message,/);
    assert.doesNotMatch(visualizerHtml, /message: `\$\{context\}\$\{message\}`/);
    const mobilePage = await fetch(`${base}/mobile.html`);
    const mobileHtml = await mobilePage.text();
    assert.equal(mobilePage.status, 200);
    assert.match(mobileHtml, /Marcus Mobile/);
    assert.match(mobileHtml, /manifest\.webmanifest/);
    assert.match(mobileHtml, /marcus-realtime\.js/);
    assert.match(mobileHtml, /marcus-operation-tracker\.js/);
    assert.match(mobileHtml, /Active durable work/);
    assert.match(mobileHtml, /Start voice/);
    assert.match(mobileHtml, /Voice mode/);
    assert.match(mobileHtml, /roast_light/);
    assert.match(mobileHtml, /Pairing code or admin token/);
    assert.match(mobileHtml, /Message providers/);
    assert.match(mobileHtml, /System acceptance/);
    assert.match(mobileHtml, /Required approvals/);
    assert.match(mobileHtml, /approvalQueue/);
    assert.match(mobileHtml, /Install Marcus/);
    assert.match(mobileHtml, /Start voice test/);
    assert.match(mobileHtml, /Confirm on this phone/);
    assert.match(mobileHtml, /Phone confirmed/);
    assert.match(mobileHtml, /Review pending message/);
    assert.match(mobileHtml, /Approve and send/);
    assert.match(mobileHtml, /Save and verify text/);
    assert.match(mobileHtml, /Save and verify email/);
    assert.match(mobileHtml, /__marcusVoiceDiagnostics/);
    assert.match(mobileHtml, /voiceTelemetryReady/);
    assert.match(mobileHtml, /marcus_voice_acceptance_session/);
    const realtimeClient = await fetch(`${base}/marcus-realtime.js`);
    assert.equal(realtimeClient.status, 200);
    const realtimeClientText = await realtimeClient.text();
    assert.match(realtimeClientText, /createMarcusRealtimeVoice/);
    assert.match(realtimeClientText, /set_marcus_personality_mode/);
    const obsPage = await fetch(`${base}/obs-marcus.html`);
    const obsHtml = await obsPage.text();
    assert.equal(obsPage.status, 200);
    assert.match(obsHtml, /MARCUS Call Console/);
    assert.match(obsHtml, /Display or system audio/);
    assert.match(obsHtml, /roast_light/);
    assert.match(obsHtml, /marcusOutput/);
    assert.match(obsHtml, /setSinkId/);
    const callPage = await fetch(`${base}/call-marcus.html`);
    assert.equal(callPage.status, 200);
    assert.match(await callPage.text(), /MARCUS Call Console/);
    assert.match(obsHtml, /meeting-notes\/checkpoint/);
    const meetingCheckpoint = await fetch(`${base}/api/marcus/meeting-notes/checkpoint`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({
        sessionId: 'live-acceptance-123',
        title: 'ScoopOS acceptance call',
        source: 'test meeting',
        startedAt: '2026-08-17T12:00:00.000Z',
        endedAt: '2026-08-17T12:05:00.000Z',
        transcript: 'We reviewed the launch.\nWe need to publish the checklist tomorrow.\npassword=supersecret',
        final: true,
      }),
    });
    const meetingCheckpointBody = await meetingCheckpoint.json();
    assert.equal(meetingCheckpoint.status, 202);
    assert.equal(meetingCheckpointBody.filename, '2026-08-17-live-acceptance-123.md');
    const claimedMeetingActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=meeting-test-agent`, { headers: adminHeaders })).json();
    const meetingAction = claimedMeetingActions.actions.find((action) => action.id === meetingCheckpointBody.actionId);
    assert.equal(meetingAction.type, 'marcus-meeting-note');
    assert.match(meetingAction.payload.content, /publish the checklist tomorrow/i);
    assert.doesNotMatch(meetingAction.payload.content, /We reviewed the launch/i);
    assert.doesNotMatch(meetingAction.payload.content, /supersecret/i);
    const manifestResponse = await fetch(`${base}/manifest.webmanifest`);
    const manifest = await manifestResponse.json();
    assert.equal(manifestResponse.status, 200);
    assert.equal(manifest.id, '/mobile.html');
    assert.equal(manifest.start_url, '/mobile.html');
    assert.equal(manifest.display, 'standalone');
    assert.ok(manifest.icons.some((icon) => icon.src === '/icons/marcus.svg'));
    assert.ok(manifest.icons.some((icon) => icon.src === '/icons/marcus-192.png' && icon.sizes === '192x192' && icon.purpose === 'any'));
    assert.ok(manifest.icons.some((icon) => icon.src === '/icons/marcus-512.png' && icon.sizes === '512x512' && icon.purpose === 'any'));
    assert.ok(manifest.icons.some((icon) => icon.src === '/icons/marcus-maskable-192.png' && icon.sizes === '192x192' && icon.purpose === 'maskable'));
    assert.ok(manifest.icons.some((icon) => icon.src === '/icons/marcus-maskable-512.png' && icon.sizes === '512x512' && icon.purpose === 'maskable'));
    const serviceWorker = await fetch(`${base}/sw.js`);
    assert.equal(serviceWorker.status, 200);
    const serviceWorkerText = await serviceWorker.text();
    assert.match(serviceWorkerText, /marcus-mobile-v23/);
    assert.match(serviceWorkerText, /marcus-maskable-512\.png/);
    const mobileIcon = await fetch(`${base}/icons/marcus.svg`);
    assert.equal(mobileIcon.status, 200);
    assert.match(await mobileIcon.text(), /<svg/);
    for (const iconPath of ['/icons/marcus-192.png', '/icons/marcus-512.png', '/icons/marcus-maskable-192.png', '/icons/marcus-maskable-512.png']) {
      const iconResponse = await fetch(`${base}${iconPath}`);
      const iconBytes = new Uint8Array(await iconResponse.arrayBuffer());
      assert.equal(iconResponse.status, 200);
      assert.match(iconResponse.headers.get('content-type') || '', /^image\/png/);
      assert.deepEqual([...iconBytes.slice(0, 8)], [137, 80, 78, 71, 13, 10, 26, 10]);
    }
    assert.equal((await fetch(`${base}/api/operations/summary`)).status, 401);
    assert.equal((await fetch(`${base}/api/marcus/memory`)).status, 401);
    assert.equal((await fetch(`${base}/api/auth/pairing-code`, { method: 'POST' })).status, 401);
    const pairingResponse = await fetch(`${base}/api/auth/pairing-code`, { method: 'POST', headers: adminHeaders, body: '{}' });
    const pairing = await pairingResponse.json();
    assert.equal(pairingResponse.status, 201);
    assert.match(pairing.code, /^\d{6}$/);
    assert.equal(Object.hasOwn(pairing, 'token'), false);
    const invalidPairingCode = pairing.code === '999999' ? '000000' : '999999';
    const invalidPair = await fetch(`${base}/api/auth/pair`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ code: invalidPairingCode }),
    });
    assert.equal(invalidPair.status, 401);
    const validPair = await fetch(`${base}/api/auth/pair`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ code: pairing.code }),
    });
    assert.equal(validPair.status, 200);
    const pairingCookie = validPair.headers.get('set-cookie');
    assert.match(pairingCookie || '', /ops_admin_token=/);
    const pairedStatus = await fetch(`${base}/api/auth/status`, { headers: { cookie: pairingCookie } });
    assert.equal((await pairedStatus.json()).authenticated, true);
    assert.equal((await fetch(`${base}/api/marcus/providers/config`, { headers: { cookie: pairingCookie } })).status, 200);
    const reusedPair = await fetch(`${base}/api/auth/pair`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ code: pairing.code }),
    });
    assert.equal(reusedPair.status, 401);
    assert.equal((await fetch(`${base}/api/tasks`, { headers: adminHeaders })).status, 200);
    assert.equal((await fetch(`${base}/api/projects`, { headers: adminHeaders })).status, 200);
    const personalMemory = await (await fetch(`${base}/api/marcus/memory?status=active`, { headers: adminHeaders })).json();
    assert.ok(personalMemory.memories.some((item) => item.kind === 'mission' && /trusted operator/i.test(item.content)));
    assert.ok(personalMemory.memories.some((item) => item.kind === 'preference' && /prebuilt voice/i.test(item.content)));
    assert.equal((await fetch(`${base}/api/tasks`, { headers: { ...adminHeaders, 'x-business-key': 'not-configured' } })).status, 403);

    const switched = await fetch(`${base}/api/businesses/active`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({ key: 'agency' }) });
    assert.equal(switched.status, 200);
    assert.equal((await fetch(`${base}/api/tasks`, { headers: { ...adminHeaders, 'x-business-key': 'agency' } })).status, 200);

    const command = await fetch(`${base}/api/marcus/command`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({ message: 'Fix the Atlas mobile layout and verify it.' }) });
    const commandBody = await command.json();
    assert.equal(command.status, 200);
    assert.equal(commandBody.intent, 'durable_operation');
    const chat = await fetch(`${base}/api/chat`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({ message: 'Implement the Atlas repository fix and verify it.' }) });
    assert.equal(chat.status, 200);
    assert.ok((await chat.json()).reply);
    const mainMemoryResponse = await fetch(`${base}/api/chat`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({
      message: 'From now on, distinguish verified facts from inferences in every project update.',
    }) });
    assert.equal(mainMemoryResponse.status, 200);
    const mainMemory = await mainMemoryResponse.json();
    assert.equal(mainMemory.missionMemory.status, 'mission_memory_created');
    assert.equal(mainMemory.missionMemory.memory.kind, 'standing_instruction');

    const sessionResponse = await fetch(`${base}/api/marcus/live/session`, { headers: adminHeaders });
    const session = await sessionResponse.json();
    assert.ok(session.token);
    const liveHeaders = { authorization: `Bearer ${session.token}`, 'x-business-key': 'agency', 'content-type': 'application/json' };
    assert.equal((await fetch(`${base}/api/marcus/memory`, { headers: liveHeaders })).status, 401);
    const rememberResponse = await fetch(`${base}/api/marcus/live/chat`, {
      method: 'POST', headers: liveHeaders, body: JSON.stringify({ message: 'Remember that I prefer project updates to lead with the operational outcome.' }),
    });
    assert.equal(rememberResponse.status, 200);
    const remembered = await rememberResponse.json();
    assert.equal(remembered.status, 'mission_memory_created');
    assert.equal(remembered.memory.kind, 'preference');
    const recallResponse = await fetch(`${base}/api/marcus/live/chat`, {
      method: 'POST', headers: liveHeaders, body: JSON.stringify({ message: 'What do you remember about project updates?' }),
    });
    assert.equal(recallResponse.status, 200);
    const recalled = await recallResponse.json();
    assert.equal(recalled.status, 'mission_memory_read');
    assert.match(recalled.reply, /operational outcome/i);
    const agencyMemory = await (await fetch(`${base}/api/marcus/memory?status=active`, { headers: { ...adminHeaders, 'x-business-key': 'agency' } })).json();
    assert.equal(agencyMemory.memories.some((item) => item.id === remembered.memory.id), true);
    assert.equal(personalMemory.memories.some((item) => item.id === remembered.memory.id), false);
    assert.equal((await fetch(`${base}/api/marcus/realtime/client-secret`, { method: 'POST' })).status, 401);
    assert.equal((await fetch(`${base}/api/marcus/realtime/telemetry`, { method: 'POST' })).status, 401);
    assert.equal((await fetch(`${base}/api/marcus/realtime/acceptance`)).status, 401);
    assert.equal((await fetch(`${base}/api/marcus/acceptance`)).status, 401);
    const staleLiveHeaders = { authorization: 'Bearer stale-after-restart', cookie: pairingCookie, 'x-business-key': 'agency', 'content-type': 'application/json' };
    const staleTokenStatus = await (await fetch(`${base}/api/auth/status`, { headers: staleLiveHeaders })).json();
    assert.equal(staleTokenStatus.authenticated, true);
    const cookieFallbackTelemetry = await fetch(`${base}/api/marcus/realtime/telemetry`, {
      method: 'POST', headers: staleLiveHeaders, body: JSON.stringify({ event: {
        eventId: 'cookie-fallback-event', sessionId: 'cookie-fallback-session', type: 'voice_state', state: 'listening',
      } }),
    });
    assert.equal(cookieFallbackTelemetry.status, 202);
    const voiceTelemetryResponse = await fetch(`${base}/api/marcus/realtime/telemetry`, {
      method: 'POST',
      headers: liveHeaders,
      body: JSON.stringify({ events: [
        {
          eventId: 'smoke-event-1',
          sessionId: 'smoke-session-1',
          type: 'client_context',
          displayMode: 'browser',
          platform: 'desktop',
          browser: 'chromium',
          installed: false,
          transcript: 'must never be retained',
          token: 'must-never-be-retained',
        },
        { eventId: 'smoke-event-2', sessionId: 'smoke-session-1', type: 'voice_state', state: 'listening' },
      ] }),
    });
    assert.equal(voiceTelemetryResponse.status, 202);
    assert.equal((await voiceTelemetryResponse.json()).accepted, 2);
    const acceptanceResponse = await fetch(`${base}/api/marcus/realtime/acceptance?sessionId=smoke-session-1`, { headers: liveHeaders });
    const acceptance = await acceptanceResponse.json();
    assert.equal(acceptanceResponse.status, 200);
    assert.equal(acceptance.latest.sessionId, 'smoke-session-1');
    assert.equal(acceptance.latest.gates.signalingConnected, true);
    assert.equal(acceptance.latest.gates.installedAndroidContext, false);
    assert.equal(acceptance.privacy.transcriptTextStored, false);
    const telemetryFile = await fs.readFile(path.join(server.root, 'data', 'businesses', 'agency', 'marcus-realtime-telemetry.json'), 'utf8');
    assert.doesNotMatch(telemetryFile, /must never be retained|must-never-be-retained/);
    const realtimeSecretWithoutOpenAi = await fetch(`${base}/api/marcus/realtime/client-secret`, { method: 'POST', headers: liveHeaders });
    assert.equal(realtimeSecretWithoutOpenAi.status, 400);
    assert.match((await realtimeSecretWithoutOpenAi.json()).error, /OpenAI is not configured/i);
    assert.equal((await fetch(`${base}/api/marcus/realtime/acceptance`)).status, 401);
    const telemetrySessionId = 'server-smoke-voice-session';
    const telemetryResponse = await fetch(`${base}/api/marcus/realtime/telemetry`, {
      method: 'POST', headers: liveHeaders, body: JSON.stringify({ events: [
        { eventId: 'context', sessionId: telemetrySessionId, type: 'client_context', platform: 'android', browser: 'chromium', displayMode: 'standalone', installed: true },
        { eventId: 'listening', sessionId: telemetrySessionId, type: 'voice_state', state: 'listening' },
        { eventId: 'transcript', sessionId: telemetrySessionId, type: 'user_transcript', length: 24, text: 'never persist this text' },
        { eventId: 'audio', sessionId: telemetrySessionId, type: 'audio_started' },
        { eventId: 'interrupt', sessionId: telemetrySessionId, type: 'audio_interrupted' },
        { eventId: 'operator', sessionId: telemetrySessionId, type: 'operator_completed', outcome: 'success' },
        { eventId: 'offline', sessionId: telemetrySessionId, type: 'network_offline' },
        { eventId: 'online', sessionId: telemetrySessionId, type: 'network_online' },
        { eventId: 'network-listening', sessionId: telemetrySessionId, type: 'voice_state', state: 'listening' },
        { eventId: 'suspended', sessionId: telemetrySessionId, type: 'background_suspended' },
        { eventId: 'resumed', sessionId: telemetrySessionId, type: 'background_resumed' },
        { eventId: 'background-listening', sessionId: telemetrySessionId, type: 'voice_state', state: 'listening' },
        { eventId: 'physical-review', sessionId: telemetrySessionId, type: 'physical_review_confirmed', confirmed: true, text: 'never retain this confirmation text' },
      ] }),
    });
    assert.equal(telemetryResponse.status, 202);
    const telemetryAcceptance = await (await fetch(`${base}/api/marcus/realtime/acceptance?sessionId=${telemetrySessionId}`, { headers: liveHeaders })).json();
    assert.equal(telemetryAcceptance.latest.readyForPhysicalReview, true);
    assert.equal(telemetryAcceptance.latest.acceptedOnPhysicalDevice, true);
    assert.equal(telemetryAcceptance.privacy.transcriptTextStored, false);
    assert.doesNotMatch(JSON.stringify(telemetryAcceptance), /never persist this text/i);
    const systemAcceptance = await (await fetch(`${base}/api/marcus/acceptance?sessionId=${telemetrySessionId}`, { headers: liveHeaders })).json();
    assert.equal(systemAcceptance.voice.session.acceptedOnPhysicalDevice, true);
    assert.equal(systemAcceptance.gates.missionMemoryReady, true);
    assert.equal(systemAcceptance.gates.physicalAndroidVoiceAccepted, true);
    assert.equal(systemAcceptance.gates.projectOperatorReady, true);
    assert.equal(systemAcceptance.gates.approvedTextSendAccepted, false);
    assert.equal(systemAcceptance.gates.approvedEmailSendAccepted, false);
    assert.equal(systemAcceptance.ready, false);
    assert.equal(systemAcceptance.missing.some((item) => item.key === 'textProviderVerified'), true);
    assert.doesNotMatch(JSON.stringify(systemAcceptance), /never retain this confirmation text/i);
    const voiceStatus = await fetch(`${base}/api/marcus/live/voice/status`, { headers: liveHeaders });
    assert.equal(voiceStatus.status, 200);
    const voiceStatusBody = await voiceStatus.json();
    assert.equal(voiceStatusBody.realtime.provider, 'openai_realtime');
    assert.equal(voiceStatusBody.realtime.model, 'gpt-realtime-2.1');
    assert.equal(voiceStatusBody.realtime.personalityMode, 'operator');
    const operatorHealth = await fetch(`${base}/api/marcus/operator-health`, { headers: liveHeaders });
    assert.equal(operatorHealth.status, 200);
    const operatorHealthBody = await operatorHealth.json();
    assert.equal(operatorHealthBody.capabilities.projectOperator.available, true);
    assert.equal(operatorHealthBody.capabilities.missionMemory.available, true);
    assert.ok(operatorHealthBody.capabilities.missionMemory.activeCount >= 4);
    assert.equal(operatorHealthBody.capabilities.projectOperator.mode, 'direct_codex');
    assert.equal(operatorHealthBody.capabilities.projectOperator.canStartCodexDirectly, true);
    assert.equal(operatorHealthBody.capabilities.projectOperator.provider, 'desktop_codex');
    const savedProviderSettings = await fetch(`${base}/api/settings`, { method: 'PUT', headers: adminHeaders, body: JSON.stringify({
      githubToken: 'ghp_savedprovider1234567890',
      githubOwner: 'markgromer',
      cloudflareApiToken: 'cf_savedprovider1234567890',
      cloudflareAccountId: 'account_123',
      cloudflareDefaultZoneId: 'zone_123',
      renderApiKey: 'rnd_savedprovider1234567890',
    }) });
    assert.equal(savedProviderSettings.status, 200);
    const settingsAfterProviderSave = await (await fetch(`${base}/api/settings`, { headers: adminHeaders })).json();
    assert.equal(Object.hasOwn(settingsAfterProviderSave, 'githubToken'), false);
    assert.equal(Object.hasOwn(settingsAfterProviderSave, 'cloudflareApiToken'), false);
    assert.equal(Object.hasOwn(settingsAfterProviderSave, 'renderApiKey'), false);
    assert.equal(Object.hasOwn(settingsAfterProviderSave, 'externalActionDrafts'), false);
    assert.equal(settingsAfterProviderSave.githubConfigured, true);
    assert.equal(settingsAfterProviderSave.githubSource, 'settings');
    assert.equal(settingsAfterProviderSave.cloudflareConfigured, true);
    assert.equal(settingsAfterProviderSave.cloudflareSource, 'settings');
    const operatorHealthWithSavedProviders = await (await fetch(`${base}/api/marcus/operator-health`, { headers: liveHeaders })).json();
    assert.equal(operatorHealthWithSavedProviders.capabilities.github.backendTokenConfigured, true);
    assert.equal(operatorHealthWithSavedProviders.capabilities.github.source, 'settings');
    assert.equal(operatorHealthWithSavedProviders.capabilities.cloudflare.backendTokenConfigured, true);
    assert.equal(operatorHealthWithSavedProviders.capabilities.cloudflare.source, 'settings');
    assert.equal(operatorHealthWithSavedProviders.blockers.some((item) => /GITHUB_TOKEN is not configured/i.test(item)), false);
    assert.equal(operatorHealthWithSavedProviders.blockers.some((item) => /CLOUDFLARE_API_TOKEN is not configured/i.test(item)), false);
    assert.equal((await fetch(`${base}/api/marcus/external-actions`, { headers: liveHeaders })).status, 401);
    for (const suffix of ['draft', 'probe/approve', 'probe/send', 'probe/reject']) {
      assert.equal((await fetch(`${base}/api/marcus/external-actions/${suffix}`, {
        method: 'POST', headers: liveHeaders, body: '{}',
      })).status, 401);
    }
    const draftExternalActionResponse = await fetch(`${base}/api/marcus/external-actions/draft`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({
        type: 'email',
        to: 'client@example.com',
        subject: 'Atlas status',
        body: 'Atlas audit is ready for review.',
        projectName: 'Atlas',
        reason: 'Marcus drafted this from the project conversation.',
      }),
    });
    assert.equal(draftExternalActionResponse.status, 201);
    const draftExternalAction = (await draftExternalActionResponse.json()).action;
    assert.equal(draftExternalAction.status, 'pending_approval');
    assert.equal(draftExternalAction.requiresApproval, true);
    assert.equal(draftExternalAction.createdBy, 'marcus');
    const liveChatApproval = await fetch(`${base}/api/marcus/live/chat`, {
      method: 'POST', headers: liveHeaders, body: JSON.stringify({ message: `approve ${draftExternalAction.id}` }),
    });
    assert.equal(liveChatApproval.status, 200);
    const liveChatApprovalBody = await liveChatApproval.json();
    assert.equal(liveChatApprovalBody.ok, false);
    assert.equal(liveChatApprovalBody.reauthenticationRequired, true);
    assert.match(liveChatApprovalBody.reply, /paired Marcus app or durable admin/i);
    const listedExternalActions = await (await fetch(`${base}/api/marcus/external-actions`, { headers: adminHeaders })).json();
    assert.equal(listedExternalActions.ok, true);
    assert.equal(listedExternalActions.actions.find((item) => item.id === draftExternalAction.id)?.status, 'pending_approval');
    const vagueApproval = await fetch(`${base}/api/marcus/external-actions/${draftExternalAction.id}/approve`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({ message: 'looks fine' }),
    });
    assert.equal(vagueApproval.status, 409);
    assert.equal((await vagueApproval.json()).approvalRequired, true);
    const explicitApproval = await fetch(`${base}/api/marcus/external-actions/${draftExternalAction.id}/approve`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({ message: `approve ${draftExternalAction.id}` }),
    });
    assert.equal(explicitApproval.status, 200);
    const explicitApprovalBody = await explicitApproval.json();
    assert.equal(explicitApprovalBody.action.status, 'approved');
    assert.match(explicitApprovalBody.note, /separate explicit provider action/i);
    const draftTextResponse = await fetch(`${base}/api/marcus/external-actions/draft`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({ type: 'text', to: '+15555550123', body: 'Can you confirm the Atlas review window?' }),
    });
    assert.equal(draftTextResponse.status, 201);
    const draftTextAction = (await draftTextResponse.json()).action;
    const rejectedText = await fetch(`${base}/api/marcus/external-actions/${draftTextAction.id}/reject`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({ message: 'do not send this version' }),
    });
    assert.equal(rejectedText.status, 200);
    assert.equal((await rejectedText.json()).action.status, 'rejected');
    const summary = await fetch(`${base}/api/operations/summary`, { headers: liveHeaders });
    assert.equal(summary.status, 200);
    const summaryBody = await summary.json();
    assert.ok(summaryBody.operations.every((operation) => !Object.hasOwn(operation, 'artifacts')));
    assert.ok(summaryBody.operations.every((operation) => !Object.hasOwn(operation, 'metadata')));
    assert.ok(summaryBody.operations.every((operation) => Object.hasOwn(operation, 'verificationSummary')));
    assert.ok(summaryBody.operations.every((operation) => Object.hasOwn(operation, 'activeBlockers')));
    assert.ok(summaryBody.operations.every((operation) => Object.hasOwn(operation, 'pendingApproval')));
    assert.ok(summaryBody.operations.every((operation) => !operation.pendingApproval
      || Object.keys(operation.pendingApproval).every((key) => ['id', 'action', 'riskLevel', 'reason', 'expiresAt'].includes(key))));
    assert.equal((await fetch(`${base}/api/operations`, { headers: liveHeaders })).status, 401);
    assert.equal((await fetch(`${base}/api/operations`, { method: 'POST', headers: liveHeaders, body: JSON.stringify({ originalRequest: 'mutate' }) })).status, 401);

    const agencyHeaders = { ...adminHeaders, 'x-business-key': 'agency' };
    const codexWorkspace = path.join(server.workspaceRoot, 'scoopFairies');
    await fs.mkdir(codexWorkspace, { recursive: true });
    const relayResponse = await fetch(`${base}/api/desktop-context/relay`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      agentId: 'agent-smoke', windowTitle: 'Codex', processName: 'ChatGPT', idleSeconds: 1,
      desktopAuthorization: {
        scope: 'full_pc', broadWorkspaceRootsAllowed: true, fullPcAccess: true,
        allowedRoots: [server.workspaceRoot], newProjectRoot: server.workspaceRoot,
        pcAccessRoots: [path.parse(server.workspaceRoot).root],
        capabilities: ['inventory', 'search_files', 'read_text_file', 'open_file_or_folder', 'launch_installed_application'],
      },
      codexWorkspaces: [{
        sessionId: 'session-smoke', workspacePath: codexWorkspace, folderName: 'scoopFairies', projectName: 'Scoop Fairies',
        modifiedAt: new Date().toISOString(), source: 'vscode', originator: 'codex_vscode', gitBranch: 'main',
        gitRemote: 'https://github.com/markgromer/scoopfairies.git', gitStatusCount: 2,
        gitStatus: [{ status: 'M', file: 'src/app/page.tsx' }], gitRecentCommits: ['abc1234 Current work'],
        handoffSummary: 'Fixed the live blocker. Refresh https://poopsites.com/admin/reggie and confirm 15 sites load.',
        handoffStatus: 'ready_for_mark',
        handoffObservedAt: new Date().toISOString(),
        transcript: 'must-not-be-relayed',
      }],
    }) });
    assert.equal(relayResponse.status, 200);
    const relayedContext = await (await fetch(`${base}/api/desktop-context`, { headers: agencyHeaders })).json();
    assert.equal(relayedContext.codexWorkspaces[0].projectName, 'Scoop Fairies');
    assert.match(relayedContext.codexWorkspaces[0].handoffSummary, /Fixed the live blocker/);
    assert.equal(relayedContext.codexWorkspaces[0].handoffStatus, 'ready_for_mark');
    assert.equal(Object.hasOwn(relayedContext.codexWorkspaces[0], 'transcript'), false);
    assert.equal(relayedContext.desktopAuthorization.scope, 'full_pc');
    assert.equal(relayedContext.desktopAuthorization.fullPcAccess, true);
    assert.deepEqual(relayedContext.desktopAuthorization.capabilities, ['inventory', 'search_files', 'read_text_file', 'open_file_or_folder', 'launch_installed_application']);
    const browserJpeg = Buffer.from([0xff, 0xd8, 0xff, 0xd9]);
    const browserRelay = await fetch(`${base}/api/marcus/browser/relay`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({
        agentId: 'agent-smoke', connected: true, sensitive: false, title: 'Skool - MARCUS', url: 'https://www.skool.com/',
        viewportWidth: 1280, viewportHeight: 720, frameBase64: browserJpeg.toString('base64'), observedAt: new Date().toISOString(),
      }),
    });
    assert.equal(browserRelay.status, 200);
    assert.equal((await browserRelay.json()).frameAccepted, true);
    const browserStatus = await (await fetch(`${base}/api/marcus/browser/status`, { headers: agencyHeaders })).json();
    assert.equal(browserStatus.online, true);
    assert.equal(browserStatus.frameAvailable, true);
    assert.equal(browserStatus.control.owner, 'marcus');
    assert.equal(browserStatus.url, 'https://www.skool.com/');
    const browserFrameResponse = await fetch(`${base}/api/marcus/browser/frame`, { headers: agencyHeaders });
    assert.equal(browserFrameResponse.status, 200);
    assert.deepEqual(Buffer.from(await browserFrameResponse.arrayBuffer()), browserJpeg);
    const blockedMarkBrowserAction = await fetch(`${base}/api/marcus/browser/actions`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({ actor: 'mark', command: 'refresh' }),
    });
    assert.equal(blockedMarkBrowserAction.status, 409);
    const markBrowserControl = await fetch(`${base}/api/marcus/browser/control`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({ owner: 'mark' }),
    });
    assert.equal(markBrowserControl.status, 200);
    const unsafeBrowserNavigation = await fetch(`${base}/api/marcus/browser/actions`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({ actor: 'mark', command: 'navigate', url: 'javascript:alert(1)' }),
    });
    assert.equal(unsafeBrowserNavigation.status, 400);
    const browserActionResponse = await fetch(`${base}/api/marcus/browser/actions`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({ actor: 'mark', command: 'open', url: 'https://mail.google.com/' }),
    });
    assert.equal(browserActionResponse.status, 202);
    const browserAction = (await browserActionResponse.json()).actionId;
    const browserActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=agent-smoke`, { headers: agencyHeaders })).json();
    const queuedBrowserAction = browserActions.actions.find((item) => item.id === browserAction);
    assert.equal(queuedBrowserAction.type, 'marcus-browser-open');
    assert.equal(queuedBrowserAction.payload.url, 'https://mail.google.com/');
    const browserActionResult = await fetch(`${base}/api/desktop-context/action-results`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({ agentId: 'agent-smoke', results: [{
        id: queuedBrowserAction.id, type: queuedBrowserAction.type, ok: true, details: { command: 'open' },
      }] }),
    });
    assert.equal(browserActionResult.status, 200);
    const publicationDraftResponse = await fetch(`${base}/api/marcus/browser/publications/draft`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({
        platform: 'skool', mode: 'reply', sourceUrl: 'https://www.skool.com/localgiants/drop-your-intro',
        sourceTitle: 'Drop Your Intro', target: 'Drop Your Intro', text: "I'm MARCUS, Mark's AI Chief of Staff.", submitLabel: 'Comment',
      }),
    });
    assert.equal(publicationDraftResponse.status, 201);
    const publicationDraft = (await publicationDraftResponse.json()).publication;
    assert.equal(publicationDraft.status, 'pending_approval');
    const publicationList = await (await fetch(`${base}/api/marcus/browser/publications`, { headers: agencyHeaders })).json();
    assert.equal(publicationList.publications[0].id, publicationDraft.id);
    const approvePublication = await fetch(`${base}/api/marcus/browser/publications/${publicationDraft.id}/approve`, {
      method: 'POST', headers: agencyHeaders, body: '{}',
    });
    assert.equal(approvePublication.status, 202);
    const publicationActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=agent-smoke`, { headers: agencyHeaders })).json();
    const publicationAction = publicationActions.actions.find((item) => item.type === 'marcus-browser-publish');
    assert.ok(publicationAction);
    assert.equal(publicationAction.payload.publicationId, publicationDraft.id);
    assert.equal(publicationAction.payload.command, 'publish-approved-draft');
    assert.equal(publicationAction.payload.text, publicationDraft.text);
    const publicationResult = await fetch(`${base}/api/desktop-context/action-results`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({ agentId: 'agent-smoke', results: [{
        id: publicationAction.id, type: publicationAction.type, publicationId: publicationDraft.id, ok: true,
        details: { command: 'publish-approved-draft', result: { publicationId: publicationDraft.id, published: true, submitLabel: 'COMMENT' } },
      }] }),
    });
    assert.equal(publicationResult.status, 200);
    const publishedList = await (await fetch(`${base}/api/marcus/browser/publications`, { headers: agencyHeaders })).json();
    assert.equal(publishedList.publications.find((item) => item.id === publicationDraft.id).status, 'published');
    const deniedDraftResponse = await fetch(`${base}/api/marcus/browser/publications/draft`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({
        platform: 'skool', mode: 'post', sourceUrl: 'https://www.skool.com/localgiants',
        sourceTitle: 'ScoopOS', target: 'Community post', text: 'Denied draft.', submitLabel: 'Post',
      }),
    });
    const deniedDraft = (await deniedDraftResponse.json()).publication;
    const denyPublication = await fetch(`${base}/api/marcus/browser/publications/${deniedDraft.id}/reject`, {
      method: 'POST', headers: agencyHeaders, body: '{}',
    });
    assert.equal(denyPublication.status, 200);
    assert.equal((await denyPublication.json()).publication.status, 'rejected');
    const sensitiveBrowserRelay = await fetch(`${base}/api/marcus/browser/relay`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({
        agentId: 'agent-smoke', connected: true, sensitive: true, title: 'Sign in', url: 'https://accounts.google.com/',
        viewportWidth: 1280, viewportHeight: 720, observedAt: new Date().toISOString(),
      }),
    });
    assert.equal(sensitiveBrowserRelay.status, 200);
    const sensitiveBrowserStatus = await (await fetch(`${base}/api/marcus/browser/status`, { headers: agencyHeaders })).json();
    assert.equal(sensitiveBrowserStatus.sensitive, true);
    assert.equal(sensitiveBrowserStatus.frameAvailable, false);
    assert.equal((await fetch(`${base}/api/marcus/browser/frame`, { headers: agencyHeaders })).status, 404);
    const pcCapabilities = await (await fetch(`${base}/api/marcus/pc/capabilities`, { headers: agencyHeaders })).json();
    assert.equal(pcCapabilities.relayOnline, true);
    assert.equal(pcCapabilities.authorization.scope, 'full_pc');
    assert.equal(pcCapabilities.authorization.fullPcAccess, true);
    assert.equal(pcCapabilities.boundaries.credentialContentBlocked, true);
    assert.deepEqual(pcCapabilities.recommendedFullPcRoots, [path.parse(server.workspaceRoot).root]);
    const existingPcAccess = await fetch(`${base}/api/marcus/pc/access-request`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({ scope: 'full_pc' }),
    });
    const existingPcAccessBody = await existingPcAccess.json();
    assert.equal(existingPcAccess.status, 200);
    assert.equal(existingPcAccessBody.alreadyConfigured, true);
    assert.equal(existingPcAccessBody.operation, null);

    const pcSearchRequest = fetch(`${base}/api/marcus/pc/actions`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({
        tool: 'pc_search_files', arguments: { query: 'Scoop Fairies' }, requestMessage: 'Find Scoop Fairies on my PC.',
      }),
    });
    await new Promise((resolve) => setTimeout(resolve, 150));
    const pcActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=agent-smoke`, { headers: agencyHeaders })).json();
    const pcSearchAction = pcActions.actions.find((item) => item.type === 'pc-search-files');
    assert.ok(pcSearchAction);
    const pcSearchRelay = await fetch(`${base}/api/desktop-context/action-results`, {
      method: 'POST', headers: agencyHeaders, body: JSON.stringify({ agentId: 'agent-smoke', results: [{
        id: pcSearchAction.id, type: pcSearchAction.type, ok: true, details: {
          query: 'scoop fairies', results: [{ name: 'Scoop Fairies', path: codexWorkspace, type: 'directory' }],
        },
      }] }),
    });
    assert.equal(pcSearchRelay.status, 200);
    const pcSearchResponse = await pcSearchRequest;
    const pcSearch = await pcSearchResponse.json();
    assert.equal(pcSearchResponse.status, 200);
    assert.equal(pcSearch.ok, true);
    assert.equal(pcSearch.details.results[0].name, 'Scoop Fairies');
    const bootstrapResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      message: 'Create a new project called Smoke Forge from scratch and publish it through GitHub and Cloudflare.',
    }) });
    const bootstrap = await bootstrapResponse.json();
    assert.equal(bootstrapResponse.status, 200);
    assert.equal(bootstrap.status, 'project_bootstrap_started');
    assert.equal(bootstrap.project.canonicalName, 'Smoke Forge');
    assert.equal(bootstrap.operation.steps.some((step) => step.toolName === 'create_repository'), true);
    assert.equal(bootstrap.operation.steps.some((step) => step.toolName === 'deploy-cloudflare-project'), true);
    const bootstrapReplayResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      message: 'Create a new project called Smoke Forge from scratch and publish it through GitHub and Cloudflare.',
    }) });
    const bootstrapReplay = await bootstrapReplayResponse.json();
    assert.equal(bootstrapReplayResponse.status, 200);
    assert.equal(bootstrapReplay.status, 'project_bootstrap_started');
    assert.equal(bootstrapReplay.operation.id, bootstrap.operation.id);
    const bootstrapActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=agent-smoke`, { headers: agencyHeaders })).json();
    const bootstrapAction = bootstrapActions.actions.find((item) => item.requestedBy === `operation:${bootstrap.operation.id}`);
    assert.ok(bootstrapAction, JSON.stringify({ operation: bootstrap.operation, actions: bootstrapActions.actions }, null, 2));
    assert.equal(bootstrapAction.type, 'create-project-workspace');
    assert.match(bootstrapAction.payload.path, /smoke-forge$/i);
    const liveStatusResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      message: 'What is the status of the Scoop Fairies project?',
    }) });
    const liveStatus = await liveStatusResponse.json();
    assert.equal(liveStatusResponse.status, 200);
    assert.equal(liveStatus.status, 'project_status');
    assert.equal(liveStatus.project.name, 'Scoop Fairies');
    assert.match(liveStatus.reply, /Codex workspace/i);
    const switchedResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      message: 'Switch to the Scoop Fairies project.',
    }) });
    const switchedProject = await switchedResponse.json();
    assert.equal(switchedProject.status, 'project_switched');
    assert.equal(switchedProject.project.name, 'Scoop Fairies');
    const workspace = path.join(server.workspaceRoot, 'desktop-smoke');
    await fs.mkdir(workspace, { recursive: true });
    await fs.writeFile(path.join(workspace, 'package.json'), JSON.stringify({ scripts: { test: 'node --test' } }));
    const registryResponse = await fetch(`${base}/api/project-registry`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      canonicalName: 'Desktop Smoke', localWorkspace: { path: workspace }, commands: { test: 'node --test' },
    }) });
    const registry = (await registryResponse.json()).project;
    assert.equal(registryResponse.status, 201);
    const manualEvidence = await fetch(`${base}/api/project-evidence/ingest`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      projectRegistryId: registry.id,
      source: 'manual',
      type: 'manual_note',
      event: 'operator_note',
      summary: 'Desktop Smoke is ready for evidence API verification.',
      actor: 'mark',
      provenance: { method: 'authenticated_smoke_test' },
    }) });
    assert.equal(manualEvidence.status, 201);
    const forgedEvidence = await fetch(`${base}/api/project-evidence/ingest`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      projectRegistryId: registry.id, source: 'github', type: 'commit', actor: 'mark', provenance: { method: 'manual' },
    }) });
    assert.equal(forgedEvidence.status, 403);
    const browserEvidence = await fetch(`${base}/api/project-evidence/browser-verification`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      projectRegistryId: registry.id, actor: 'mark', url: 'https://example.com/desktop-smoke', status: 'passed',
      viewports: [{ width: 1440, height: 900 }], screenshots: ['external://desktop-smoke.png'],
    }) });
    assert.equal(browserEvidence.status, 201);
    assert.equal((await browserEvidence.json()).mode, 'external_manual');
    const recalculatedActivity = await fetch(`${base}/api/project-activity/recalculate`, { method: 'POST', headers: agencyHeaders, body: '{}' });
    assert.equal(recalculatedActivity.status, 200);
    const projectActivity = await fetch(`${base}/api/project-activity/${registry.id}`, { headers: agencyHeaders });
    assert.equal(projectActivity.status, 200);
    assert.equal((await projectActivity.json()).activity.projectRegistryId, registry.id);
    const activeBrief = await fetch(`${base}/api/marcus/active-brief`, { headers: agencyHeaders });
    const activeBriefBody = await activeBrief.json();
    assert.equal(activeBrief.status, 200);
    assert.ok(activeBriefBody.projectEvidenceActivity.snapshots.some((item) => item.projectRegistryId === registry.id));
    assert.ok(Array.isArray(activeBriefBody.projectActivity));
    assert.equal((await fetch(`${base}/api/project-activity/current-focus`, { headers: agencyHeaders })).status, 200);
    assert.equal((await fetch(`${base}/api/project-activity/stale`, { headers: agencyHeaders })).status, 200);
    assert.equal((await fetch(`${base}/api/project-activity/bottlenecks`, { headers: agencyHeaders })).status, 200);
    const approvedWorkspace = await fetch(`${base}/api/project-registry/${registry.id}/approve-workspace`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ desktopAgentId: 'agent-smoke' }) });
    assert.equal(approvedWorkspace.status, 200);
    const operatorRegistryResponse = await fetch(`${base}/api/project-registry`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      canonicalName: 'Operator Smoke', aliases: ['Operator Smoke repository'],
    }) });
    const operatorRegistry = (await operatorRegistryResponse.json()).project;
    assert.equal(operatorRegistryResponse.status, 201);
    const operatorResponse = await fetch(`${base}/api/marcus/project-operator`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      message: 'Audit the Operator Smoke repository and get Codex fixing it.',
    }) });
    assert.equal(operatorResponse.status, 201);
    const operatorBody = await operatorResponse.json();
    assert.equal(operatorBody.status, 'codex_prepared');
    assert.match(operatorBody.codexPrompt, /Goal for Codex/);
    assert.match(operatorBody.auditBrief, /Operator Smoke/);
    assert.equal(operatorBody.operation.status, 'failed');
    assert.match(JSON.stringify(operatorBody.operation), /approved workspace path bound to a desktop agent/i);
    const freedomRegistryResponse = await fetch(`${base}/api/project-registry`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      canonicalName: 'Freedom Scoopers',
      aliases: ['Freedom Scoopers website', 'freedom scoopers website'],
      repo: { fullName: 'markgromer/freedom-scoopers' },
    }) });
    const freedomRegistry = (await freedomRegistryResponse.json()).project;
    assert.equal(freedomRegistryResponse.status, 201);
    const liveOperatorResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      message: 'The Freedom Scoopers website needs the new Reggie and Reggie hub installed and replace the legacy Reggie.',
    }) });
    assert.equal(liveOperatorResponse.status, 200);
    const liveOperatorBody = await liveOperatorResponse.json();
    assert.equal(liveOperatorBody.status, 'codex_prepared');
    assert.equal(liveOperatorBody.project.name, 'Freedom Scoopers');
    assert.equal(liveOperatorBody.operation.projectRegistryId, freedomRegistry.id);
    assert.match(liveOperatorBody.reply, /audited the available context/i);
    const operationResponse = await fetch(`${base}/api/operations`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      originalRequest: 'Verify Desktop Smoke tests.', projectRegistryId: registry.id, autoPlan: false,
    }) });
    const operation = (await operationResponse.json()).operation;
    const plannedResponse = await fetch(`${base}/api/operations/${operation.id}/plan`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ steps: [{
      title: 'Run tests', type: 'verification', provider: 'verification', toolName: 'verify_operation', input: { requirements: [{ type: 'test', required: true }] },
    }] }) });
    assert.equal(plannedResponse.status, 200);
    const startedResponse = await fetch(`${base}/api/operations/${operation.id}/start`, { method: 'POST', headers: agencyHeaders, body: '{}' });
    assert.equal(startedResponse.status, 200);
    const wrongAgentActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=wrong-agent`, { headers: agencyHeaders })).json();
    assert.equal(wrongAgentActions.actions.some((item) => item.requestedBy === `operation:${operation.id}`), false);
    const actions = await (await fetch(`${base}/api/desktop-context/actions?agentId=agent-smoke`, { headers: agencyHeaders })).json();
    const action = actions.actions.find((item) => item.requestedBy === `operation:${operation.id}`);
    assert.ok(action?.id);
    const resultResponse = await fetch(`${base}/api/desktop-context/action-results`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ agentId: 'agent-smoke', results: [{
      id: action.id, type: action.type, businessKey: 'agency', operationId: action.payload.operationId, stepId: action.payload.stepId,
      projectRegistryId: registry.id, desktopAgentId: 'agent-smoke', idempotencyKey: action.payload.idempotencyKey,
      attemptNumber: action.payload.attemptNumber, ok: true, details: { stdout: 'tests passed' },
    }] }) });
    const resultBody = await resultResponse.json();
    assert.equal(resultBody.received, 1);
    const acknowledgedActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=agent-smoke`, { headers: agencyHeaders })).json();
    assert.equal(acknowledgedActions.actions.some((item) => item.id === action.id), false);
    const unknownResult = await fetch(`${base}/api/desktop-context/action-results`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ agentId: 'agent-smoke', results: [{
      id: 'unknown-action', type: 'run-project-script', businessKey: 'agency', projectRegistryId: registry.id, desktopAgentId: 'agent-smoke', ok: true,
    }] }) });
    assert.equal((await unknownResult.json()).rejected[0].code, 'DESKTOP_ACTION_UNKNOWN');
  } finally { await server.close(); }
});

test('mobile pairing survives a server restart and remains single-use', async () => {
  const first = await spawnServer();
  const headers = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
  let second = null;
  try {
    await first.waitForReady();
    const pairingResponse = await fetch(`http://127.0.0.1:${first.port}/api/auth/pairing-code`, { method: 'POST', headers, body: '{}' });
    assert.equal(pairingResponse.status, 201);
    const pairing = await pairingResponse.json();
    await first.close({ preserveRoot: true });

    second = await spawnServer({ testRoot: first.root });
    await second.waitForReady();
    const base = `http://127.0.0.1:${second.port}`;
    const accepted = await fetch(`${base}/api/auth/pair`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ code: pairing.code }),
    });
    assert.equal(accepted.status, 200);
    assert.match(accepted.headers.get('set-cookie') || '', /ops_admin_token=/);
    const replay = await fetch(`${base}/api/auth/pair`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ code: pairing.code }),
    });
    assert.equal(replay.status, 401);
  } finally {
    if (first.child.exitCode === null && first.child.signalCode === null) await first.close();
    if (second) await second.close();
    else await fs.rm(first.root, { recursive: true, force: true });
  }
});

test('mission memory survives a server restart, rejects secrets, and remains business-scoped', async () => {
  const headers = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
  const first = await spawnServer();
  let second = null;
  try {
    await first.waitForReady();
    const firstBase = `http://127.0.0.1:${first.port}`;
    const createdResponse = await fetch(`${firstBase}/api/marcus/memory`, {
      method: 'POST', headers, body: JSON.stringify({
        kind: 'decision',
        title: 'Production reporting',
        content: 'Report production changes only after checking the durable runtime evidence.',
        priority: 5,
      }),
    });
    assert.equal(createdResponse.status, 201);
    const created = (await createdResponse.json()).memory;
    const rejected = await fetch(`${firstBase}/api/marcus/memory`, {
      method: 'POST', headers, body: JSON.stringify({ kind: 'fact', content: 'api_key="sk-never-store-this-secret"' }),
    });
    assert.equal(rejected.status, 400);
    assert.equal((await rejected.json()).code, 'MEMORY_SECRET_REJECTED');
    await first.close({ preserveRoot: true });

    second = await spawnServer({ testRoot: first.root });
    await second.waitForReady();
    const secondBase = `http://127.0.0.1:${second.port}`;
    const personal = await (await fetch(`${secondBase}/api/marcus/memory?status=active`, { headers })).json();
    assert.equal(personal.memories.some((item) => item.id === created.id), true);
    assert.doesNotMatch(JSON.stringify(personal), /sk-never-store-this-secret/);
    const crossBusiness = await fetch(`${secondBase}/api/marcus/memory?status=active`, { headers: { ...headers, 'x-business-key': 'agency' } });
    assert.equal(crossBusiness.status, 403);
  } finally {
    if (first.child.exitCode === null && first.child.signalCode === null) await first.close();
    if (second) await second.close();
    else await fs.rm(first.root, { recursive: true, force: true });
  }
});

test('server enables direct Codex mode when HTTP adapter URL is configured', async () => {
  await withMockCodexAdapter(async (adapterUrl) => {
    const server = await spawnServer({ extraEnv: { MARCUS_DESKTOP_CODEX_ENABLED: 'false', MARCUS_CODEX_ADAPTER_URL: adapterUrl, MARCUS_CODEX_ADAPTER_TOKEN: 'test-token' } });
    const base = `http://127.0.0.1:${server.port}`;
    const adminHeaders = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
    try {
      await server.waitForReady();
      const session = await (await fetch(`${base}/api/marcus/live/session`, { headers: adminHeaders })).json();
      const liveHeaders = { authorization: `Bearer ${session.token}`, 'content-type': 'application/json' };
      const health = await (await fetch(`${base}/api/marcus/operator-health`, { headers: liveHeaders })).json();
      assert.equal(health.capabilities.projectOperator.mode, 'direct_codex');
      assert.equal(health.capabilities.projectOperator.canStartCodexDirectly, true);
      assert.equal(health.capabilities.projectOperator.provider, 'http_codex');
      assert.equal(health.capabilities.codexResultReview.authoritativeEvidence, false);
      assert.equal(health.capabilities.codexResultReview.available, false);
      assert.equal(health.blockers.some((item) => /direct Codex launch adapter/i.test(item)), false);
    } finally { await server.close(); }
  });
});

test('Marcus Live approval follow-up advances a waiting project-operator operation', async () => {
  await withMockCodexAdapter(async (adapterUrl) => {
    const server = await spawnServer({ extraEnv: { MARCUS_CODEX_ADAPTER_URL: adapterUrl, MARCUS_CODEX_ADAPTER_TOKEN: 'test-token' } });
    const base = `http://127.0.0.1:${server.port}`;
    const adminHeaders = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
    try {
      await server.waitForReady();
      const registryResponse = await fetch(`${base}/api/project-registry`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({
        canonicalName: 'Freedom Scoopers',
        aliases: ['Freedom Scoopers website', 'freedom scoopers website'],
        repo: { fullName: 'markgromer/freedom-scoopers' },
      }) });
      assert.equal(registryResponse.status, 201);
      const createResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({
        message: 'Audit the Freedom Scoopers website and prepare an implementation plan.',
      }) });
      assert.equal(createResponse.status, 200);
      const created = await createResponse.json();
      assert.equal(created.status, 'codex_prepared');
      assert.equal(created.operation.status, 'waiting_for_approval');
      assert.equal(created.operation.approvals.filter((approval) => approval.status === 'pending').length, 1);

      const liveSession = await (await fetch(`${base}/api/marcus/live/session`, { headers: adminHeaders })).json();
      const liveOnlyHeaders = { authorization: `Bearer ${liveSession.token}`, 'content-type': 'application/json' };
      const deniedLiveApproval = await fetch(`${base}/api/marcus/live/chat`, {
        method: 'POST', headers: liveOnlyHeaders, body: JSON.stringify({ message: 'Get it done' }),
      });
      assert.equal(deniedLiveApproval.status, 200);
      const deniedLiveApprovalBody = await deniedLiveApproval.json();
      assert.equal(deniedLiveApprovalBody.ok, false);
      assert.equal(deniedLiveApprovalBody.reauthenticationRequired, true);
      const pendingAfterLiveAttempt = await (await fetch(`${base}/api/operations/${created.operation.id}`, { headers: adminHeaders })).json();
      assert.equal(pendingAfterLiveAttempt.operation.status, 'waiting_for_approval');

      const approvalResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({ message: 'Get it done' }) });
      assert.equal(approvalResponse.status, 200);
      const approved = await approvalResponse.json();
      assert.equal(approved.ok, true);
      assert.equal(approved.operation.id, created.operation.id);
      assert.notEqual(approved.operation.status, 'waiting_for_approval');
      assert.match(approved.reply, /Approved/);
      assert.equal(Object.values(approved.operation.metadata.codexJobs || {}).some((job) => job.provider === 'mock_http_codex'), true);
    } finally { await server.close(); }
  });
});

test('Marcus Mobile remembers an explicit Reggie repo and carries requirements into the Codex operation', async () => {
  await withMockCodexAdapter(async (adapterUrl) => {
    const server = await spawnServer({ extraEnv: { MARCUS_CODEX_ADAPTER_URL: adapterUrl, MARCUS_CODEX_ADAPTER_TOKEN: 'test-token', GITHUB_TOKEN: 'test-github-token' } });
    const base = `http://127.0.0.1:${server.port}`;
    const headers = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
    try {
      await server.waitForReady();
      const contextResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers, body: JSON.stringify({
        message: 'Reggie is my GitHub project at markgromer/Reggie.git. Sweep and Go needs a settings popup for its API token and slug.',
      }) });
      assert.equal(contextResponse.status, 200);
      const context = await contextResponse.json();
      assert.equal(context.status, 'project_context_set');
      assert.equal(context.project.name, 'Reggie');
      const settingsWithMemory = await (await fetch(`${base}/api/settings`, { headers })).json();
      const reggieMemory = settingsWithMemory.marcusLiveConversation.projectMemories.find((memory) => memory.project.name === 'Reggie');
      assert.match(reggieMemory.requirements.join(' '), /settings popup/i);
      assert.match(reggieMemory.requirements.join(' '), /API token and slug/i);

      const atlasResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers, body: JSON.stringify({
        message: 'Atlas is my GitHub project at markgromer/Atlas. Atlas needs a blue launch banner.',
      }) });
      assert.equal(atlasResponse.status, 200);
      const atlas = await atlasResponse.json();
      assert.equal(atlas.status, 'project_context_set');
      assert.equal(atlas.project.name, 'Atlas');

      const readOnlyResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers, body: JSON.stringify({
        message: 'For a read-only continuity check, use Reggie at markgromer/Reggie. Tell me the repository and retained requirements. Do not audit or start Codex.',
      }) });
      assert.equal(readOnlyResponse.status, 200);
      const readOnly = await readOnlyResponse.json();
      assert.equal(readOnly.status, 'project_context_set');
      assert.match(readOnly.reply, /markgromer\/Reggie/i);
      assert.match(readOnly.reply, /API token and slug/i);
      assert.match(readOnly.reply, /did not audit.*or start Codex/i);
      assert.doesNotMatch(readOnly.reply, /blue launch banner/i);
      assert.doesNotMatch(readOnly.reply, /Current request retained/i);
      assert.ok(readOnly.reply.length < 1_200);
      const beforeExecution = await (await fetch(`${base}/api/operations`, { headers })).json();
      assert.equal(beforeExecution.operations.length, 0);

      const secondAtlasResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers, body: JSON.stringify({
        message: 'Switch back to Atlas at markgromer/Atlas. Keep its blue launch banner requirement.',
      }) });
      assert.equal(secondAtlasResponse.status, 200);
      assert.equal((await secondAtlasResponse.json()).project.name, 'Atlas');

      const operationResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers, body: JSON.stringify({
        message: 'For Reggie at markgromer/Reggie, check the git repo and set up the plan, then get it going in Codex.',
      }) });
      assert.equal(operationResponse.status, 200);
      const result = await operationResponse.json();
      assert.equal(result.status, 'codex_prepared');
      assert.equal(result.operation.projectName, 'Reggie');
      assert.match(result.operation.originalRequest, /settings popup/i);
      assert.match(result.operation.originalRequest, /API token and slug/i);
      assert.doesNotMatch(result.operation.originalRequest, /blue launch banner/i);
      assert.equal(Object.values(result.operation.metadata.codexJobs || {}).some((job) => job.provider === 'mock_http_codex'), true);

      const settingsBeforeMigrationCheck = await (await fetch(`${base}/api/settings`, { headers })).json();
      const resetConversationResponse = await fetch(`${base}/api/settings`, { method: 'PUT', headers, body: JSON.stringify({
        marcusLiveConversation: {
          messages: [],
          activeProject: settingsBeforeMigrationCheck.marcusLiveConversation.activeProject,
          projectMemories: [],
          updatedAt: new Date().toISOString(),
        },
      }) });
      assert.equal(resetConversationResponse.status, 200);
      const recoveredResponse = await fetch(`${base}/api/marcus/live/chat`, { method: 'POST', headers, body: JSON.stringify({
        message: 'For a read-only continuity check, use Reggie at markgromer/Reggie. Repeat the saved Sweep and Go requirements. Do not audit or start Codex.',
      }) });
      assert.equal(recoveredResponse.status, 200);
      const recovered = await recoveredResponse.json();
      assert.match(recovered.reply, /settings popup/i);
      assert.match(recovered.reply, /API token and slug/i);
      assert.doesNotMatch(recovered.reply, /blue launch banner/i);
      const afterRecovery = await (await fetch(`${base}/api/operations`, { headers })).json();
      assert.equal(afterRecovery.operations.length, 1);
    } finally { await server.close(); }
  });
});

test('approved Marcus text actions send exactly once through Quo', async () => {
  await withMockQuoApi(async ({ baseUrl: quoBaseUrl, received }) => {
    const server = await spawnServer({ extraEnv: {
      QUO_API_KEY: 'test-quo-key',
      QUO_DEFAULT_PHONE_NUMBER_ID: 'PN_TEST',
      QUO_API_BASE_URL: quoBaseUrl,
    } });
    const base = `http://127.0.0.1:${server.port}`;
    const headers = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
    try {
      await server.waitForReady();
      const health = await (await fetch(`${base}/api/marcus/operator-health`, { headers })).json();
      assert.equal(health.capabilities.communication.textSendConfigured, true);

      const draftResponse = await fetch(`${base}/api/marcus/external-actions/draft`, { method: 'POST', headers, body: JSON.stringify({
        type: 'text', to: '+15550002222', body: 'Marcus communication acceptance test.',
      }) });
      assert.equal(draftResponse.status, 201);
      const draft = (await draftResponse.json()).action;

      const unapproved = await fetch(`${base}/api/marcus/external-actions/${draft.id}/send`, { method: 'POST', headers, body: '{}' });
      assert.equal(unapproved.status, 409);
      assert.equal(received.length, 0);

      const approval = await fetch(`${base}/api/marcus/external-actions/${draft.id}/approve`, { method: 'POST', headers, body: JSON.stringify({ message: 'Send the text now.' }) });
      assert.equal(approval.status, 200);
      const sent = await fetch(`${base}/api/marcus/external-actions/${draft.id}/send`, { method: 'POST', headers, body: '{}' });
      assert.equal(sent.status, 200);
      assert.equal((await sent.json()).action.status, 'sent');
      assert.equal(received.length, 1);
      assert.equal(received[0].authorization, 'test-quo-key');
      assert.equal(received[0].body.content, 'Marcus communication acceptance test.');
      assert.deepEqual(received[0].body.to, ['+15550002222']);

      const replay = await fetch(`${base}/api/marcus/external-actions/${draft.id}/send`, { method: 'POST', headers, body: '{}' });
      assert.equal(replay.status, 200);
      assert.equal((await replay.json()).reused, true);
      assert.equal(received.length, 1);
    } finally { await server.close(); }
  });
});

test('approved Marcus email actions can retry a provider failure and then send exactly once through SMTP', async () => {
  await withMockSmtp(async ({ port, received, rejectNextMessage }) => {
    const server = await spawnServer({ extraEnv: {
      SMTP_HOST: '127.0.0.1',
      SMTP_PORT: String(port),
      SMTP_SECURE: 'false',
      SMTP_USERNAME: 'marcus@example.com',
      SMTP_PASSWORD: 'test-password',
      SMTP_FROM_ADDRESS: 'marcus@example.com',
    } });
    const base = `http://127.0.0.1:${server.port}`;
    const headers = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
    try {
      await server.waitForReady();
      const health = await (await fetch(`${base}/api/marcus/operator-health`, { headers })).json();
      assert.equal(health.capabilities.communication.emailSendConfigured, true);

      const draftResponse = await fetch(`${base}/api/marcus/external-actions/draft`, { method: 'POST', headers, body: JSON.stringify({
        type: 'email', to: 'client@example.com', subject: 'Marcus acceptance', body: 'The approved email path is working.',
      }) });
      assert.equal(draftResponse.status, 201);
      const draft = (await draftResponse.json()).action;

      const approval = await fetch(`${base}/api/marcus/external-actions/${draft.id}/approve`, { method: 'POST', headers, body: JSON.stringify({ message: 'Approve and send the email.' }) });
      const approvalBody = await approval.json();
      assert.equal(approval.status, 200, JSON.stringify(approvalBody));
      rejectNextMessage();
      const failed = await fetch(`${base}/api/marcus/external-actions/${draft.id}/send`, { method: 'POST', headers, body: '{}' });
      assert.equal(failed.status, 502);
      assert.match((await failed.json()).error, /sender is not authorized/i);
      assert.equal(received.length, 0);

      const failedAction = (await (await fetch(`${base}/api/marcus/external-actions`, { headers })).json()).actions
        .find((action) => action.id === draft.id);
      assert.equal(failedAction.status, 'failed');
      assert.match(failedAction.approvedAt, /^\d{4}-\d{2}-\d{2}T/);

      const sent = await fetch(`${base}/api/marcus/external-actions/${draft.id}/send`, { method: 'POST', headers, body: '{}' });
      assert.equal(sent.status, 200);
      const sentBody = await sent.json();
      assert.equal(sentBody.action.status, 'sent');
      assert.equal(received.length, 1);
      assert.match(received[0], /Subject: Marcus acceptance/i);
      assert.match(received[0], /The approved email path is working\./i);

      const replay = await fetch(`${base}/api/marcus/external-actions/${draft.id}/send`, { method: 'POST', headers, body: '{}' });
      assert.equal(replay.status, 200);
      assert.equal((await replay.json()).reused, true);
      assert.equal(received.length, 1);
    } finally { await server.close(); }
  });
});

test('paired admin can configure, verify, and acceptance-test Quo and SMTP safely', async () => {
  await withMockQuoApi(async ({ baseUrl: quoBaseUrl, received: texts }) => {
    await withMockSmtp(async ({ port: smtpPort, received: emails }) => {
      const server = await spawnServer({ extraEnv: {
        QUO_API_KEY: '', OPENPHONE_API_KEY: '',
        QUO_DEFAULT_PHONE_NUMBER_ID: '', OPENPHONE_DEFAULT_PHONE_NUMBER_ID: '',
        QUO_FROM_NUMBER: '', OPENPHONE_FROM_NUMBER: '',
        QUO_USER_ID: '', OPENPHONE_USER_ID: '',
        QUO_API_BASE_URL: quoBaseUrl,
        SMTP_HOST: '', SMTP_PORT: '', SMTP_SECURE: '', SMTP_USERNAME: '', SMTP_PASSWORD: '', SMTP_FROM_ADDRESS: '',
      } });
      const base = `http://127.0.0.1:${server.port}`;
      const headers = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
      try {
        await server.waitForReady();
        const session = await (await fetch(`${base}/api/marcus/live/session`, { headers })).json();
        const liveHeaders = { authorization: `Bearer ${session.token}`, 'content-type': 'application/json' };
        assert.equal((await fetch(`${base}/api/marcus/providers/config`, { headers: liveHeaders })).status, 401);
        assert.equal((await fetch(`${base}/api/marcus/providers/config`, { method: 'PUT', headers: liveHeaders, body: '{}' })).status, 401);
        assert.equal((await fetch(`${base}/api/marcus/providers/verify`, { method: 'POST', headers: liveHeaders, body: JSON.stringify({ type: 'text' }) })).status, 401);

        const invalidUpdate = await fetch(`${base}/api/marcus/providers/config`, {
          method: 'PUT', headers, body: JSON.stringify({ email: { fromAddress: 'not-an-email' } }),
        });
        assert.equal(invalidUpdate.status, 400);
        assert.match((await invalidUpdate.json()).error, /valid email address/i);

        const updateResponse = await fetch(`${base}/api/marcus/providers/config`, {
          method: 'PUT', headers, body: JSON.stringify({
            text: { apiKey: 'saved-quo-key', defaultPhoneNumberId: '', fromNumber: '', userId: '' },
            email: {
              host: '127.0.0.1', port: smtpPort, secure: false,
              username: 'marcus@example.com', password: 'saved-smtp-password', fromAddress: 'Marcus <marcus@example.com>',
            },
          }),
        });
        const configured = await updateResponse.json();
        assert.equal(updateResponse.status, 200, JSON.stringify(configured));
        assert.equal(configured.text.apiKeyConfigured, true);
        assert.equal(configured.text.apiKey, undefined);
        assert.equal(configured.email.passwordConfigured, true);
        assert.equal(configured.email.password, undefined);
        assert.equal(configured.email.fromAddress, 'Marcus <marcus@example.com>');

        const textVerifyResponse = await fetch(`${base}/api/marcus/providers/verify`, {
          method: 'POST', headers, body: JSON.stringify({ type: 'text' }),
        });
        const textVerify = await textVerifyResponse.json();
        assert.equal(textVerifyResponse.status, 200, JSON.stringify(textVerify));
        assert.equal(textVerify.verified, true);
        assert.equal(textVerify.sent, false);
        assert.equal(textVerify.sender.phoneNumberId, 'PN_TEST');
        assert.equal(textVerify.sender.fromNumber, '+15550001111');
        assert.equal(textVerify.sender.userId, 'US_TEST');
        assert.equal(texts.length, 0);

        const emailVerifyResponse = await fetch(`${base}/api/marcus/providers/verify`, {
          method: 'POST', headers, body: JSON.stringify({ type: 'email' }),
        });
        const emailVerify = await emailVerifyResponse.json();
        assert.equal(emailVerifyResponse.status, 200, JSON.stringify(emailVerify));
        assert.equal(emailVerify.verified, true);
        assert.equal(emailVerify.sent, false);
        assert.equal(emails.length, 0);

        const verifiedConfiguration = await (await fetch(`${base}/api/marcus/providers/config`, { headers })).json();
        assert.equal(verifiedConfiguration.text.verification.verified, true);
        assert.equal(verifiedConfiguration.email.verification.verified, true);
        assert.match(verifiedConfiguration.text.verification.verifiedAt, /^\d{4}-\d{2}-\d{2}T/);
        assert.match(verifiedConfiguration.email.verification.verifiedAt, /^\d{4}-\d{2}-\d{2}T/);

        const unchangedUpdateResponse = await fetch(`${base}/api/marcus/providers/config`, {
          method: 'PUT', headers, body: JSON.stringify({
            text: { apiKey: '', defaultPhoneNumberId: 'PN_TEST', fromNumber: '+15550001111', userId: 'US_TEST' },
            email: {
              host: '127.0.0.1', port: smtpPort, secure: false,
              username: 'marcus@example.com', password: '', fromAddress: 'Marcus <marcus@example.com>',
            },
          }),
        });
        const unchangedConfiguration = await unchangedUpdateResponse.json();
        assert.equal(unchangedUpdateResponse.status, 200, JSON.stringify(unchangedConfiguration));
        assert.equal(unchangedConfiguration.text.verification.verifiedAt, verifiedConfiguration.text.verification.verifiedAt);
        assert.equal(unchangedConfiguration.email.verification.verifiedAt, verifiedConfiguration.email.verification.verifiedAt);

        const storedSettings = await (await fetch(`${base}/api/settings`, { headers })).json();
        assert.equal(storedSettings.quoApiKey, undefined);
        assert.equal(storedSettings.smtpPassword, undefined);
        assert.equal(storedSettings.marcusProviderVerification, undefined);
        const health = await (await fetch(`${base}/api/marcus/operator-health`, { headers })).json();
        assert.equal(health.capabilities.communication.textSendConfigured, true);
        assert.equal(health.capabilities.communication.emailSendConfigured, true);
        assert.equal(health.capabilities.communication.textProviderVerified, true);
        assert.equal(health.capabilities.communication.emailProviderVerified, true);

        const createApprovedSend = async (draft) => {
          const draftResponse = await fetch(`${base}/api/marcus/external-actions/draft`, {
            method: 'POST', headers, body: JSON.stringify(draft),
          });
          assert.equal(draftResponse.status, 201);
          const action = (await draftResponse.json()).action;
          const approvalResponse = await fetch(`${base}/api/marcus/external-actions/${action.id}/approve`, {
            method: 'POST', headers, body: JSON.stringify({ message: `Approve and send this ${draft.type}.` }),
          });
          assert.equal(approvalResponse.status, 200);
          const sendResponse = await fetch(`${base}/api/marcus/external-actions/${action.id}/send`, { method: 'POST', headers, body: '{}' });
          assert.equal(sendResponse.status, 200);
          return (await sendResponse.json()).action;
        };
        const sentText = await createApprovedSend({ type: 'text', to: '+15550002222', body: 'Provider acceptance text.' });
        const sentEmail = await createApprovedSend({ type: 'email', to: 'client@example.com', subject: 'Provider acceptance', body: 'Provider acceptance email.' });
        assert.equal(sentText.status, 'sent');
        assert.equal(sentEmail.status, 'sent');
        assert.equal(texts.length, 1);
        assert.equal(emails.length, 1);
        const providerAcceptance = await (await fetch(`${base}/api/marcus/acceptance`, { headers })).json();
        assert.equal(providerAcceptance.gates.textProviderVerified, true);
        assert.equal(providerAcceptance.gates.emailProviderVerified, true);
        assert.equal(providerAcceptance.gates.approvedTextSendAccepted, true);
        assert.equal(providerAcceptance.gates.approvedEmailSendAccepted, true);
        assert.equal(providerAcceptance.providers.text.acceptedSend.actionId, sentText.id);
        assert.equal(providerAcceptance.providers.email.acceptedSend.actionId, sentEmail.id);

        const changedSettingsResponse = await fetch(`${base}/api/settings`, {
          method: 'PUT', headers, body: JSON.stringify({ quoApiKey: 'changed-after-verification' }),
        });
        assert.equal(changedSettingsResponse.status, 200);
        const changedConfiguration = await (await fetch(`${base}/api/marcus/providers/config`, { headers })).json();
        assert.equal(changedConfiguration.text.verification, null);
        assert.equal(changedConfiguration.email.verification.verified, true);
        const invalidatedAcceptance = await (await fetch(`${base}/api/marcus/acceptance`, { headers })).json();
        assert.equal(invalidatedAcceptance.gates.textProviderVerified, false);
        assert.equal(invalidatedAcceptance.gates.approvedTextSendAccepted, false);
        assert.equal(invalidatedAcceptance.gates.approvedEmailSendAccepted, true);
      } finally { await server.close(); }
    });
  });
});

test('server enables Reggie-style GitHub Actions Codex mode when configured', async () => {
  const server = await spawnServer({ extraEnv: {
    MARCUS_DESKTOP_CODEX_ENABLED: 'false',
    MARCUS_CODEX_GITHUB_ACTIONS_ENABLED: 'true',
    MARCUS_CODEX_GITHUB_TOKEN: 'ghp_testcodexactions1234567890',
    MARCUS_CODEX_RUNNER_REPO: 'markgromer/os1',
  } });
  const base = `http://127.0.0.1:${server.port}`;
  const adminHeaders = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
  try {
    await server.waitForReady();
    const session = await (await fetch(`${base}/api/marcus/live/session`, { headers: adminHeaders })).json();
    const liveHeaders = { authorization: `Bearer ${session.token}`, 'content-type': 'application/json' };
    const health = await (await fetch(`${base}/api/marcus/operator-health`, { headers: liveHeaders })).json();
    assert.equal(health.capabilities.projectOperator.mode, 'direct_codex');
    assert.equal(health.capabilities.projectOperator.canStartCodexDirectly, true);
    assert.equal(health.capabilities.projectOperator.provider, 'github_actions_codex');
    assert.equal(health.capabilities.codexResultReview.authoritativeEvidence, true);
    assert.equal(health.capabilities.codexResultReview.independentReviewerConfigured, true);
    assert.equal(health.capabilities.codexResultReview.available, health.capabilities.openai.configured);
    assert.equal(health.blockers.some((item) => /direct Codex launch adapter/i.test(item)), false);
  } finally { await server.close(); }
});
