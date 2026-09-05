import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import vm from 'node:vm';
import test from 'node:test';

test('context heartbeat does not wait on the independent action runner and records upload status', async () => {
  const source = await fs.readFile(new URL('../desktop-agent.cjs', import.meta.url), 'utf8');
  const tickSource = source.slice(source.indexOf('async function tick() {'), source.indexOf('let browserRelayInFlight = false;'));
  let actionCalls = 0, uploads = 0;
  const observations = [];
  const context = vm.createContext({
    Date, console: { log() {}, error() {} }, process: { stdout: { write() {} } },
    checkDesktopActions: () => { actionCalls++; return new Promise(() => {}); },
    writeDesktopAgentStatus: (value) => observations.push(value.contextRelay),
    captureDesktop: async () => ({ windowTitle: '', processName: 'fixture', idleSeconds: 0 }),
    consecutive: 0, lastTitle: '', lastWorkspacePath: '', EDITOR_PROCESSES: /editor/,
    cachedCodexWorkspaces: [{ sessionId: 'fixture' }], lastCodexWorkspaceScanAt: Date.now(), CODEX_WORKSPACE_SCAN_INTERVAL_MS: 30000,
    DESKTOP_AGENT_ID: 'fixture', FULL_PC_ACCESS: false, ALLOW_BROAD_WORKSPACE_ROOTS: false, ALLOWED_WORKSPACE_ROOT_VALUES: [], NEW_PROJECT_ROOT: '', PC_ACCESS_POLICY: { roots: [], capabilities: [] },
    cachedSystemHealth: {}, lastSystemHealthAt: Date.now(), SYSTEM_HEALTH_INTERVAL_MS: 60000,
    relay: async () => { uploads++; return { status: 200, body: 'ok' }; },
  });
  vm.runInContext(`${tickSource}\nglobalThis.testTick = tick;`, context);
  await context.testTick();
  assert.equal(actionCalls, 0, 'actions are polled only by the existing action loop');
  assert.equal(uploads, 1);
  assert.equal(observations.at(-1).phase, 'uploaded');
  assert.equal(observations.at(-1).ok, true);
  assert.equal(observations.at(-1).status, 200);
  context.relay = async () => ({ status: 503, body: 'unavailable' });
  await context.testTick();
  assert.equal(observations.at(-1).ok, false);
  assert.equal(observations.at(-1).status, 503);
  assert.ok(source.includes('async function runBrowserActionLoop()'));
  context.cachedCodexWorkspaces = [];
  let scans = 0;
  context.refreshCodexWorkspaceObservations = () => { scans++; return new Promise(() => {}); };
  await context.testTick();
  assert.equal(scans, 1, 'a stuck repository scan does not hold up the heartbeat');
  assert.equal(observations.at(-1).phase, 'uploaded');
});

test('Git observations have a deadline even when a killed child never closes its pipes', async () => {
  const source = await fs.readFile(new URL('../desktop-agent.cjs', import.meta.url), 'utf8');
  const gitSource = source.slice(source.indexOf('function gitCmd('), source.indexOf('function runLocalCommand('));
  let expire, timeout;
  const context = vm.createContext({ execFile() {}, setTimeout: (fn, ms) => { expire = fn; timeout = ms; }, clearTimeout() {} });
  vm.runInContext(`${gitSource}\nglobalThis.testGit = gitCmd;`, context);
  const pending = context.testGit('fixture', ['status']);
  assert.equal(timeout, 6000);
  expire();
  assert.equal(await pending, '');
});

test('session enrichment is bounded to four workers and preserves separate tasks', async () => {
  const source = await fs.readFile(new URL('../desktop-agent.cjs', import.meta.url), 'utf8');
  const refreshSource = source.slice(source.indexOf('let codexWorkspaceScanInFlight = false;'), source.indexOf('async function tick() {'));
  let running = 0, peak = 0, scans = 0;
  const context = vm.createContext({
    Date, cachedCodexWorkspaces: [], lastCodexWorkspaceScanAt: 0, writeDesktopAgentStatus() {},
    discoverRecentCodexWorkspaces: () => { scans++; return Array.from({ length: 12 }, (_, id) => ({ id })); },
    scanCodexWorkspaceSummary: async (session) => { running++; peak = Math.max(peak, running); await new Promise(resolve => setImmediate(resolve)); running--; return session; },
  });
  vm.runInContext(`${refreshSource}\nglobalThis.testRefresh = refreshCodexWorkspaceObservations;`, context);
  await Promise.all([context.testRefresh(), context.testRefresh()]);
  assert.equal(scans, 1); assert.equal(peak, 4);
  assert.equal(context.cachedCodexWorkspaces.length, 12);
  assert.equal(context.cachedCodexWorkspaces[11].id, 11);
});
