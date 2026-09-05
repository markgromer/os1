import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import vm from 'node:vm';
import { matchWorkProject, reportState, evidenceStages, workOverviewHtml, safeWorkUrl, projectBrief } from '../public/work-status-view.js';

const linked = { id: 'registry-exact', name: 'Task Tracker', workspacePath: 'C:/Work/Task Tracker', repository: 'owner/project', workCount: 0, items: [], operationCount: 0, operations: [], decisions: [], engineering: { lifecycle: 'inactive', granted: false, autoAdvance: false }, deployment: null, recentChanges: [] };
const overview = { ok: true, evidenceAvailable: true, projects: [linked], observedAt: '2026-09-05T00:00:00Z' };
const project = { name: 'Task Tracker', workspacePath: 'c:\\Work\\Task Tracker\\', state: 'needs_mark', response: '<img src=x onerror=alert(1)> Finished and deployed!', raw: {} };

test('exact workspace/registry binding excludes fuzzy names, ambiguity, wrong IDs and unavailable data', () => {
  assert.equal(matchWorkProject(project, overview).id, linked.id);
  assert.equal(matchWorkProject({ ...project, workspacePath: '', name: linked.name }, overview), null);
  assert.equal(matchWorkProject(project, { ...overview, projects: [linked, { ...linked, id: 'duplicate' }] }), null);
  assert.equal(matchWorkProject({ ...project, raw: { projectRegistryId: 'wrong' } }, overview), null);
  assert.equal(matchWorkProject(project, { ...overview, ok: false }), null);
});

test('handoff, verification, deployment and acceptance cannot promote one another', () => {
  assert.equal(reportState('failed', 'needs_mark'), 'blocked');
  const stages = evidenceStages(project, linked);
  assert.ok(stages.every((row) => !row.ok));
  assert.equal(stages[1].value, 'Not linked'); assert.equal(stages[2].value, 'Not verified');
  assert.equal(stages[3].value, 'Not established here');
  const html = workOverviewHtml(project, overview);
  assert.match(html, /DISPLAY V3/); assert.match(html, /Owner acceptance is not established/);
  assert.match(html, /&lt;img/); assert.doesNotMatch(html, /<img|is complete and waiting|Accept &amp; archive/);
  assert.match(html, /Automatic advancement: off/);
  const checked = evidenceStages(project, { ...linked, operations: [{ status: 'completed', verified: true }] });
  assert.equal(checked[1].ok, true); assert.equal(checked[2].ok, false); assert.equal(checked[3].ok, false);
});

test('failed reads show unknown counts, and provider URLs cannot inject executable or credential-bearing links', () => {
  assert.match(workOverviewHtml(project, { ok: false }), /Work data is unavailable/);
  assert.equal(safeWorkUrl('javascript:alert(1)'), '');
  assert.equal(safeWorkUrl('https://user:secret@example.com'), '');
  assert.equal(safeWorkUrl('https://example.com/live'), 'https://example.com/live');
});

test('a positive recorded deployment is distinct from current health and owner acceptance', () => {
  const deployment = { type: 'production_published', status: 'live', commit: 'abcdef123456', timestamp: '2026-09-05' };
  const stages = evidenceStages(project, { ...linked, deployment });
  assert.equal(stages[2].ok, true); assert.match(stages[2].value, /^Recorded live/);
  assert.equal(stages[0].ok, false); assert.equal(stages[1].ok, false); assert.equal(stages[3].ok, false);
  const failed = evidenceStages(project, { ...linked, deployment: { ...deployment, type: 'deployment_failed', status: 'failed' } });
  assert.equal(failed[2].ok, false);
  assert.match(workOverviewHtml(project, { ...overview, projects: [{ ...linked, deployment }] }), /not a fresh health check or acceptance/);
});

test('only runnable work gets an explicit start control; saving or reading is not a start', () => {
  const item = { id: 'work-1', objective: 'Scoped follow-up', status: 'ready', acceptanceCriteria: ['Verify outcome'], readiness: { runnable: true, blockers: [] } };
  assert.match(workOverviewHtml(project, { ...overview, projects: [{ ...linked, workCount: 1, items: [item] }] }), /data-work-start="work-1"/);
  item.readiness = { runnable: false, blockers: [{ message: 'Exact approval missing' }] };
  assert.doesNotMatch(workOverviewHtml(project, { ...overview, projects: [{ ...linked, workCount: 1, items: [item] }] }), /data-work-start/);
});

test('existing sessions are useful without duplicate work; handoff prose is not an approval', () => {
  const brief = projectBrief(project, overview);
  assert.equal(brief.needs, 0); assert.match(brief.next, /existing session/);
  const html = workOverviewHtml({ ...project, request: 'Make the portrait display useful' }, overview);
  assert.match(html, /Make the portrait display useful/);
  assert.match(html, /No approval is requested by the linked work records/);
  assert.doesNotMatch(html, /No work-graph items yet|Accept &amp; archive|waiting for your acceptance/);
  assert.equal(projectBrief(project, { ...overview, projects: [{ ...linked, needsYouCount: 1 }] }).needs, 1);
});

test('deployment provider errors suppress a current-success headline without erasing historical receipts', () => {
  const row = { ...linked, deployment: { type: 'production_published', status: 'success' },
    release: { refreshErrors: [{ endpoint: 'deployment_status' }] } };
  assert.equal(projectBrief(project, { ...overview, projects: [row] }).deployed, false);
});

test('project status shortcut uses a read-only endpoint and drops a reply after a context switch', async () => {
  const html = await fs.readFile(new URL('../public/visualizer.html', import.meta.url), 'utf8');
  const start = html.indexOf('async function sendToMarcus(');
  const end = html.indexOf('function updateVoiceUi(', start);
  assert.ok(start >= 0, 'sendToMarcus start marker must exist');
  assert.ok(end > start, 'updateVoiceUi end marker must follow sendToMarcus');
  const source = html.slice(start, end);
  let respond; const paths = []; const messages = [];
  const context = vm.createContext({ chatContextId: 'selected', snapshot: { projects: [{ ...project, id: 'selected' }] }, workOverview: overview,
    matchWorkProject, businessKey: () => 'personal', addMessage: (...args) => messages.push(args),
    api: (path) => { paths.push(path); return new Promise((resolve) => { respond = resolve; }); },
    load: async () => {}, activeTab: 'preview', workOverviewReadAt: 0 });
  const command = vm.runInContext(`${source}; sendToMarcus`, context);
  const pending = command('What needs me?', { echoUser: false });
  context.chatContextId = 'different-project'; respond({ reply: 'Private selected-project response' }); await pending;
  assert.deepEqual(paths, ['/api/work/overview?projectId=registry-exact']); assert.deepEqual(messages, []);
});

test('dedicated display loads the new surface, reads saved policy and parses as a module body', async () => {
  const html = await fs.readFile(new URL('../public/visualizer.html', import.meta.url), 'utf8');
  const script = html.match(/<script type="module">([\s\S]*?)<\/script>/)[1];
  new vm.Script(script.replace(/^\s*import .*?;\s*$/m, ''));
  assert.match(script, /workOverviewHtml\(project, workOverview\)/);
  assert.match(script, /api\('\/api\/work\/overview'\)/);
  assert.match(script, /savedWorkPolicy\.autoAdvance/);
  assert.match(html, /class="btn" type="button" id="autoToggle">Execution policy/);
  assert.doesNotMatch(html, /id="autoToggle" aria-pressed/);
  assert.match(script, /if \(!window\.confirm\(`Start this work in/);
  assert.match(script, /cached\.businessKey !== businessKey\(\)/);
  assert.doesNotMatch(script, /autoContinue|is complete and waiting for your acceptance|The latest reported work is complete/);
  assert.match(script, /chatContextId !== requestContext \|\| businessKey\(\) !== requestBusiness/);
});
