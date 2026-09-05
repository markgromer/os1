import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import express from 'express';
import { workFixture } from './helpers/work-fixture.js';
import { EngineeringDirector } from '../marcus/work/engineering_director.js';
import { registerCollaborationRoutes } from '../marcus/api/collaboration_routes.js';
import { registerWorkRoutes } from '../marcus/api/work_routes.js';

test('permanent director grants, accountable assignment, revocation, restart, and evaluation', async () => {
  const f = await workFixture(); const work = await f.create('Implement a bounded local change');
  await assert.rejects(f.director.supervise('personal', work.id, { start: false }), /not active/);
  await f.director.configure('personal', { projectIds: [f.project.id] });
  const assignment = await f.director.supervise('personal', work.id, { start: false });
  assert.ok(assignment.operationId); assert.ok(assignment.contextPacketId);
  assert.equal((await f.engine.store.listAll('personal')).length, 1);
  await f.director.supervise('personal', work.id, { start: false });
  assert.equal((await f.director.store.read('personal')).assignments.length, 1);
  await f.director.configure('personal', { projectIds: [f.project.id], lifecycle: 'paused' });
  await assert.rejects(f.director.assertOperationGrant('personal', await f.engine.getOperation('personal', assignment.operationId)), /paused/);
  const restored = new EngineeringDirector({ dataDir: f.dataDir, graph: f.graph, context: f.context });
  assert.equal((await restored.store.read('personal')).lifecycle, 'paused');
  // State-machine unit fixture, not real acceptance evidence.
  await f.engine.store.update('personal', assignment.operationId, (operation) => { operation.status = 'completed'; operation.verification = [{ id: 'unit', type: 'test', status: 'passed', required: true }]; return operation; });
  await restored.reconcile('personal'); await restored.reconcile('personal');
  const state = await restored.store.read('personal');
  assert.equal(state.performance.completed, 1); assert.equal(state.memory.length, 1); assert.ok(state.memory[0].evidence.length);
});

test('project credentials complete one human request without horizontal or owner escalation', async (t) => {
  const f = await workFixture();
  const human = await f.create('Confirm the acceptance checklist', { kind: 'human' });
  const hidden = await f.create('Secret separate project objective', { kind: 'human', projectId: f.other.id });
  const issued = await f.identities.issue('personal', { displayName: 'Acceptance collaborator', projectId: f.project.id });
  await f.identities.assign('personal', human.id, issued.identity.id);
  await assert.rejects(f.identities.issue('personal', { displayName: 'Escalator', projectId: f.project.id, role: 'owner' }), /Owner authority/);
  assert.ok(!(await fs.readFile(f.identities.store.file('personal'), 'utf8')).includes(issued.token));
  const app = express(); app.use(express.json());
  registerCollaborationRoutes(app, { identities: f.identities, getBusinessKey: (req) => req.headers['x-business-key'] || 'personal' });
  app.use((req, res, next) => req.headers.authorization === 'Bearer owner-test-only' ? next() : res.status(401).end());
  registerWorkRoutes(app, { ...f, getBusinessKey: () => 'personal' });
  const server = app.listen(0, '127.0.0.1'); await new Promise((resolve) => server.once('listening', resolve));
  t.after(() => server.close()); const base = `http://127.0.0.1:${server.address().port}`;
  const call = (url, body, token = issued.token, headers = {}) => fetch(`${base}${url}`, { method: body ? 'POST' : 'GET', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', ...headers }, ...(body ? { body: JSON.stringify(body) } : {}) });
  const view = await (await call('/api/collaboration/work?projectId=' + f.other.id)).json();
  assert.equal(view.items.length, 1); assert.equal(view.items[0].id, human.id); assert.ok(!JSON.stringify(view).includes(hidden.objective));
  assert.equal((await call('/api/collaboration/work', null, issued.token, { 'x-business-key': 'agency' })).status, 401);
  assert.equal((await call('/api/collaboration/work/' + hidden.id + '/submit', { note: 'Try crossing scope' })).status, 404);
  assert.equal((await call('/api/work/identities/issue', { role: 'owner' })).status, 401);
  assert.equal((await call('/api/collaboration/work/' + human.id + '/accept', {})).status, 403);
  const submitted = await (await call('/api/collaboration/work/' + human.id + '/submit', { note: 'Checklist reviewed; source is the supplied work acceptance criteria.', evidenceRefs: ['work:' + human.id], revision: view.items[0].revision, status: 'completed', role: 'owner' })).json();
  assert.ok(submitted.submission.id); assert.equal(submitted.submission.trustedForVerification, false);
  let item = (await f.graph.snapshot('personal')).items.find((row) => row.id === human.id); assert.equal(item.status, 'ready');
  const accepted = await call('/api/work/' + human.id + '/accept', { revision: item.revision, submissionId: submitted.submission.id, reviewNote: 'Acceptance harness checked the exact checklist evidence.' }, 'owner-test-only');
  assert.equal(accepted.status, 200); item = (await accepted.json()).item; assert.equal(item.status, 'completed');
  await f.identities.revoke('personal', issued.grant.id);
  assert.equal((await call('/api/collaboration/work')).status, 401);
});
