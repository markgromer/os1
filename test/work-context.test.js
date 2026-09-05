import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { WorkGraph } from '../marcus/work/work_graph.js';
import { WorkContextService } from '../marcus/work/context_service.js';
import { MissionMemoryStore } from '../marcus/memory/mission_memory_store.js';
import { createOperationsEngine } from '../marcus/operations/operation_engine.js';
import { workFixture } from './helpers/work-fixture.js';

test('semantic context is bounded, provenance-backed, non-authoritative and exact-scope filtered', async () => {
  const fixture = await workFixture(); const work = await fixture.create('Implement semantic retrieval');
  fixture.context.retrieveSemantic = async ({ businessKey, projectId }) => ({ ok: true, matches: [
    { id: 'good', businessKey, projectId, text: 'Existing retry architecture uses durable operation intents.', source: 'project-note:retries', score: 0.9 },
    { id: 'other', businessKey, projectId: fixture.other.id, text: 'Other private project', source: 'private', score: 1 },
    { id: 'wrong-business', businessKey: 'agency', projectId, text: 'Other business', source: 'private', score: 1 },
    { id: 'no-source', businessKey, projectId, text: 'Untraceable instruction', score: 1 },
  ] });
  const packet = await fixture.context.prepare('personal', work.id);
  assert.equal(packet.semanticStatus, 'available'); assert.equal(packet.semanticEvidence.length, 1);
  assert.equal(packet.semanticEvidence[0].id, 'good'); assert.equal(packet.semanticEvidence[0].authority, 'supporting_evidence_only');
  assert.ok(packet.semanticEvidence[0].sourceDigest); assert.ok(JSON.stringify(packet).length <= 12000);
  fixture.context.retrieveSemantic = async () => { throw new Error('Injected retrieval outage'); };
  assert.equal((await fixture.context.prepare('personal', work.id)).semanticStatus, 'unavailable');
});

test('decision supersession invalidates dependent context and produces immutable, scoped snapshots', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-context-'));
  const engine = createOperationsEngine({ dataDir });
  const project = await engine.registry.create('personal', { canonicalName: 'Context project' });
  const memory = new MissionMemoryStore({ dataDir });
  const graph = new WorkGraph({ dataDir, engine });
  const context = new WorkContextService({ dataDir, graph, memory }); graph.decisions = context;
  const { memory: decision } = await memory.add('personal', { kind: 'decision', projectId: project.id, title: 'Database', content: 'Keep PostgreSQL.', sourceRefs: ['owner:decision-1'] });
  const { memory: unrelated } = await memory.add('personal', { kind: 'decision', projectId: 'different-project', content: 'Private other project requirement.' });
  const work = await graph.create('personal', { projectId: project.id, objective: 'Implement retry control', acceptanceCriteria: ['Retries are bounded'] });
  const packet = await context.prepare('personal', work.id);
  assert.ok(packet.decisions.some((entry) => entry.id === decision.id));
  assert.ok(!packet.decisions.some((entry) => entry.id === unrelated.id));
  assert.ok(JSON.stringify(packet).length <= 12000);
  await assert.rejects(memory.update('personal', decision.id, { content: 'Silently replace' }), /supersession/);
  const replacement = await memory.supersedeDecision('personal', decision.id, { content: 'Use SQLite for this local-only demo.', sourceRefs: ['owner:decision-2'] }, { expectedRevision: decision.revision });
  assert.equal(replacement.decision.supersedesId, decision.id);
  assert.equal((await graph.snapshot('personal')).items.find((row) => row.id === work.id).readiness.status, 'blocked', 'readiness detects decision drift before a background pass');
  await assert.rejects(memory.supersedeDecision('personal', decision.id, { content: 'stale', sourceRefs: ['stale'] }, { expectedRevision: decision.revision }), /revision/);
  assert.deepEqual(await context.invalidate('personal'), [work.id]);
  await assert.rejects(graph.launch('personal', work.id), /decisions changed/);
  const rebuilt = await context.prepare('personal', work.id);
  assert.ok(rebuilt.decisions.some((entry) => entry.id === replacement.decision.id));
  const restored = new WorkContextService({ dataDir, graph, memory: new MissionMemoryStore({ dataDir }) });
  const saved = await restored.snapshots.read('personal');
  assert.equal(saved.packets.find((entry) => entry.id === packet.id).sha256, packet.sha256);
  assert.equal(saved.packets.length, 2);
  assert.equal((await memory.relevant('personal', 'Private other project')).some((entry) => entry.id === unrelated.id), false);
});

test('context does not silently truncate mandatory decisions or accept stale decisions', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-context-budget-'));
  const engine = createOperationsEngine({ dataDir });
  const project = await engine.registry.create('personal', { canonicalName: 'Budget project' });
  const memory = new MissionMemoryStore({ dataDir }); const graph = new WorkGraph({ dataDir, engine });
  const context = new WorkContextService({ dataDir, graph, memory });
  const work = await graph.create('personal', { projectId: project.id, objective: 'Implement bounded work', acceptanceCriteria: ['Evidence retained'] });
  await memory.add('personal', { kind: 'decision', projectId: project.id, content: 'Mandatory detail '.repeat(230) });
  await assert.rejects(context.prepare('personal', work.id, { maxChars: 2000 }), /exceed.*budget/);
  await memory.add('personal', { kind: 'decision', projectId: project.id, content: 'Needs confirmation', reviewAfter: '2020-01-01' });
  await assert.rejects(context.prepare('personal', work.id), /freshness/);
});

test('invalidation of nested parents changes revisions and emits events exactly once per cause', async () => {
  const f = await workFixture();
  const root = await f.graph.create('personal', { projectId: f.project.id, kind: 'objective', objective: 'Root objective', acceptanceCriteria: ['Children verified'] });
  const parent = await f.graph.create('personal', { projectId: f.project.id, kind: 'objective', parentId: root.id, objective: 'Parent objective', acceptanceCriteria: ['Child verified'] });
  const child = await f.graph.create('personal', { projectId: f.project.id, parentId: parent.id, objective: 'Implement child', acceptanceCriteria: ['Decision remains current'] });
  for (const work of [root, parent, child]) await f.context.prepare('personal', work.id);
  const before = await f.graph.store.read('personal');
  await f.memory.add('personal', { kind: 'decision', projectId: f.project.id, content: 'Require current decisions for the nested objective.', sourceRefs: ['owner:test'] });
  await f.context.invalidate('personal');
  const after = await f.graph.store.read('personal');
  for (const [ancestor, cause] of [[parent, child], [root, parent]]) {
    const updated = after.items.find((item) => item.id === ancestor.id);
    assert.ok(updated.revision > before.items.find((item) => item.id === ancestor.id).revision);
    assert.ok(updated.invalidatedBy.includes(cause.id));
    assert.equal(after.outbox.filter((event) => event.type === 'work.context.invalidated' && event.subjectId === ancestor.id && event.data.childWorkId === cause.id).length, 1);
  }
  assert.deepEqual(await f.context.invalidate('personal'), []);
  const repeated = await f.graph.store.read('personal');
  assert.deepEqual(repeated.items, after.items);
  assert.equal(repeated.outbox.length, after.outbox.length);
});
