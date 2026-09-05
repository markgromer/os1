import { DomainStore, domainError, newDomainId } from '../operations/domain_store.js';
import { redactSecrets, requiredVerificationPassed } from '../operations/operation_types.js';

const bounded = (value, size = 4000) => redactSecrets(String(value || '').trim().slice(0, size), size);
const timestamp = () => new Date().toISOString();
const itemFor = (doc, id) => {
  const item = doc.items.find((row) => row.id === id);
  if (!item) throw domainError('WORK_NOT_FOUND', 'Work item not found in this business.');
  return item;
};
export function emitWorkEvent(doc, type, item, data = {}) {
  if (doc.outbox.filter((event) => !event.deliveredAt).length >= 5000) throw domainError('OUTBOX_CAPACITY', 'Undelivered event capacity reached.');
  doc.outbox.push({ id: newDomainId('event'), version: 1, type, businessKey: doc.businessKey,
    projectId: item.projectId, subjectId: item.id, correlationId: item.id, causationId: data.causationId || '',
    occurredAt: timestamp(), data, deliveredAt: '' });
}
export function graphCycle(doc) {
  const edges = new Map(doc.items.map((item) => [item.id, []]));
  for (const edge of doc.dependencies) if (edge.prerequisiteId) edges.get(edge.itemId)?.push(edge.prerequisiteId);
  for (const item of doc.items) if (item.parentId) edges.get(item.parentId)?.push(item.id);
  const visited = new Set(); const active = new Set();
  const walk = (id, trail) => {
    if (active.has(id)) return [...trail, id];
    if (visited.has(id)) return null;
    active.add(id);
    for (const next of edges.get(id) || []) { const found = walk(next, [...trail, id]); if (found) return found; }
    active.delete(id); visited.add(id); return null;
  };
  for (const item of doc.items) { const cycle = walk(item.id, []); if (cycle) return cycle; }
  return null;
}
export function workReadiness(doc, item, operations = new Map()) {
  const blockers = [];
  if (item.status === 'cancelled') blockers.push({ type: 'cancelled', message: 'Work was cancelled.' });
  if (item.invalidatedBy?.length) blockers.push({ type: 'decision', message: 'A decision changed; review and rebind current context.', ids: item.invalidatedBy });
  if (item.launchState === 'creating') blockers.push({ type: 'recovery', message: 'Launch outcome is uncertain; reconcile the existing intent before retrying.' });
  const ownOperation = operations.get(item.operationId);
  if (item.operationId && (!ownOperation || ownOperation.projectRegistryId !== item.projectId)) blockers.push({ type: 'recovery', message: 'The bound operation is missing or belongs to a different project; reconcile evidence before continuing.' });
  for (const blocker of ownOperation?.blockers || []) if (blocker.status === 'active') blockers.push({ type: blocker.type === 'approval' ? 'approval' : 'operation', message: blocker.message || blocker.reason || 'Execution is blocked.', operationId: ownOperation.id });
  if (ownOperation?.status === 'waiting_for_approval') blockers.push({ type: 'approval', message: 'The execution needs an exact owner approval.', operationId: ownOperation.id });
  if (['failed', 'recovery_required', 'cancelled', 'paused'].includes(ownOperation?.status)) blockers.push({ type: 'recovery', message: `Execution is ${ownOperation.status}; inspect its receipt before continuing.`, operationId: ownOperation.id });
  for (const blocker of item.blockers || []) if (!blocker.resolvedAt) blockers.push(blocker);
  for (const edge of doc.dependencies.filter((edge) => edge.itemId === item.id)) {
    let satisfied = false;
    if (edge.type === 'work') {
      const prerequisite = doc.items.find((row) => row.id === edge.prerequisiteId);
      satisfied = prerequisite?.status === 'completed' && !prerequisite.invalidatedBy?.length
        && workReadiness(doc, prerequisite, operations).status === 'completed';
    } else {
      const operation = operations.get(edge.operationId);
      if (operation?.projectRegistryId === item.projectId) {
        if (edge.type === 'verification') satisfied = operation.verification?.some((row) => row.id === edge.requirementId && row.status === 'passed') === true;
        if (edge.type === 'approval') satisfied = operation.approvals?.some((row) => row.id === edge.requirementId && row.status === 'approved' && (!row.expiresAt || Date.parse(row.expiresAt) > Date.now())) === true;
      }
    }
    if (!satisfied) blockers.push({ type: edge.type, dependencyId: edge.id, message: edge.reason || `Waiting for ${edge.type} ${edge.prerequisiteId || edge.requirementId}.` });
  }
  return { runnable: !blockers.length && item.kind === 'task' && item.status === 'ready', blockers,
    needsMark: blockers.some((row) => ['approval', 'decision', 'owner'].includes(row.type)),
    status: blockers.length ? 'blocked' : item.status };
}

export class WorkGraph {
  constructor({ dataDir, engine, bus, decisions }) {
    this.engine = engine; this.bus = bus; this.decisions = decisions;
    this.store = new DomainStore({ dataDir, name: 'work-graph', empty: () => ({ items: [], dependencies: [], outbox: [] }),
      validate: (doc) => Array.isArray(doc.items) && Array.isArray(doc.dependencies) && Array.isArray(doc.outbox)
        && new Set(doc.items.map((item) => item.id)).size === doc.items.length && !graphCycle(doc) });
  }
  async operations(key) { return new Map((await this.engine.store.listAll(key)).map((operation) => [operation.id, operation])); }
  async create(key, input, actor = 'mark') {
    if (!await this.engine.registry.get(key, input.projectId)) throw domainError('PROJECT_NOT_FOUND', 'Project not found in this business.');
    const objective = bounded(input.objective);
    if (!objective || !Array.isArray(input.acceptanceCriteria) || !input.acceptanceCriteria.some((value) => bounded(value))) throw domainError('WORK_INVALID', 'Objective and acceptance criteria are required.');
    return this.store.mutate(key, (doc) => {
      if (doc.items.length >= 2000) throw domainError('WORK_CAPACITY', 'Work graph capacity reached.');
      if (input.parentId) {
        const parent = itemFor(doc, input.parentId);
        if (parent.projectId !== input.projectId || parent.kind !== 'objective' || parent.status === 'completed') throw domainError('WORK_SCOPE', 'Parent must be an open objective in the same project.');
      }
      const item = { id: newDomainId('work'), projectId: input.projectId, parentId: input.parentId || '', kind: ['objective', 'human'].includes(input.kind) ? input.kind : 'task',
        objective, acceptanceCriteria: input.acceptanceCriteria.slice(0, 30).map((entry) => bounded(entry, 2000)).filter(Boolean),
        status: 'ready', revision: 1, owner: actor, operationId: '', launchToken: '', launchState: '',
        legacyTaskId: bounded(input.legacyTaskId, 160), decisionRefs: [], invalidatedBy: [], blockers: [], evidence: [],
        createdAt: timestamp(), updatedAt: timestamp() };
      doc.items.push(item); emitWorkEvent(doc, 'work.created', item); return item;
    });
  }
  async addDependency(key, input) {
    if (!['work', 'approval', 'verification'].includes(input.type)) throw domainError('DEPENDENCY_INVALID', 'Unsupported dependency type.');
    const operations = await this.operations(key);
    return this.store.mutate(key, (doc) => {
      const item = itemFor(doc, input.itemId);
      if (item.status !== 'ready' || item.launchState) throw domainError('WORK_IMMUTABLE', 'Dependencies are immutable after launch.');
      if (input.type === 'work') {
        const prerequisite = itemFor(doc, input.prerequisiteId);
        if (prerequisite.projectId !== item.projectId) throw domainError('WORK_SCOPE', 'Dependencies must remain project-scoped.');
      } else if (operations.get(input.operationId)?.projectRegistryId !== item.projectId || !input.requirementId) throw domainError('WORK_SCOPE', 'Approval/verification must reference this project operation.');
      const edge = { id: newDomainId('dependency'), itemId: item.id, type: input.type, prerequisiteId: input.type === 'work' ? input.prerequisiteId : '',
        operationId: input.type !== 'work' ? input.operationId : '', requirementId: input.type !== 'work' ? input.requirementId : '', reason: bounded(input.reason, 500) };
      const existing = doc.dependencies.find((row) => row.itemId === edge.itemId && row.type === edge.type && row.prerequisiteId === edge.prerequisiteId && row.operationId === edge.operationId && row.requirementId === edge.requirementId);
      if (existing) return existing;
      doc.dependencies.push(edge);
      const cycle = graphCycle(doc);
      if (cycle) throw domainError('DEPENDENCY_CYCLE', `Dependency cycle: ${cycle.join(' -> ')}`);
      item.revision++; emitWorkEvent(doc, 'work.dependency.added', item, { dependencyId: edge.id }); return edge;
    });
  }
  async snapshot(key, projectId = '') {
    const [doc, operations] = await Promise.all([this.store.read(key), this.operations(key)]);
    // Read-time freshness must not depend on the background loop having run.
    if (this.decisions) {
      const current = new Map();
      for (const item of doc.items.filter((row) => row.contextPacketId)) {
        if (!current.has(item.projectId)) current.set(item.projectId, await this.decisions.currentDecisions(key, item.projectId));
        item.invalidatedBy = [...new Set([...item.invalidatedBy, ...this.decisions.staleIds(item, current.get(item.projectId))])];
      }
    }
    return { revision: doc.revision, items: doc.items.filter((item) => !projectId || item.projectId === projectId).map((item) => ({ ...item, readiness: workReadiness(doc, item, operations) })),
      dependencies: doc.dependencies.filter((edge) => !projectId || doc.items.find((item) => item.id === edge.itemId)?.projectId === projectId) };
  }
  async impact(key, id) {
    const doc = await this.store.read(key); itemFor(doc, id);
    const affected = new Set([id]);
    for (let changed = true; changed;) {
      changed = false;
      for (const edge of doc.dependencies) if (affected.has(edge.prerequisiteId) && !affected.has(edge.itemId)) { affected.add(edge.itemId); changed = true; }
      for (const item of doc.items) if (affected.has(item.id) && item.parentId && !affected.has(item.parentId)) { affected.add(item.parentId); changed = true; }
    }
    return [...affected];
  }
  async assertOperationReady(key, operation) {
    const workId = operation?.metadata?.extra?.workItemId;
    if (!workId) return;
    const [doc, operations] = await Promise.all([this.store.read(key), this.operations(key)]);
    const item = itemFor(doc, workId);
    if (this.decisions) await this.decisions.assertCurrent(key, item);
    if (item.operationId !== operation.id || item.projectId !== operation.projectRegistryId || item.status === 'cancelled'
      || workReadiness(doc, item, operations).blockers.some((blocker) => !['operation', 'approval', 'recovery'].includes(blocker.type) || !blocker.operationId)) throw domainError('WORK_NOT_RUNNABLE', 'Bound work is blocked, stale, or belongs to a different execution attempt.');
  }
  async launch(key, id, { start = false } = {}) {
    if (this.decisions) {
      let item = itemFor(await this.store.read(key), id);
      if (!item.contextPacketId && !item.operationId && !item.launchState) {
        await this.decisions.prepare(key, id);
        item = itemFor(await this.store.read(key), id);
      }
      await this.decisions.assertCurrent(key, item);
    }
    const operations = await this.operations(key);
    const reserved = await this.store.mutate(key, (doc) => {
      const item = itemFor(doc, id);
      if (item.operationId) return item;
      if (item.launchState === 'creating') throw domainError('WORK_RECONCILE_REQUIRED', 'Launch intent already exists; reconcile before retry.');
      const readiness = workReadiness(doc, item, operations);
      if (!readiness.runnable) throw domainError('WORK_NOT_RUNNABLE', readiness.blockers.map((row) => row.message).join(' ') || 'Work is not runnable.');
      item.launchToken = newDomainId('attempt'); item.launchState = 'creating'; item.revision++;
      emitWorkEvent(doc, 'work.launch.reserved', item, { launchToken: item.launchToken }); return item;
    });
    if (!reserved.operationId) {
      const result = await this.engine.createFromRequest(key, {
        request: `Implement ${reserved.objective}. Verify the acceptance criteria.`, objective: reserved.objective,
        projectRegistryId: reserved.projectId, acceptanceCriteria: reserved.acceptanceCriteria, allowDuplicate: true,
        source: 'work_graph', requestedBy: reserved.owner,
        metadata: { workItemId: id, workAttemptId: reserved.launchToken },
        currentArchitecture: reserved.contextPacket ? JSON.stringify(reserved.contextPacket) : '',
      });
      await this.store.mutate(key, (doc) => {
        const item = itemFor(doc, id);
        if (item.launchToken !== reserved.launchToken) throw domainError('WORK_LAUNCH_CONFLICT', 'Launch intent changed.');
        item.operationId = result.operation.id; item.launchState = 'bound'; item.status = 'running'; item.revision++;
        emitWorkEvent(doc, 'work.operation.bound', item, { operationId: item.operationId });
      });
    }
    const item = itemFor(await this.store.read(key), id);
    if (start) {
      const operation = await this.engine.getOperation(key, item.operationId);
      await this.assertOperationReady(key, operation);
      if (operation.status === 'draft') await this.engine.planOperation(key, operation.id, {});
      if (['draft', 'planned'].includes(operation.status)) await this.engine.startOperation(key, operation.id, {});
    }
    return item;
  }
  async reconcile(key) {
    if (this.decisions) await this.decisions.invalidate(key);
    const operations = await this.operations(key);
    return this.store.mutate(key, (doc) => {
      const changes = [];
      for (const item of doc.items) {
        if (item.launchState === 'creating' && !item.operationId) {
          const matches = [...operations.values()].filter((operation) => operation.metadata?.extra?.workAttemptId === item.launchToken && operation.metadata?.extra?.workItemId === item.id && operation.projectRegistryId === item.projectId);
          if (matches.length === 1) { item.operationId = matches[0].id; item.launchState = 'bound'; item.status = 'running'; }
          // No match remains recovery-required. Never guess that dispatch failed.
        }
        const operation = operations.get(item.operationId);
        if (item.status === 'running' && operation?.projectRegistryId === item.projectId && operation?.status === 'completed' && requiredVerificationPassed(operation) && !item.invalidatedBy.length) {
          item.status = 'completed'; item.completedAt = timestamp(); item.evidence = [{ type: 'operation', id: operation.id, revision: operation.revision }];
          item.revision++; changes.push(item.id); emitWorkEvent(doc, 'work.completed', item, { operationId: operation.id });
        }
      }
      for (let changed = true; changed;) {
        changed = false;
        for (const item of doc.items.filter((row) => row.kind === 'objective' && row.status !== 'completed')) {
          const children = doc.items.filter((row) => row.parentId === item.id);
          if (children.length && children.every((row) => row.status === 'completed' && !row.invalidatedBy.length) && !workReadiness(doc, item, operations).blockers.length) {
            item.status = 'completed'; item.completedAt = timestamp(); item.revision++; changes.push(item.id); emitWorkEvent(doc, 'work.completed', item); changed = true;
          }
        }
      }
      return changes;
    });
  }
  async cancel(key, id, reason) {
    if (!bounded(reason)) throw domainError('CANCEL_REASON_REQUIRED', 'A cancellation reason is required.');
    const item = await this.store.mutate(key, (doc) => {
      const item = itemFor(doc, id);
      if (item.status === 'completed') throw domainError('WORK_IMMUTABLE', 'Completed work retains its evidence; create an explicit follow-up.');
      if (item.status !== 'cancelled') { item.status = 'cancelled'; item.revision++; emitWorkEvent(doc, 'work.cancelled', item, { reason: bounded(reason), actor: 'mark' }); }
      return item;
    });
    if (item.operationId) {
      const operation = await this.engine.getOperation(key, item.operationId);
      if (operation && !['completed', 'cancelled', 'failed'].includes(operation.status)) await this.engine.cancelOperation(key, operation.id, { actor: 'mark', reason: bounded(reason) });
    }
    return item;
  }
}
