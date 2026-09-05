import { DomainStore, domainError } from '../operations/domain_store.js';

export const ENGINEERING_CHARTER = Object.freeze({
  version: 1, id: 'agent:engineering-director', name: 'Engineering Director', reportsTo: 'mark',
  purpose: 'Own engineering delivery from scoped work through independently verified outcomes.',
  responsibilities: ['assemble current decision context', 'delegate implementation to the existing Codex operation provider', 'inspect failures and evidence', 'report blockers and delivery outcomes'],
  tools: ['work.snapshot', 'work.context', 'work.launch', 'operation.inspect', 'work.reconcile'],
  authority: { projectGrantRequired: true, operationPolicy: 'inherited', approveOwnWork: false, changePermissions: false, externalCommunication: 'draft_then_exact_owner_approval' },
  evaluation: { version: 1, success: 'required operation verification passed and work completed', failure: 'failed/cancelled operation or stale context', promotion: 'explicit owner decision' },
});

export class EngineeringDirector {
  constructor({ dataDir, graph, context }) {
    this.graph = graph; this.context = context;
    this.store = new DomainStore({ dataDir, name: 'engineering-director', empty: () => ({ charter: ENGINEERING_CHARTER, lifecycle: 'inactive', projectIds: [], assignments: [], memory: [], performance: { completed: 0, failed: 0 } }),
      validate: (doc) => doc.charter?.id === ENGINEERING_CHARTER.id && Array.isArray(doc.assignments) && Array.isArray(doc.projectIds) });
  }
  async configure(key, { projectIds, lifecycle = 'probation' }) {
    if (!['probation', 'active', 'paused', 'retired'].includes(lifecycle) || !Array.isArray(projectIds) || projectIds.length > 50) throw domainError('AGENT_INVALID', 'An explicit project grant and valid lifecycle are required.');
    for (const id of projectIds) if (!await this.graph.engine.registry.get(key, id)) throw domainError('PROJECT_NOT_FOUND', 'Unknown project grant.');
    return this.store.mutate(key, (doc) => { doc.projectIds = [...new Set(projectIds)]; doc.lifecycle = lifecycle; return doc; });
  }
  async supervise(key, workId, { start = true } = {}) {
    const item = (await this.graph.snapshot(key)).items.find((row) => row.id === workId);
    if (!item || item.kind !== 'task') throw domainError('WORK_NOT_FOUND', 'Engineering supervises an existing engineering task.');
    await this.store.mutate(key, (doc) => {
      if (!['probation', 'active'].includes(doc.lifecycle) || !doc.projectIds.includes(item.projectId)) throw domainError('AGENT_FORBIDDEN', 'Engineering is not active with a grant for this project.');
      if (!doc.assignments.some((row) => row.workId === workId)) doc.assignments.push({ workId, projectId: item.projectId, status: 'delegating', charterVersion: doc.charter.version, assignedAt: new Date().toISOString(), operationId: '', outcomeRecorded: false });
      return null;
    });
    try {
      const bound = await this.graph.launch(key, workId, { start });
      return await this.store.mutate(key, (doc) => {
        const row = doc.assignments.find((entry) => entry.workId === workId);
        row.operationId = bound.operationId; row.contextPacketId = bound.contextPacketId; row.status = 'supervising'; return row;
      });
    } catch (error) {
      await this.store.mutate(key, (doc) => { const row = doc.assignments.find((entry) => entry.workId === workId); row.status = 'blocked'; row.blockerCode = error.code || 'DELEGATION_FAILED'; return null; });
      throw error;
    }
  }
  async reconcile(key) {
    await this.graph.reconcile(key);
    const state = await this.graph.snapshot(key);
    return this.store.mutate(key, (doc) => {
      for (const assignment of doc.assignments) {
        const item = state.items.find((row) => row.id === assignment.workId);
        if (!item) continue;
        assignment.operationId = item.operationId; assignment.evidence = item.evidence; assignment.blockers = item.readiness.blockers;
        assignment.status = item.readiness.status;
        if (!assignment.outcomeRecorded && (item.readiness.status === 'completed' || item.readiness.blockers.some((row) => row.message.includes('failed')))) {
          const passed = item.readiness.status === 'completed';
          assignment.outcomeRecorded = true; assignment.evaluatedAt = new Date().toISOString();
          doc.performance[passed ? 'completed' : 'failed']++;
          doc.memory.push({ workId: item.id, projectId: item.projectId, charterVersion: assignment.charterVersion, outcome: passed ? 'verified_delivery' : 'failure_requires_review', evidence: item.evidence, blockerCodes: item.readiness.blockers.map((row) => row.type), at: assignment.evaluatedAt });
        }
      }
      return doc;
    });
  }
  async assertOperationGrant(key, operation) {
    if (!operation?.metadata?.extra?.workItemId) return;
    const doc = await this.store.read(key);
    const assignment = doc.assignments.find((row) => row.workId === operation.metadata.extra.workItemId);
    if (assignment && (!['probation', 'active'].includes(doc.lifecycle) || !doc.projectIds.includes(operation.projectRegistryId))) throw domainError('AGENT_FORBIDDEN', 'The supervising agent grant was paused, retired, or revoked.');
  }
}
