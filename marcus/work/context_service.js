import crypto from 'node:crypto';
import { DomainStore, domainError, newDomainId } from '../operations/domain_store.js';
import { emitWorkEvent } from './work_graph.js';
import { redactSecrets } from '../operations/operation_types.js';

const digest = (value) => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');

export class WorkContextService {
  constructor({ dataDir, graph, memory, retrieveSemantic = null }) {
    this.graph = graph; this.memory = memory; this.retrieveSemantic = retrieveSemantic;
    this.snapshots = new DomainStore({ dataDir, name: 'work-context', empty: () => ({ packets: [] }), validate: (doc) => Array.isArray(doc.packets) });
  }
  async currentDecisions(key, projectId) {
    const { memories } = await this.memory.list(key, { status: 'active', kind: 'decision', projectId, limit: 500 });
    return memories;
  }
  staleIds(item, current) {
    const refs = item.decisionRefs || [];
    return [...new Set([
      ...refs.filter((ref) => !current.some((entry) => entry.id === ref.id && entry.revision === ref.revision)).map((ref) => ref.id),
      ...current.filter((entry) => !refs.some((ref) => ref.id === entry.id && ref.revision === entry.revision) || (entry.reviewAfter && Date.parse(entry.reviewAfter) <= Date.now())).map((entry) => entry.id),
    ])];
  }
  async assertCurrent(key, item) {
    if (!item.contextPacketId) return;
    const current = await this.currentDecisions(key, item.projectId);
    if (current.some((decision) => decision.reviewAfter && Date.parse(decision.reviewAfter) <= Date.now())) throw domainError('DECISION_STALE', 'A scoped decision needs freshness review before execution.');
    if (this.staleIds(item, current).length) {
      throw domainError('CONTEXT_STALE', 'Project decisions changed; prepare a new task packet before execution.');
    }
  }
  async prepare(key, workId, { maxChars = 12000 } = {}) {
    const state = await this.graph.snapshot(key);
    const item = state.items.find((row) => row.id === workId);
    if (!item) throw domainError('WORK_NOT_FOUND', 'Work item not found.');
    if (item.operationId || item.launchState) throw domainError('WORK_IMMUTABLE', 'Execution context is already bound; create an explicit follow-up work item.');
    const limit = Math.max(2000, Math.min(24000, Number(maxChars) || 12000));
    const decisions = await this.currentDecisions(key, item.projectId);
    if (decisions.some((decision) => decision.reviewAfter && Date.parse(decision.reviewAfter) <= Date.now())) throw domainError('DECISION_STALE', 'A scoped decision needs freshness review before new execution.');
    const memories = await this.memory.relevant(key, item.objective, { projectId: item.projectId, limit: 30 });
    const packet = {
      id: newDomainId('packet'), schemaVersion: 1, projectId: item.projectId, workItemId: item.id, workRevision: item.revision,
      createdAt: new Date().toISOString(), objective: item.objective, acceptanceCriteria: item.acceptanceCriteria,
      authority: { owner: 'mark', execution: 'existing_operation_policy', externalActions: 'exact_approval_required' },
      decisions: decisions.map((entry) => ({ id: entry.id, revision: entry.revision, content: entry.content, sourceRefs: entry.sourceRefs, confidence: entry.confidence })),
      memories: [], omittedMemoryIds: [], semanticEvidence: [], semanticStatus: 'not_configured',
      dependencies: state.dependencies.filter((edge) => edge.itemId === item.id),
    };
    if (JSON.stringify(packet).length > limit - 256) throw domainError('CONTEXT_BUDGET', 'Required decisions and work constraints exceed the context budget; narrow the work.');
    for (const memory of memories.filter((entry) => entry.kind !== 'decision')) {
      const entry = { id: memory.id, revision: memory.revision, kind: memory.kind, content: memory.content, digest: digest(memory) };
      if (JSON.stringify({ ...packet, memories: [...packet.memories, entry] }).length < limit - 1000) packet.memories.push(entry);
      else packet.omittedMemoryIds.push(memory.id);
    }
    if (this.retrieveSemantic) {
      try {
        const result = await this.retrieveSemantic({ businessKey: key, projectId: item.projectId, query: item.objective, limit: 5 });
        packet.semanticStatus = result?.ok ? 'available' : 'unavailable';
        for (const match of (result?.matches || []).slice(0, 5)) {
          if (match.businessKey !== key || match.projectId !== item.projectId || typeof match.text !== 'string' || !match.source) continue;
          const entry = { id: String(match.id || '').slice(0, 160), source: redactSecrets(String(match.source).slice(0, 500)), score: Math.max(0, Math.min(1, Number(match.score) || 0)),
            excerpt: redactSecrets(match.text.slice(0, 1600)), sourceDigest: digest(match.text), authority: 'supporting_evidence_only' };
          if (JSON.stringify({ ...packet, semanticEvidence: [...packet.semanticEvidence, entry] }).length < limit - 256) packet.semanticEvidence.push(entry);
        }
      } catch { packet.semanticStatus = 'unavailable'; }
    }
    packet.sha256 = digest(packet);
    if (JSON.stringify(packet).length > limit) throw domainError('CONTEXT_BUDGET', 'The complete provenance packet exceeds its budget; narrow the work.');
    await this.snapshots.mutate(key, (document) => { document.packets.push(packet); return null; });
    await this.graph.store.mutate(key, (doc) => {
      const current = doc.items.find((row) => row.id === item.id);
      if (!current || current.revision !== item.revision || current.launchState) throw domainError('REVISION_MISMATCH', 'Work changed while assembling context.');
      current.contextPacketId = packet.id; current.contextPacket = packet;
      current.decisionRefs = packet.decisions.map(({ id, revision }) => ({ id, revision }));
      current.invalidatedBy = []; current.revision++;
      emitWorkEvent(doc, 'work.context.prepared', current, { packetId: packet.id, digest: packet.sha256 });
      return null;
    });
    return packet;
  }
  async invalidate(key) {
    const doc = await this.graph.store.read(key);
    const currentByProject = new Map();
    for (const projectId of new Set(doc.items.map((item) => item.projectId))) currentByProject.set(projectId, await this.currentDecisions(key, projectId));
    return this.graph.store.mutate(key, (state) => {
      const affected = [];
      for (const item of state.items.filter((row) => row.contextPacketId)) {
        const current = currentByProject.get(item.projectId) || [];
        const stale = this.staleIds(item, current);
        if (stale.length && JSON.stringify(item.invalidatedBy) !== JSON.stringify(stale)) {
          item.invalidatedBy = stale; item.revision++; affected.push(item.id);
          emitWorkEvent(state, 'work.context.invalidated', item, { decisionIds: stale });
        }
      }
      // Downstream work is blocked by the prerequisite's invalidation even if it
      // has never assembled a packet. Parent completion is no longer current.
      for (let changed = true; changed;) {
        changed = false;
        for (const item of state.items) {
          if (item.parentId && item.invalidatedBy.length) {
            const parent = state.items.find((row) => row.id === item.parentId);
            if (parent && !parent.invalidatedBy.includes(item.id)) { parent.invalidatedBy.push(item.id); affected.push(parent.id); changed = true; }
          }
        }
      }
      return [...new Set(affected)];
    });
  }
}
