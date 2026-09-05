import crypto from 'node:crypto';
import { DomainStore, domainError, newDomainId } from '../operations/domain_store.js';

const digestOf = (value) => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');

export class ProactiveOperator {
  constructor({ dataDir, graph, execution, director, attention, now = () => Date.now() }) {
    this.graph = graph; this.execution = execution; this.director = director; this.attention = attention; this.now = now;
    this.store = new DomainStore({ dataDir, name: 'operator-digests', empty: () => ({ presence: 'available', quietUntil: '', maxInterruptionsPerHour: 2, interruptions: [], digests: [], deliveries: [] }),
      validate: (doc) => Array.isArray(doc.digests) && Array.isArray(doc.deliveries) });
  }
  async setPresence(key, { presence, quietUntil = '', maxInterruptionsPerHour = 2 }) {
    if (!['available', 'busy', 'away'].includes(presence) || (quietUntil && !Number.isFinite(Date.parse(quietUntil))) || !Number.isInteger(maxInterruptionsPerHour) || maxInterruptionsPerHour < 0 || maxInterruptionsPerHour > 10) throw domainError('PRESENCE_INVALID', 'Use a valid presence, quiet-until time, and 0–10 interruptions per hour.');
    return this.store.mutate(key, (doc) => { Object.assign(doc, { presence, quietUntil, maxInterruptionsPerHour }); return { presence, quietUntil, maxInterruptionsPerHour }; });
  }
  async summary(key, { since = '' } = {}) {
    if (since && !Number.isFinite(Date.parse(since))) throw domainError('SUMMARY_INVALID', 'since must be an ISO timestamp.');
    const [state, execution, attention, director] = await Promise.all([this.graph.snapshot(key), this.execution.store.read(key), this.attention.list(key, { limit: 1000 }), this.director.store.read(key)]);
    const needsMark = []; const anomalies = []; const opportunities = []; const canContinue = [];
    for (const item of state.items) {
      const evidence = [{ type: 'work', id: item.id, revision: item.revision }, ...(item.operationId ? [{ type: 'operation', id: item.operationId }] : [])];
      if (item.readiness.needsMark || (item.kind === 'human' && item.submission && item.status !== 'completed')) needsMark.push({ id: item.id, reason: item.kind === 'human' ? 'Review the exact collaborator submission.' : item.readiness.blockers.map((row) => row.message).join(' '), evidence });
      if (item.readiness.blockers.some((row) => ['recovery', 'operation'].includes(row.type))) anomalies.push({ id: item.id, reason: item.readiness.blockers.map((row) => row.message).join(' '), evidence });
      if (item.readiness.runnable) {
        const automatic = execution.policies.some((row) => row.projectId === item.projectId && row.autoAdvance)
          && ['active', 'probation'].includes(director.lifecycle) && director.projectIds.includes(item.projectId);
        if (automatic) canContinue.push({ id: item.id, reason: 'Ready under an explicit project advancement policy.', evidence });
        else opportunities.push({ id: item.id, reason: 'Dependency-ready; owner may start this work or grant bounded automatic advancement.', evidence });
      } else if (item.status === 'running' && !item.readiness.blockers.length) canContinue.push({ id: item.id, reason: 'Existing execution is in progress; completion still requires verification.', evidence });
    }
    for (const run of execution.runs.filter((row) => row.status === 'dead_letter')) anomalies.push({ id: run.id, reason: 'Retry budget exhausted; inspect checkpoints before creating follow-up work.', evidence: [{ type: 'execution_run', id: run.id, attempts: run.attempts, causationId: run.causationId }] });
    for (const item of attention.filter((row) => ['open', 'acknowledged'].includes(row.status) && row.owner === 'mark' && !row.fingerprint.startsWith('work-operator:'))) needsMark.push({ id: item.id, reason: item.reason, evidence: item.evidence });
    const operations = [...(await this.graph.operations(key)).values()];
    for (const operation of operations.filter((row) => !row.metadata?.extra?.workItemId && row.status === 'waiting_for_approval')) needsMark.push({ id: operation.id, reason: 'Existing operation requires owner approval.', evidence: [{ type: 'operation', id: operation.id, revision: operation.revision }] });
    const events = (await this.graph.store.read(key)).outbox.filter((event) => !since || Date.parse(event.occurredAt) > Date.parse(since));
    return { observedAt: new Date(this.now()).toISOString(), scope: 'current local durable state; no claim about disconnected external systems', trackedWorkCount: state.items.length,
      needsMark, canContinue, anomalies, opportunities,
      away: { since: since || null, changes: events.slice(-100).map((event) => ({ id: event.id, type: event.type, workId: event.subjectId, at: event.occurredAt })), truncated: events.length > 100 },
      uncertainty: state.items.length ? [] : ['No work-graph items are tracked here; absence of alerts does not prove there is no outstanding work.'] };
  }
  async pass(key) {
    // Routine dependency clearing uses actual operation evidence, not a model claim.
    if (this.graph.decisions) await this.graph.decisions.invalidate(key);
    await this.graph.reconcile(key);
    const summary = await this.summary(key);
    const content = { needsMark: summary.needsMark, anomalies: summary.anomalies, opportunities: summary.opportunities, canContinue: summary.canContinue, uncertainty: summary.uncertainty };
    const fingerprint = digestOf(content);
    await this.store.mutate(key, (doc) => {
      if (doc.digests.at(-1)?.fingerprint !== fingerprint) doc.digests.push({ id: newDomainId('digest'), fingerprint, createdAt: summary.observedAt, content });
      doc.interruptions = doc.interruptions.filter((row) => row.at > this.now() - 3600000);
      const quiet = doc.presence !== 'available' || (doc.quietUntil && Date.parse(doc.quietUntil) > this.now());
      if (!quiet) for (const item of [...summary.needsMark, ...summary.anomalies]) {
        if (doc.interruptions.length >= doc.maxInterruptionsPerHour) break;
        const itemFingerprint = `work-operator:${item.id}:${digestOf(item)}`;
        if (doc.deliveries.some((row) => row.fingerprint === itemFingerprint)) continue;
        doc.interruptions.push({ at: this.now(), fingerprint: itemFingerprint });
        doc.deliveries.push({ fingerprint: itemFingerprint, item, deliveredAt: '' });
      }
      return null;
    });
    const activeIds = new Set([...summary.needsMark, ...summary.anomalies].map((row) => row.id));
    const doc = await this.store.read(key);
    for (const delivery of doc.deliveries.filter((row) => !row.deliveredAt)) {
      if (activeIds.has(delivery.item.id)) await this.attention.raise(key, { fingerprint: delivery.fingerprint, signalType: 'work.operator.attention', subject: { type: 'work_or_run', id: delivery.item.id }, owner: 'mark', title: 'Work needs your attention', reason: delivery.item.reason, evidence: delivery.item.evidence });
      await this.store.mutate(key, (state) => { state.deliveries.find((row) => row.fingerprint === delivery.fingerprint).deliveredAt = new Date(this.now()).toISOString(); return null; });
    }
    for (const item of await this.attention.list(key, { limit: 1000 })) if (item.fingerprint.startsWith('work-operator:') && ['open', 'acknowledged', 'deferred'].includes(item.status) && !activeIds.has(item.subject.id)) await this.attention.transition(key, item.id, 'resolved', { resolution: 'Current durable work no longer reports this blocker.' });
    return { digestId: doc.digests.at(-1)?.id, ...summary };
  }
}
