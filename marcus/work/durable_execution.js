import { DomainStore, domainError, newDomainId } from '../operations/domain_store.js';

// A durable inbox/run ledger for the existing SignalBus and operation engine.
// Delivery is at least once. Unique logical run keys and WorkGraph launch intents
// make processing idempotent; no claim of exactly-once external side effects.
export class DurableExecution {
  constructor({ dataDir, graph, director, bus, now = () => Date.now(), leaseMs = 60000 }) {
    this.graph = graph; this.director = director; this.bus = bus; this.now = now; this.leaseMs = leaseMs;
    this.store = new DomainStore({ dataDir, name: 'execution-runs', empty: () => ({ receipts: [], runs: [], schedules: [], policies: [] }),
      validate: (doc) => Array.isArray(doc.receipts) && Array.isArray(doc.runs) && Array.isArray(doc.schedules) && new Set(doc.runs.map((run) => run.key)).size === doc.runs.length });
  }
  async setPolicy(key, projectId, autoAdvance) {
    if (!await this.graph.engine.registry.get(key, projectId) || typeof autoAdvance !== 'boolean') throw domainError('POLICY_INVALID', 'An existing project and explicit boolean policy are required.');
    return this.store.mutate(key, (doc) => { const policy = { projectId, autoAdvance, actor: 'mark', updatedAt: new Date(this.now()).toISOString() }; doc.policies = [...doc.policies.filter((row) => row.projectId !== projectId), policy]; return policy; });
  }
  async schedule(key, { projectId, everyMs, firstDueAt }) {
    if (!await this.graph.engine.registry.get(key, projectId) || !Number.isFinite(everyMs) || everyMs < 60000 || everyMs > 30 * 86400000 || !Number.isFinite(Date.parse(firstDueAt))) throw domainError('SCHEDULE_INVALID', 'A registered project, 1 minute–30 day interval, and ISO start time are required.');
    return this.store.mutate(key, (doc) => {
      const existing = doc.schedules.find((row) => row.projectId === projectId && row.kind === 'project_review');
      if (existing) { existing.everyMs = everyMs; existing.nextDueAt = firstDueAt; existing.enabled = true; return existing; }
      const row = { id: newDomainId('schedule'), kind: 'project_review', projectId, everyMs, nextDueAt: firstDueAt, enabled: true }; doc.schedules.push(row); return row;
    });
  }
  enqueue(doc, input) {
    const existing = doc.runs.find((row) => row.key === input.key); if (existing) return existing;
    if (doc.runs.length >= 10000) throw domainError('RUN_CAPACITY', 'Run ledger needs explicit archival.');
    const run = { id: newDomainId('run'), ...input, status: 'queued', attempts: 0, maxAttempts: 3, availableAt: this.now(), lease: null, checkpoints: [], createdAt: new Date(this.now()).toISOString() };
    doc.runs.push(run); return run;
  }
  async receive(key, event) {
    if (event.businessKey !== key || event.version !== 1 || !event.id || !event.correlationId) throw domainError('EVENT_SCOPE', 'Event envelope is invalid.');
    const state = await this.graph.snapshot(key, event.projectId);
    return this.store.mutate(key, (doc) => {
      const prior = doc.receipts.find((receipt) => receipt.eventId === event.id); if (prior) return prior;
      const runIds = [];
      for (const item of state.items.filter((row) => row.readiness.runnable)) {
        const run = this.enqueue(doc, { key: `advance:${item.id}`, kind: 'advance_work', projectId: item.projectId, workId: item.id, correlationId: event.correlationId, causationId: event.id }); runIds.push(run.id);
      }
      const receipt = { eventId: event.id, version: event.version, type: event.type, correlationId: event.correlationId, causationId: event.causationId, subjectId: event.subjectId, runIds, processedAt: new Date(this.now()).toISOString() };
      doc.receipts.push(receipt); return receipt;
    });
  }
  async drainOutbox(key) {
    const doc = await this.graph.store.read(key);
    let delivered = 0;
    for (const event of doc.outbox.filter((row) => !row.deliveredAt).slice(0, 100)) {
      // Persist this consumer before publishing; retries always revisit receive.
      await this.receive(key, event);
      if (this.bus) {
        const result = await this.bus.publish({ id: event.id, type: event.type, businessKey: key, source: 'work-outbox', subject: { type: 'work', id: event.subjectId }, correlationId: event.correlationId,
          context: { eventVersion: event.version, causationId: event.causationId, projectId: event.projectId, ...event.data } });
        if (result.delivery.failed.length) { this.bus.seen.delete(event.id); continue; }
      }
      await this.graph.store.mutate(key, (state) => { const saved = state.outbox.find((row) => row.id === event.id); if (saved && !saved.deliveredAt) saved.deliveredAt = new Date(this.now()).toISOString(); return null; });
      delivered++;
    }
    return delivered;
  }
  async queueSchedules(key) {
    return this.store.mutate(key, (doc) => {
      const ids = [];
      for (const schedule of doc.schedules.filter((row) => row.enabled && Date.parse(row.nextDueAt) <= this.now())) {
        const due = Date.parse(schedule.nextDueAt);
        const run = this.enqueue(doc, { key: `schedule:${schedule.id}:${due}`, kind: 'project_review', projectId: schedule.projectId, workId: '', correlationId: schedule.id, causationId: schedule.nextDueAt }); ids.push(run.id);
        // Coalesce missed ticks; do not flood the user after a long shutdown.
        schedule.nextDueAt = new Date(due + (Math.floor((this.now() - due) / schedule.everyMs) + 1) * schedule.everyMs).toISOString();
      }
      return ids;
    });
  }
  async claim(key, owner) {
    if (!owner) throw domainError('LEASE_INVALID', 'A worker owner is required.');
    const director = await this.director.store.read(key);
    return this.store.mutate(key, (doc) => {
      for (const run of doc.runs.filter((row) => row.status === 'running' && row.lease?.expiresAt <= this.now())) {
        run.checkpoints.push({ at: this.now(), phase: 'lease_expired', leaseToken: run.lease.token });
        run.status = run.attempts >= run.maxAttempts ? 'dead_letter' : 'queued'; run.lease = null;
      }
      const run = doc.runs.find((row) => row.status === 'queued' && row.availableAt <= this.now()
        && (row.kind !== 'advance_work' || (
          doc.policies.some((policy) => policy.projectId === row.projectId && policy.autoAdvance)
          && ['probation', 'active'].includes(director.lifecycle) && director.projectIds.includes(row.projectId))));
      if (!run) return null;
      run.status = 'running'; run.attempts++; run.lease = { owner, token: newDomainId('lease'), expiresAt: this.now() + this.leaseMs };
      run.checkpoints.push({ at: this.now(), phase: 'claimed', attempt: run.attempts }); return run;
    });
  }
  async checkpoint(key, id, token, phase, evidence = {}) {
    return this.store.mutate(key, (doc) => { const run = this.owned(doc, id, token); run.checkpoints.push({ at: this.now(), phase, evidence }); run.lease.expiresAt = this.now() + this.leaseMs; return run; });
  }
  owned(doc, id, token) {
    const run = doc.runs.find((row) => row.id === id);
    if (!run || run.status !== 'running' || run.lease?.token !== token || run.lease.expiresAt <= this.now()) throw domainError('LEASE_LOST', 'The worker no longer owns this run. Reconcile before retrying.');
    return run;
  }
  async settle(key, id, token, { evidence, error } = {}) {
    return this.store.mutate(key, (doc) => {
      const run = this.owned(doc, id, token);
      run.status = error ? (run.attempts >= run.maxAttempts ? 'dead_letter' : 'queued') : 'completed';
      run.availableAt = this.now() + (error ? Math.min(60000, 1000 * 2 ** run.attempts) : 0);
      run.checkpoints.push({ at: this.now(), phase: error ? 'failed' : 'completed', evidence: evidence || {}, errorCode: error?.code || (error ? 'RUN_FAILED' : '') });
      run.lease = null; return run;
    });
  }
  async pass(key, owner = `server:${process.pid}`) {
    if (this.graph.decisions) await this.graph.decisions.invalidate(key);
    await this.graph.reconcile(key); await this.drainOutbox(key); await this.queueSchedules(key);
    const results = [];
    for (let count = 0; count < 2; count++) {
      const run = await this.claim(key, owner); if (!run) break;
      const token = run.lease.token;
      try {
        // Always reconcile the durable operation binding before any possible retry.
        await this.graph.reconcile(key);
        await this.checkpoint(key, run.id, token, 'reconciled');
        let evidence;
        if (run.kind === 'advance_work') {
          const state = await this.graph.snapshot(key, run.projectId); const item = state.items.find((row) => row.id === run.workId);
          if (item?.readiness.status === 'completed') evidence = { workId: item.id, operationId: item.operationId, alreadyCompleted: true };
          else {
            const policy = (await this.store.read(key)).policies.find((row) => row.projectId === run.projectId);
            if (!policy?.autoAdvance) throw domainError('POLICY_REVOKED', 'Automatic advancement is no longer permitted.');
            const assignment = await this.director.supervise(key, run.workId);
            evidence = { workId: run.workId, operationId: assignment.operationId };
          }
        } else evidence = { projectId: run.projectId, workRevision: (await this.graph.snapshot(key, run.projectId)).revision };
        results.push(await this.settle(key, run.id, token, { evidence }));
      } catch (error) {
        try { results.push(await this.settle(key, run.id, token, { error })); }
        catch (leaseError) { if (leaseError.code !== 'LEASE_LOST') throw leaseError; }
      }
    }
    await this.director.reconcile(key); return results;
  }
}
