export class ReflexEngine {
  constructor({ attentionStore, outcomeLedger, onError = () => {} } = {}) {
    this.attentionStore = attentionStore;
    this.outcomeLedger = outcomeLedger;
    this.onError = onError;
    this.reflexes = [];
  }

  register({ name, priority = 100, when, act, owner = 'marcus' } = {}) {
    if (!name || typeof when !== 'function' || typeof act !== 'function') throw new Error('A reflex requires name, when, and act.');
    const reflex = { name: String(name), priority: Number(priority) || 100, when, act, owner };
    this.reflexes.push(reflex); this.reflexes.sort((a, b) => a.priority - b.priority || a.name.localeCompare(b.name));
    return () => { this.reflexes = this.reflexes.filter((item) => item !== reflex); };
  }

  async handle(signal) {
    const outcomes = [];
    for (const reflex of this.reflexes) {
      let applies = false;
      try { applies = await reflex.when(signal); }
      catch (error) { this.onError(error, { signal, reflex: reflex.name, phase: 'evaluate' }); continue; }
      if (!applies) continue;
      try {
        const result = await reflex.act(signal);
        let outcome = null;
        try { outcome = this.outcomeLedger ? await this.outcomeLedger.record(signal.businessKey, { traceId: signal.traceId, signalId: signal.id, signalType: signal.type, pathway: reflex.name, response: result?.response || reflex.name, status: result?.status || 'succeeded', evidence: result?.evidence }) : null; }
        catch (error) { this.onError(error, { signal, reflex: reflex.name, phase: 'record_outcome' }); }
        outcomes.push({ reflex: reflex.name, status: 'succeeded', outcomeId: outcome?.id || '' });
      } catch (error) {
        this.onError(error, { signal, reflex: reflex.name, phase: 'act' });
        let outcome = null; let attention = null;
        try { outcome = this.outcomeLedger ? await this.outcomeLedger.record(signal.businessKey, { traceId: signal.traceId, signalId: signal.id, signalType: signal.type, pathway: reflex.name, response: String(error?.message || error), status: 'failed' }) : null; }
        catch (recordError) { this.onError(recordError, { signal, reflex: reflex.name, phase: 'record_failure' }); }
        try { attention = this.attentionStore ? await this.attentionStore.raise(signal.businessKey, { fingerprint: `reflex-failed:${reflex.name}:${signal.subject?.id || signal.type}`, signalId: signal.id, signalType: signal.type, subject: signal.subject, title: `Reflex failed: ${reflex.name}`, reason: String(error?.message || error), severity: 'warning', confidence: signal.confidence, owner: reflex.owner }) : null; }
        catch (attentionError) { this.onError(attentionError, { signal, reflex: reflex.name, phase: 'raise_attention' }); }
        outcomes.push({ reflex: reflex.name, status: 'failed', outcomeId: outcome?.id || '', attentionId: attention?.id || '' });
      }
    }
    return outcomes;
  }
}
