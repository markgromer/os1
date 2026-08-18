import { createSignal, signalMatches } from './signal.js';

export class SignalBus {
  constructor({ journal = null, onError = () => {} } = {}) {
    this.journal = journal;
    this.onError = onError;
    this.pathways = [];
    this.seen = new Map();
  }

  register({ name, accepts = ['*'], priority = 100, handle } = {}) {
    if (!name || typeof handle !== 'function') throw new Error('A pathway requires a name and handle function.');
    const pathway = { name: String(name), accepts: Array.isArray(accepts) ? accepts : [accepts], priority: Number(priority) || 100, handle };
    this.pathways.push(pathway);
    this.pathways.sort((a, b) => a.priority - b.priority || a.name.localeCompare(b.name));
    return () => { this.pathways = this.pathways.filter((item) => item !== pathway); };
  }

  async publish(input) {
    const signal = createSignal(input);
    if (this.seen.has(signal.id)) return { signal, delivery: { handled: [], failed: [], duplicate: true } };
    this.seen.set(signal.id, signal.observedAt);
    if (this.seen.size > 5_000) this.seen.delete(this.seen.keys().next().value);
    const matching = this.pathways.filter((pathway) => pathway.accepts.some((pattern) => signalMatches(pattern, signal.type)));
    const delivery = { handled: [], failed: [] };
    for (const pathway of matching) {
      try {
        const result = await pathway.handle(signal);
        delivery.handled.push({ pathway: pathway.name, result: result ?? null });
      } catch (error) {
        delivery.failed.push({ pathway: pathway.name, code: error?.code || 'PATHWAY_FAILED', message: String(error?.message || error).slice(0, 1_000) });
        this.onError(error, { signal, pathway: pathway.name });
      }
    }
    if (this.journal) {
      try { await this.journal.append(signal, delivery); }
      catch (error) {
        delivery.failed.push({ pathway: 'signal-journal', code: error?.code || 'SIGNAL_JOURNAL_FAILED', message: String(error?.message || error).slice(0, 1_000) });
        this.onError(error, { signal, pathway: 'signal-journal' });
      }
    }
    return { signal, delivery };
  }
}
