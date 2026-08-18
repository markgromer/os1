import fs from 'node:fs/promises';
import path from 'node:path';

import { safeBusinessKey, sanitizeStructured } from '../operations/operation_types.js';

export class SignalJournal {
  constructor({ dataDir, maxEntries = 2_000 } = {}) {
    if (!dataDir) throw new Error('SignalJournal requires dataDir.');
    this.dataDir = path.resolve(String(dataDir));
    this.maxEntries = Math.max(100, Number(maxEntries) || 2_000);
    this.queues = new Map();
    this.appendCounts = new Map();
  }

  fileForBusiness(businessKey) {
    return path.join(this.dataDir, 'businesses', safeBusinessKey(businessKey), 'marcus-signal-journal.jsonl');
  }

  async append(signal, delivery = {}) {
    const key = safeBusinessKey(signal?.businessKey);
    const previous = this.queues.get(key) || Promise.resolve();
    const queued = previous.catch(() => {}).then(async () => {
      const file = this.fileForBusiness(key);
      await fs.mkdir(path.dirname(file), { recursive: true });
      const entry = JSON.stringify(sanitizeStructured({ signal, delivery }, 40_000));
      await fs.appendFile(file, `${entry}\n`, 'utf8');
      const count = (this.appendCounts.get(key) || 0) + 1;
      this.appendCounts.set(key, count);
      if (count % 100 === 0) {
        const lines = (await fs.readFile(file, 'utf8')).split(/\r?\n/).filter(Boolean);
        if (lines.length > this.maxEntries) {
          const temporary = `${file}.tmp-${process.pid}-${Date.now()}`;
          await fs.writeFile(temporary, `${lines.slice(-this.maxEntries).join('\n')}\n`, 'utf8');
          await fs.rename(temporary, file);
        }
      }
    });
    this.queues.set(key, queued);
    try { await queued; } finally { if (this.queues.get(key) === queued) this.queues.delete(key); }
  }

  async recent(businessKey, { limit = 100, type = '' } = {}) {
    const file = this.fileForBusiness(businessKey);
    try {
      const entries = (await fs.readFile(file, 'utf8')).split(/\r?\n/).filter(Boolean).flatMap((line) => {
        try { return [JSON.parse(line)]; } catch { return []; }
      });
      return entries.filter((entry) => !type || entry?.signal?.type === type).slice(-Math.max(1, Number(limit) || 100));
    } catch (error) {
      if (error?.code === 'ENOENT') return [];
      throw error;
    }
  }
}
