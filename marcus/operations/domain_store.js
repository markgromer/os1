import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

const queues = new Map();
export const domainError = (code, message) => Object.assign(new Error(message), { code });
export function exactBusinessKey(value) {
  if (typeof value !== 'string' || !/^[a-z0-9][a-z0-9_-]{0,63}$/.test(value)) throw domainError('INVALID_SCOPE', 'An exact business key is required.');
  return value;
}
export const newDomainId = (prefix) => `${prefix}_${crypto.randomBytes(12).toString('hex')}`;

export async function replaceDomainFile(temporary, file, { platform = process.platform, rename = fs.rename, delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms)) } = {}) {
  // Windows readers/scanners can briefly block atomic replacement. Retry only
  // that rename, never the transaction or an external effect; do not unlink the primary.
  const attempts = platform === 'win32' ? 6 : 1;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try { await rename(temporary, file); return; }
    catch (error) {
      if (attempt === attempts || !['EACCES', 'EBUSY', 'EPERM'].includes(error.code)) throw error;
      await delay(20 * attempt);
    }
  }
}

// Versioned JSON persistence follows OperationStore's atomic replacement/backup
// convention. New domains share this implementation, not another task engine.
export class DomainStore {
  constructor({ dataDir, name, empty, validate = () => true }) {
    if (!dataDir || !/^[a-z][a-z0-9-]+$/.test(name)) throw new Error('Invalid domain store configuration.');
    this.dataDir = path.resolve(dataDir); this.name = name; this.empty = empty; this.validate = validate;
  }
  file(key) { return path.join(this.dataDir, 'businesses', exactBusinessKey(key), `${this.name}.json`); }
  initial(key) { return { schemaVersion: 1, businessKey: key, revision: 0, updatedAt: new Date(0).toISOString(), ...this.empty() }; }
  decode(raw, key) {
    const value = JSON.parse(raw);
    if (value.schemaVersion !== 1) throw domainError('DOMAIN_VERSION_UNSUPPORTED', 'Unsupported domain version; use a compatible release.');
    if (value.schemaVersion !== 1 || value.businessKey !== key || !Number.isInteger(value.revision) || !this.validate(value)) throw domainError('DOMAIN_SCHEMA_INVALID', 'Invalid or unsupported domain document.');
    return value;
  }
  async load(key) {
    const file = this.file(key);
    try { return { document: this.decode(await fs.readFile(file, 'utf8'), key), recovered: false }; }
    catch (error) {
      if (error.code === 'DOMAIN_VERSION_UNSUPPORTED') throw error;
      if (error.code === 'ENOENT') {
        try { return { document: this.decode(await fs.readFile(`${file}.bak`, 'utf8'), key), recovered: true }; }
        catch (backupError) { if (backupError.code === 'ENOENT') return { document: this.initial(key), recovered: false }; throw domainError('DOMAIN_STORE_CORRUPT', 'Missing primary and invalid backup.'); }
      }
      try { return { document: this.decode(await fs.readFile(`${file}.bak`, 'utf8'), key), recovered: true }; }
      catch { throw domainError('DOMAIN_STORE_CORRUPT', `${this.name} is unreadable; original files are preserved.`); }
    }
  }
  async read(key) { return structuredClone((await this.load(exactBusinessKey(key))).document); }
  async acquire(file) {
    const lock = `${file}.lock`;
    for (let attempt = 0; attempt < 100; attempt++) {
      try {
        const handle = await fs.open(lock, 'wx');
        await handle.writeFile(JSON.stringify({ pid: process.pid, createdAt: new Date().toISOString() }));
        await handle.close();
        return async () => { await fs.unlink(lock); };
      } catch (error) {
        if (error.code !== 'EEXIST') throw error;
        // Never steal a live process's lock on a timer. Recover only a dead owner.
        try {
          const owner = JSON.parse(await fs.readFile(lock, 'utf8'));
          if (Number.isInteger(owner.pid) && owner.pid > 0) {
            try { process.kill(owner.pid, 0); }
            catch (pidError) { if (pidError.code === 'ESRCH') await fs.rename(lock, `${lock}.abandoned-${crypto.randomUUID()}`).catch(() => {}); }
          }
        } catch { /* fail closed for an unreadable lock */ }
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }
    throw domainError('DOMAIN_BUSY', 'The domain is being updated; retry after reloading.');
  }
  async mutate(key, mutate, { revision } = {}) {
    const file = this.file(key);
    const previous = queues.get(file) || Promise.resolve();
    const run = previous.catch(() => {}).then(async () => {
      await fs.mkdir(path.dirname(file), { recursive: true });
      const release = await this.acquire(file);
      try {
        const { document, recovered } = await this.load(key);
        if (revision !== undefined && document.revision !== revision) throw domainError('REVISION_MISMATCH', 'Reload the current domain revision before changing it.');
        const result = mutate(document);
        if (result?.then) throw domainError('ASYNC_TRANSACTION', 'External I/O is not allowed inside a domain transaction.');
        document.revision++;
        document.updatedAt = new Date().toISOString();
        if (!this.validate(document)) throw domainError('DOMAIN_SCHEMA_INVALID', 'Mutation violates domain invariants.');
        const json = `${JSON.stringify(document, null, 2)}\n`;
        if (Buffer.byteLength(json) > 16_000_000) throw domainError('DOMAIN_CAPACITY', 'Domain capacity reached; archive explicitly before continuing.');
        if (recovered) await fs.rename(file, `${file}.corrupt-${crypto.randomUUID()}`).catch((error) => { if (error.code !== 'ENOENT') throw error; });
        else await fs.copyFile(file, `${file}.bak`).catch((error) => { if (error.code !== 'ENOENT') throw error; });
        const temporary = `${file}.tmp-${crypto.randomUUID()}`;
        const handle = await fs.open(temporary, 'wx');
        try { await handle.writeFile(json); await handle.sync(); } finally { await handle.close(); }
        try { await replaceDomainFile(temporary, file); }
        catch (error) { await fs.unlink(temporary).catch(() => {}); throw error; }
        return structuredClone(result);
      } finally { await release(); }
    });
    queues.set(file, run);
    try { return await run; } finally { if (queues.get(file) === run) queues.delete(file); }
  }
}
