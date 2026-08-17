import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

const STORE_VERSION = 1;

function safeRevision(value) {
  const revision = Number(value);
  return Number.isSafeInteger(revision) && revision > 0 ? revision : 1;
}

export class BrowserPublicationStore {
  constructor({ dataDir, normalize } = {}) {
    if (!dataDir) throw new Error('BrowserPublicationStore requires dataDir.');
    if (typeof normalize !== 'function') throw new Error('BrowserPublicationStore requires a normalizer.');
    this.file = path.join(path.resolve(String(dataDir)), 'marcus-browser-publications.json');
    this.normalize = normalize;
    this.writeQueue = Promise.resolve();
  }

  async list({ legacyLoader } = {}) {
    return this.withLock(async () => {
      const document = await this.readDocumentUnlocked({ legacyLoader });
      return structuredClone(document.publications);
    });
  }

  async replace(publications) {
    return this.withLock(async () => {
      const current = await this.readDocumentUnlocked();
      const document = {
        version: STORE_VERSION,
        revision: current.revision + 1,
        updatedAt: new Date().toISOString(),
        publications: this.normalize(publications),
      };
      await this.atomicWrite(document);
      return structuredClone(document.publications);
    });
  }

  async withLock(work) {
    const run = this.writeQueue.catch(() => {}).then(work);
    this.writeQueue = run;
    try {
      return await run;
    } finally {
      if (this.writeQueue === run) this.writeQueue = Promise.resolve();
    }
  }

  normalizeDocument(input) {
    const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
    return {
      version: STORE_VERSION,
      revision: safeRevision(raw.revision),
      updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : new Date(0).toISOString(),
      publications: this.normalize(raw.publications),
    };
  }

  async readDocumentUnlocked({ legacyLoader } = {}) {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    try {
      return this.normalizeDocument(JSON.parse(await fs.readFile(this.file, 'utf8')));
    } catch (primaryError) {
      if (primaryError?.code === 'ENOENT') {
        const legacy = typeof legacyLoader === 'function' ? await legacyLoader() : [];
        const document = this.normalizeDocument({ publications: legacy });
        await this.atomicWrite(document, { createBackup: false });
        return document;
      }
      try {
        const recovered = this.normalizeDocument(JSON.parse(await fs.readFile(`${this.file}.bak`, 'utf8')));
        await fs.rename(this.file, `${this.file}.corrupt-${Date.now()}`).catch(() => {});
        await this.atomicWrite(recovered, { createBackup: false });
        return recovered;
      } catch {
        const error = new Error('The durable MARCUS browser publication store is corrupt; the original file was preserved.');
        error.code = 'CORRUPT_BROWSER_PUBLICATION_STORE';
        error.cause = primaryError;
        throw error;
      }
    }
  }

  async atomicWrite(value, { createBackup = true } = {}) {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    const temporary = `${this.file}.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    if (createBackup) await fs.copyFile(this.file, `${this.file}.bak`).catch(() => {});
    await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    try {
      await fs.rename(temporary, this.file);
    } catch (error) {
      await fs.unlink(temporary).catch(() => {});
      throw error;
    }
  }
}
