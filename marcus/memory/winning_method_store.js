import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

function clean(value, max = 2_000) { return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max); }

export class WinningMethodStore {
  constructor({ dataDir }) { this.dataDir = path.resolve(String(dataDir)); }
  file(businessKey) { return path.join(this.dataDir, 'businesses', clean(businessKey, 80) || 'personal', 'marcus-winning-methods.json'); }
  async list(businessKey) {
    try { return JSON.parse(await fs.readFile(this.file(businessKey), 'utf8')).methods || []; } catch (error) { if (error?.code === 'ENOENT') return []; throw error; }
  }
  async recordRecoveredOperation(businessKey, operation) {
    const failures = (operation.activityLog || []).filter((item) => /failed|recovery|required|retry/i.test(item.type));
    if (!failures.length || operation.status !== 'completed') return null;
    const methods = await this.list(businessKey);
    const key = clean(`${operation.projectRegistryId}:${operation.title}`, 500).toLowerCase();
    const record = {
      id: `method_${crypto.createHash('sha256').update(key).digest('hex').slice(0, 16)}`,
      projectRegistryId: clean(operation.projectRegistryId, 160),
      operationType: clean(operation.title, 300),
      deadEnd: clean(failures.at(-1)?.message || failures.at(-1)?.type),
      winningMethod: clean((operation.activityLog || []).filter((item) => /completed|verified|reconciled|approved/i.test(item.type)).at(-1)?.message || 'Completed using the verified operation path.'),
      verification: clean((operation.verification || []).filter((item) => item.status === 'passed').map((item) => item.type).join(', '), 500),
      lastVerifiedAt: operation.completedAt || new Date().toISOString(),
    };
    const next = [...methods.filter((item) => item.id !== record.id), record].slice(-200);
    const file = this.file(businessKey);
    await fs.mkdir(path.dirname(file), { recursive: true });
    const temporary = `${file}.tmp-${process.pid}`;
    await fs.writeFile(temporary, `${JSON.stringify({ version: 1, methods: next }, null, 2)}\n`, 'utf8');
    await fs.rename(temporary, file);
    return record;
  }
}
