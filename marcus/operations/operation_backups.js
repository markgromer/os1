import fs from 'node:fs/promises';
import path from 'node:path';

import { safeBusinessKey } from './operation_types.js';

export async function discoverDurableBackupSources({ businessDataDir, configuredBusinessKeys = [] } = {}) {
  const root = path.resolve(String(businessDataDir || ''));
  let entries = [];
  try { entries = await fs.readdir(root, { withFileTypes: true }); } catch { entries = []; }
  const keys = new Set((Array.isArray(configuredBusinessKeys) ? configuredBusinessKeys : []).map((key) => safeBusinessKey(key, '')).filter(Boolean));
  for (const entry of entries) if (entry.isDirectory()) {
    const key = safeBusinessKey(entry.name, '');
    if (key) keys.add(key);
  }
  const sources = [];
  for (const businessKey of [...keys].sort()) {
    for (const [fileName, prefix] of [
      ['operations.json', `operations-${businessKey}`],
      ['project-registry.json', `project-registry-${businessKey}`],
      ['project-evidence.json', `project-evidence-${businessKey}`],
      ['marcus-mission-memory.json', `mission-memory-${businessKey}`],
      ['work-graph.json', `work-graph-${businessKey}`],
      ['work-context.json', `work-context-${businessKey}`],
      ['engineering-director.json', `engineering-director-${businessKey}`],
      ['human-identities.json', `human-identities-${businessKey}`],
      ['execution-runs.json', `execution-runs-${businessKey}`],
      ['operator-digests.json', `operator-digests-${businessKey}`],
    ]) {
      const sourceFile = path.join(root, businessKey, fileName);
      try { await fs.access(sourceFile); sources.push({ businessKey, fileName, sourceFile, prefix }); } catch { /* absent files are not fabricated */ }
    }
  }
  return sources;
}
