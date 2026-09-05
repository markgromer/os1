// Summarize provider receipts, not agent prose. Verification stays commit-specific.
export function summarizeRelease(receipts, sourceState = {}) {
  const deployments = receipts.filter((row) => ['github', 'render', 'cloudflare'].includes(row.source)
    && row.deployment?.environment === 'production' && ['production_published', 'deployment_completed', 'deployment_failed', 'deployment_started'].includes(row.type))
    .sort((a, b) => String(b.metadata?.deploymentCreatedAt || b.timestamp).localeCompare(String(a.metadata?.deploymentCreatedAt || a.timestamp))
      || String(b.timestamp).localeCompare(String(a.timestamp)));
  const deployment = deployments[0] || null;
  const commit = deployment?.deployment?.commitSha || deployment?.commitSha || sourceState.defaultBranchHead || '';
  const runs = new Map();
  for (const row of receipts.filter((entry) => commit && entry.source === 'github' && entry.commitSha === commit
    && ['build_run', 'test_run', 'lint_run', 'typecheck_run'].includes(entry.type))
    .sort((a, b) => String(b.timestamp).localeCompare(String(a.timestamp)))) {
    // Receipt URL identifies the run across attempt/status observations.
    const key = row.metadata?.url || row.externalId || row.id;
    if (!runs.has(key)) runs.set(key, row);
  }
  const checks = [...runs.values()];
  const passed = checks.filter((row) => row.metadata?.status === 'completed' && row.metadata?.conclusion === 'success').length;
  const changes = new Map();
  for (const row of receipts.filter((entry) => entry.source === 'github' && entry.type === 'pull_request_merged')
    .sort((a, b) => String(b.timestamp).localeCompare(String(a.timestamp)))) {
    const key = row.pullRequest?.url || row.pullRequest?.number || row.id;
    if (!changes.has(key)) changes.set(key, row);
  }
  return {
    deployment: deployment ? { id: deployment.id, type: deployment.type, source: deployment.source, observedAt: deployment.observedAt, timestamp: deployment.timestamp,
      commit, status: deployment.deployment.status, url: deployment.deployment.url || '', receiptUrl: deployment.metadata?.url || '', mapping: deployment.metadata?.targetMapping || 'provider_mapping' } : null,
    commit, headCommit: sourceState.defaultBranchHead || '',
    checks: { count: checks.length, passed, allRecordedPassed: checks.length > 0 && passed === checks.length,
      runs: checks.slice(0, 12).map((row) => ({ id: row.id, name: row.metadata?.workflowName || row.summary, status: row.metadata?.conclusion || row.metadata?.status || 'unknown', timestamp: row.timestamp, url: row.metadata?.url || '' })) },
    mergedChanges: [...changes.values()]
      .slice(0, 4).map((row) => ({ id: row.id, title: row.pullRequest?.title || row.summary, number: row.pullRequest?.number, url: row.pullRequest?.url || '', timestamp: row.timestamp })),
    refreshedAt: sourceState.lastRefreshedAt || '',
    refreshErrors: (sourceState.errors || []).map((row) => ({ endpoint: row.endpoint, error: row.error })),
  };
}
