import { requiredVerificationPassed } from '../operations/operation_types.js';

// A read projection over existing domains, never another task/approval store.
export async function readWorkOverview(key, { graph, director, execution, memory, evidence }) {
  // snapshot also reads the legacy operation store. Do not race its first-file
  // initialization with a second operation read (not all legacy stores coalesce it).
  const graphState = await graph.snapshot(key);
  const [registry, operationMap, agent, queue, decisions, evidenceResult] = await Promise.all([
    graph.engine.registry.list(key), graph.operations(key), director.store.read(key), execution.store.read(key),
    memory.list(key, { kind: 'decision', status: 'active', limit: 500 }),
    evidence.store.readDocument(key).then((doc) => ({ available: true, rows: doc.evidence })).catch(() => ({ available: false, rows: [] })),
  ]);
  const operations = [...operationMap.values()];
  return { observedAt: new Date().toISOString(), businessKey: key, evidenceAvailable: evidenceResult.available,
    decisionsMayBeTruncated: decisions.memories.length >= 500,
    projects: registry.map((project) => {
      const items = graphState.items.filter((row) => row.projectId === project.id);
      const runs = operations.filter((row) => row.projectRegistryId === project.id).sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
      const receipts = evidenceResult.rows.filter((row) => row.projectRegistryId === project.id && row.provenance?.trusted === true)
        .sort((a, b) => String(b.timestamp).localeCompare(String(a.timestamp)));
      // Latest deployment event wins, including failures. Never promote an older success over it.
      const deployment = receipts.find((row) => ['render', 'cloudflare', 'github'].includes(row.source)
        && ['production_published', 'deployment_completed', 'deployment_failed', 'deployment_started'].includes(row.type)
        && row.deployment?.environment === 'production');
      const granted = ['active', 'probation'].includes(agent.lifecycle) && agent.projectIds.includes(project.id);
      const boundIds = new Set(items.map((row) => row.operationId).filter(Boolean));
      const attention = [
        ...items.filter((row) => row.readiness.needsMark).map((row) => ({ id: row.id, source: 'work', title: row.objective, reason: row.readiness.blockers.map((blocker) => blocker.message).join(' ') })),
        ...runs.filter((row) => row.status === 'waiting_for_approval' && !boundIds.has(row.id)).map((row) => ({ id: row.id, source: 'operation', title: row.title || row.objective, reason: 'Existing execution requires an exact owner approval. Inspect the operation before approving.' })),
      ];
      return { id: project.id, name: project.canonicalName, workspacePath: project.localWorkspace?.path || '', repository: project.repo?.fullName || '',
        needsYouCount: attention.length, attention: attention.slice(0, 50),
        runningCount: runs.filter((row) => row.status === 'running').length,
        readyCount: items.filter((row) => row.readiness.runnable).length,
        workCount: items.length, items: items.slice(0, 100).map((row) => ({ id: row.id, objective: row.objective, kind: row.kind, status: row.readiness.status,
          revision: row.revision, updatedAt: row.updatedAt, operationId: row.operationId, acceptanceCriteria: row.acceptanceCriteria, readiness: row.readiness })),
        dependencies: graphState.dependencies.filter((edge) => items.some((item) => item.id === edge.itemId)),
        operationCount: runs.length, operations: runs.slice(0, 10).map((row) => ({ id: row.id, title: row.title || row.objective, status: row.status, updatedAt: row.updatedAt,
          verified: row.status === 'completed' && requiredVerificationPassed(row),
          verification: (row.verification || []).map((check) => ({ id: check.id, type: check.type, status: check.status, required: check.required !== false, waived: check.waived === true })),
          blockers: (row.blockers || []).filter((blocker) => blocker.status === 'active').map((blocker) => ({ type: blocker.type, message: blocker.message || blocker.reason })) })),
        decisions: decisions.memories.filter((row) => row.projectId === project.id).slice(0, 20).map((row) => ({ id: row.id, revision: row.revision, content: row.content, updatedAt: row.updatedAt })),
        engineering: { lifecycle: agent.lifecycle, granted, autoAdvance: granted && queue.policies.some((row) => row.projectId === project.id && row.autoAdvance === true) },
        deployment: deployment ? { id: deployment.id, type: deployment.type, source: deployment.source, observedAt: deployment.observedAt, timestamp: deployment.timestamp,
          commit: deployment.deployment.commitSha || deployment.commitSha || '', status: deployment.deployment.status, url: deployment.deployment.url || '' } : null,
        recentChanges: receipts.slice(0, 8).map((row) => ({ id: row.id, type: row.type, source: row.source, timestamp: row.timestamp, summary: row.summary })),
      };
    }) };
}

export function workOverviewReply(project) {
  const blocked = project.items.filter((row) => row.readiness.blockers.length);
  const ready = project.items.filter((row) => row.readiness.runnable);
  return [ `${project.name}: ${project.workCount} tracked work item(s), ${project.operationCount} existing execution record(s).`,
    ...project.attention.filter((row) => row.source === 'operation').map((row) => `${row.title}: ${row.reason}`),
    ...blocked.map((row) => `${row.objective}: ${row.readiness.blockers.map((blocker) => blocker.message).join(' ')}`),
    ...ready.map((row) => `${row.objective}: ready${project.engineering.autoAdvance ? ' under the saved project policy' : ', not automatically authorized to advance'}.`),
    !project.workCount ? 'This project has no work-graph items yet. Its Codex handoff is a report, not verified completion.' : '',
    `Engineering: ${project.engineering.lifecycle}; project grant ${project.engineering.granted ? 'present' : 'absent'}; automatic advancement ${project.engineering.autoAdvance ? 'enabled' : 'off'}.`,
    'This read-only answer does not launch work, approve an action, or accept a result.' ].filter(Boolean).join('\n');
}
