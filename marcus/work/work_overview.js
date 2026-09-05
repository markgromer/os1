import { requiredVerificationPassed } from '../operations/operation_types.js';
import { summarizeRelease } from './release_summary.js';

function groupBy(rows, key) {
  const groups = new Map();
  for (const row of rows) {
    const id = row[key];
    if (!groups.has(id)) groups.set(id, []);
    groups.get(id).push(row);
  }
  return groups;
}

// A read projection over existing domains, never another task/approval store.
export async function readWorkOverview(key, { graph, director, execution, memory, evidence }) {
  // snapshot also reads the legacy operation store. Do not race its first-file
  // initialization with a second operation read (not all legacy stores coalesce it).
  const graphState = await graph.snapshot(key);
  const [registry, operationMap, agent, queue, decisions, evidenceResult] = await Promise.all([
    graph.engine.registry.list(key), graph.operations(key), director.store.read(key), execution.store.read(key),
    memory.list(key, { kind: 'decision', status: 'active', limit: 500 }),
    evidence.store.readDocument(key).then((doc) => ({ available: true, rows: doc.evidence, sourceState: doc.sourceState || {} })).catch(() => ({ available: false, rows: [], sourceState: {} })),
  ]);
  const operations = [...operationMap.values()];
  const itemsByProject = groupBy(graphState.items, 'projectId');
  const operationsByProject = groupBy(operations, 'projectRegistryId');
  const receiptsByProject = groupBy(evidenceResult.rows.filter((row) => row.provenance?.trusted === true), 'projectRegistryId');
  const decisionsByProject = groupBy(decisions.memories, 'projectId');
  const dependenciesByItem = groupBy(graphState.dependencies, 'itemId');
  const grantedProjects = new Set(agent.projectIds);
  const automaticProjects = new Set(queue.policies.filter((row) => row.autoAdvance === true).map((row) => row.projectId));
  return { observedAt: new Date().toISOString(), businessKey: key, evidenceAvailable: evidenceResult.available,
    decisionsMayBeTruncated: decisions.memories.length >= 500,
    projects: registry.map((project) => {
      const items = itemsByProject.get(project.id) || [];
      const runs = (operationsByProject.get(project.id) || []).sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
      const receipts = (receiptsByProject.get(project.id) || [])
        .sort((a, b) => String(b.timestamp).localeCompare(String(a.timestamp)));
      const githubState = evidenceResult.sourceState['github:' + project.id] || {};
      const deploymentState = evidenceResult.sourceState['deployments:' + project.id] || {};
      const release = summarizeRelease(receipts, { ...githubState,
        lastRefreshedAt: [githubState.lastRefreshedAt, deploymentState.lastRefreshedAt].filter(Boolean).sort().at(-1),
        errors: [...(githubState.errors || []), ...(deploymentState.errors || [])],
      });
      release.providerRefreshSkipped = deploymentState.skipped || [];
      const granted = ['active', 'probation'].includes(agent.lifecycle) && grantedProjects.has(project.id);
      const boundIds = new Set(items.map((row) => row.operationId).filter(Boolean));
      const attention = [
        ...items.filter((row) => row.readiness.needsMark).map((row) => ({ id: row.id, source: 'work', title: row.objective, reason: row.readiness.blockers.map((blocker) => blocker.message).join(' ') })),
        ...runs.filter((row) => row.status === 'waiting_for_approval' && !boundIds.has(row.id)).map((row) => ({ id: row.id, source: 'operation', title: row.title || row.objective, reason: 'Existing execution requires an exact owner approval. Inspect the operation before approving.' })),
      ];
      return { id: project.id, name: project.canonicalName, workspacePath: project.localWorkspace?.path || '', repository: project.repo?.fullName || '',
        objective: project.currentObjective?.desiredOutcome || project.currentObjective?.objective || project.currentObjective?.title || project.currentObjective?.summary || '',
        release,
        needsYouCount: attention.length, attention: attention.slice(0, 50),
        runningCount: runs.filter((row) => row.status === 'running').length,
        readyCount: items.filter((row) => row.readiness.runnable).length,
        workCount: items.length, items: items.slice(0, 100).map((row) => ({ id: row.id, objective: row.objective, kind: row.kind, status: row.readiness.status,
          revision: row.revision, updatedAt: row.updatedAt, operationId: row.operationId, acceptanceCriteria: row.acceptanceCriteria, readiness: row.readiness })),
        dependencies: items.flatMap((item) => dependenciesByItem.get(item.id) || []),
        operationCount: runs.length, operations: runs.slice(0, 10).map((row) => ({ id: row.id, title: row.title || row.objective, status: row.status, updatedAt: row.updatedAt,
          verified: row.status === 'completed' && requiredVerificationPassed(row),
          verification: (row.verification || []).map((check) => ({ id: check.id, type: check.type, status: check.status, required: check.required !== false, waived: check.waived === true })),
          blockers: (row.blockers || []).filter((blocker) => blocker.status === 'active').map((blocker) => ({ type: blocker.type, message: blocker.message || blocker.reason })) })),
        decisions: (decisionsByProject.get(project.id) || []).slice(0, 20).map((row) => ({ id: row.id, revision: row.revision, content: row.content, updatedAt: row.updatedAt })),
        engineering: { lifecycle: agent.lifecycle, granted, autoAdvance: granted && automaticProjects.has(project.id) },
        deployment: release.deployment,
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
    project.deployment ? `Production deployment: ${project.deployment.status}, commit ${project.deployment.commit ? project.deployment.commit.slice(0, 8) : 'unknown'}, recorded ${project.deployment.timestamp}. This does not accept the current request.` : 'No production deployment receipt is connected yet.',
    project.release?.checks.count ? `For commit ${project.release.commit.slice(0, 8)}: ${project.release.checks.passed}/${project.release.checks.count} recorded CI runs passed.` : 'No matching CI receipts returned.',
    ...((project.release?.mergedChanges || []).slice(0, 3).map((row) => `Merged: ${row.title}`)),
    !project.workCount ? 'Existing session and repository evidence are shown without creating duplicate work items. No graph work is linked; its Codex handoff is a report, not verified completion.' : '',
    `Engineering: ${project.engineering.lifecycle}; project grant ${project.engineering.granted ? 'present' : 'absent'}; automatic advancement ${project.engineering.autoAdvance ? 'enabled' : 'off'}.`,
    'This read-only answer does not launch work, approve an action, or accept a result.' ].filter(Boolean).join('\n');
}
