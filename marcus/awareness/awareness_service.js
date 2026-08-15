import { safeBusinessKey, safeObject, safeString } from '../operations/operation_types.js';
import { registryStatusForLifecycle } from '../projects/project_lifecycle.js';

const ACTIVE_OPERATION_STATUSES = new Set(['draft', 'planned', 'queued', 'running', 'verifying', 'awaiting_provider']);
const BLOCKED_OPERATION_STATUSES = new Set(['blocked', 'failed', 'recovery_required']);

function timeValue(value) {
  const parsed = Date.parse(value || '');
  return Number.isFinite(parsed) ? parsed : 0;
}

function attentionState({ awareness, activity, operations }) {
  if (awareness.lifecycle === 'archived') return 'archived';
  if (awareness.lifecycle === 'completed') return 'completed';
  if (awareness.lifecycle === 'dormant') return 'intentionally_dormant';
  if (operations.some((item) => item.needsApproval || item.status === 'waiting_for_approval')) return 'waiting_on_mark';
  if (operations.some((item) => BLOCKED_OPERATION_STATUSES.has(item.status))) return 'blocked';
  if (operations.some((item) => item.status === 'verifying')) return 'verifying';
  if (operations.some((item) => ACTIVE_OPERATION_STATUSES.has(item.status))) return 'moving';
  const operational = safeString(activity?.operationalState || activity?.state, 100).toLowerCase();
  if (operational === 'active') return 'moving';
  if (operational === 'verifying') return 'verifying';
  if (['at_risk', 'blocked'].includes(operational)) return 'blocked';
  if (['decaying', 'stale', 'abandoned_candidate'].includes(operational)) return 'losing_thread';
  if (['dormant', 'quiet'].includes(operational)) return operational === 'dormant' ? 'intentionally_dormant' : 'quiet';
  if (awareness.lifecycle === 'waiting') return 'waiting_external';
  return 'monitoring';
}

function latestOperationForProject(operations) {
  return [...operations].sort((left, right) => timeValue(right.updatedAt) - timeValue(left.updatedAt))[0] || null;
}

function searchableText(project, awareness) {
  return [
    project?.canonicalName,
    ...(Array.isArray(project?.aliases) ? project.aliases : []),
    project?.repo?.fullName,
    project?.repo?.url,
    project?.localWorkspace?.path,
    project?.deployments?.productionUrl,
    awareness?.objectiveBelief,
    awareness?.memory?.summary,
  ].filter(Boolean).join(' ').toLowerCase();
}

export class AwarenessService {
  constructor({ store, listProjects, updateProject, listOperations, evidenceService, memoryIndexer } = {}) {
    if (!store || typeof listProjects !== 'function' || typeof updateProject !== 'function' || typeof listOperations !== 'function') {
      throw new Error('AwarenessService requires store, project registry callbacks, and operations access.');
    }
    this.store = store;
    this.listProjects = listProjects;
    this.updateProject = updateProject;
    this.listOperations = listOperations;
    this.evidenceService = evidenceService;
    this.memoryIndexer = memoryIndexer;
  }

  async synchronize(businessKey, { indexMissing = true } = {}) {
    const key = safeBusinessKey(businessKey);
    const registry = await this.listProjects(key);
    let awareness = await this.store.synchronize(key, registry);
    if (indexMissing && this.memoryIndexer) {
      const byRegistry = new Map(registry.map((item) => [item.id, item]));
      for (const record of awareness) {
        if (record.memory?.lastIndexedAt || !byRegistry.has(record.projectRegistryId)) continue;
        const project = byRegistry.get(record.projectRegistryId);
        if (!this.memoryIndexer.canAccessWorkspace(project)) continue;
        try {
          const memory = await this.memoryIndexer.indexProject(project);
          await this.store.updateMemory(key, record.id, memory);
        } catch {
          // A missing or offline workspace must not break the awareness feed.
        }
      }
      awareness = await this.store.list(key);
    }
    return { registry, awareness };
  }

  async feed(businessKey, { includeArchived = false, query = '' } = {}) {
    const key = safeBusinessKey(businessKey);
    const [{ registry, awareness }, operations, activity] = await Promise.all([
      this.synchronize(key),
      this.listOperations(key, { limit: 200 }).catch(() => []),
      this.evidenceService?.getActivity(key).catch(() => ({ snapshots: [] })) || { snapshots: [] },
    ]);
    const registryById = new Map(registry.map((item) => [item.id, item]));
    const activityById = new Map((Array.isArray(activity?.snapshots) ? activity.snapshots : []).map((item) => [item.projectRegistryId, item]));
    const normalizedQuery = safeString(query, 500).toLowerCase();
    const projects = awareness.map((record) => {
      const project = registryById.get(record.projectRegistryId) || {};
      const projectOperations = operations.filter((item) => item.projectRegistryId === record.projectRegistryId || (project.projectId && item.projectId === project.projectId));
      const projectActivity = activityById.get(record.projectRegistryId) || null;
      const latestOperation = latestOperationForProject(projectOperations);
      const state = attentionState({ awareness: record, activity: projectActivity, operations: projectOperations });
      const updatedAt = [record.updatedAt, project.updatedAt, projectActivity?.lastActivityAt, latestOperation?.updatedAt]
        .sort((left, right) => timeValue(right) - timeValue(left))[0] || record.updatedAt;
      const completedIsRecent = state === 'completed' && Date.now() - timeValue(updatedAt) <= 14 * 24 * 60 * 60_000;
      const dashboardVisible = !['archived', 'intentionally_dormant', 'completed'].includes(state) || completedIsRecent;
      return {
        id: record.id,
        awarenessProjectId: record.id,
        projectRegistryId: record.projectRegistryId,
        projectId: project.projectId || '',
        canonicalName: record.canonicalName || project.canonicalName,
        aliases: record.aliases,
        lifecycle: record.lifecycle,
        archived: record.lifecycle === 'archived',
        dashboardVisible,
        attentionState: state,
        objectiveBelief: record.objectiveBelief || project.currentObjective?.desiredOutcome || project.description || '',
        latestMeaningfulChange: projectActivity?.latestMeaningfulChange || record.latestMeaningfulChange || latestOperation?.title || '',
        currentActivity: latestOperation?.currentStep?.title || record.currentActivity || projectActivity?.reason || '',
        blockerOrDependency: projectActivity?.risks?.[0]?.summary || record.blockerOrDependency || '',
        likelyNextStep: projectActivity?.nextExpectedEvent?.summary || projectActivity?.suggestedAction || record.likelyNextStep || '',
        confidence: Math.max(record.confidence, Number(projectActivity?.confidence || 0)),
        knowledge: {
          status: record.memory?.status || 'unavailable',
          summary: record.memory?.summary || '',
          lastIndexedAt: record.memory?.lastIndexedAt || '',
          sourceCount: record.memory?.sources?.length || 0,
          repositoryFileCount: Number(record.memory?.repositoryManifest?.fileCount || 0),
          repositoryTruncated: record.memory?.repositoryManifest?.truncated === true,
        },
        repo: project.repo || {},
        localWorkspace: project.localWorkspace || {},
        deployments: project.deployments || {},
        status: project.status || '',
        activity: projectActivity,
        latestOperation,
        operationCount: projectOperations.length,
        updatedAt,
      };
    }).filter((item) => (includeArchived || !item.archived)
      && (!normalizedQuery || searchableText(registryById.get(item.projectRegistryId), awareness.find((record) => record.id === item.id)).includes(normalizedQuery)));
    projects.sort((left, right) => {
      const rank = { waiting_on_mark: 0, blocked: 1, verifying: 2, moving: 3, losing_thread: 4, monitoring: 5, quiet: 6, waiting_external: 7, intentionally_dormant: 8, completed: 9, archived: 10 };
      return (rank[left.attentionState] ?? 11) - (rank[right.attentionState] ?? 11) || timeValue(right.updatedAt) - timeValue(left.updatedAt);
    });
    return {
      version: 1,
      businessKey: key,
      generatedAt: new Date().toISOString(),
      projects,
      counts: {
        total: awareness.length,
        visible: projects.length,
        activeAttention: projects.filter((item) => !['archived', 'completed', 'intentionally_dormant'].includes(item.attentionState)).length,
        archived: awareness.filter((item) => item.lifecycle === 'archived').length,
        dormant: awareness.filter((item) => item.lifecycle === 'dormant').length,
        completed: awareness.filter((item) => item.lifecycle === 'completed').length,
        indexed: awareness.filter((item) => ['fresh', 'indexed'].includes(item.memory?.status)).length,
      },
    };
  }

  async detail(businessKey, id) {
    const key = safeBusinessKey(businessKey);
    const feed = await this.feed(key, { includeArchived: true });
    const project = feed.projects.find((item) => item.id === id || item.projectRegistryId === id);
    if (!project) throw Object.assign(new Error('Awareness project not found.'), { code: 'AWARENESS_PROJECT_NOT_FOUND' });
    const record = await this.store.get(key, project.id);
    const evidence = this.evidenceService
      ? await this.evidenceService.getProjectEvidence(key, project.projectRegistryId, { limit: 40 }).catch(() => [])
      : [];
    return { ...project, memory: record?.memory || {}, lifecycleHistory: record?.lifecycleHistory || [], workHistory: record?.workHistory || [], evidence };
  }

  async search(businessKey, query, { limit = 20 } = {}) {
    const q = safeString(query, 500).trim();
    if (!q) return [];
    const result = await this.feed(businessKey, { includeArchived: true, query: q });
    return result.projects.slice(0, Math.max(1, Math.min(100, Number(limit) || 20)));
  }

  async setLifecycle(businessKey, id, lifecycle, options = {}) {
    const key = safeBusinessKey(businessKey);
    const { registry } = await this.synchronize(key, { indexMissing: false });
    const record = await this.store.get(key, id);
    if (!record) throw Object.assign(new Error('Awareness project not found.'), { code: 'AWARENESS_PROJECT_NOT_FOUND' });
    const project = registry.find((item) => item.id === record.projectRegistryId);
    if (!project) throw Object.assign(new Error('Project registry record not found.'), { code: 'PROJECT_REGISTRY_NOT_FOUND' });
    const updated = await this.store.setLifecycle(key, record.id, lifecycle, options);
    try {
      await this.updateProject(key, project.id, { status: registryStatusForLifecycle(updated.lifecycle) });
    } catch (error) {
      await this.store.setLifecycle(key, record.id, record.lifecycle, { actor: 'system', reason: 'Registry update failed; awareness lifecycle rolled back.', source: 'rollback' });
      throw error;
    }
    return this.detail(key, record.id);
  }

  async refreshMemory(businessKey, id, { createRootNote = true } = {}) {
    if (!this.memoryIndexer) throw Object.assign(new Error('Project memory indexing is unavailable.'), { code: 'AWARENESS_MEMORY_UNAVAILABLE' });
    const key = safeBusinessKey(businessKey);
    const { registry } = await this.synchronize(key, { indexMissing: false });
    const record = await this.store.get(key, id);
    if (!record) throw Object.assign(new Error('Awareness project not found.'), { code: 'AWARENESS_PROJECT_NOT_FOUND' });
    const project = registry.find((item) => item.id === record.projectRegistryId);
    const memory = await this.memoryIndexer.indexProject(project, { createRootNote });
    await this.store.updateMemory(key, record.id, memory);
    return this.detail(key, record.id);
  }

  async projectContext(businessKey, id, { maxChars = 10_000 } = {}) {
    const detail = await this.detail(businessKey, id);
    const memory = safeObject(detail.memory);
    const lines = [
      `Awareness project: ${detail.canonicalName}`,
      `Project registry id: ${detail.projectRegistryId}`,
      `Lifecycle: ${detail.lifecycle}; attention: ${detail.attentionState}`,
      detail.objectiveBelief ? `Objective: ${detail.objectiveBelief}` : '',
      detail.knowledge?.summary ? `Durable project synopsis: ${detail.knowledge.summary}` : '',
      detail.currentActivity ? `Current activity: ${detail.currentActivity}` : '',
      detail.blockerOrDependency ? `Blocker or dependency: ${detail.blockerOrDependency}` : '',
      detail.likelyNextStep ? `Likely next step: ${detail.likelyNextStep}` : '',
      memory.repositoryManifest?.fileCount ? `Indexed repository manifest: ${memory.repositoryManifest.fileCount} files${memory.repositoryManifest.truncated ? ' (bounded scan)' : ''}.` : '',
      ...(Array.isArray(detail.workHistory) ? detail.workHistory : []).slice(-10).map((item) => `Work history [${item.recordedAt || 'unknown date'}; ${item.status || item.type}]: ${item.summary}`),
      ...(Array.isArray(memory.sources) ? memory.sources : []).slice(0, 8).map((source) => `Memory source [${source.type}] ${source.path}: ${safeString(source.excerpt, 1_500)}`),
    ].filter(Boolean);
    return { project: detail, text: lines.join('\n').slice(0, Math.max(1_000, Math.min(30_000, Number(maxChars) || 10_000))) };
  }

  async recordWorkEvent(businessKey, projectRegistryId, event = {}) {
    if (!projectRegistryId) return { recorded: false, reason: 'project_registry_id_required' };
    const key = safeBusinessKey(businessKey);
    const { registry } = await this.synchronize(key, { indexMissing: false });
    const awareness = await this.store.get(key, projectRegistryId);
    const project = registry.find((item) => item.id === projectRegistryId);
    if (!awareness || !project) return { recorded: false, reason: 'project_not_found' };
    const eventId = safeString(event.eventId || event.id, 200);
    if (!eventId) return { recorded: false, reason: 'event_id_required' };
    if (awareness.workHistory?.some((item) => item.eventId === eventId)) return { recorded: false, duplicate: true };
    const note = this.memoryIndexer
      ? await this.memoryIndexer.appendWorkNote(project, { ...event, eventId }).catch((error) => ({ appended: false, error: error?.message || String(error) }))
      : { appended: false, reason: 'memory_indexer_unavailable' };
    const stored = await this.store.appendWorkEvent(key, awareness.id, { ...event, eventId });
    if (note.appended) {
      const memory = await this.memoryIndexer.indexProject(project, { createRootNote: false });
      await this.store.updateMemory(key, awareness.id, memory);
    }
    return { recorded: stored.created, note };
  }
}
