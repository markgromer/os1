import { ApprovalPolicy } from '../approvals/approval_policy.js';
import { ApprovalService } from '../approvals/approval_service.js';
import { CodexProvider } from '../providers/codex_provider.js';
import { DesktopProvider } from '../providers/desktop_provider.js';
import { GitHubReadProvider } from '../providers/github_provider.js';
import { ProjectRegistry } from '../projects/project_registry.js';
import { ProjectResolver } from '../projects/project_resolver.js';
import { OperationRecovery } from './operation_recovery.js';
import { OperationRunner } from './operation_runner.js';
import { OperationService } from './operation_service.js';
import { OperationStore } from './operation_store.js';
import { OperationVerification } from './operation_verification.js';
import { makeOperationId, nowIso, safeBusinessKey, safeObject, safeString, summarizeOperationProgress } from './operation_types.js';

function objectiveFromRequest(request) {
  const text = safeString(request, 8_000);
  if (!text) return 'Complete the requested project outcome.';
  const firstSentence = text.split(/(?<=[.!?])\s+/)[0].replace(/[.!?]+$/, '').trim();
  const condition = firstSentence.match(/^the\s+(.+?)\s+is\s+(?:still\s+)?unusable\s+on\s+(.+)$/i);
  if (condition) return `Make ${condition[1]} usable on ${condition[2]} and verify the result against recorded acceptance criteria.`;
  const action = firstSentence.replace(/^(please\s+)?(own\s+(?:the\s+)?problem\s+and\s+)?/i, '').trim();
  return `${action || firstSentence}. Verify the result and preserve existing functionality.`.slice(0, 8_000);
}

function titleFromObjective(objective) {
  const text = safeString(objective, 300).replace(/[.!?]+$/, '');
  return text || 'Untitled operation';
}

function acceptanceCriteriaFromRequest(request, objective) {
  const text = safeString(request, 12_000).toLowerCase();
  const criteria = [safeString(objective, 2_000)];
  if (/\bmobile|responsive|phone|small screen\b/.test(text)) {
    criteria.push('The primary workflow is usable at 360px and 390px viewport widths without horizontal page overflow.');
    criteria.push('Primary controls remain visible, readable, and usable with touch input.');
    criteria.push('Existing desktop behavior remains intact outside the responsive changes.');
  } else {
    criteria.push('Existing behavior and data remain intact outside the requested scope.');
  }
  criteria.push('Implementation evidence and required verification results are attached to the durable operation.');
  return criteria.filter(Boolean);
}

export function createOperationsEngine({
  dataDir,
  getLegacyProjects = async () => [],
  getDesktopContext = async () => ({}),
  queueDesktopAction = null,
  githubReadAdapter = null,
  directCodexAdapter = null,
} = {}) {
  const store = new OperationStore({ dataDir });
  const registry = new ProjectRegistry({ dataDir });
  const resolver = new ProjectResolver({ registry });
  const policy = new ApprovalPolicy();
  const approvalService = new ApprovalService({ store });
  const verification = new OperationVerification({ queueDesktopAction });
  const codex = new CodexProvider({ mode: directCodexAdapter ? 'direct' : 'external_handoff', directAdapter: directCodexAdapter });
  const desktop = new DesktopProvider({ queueAction: queueDesktopAction });
  const githubRead = new GitHubReadProvider({ readAdapter: githubReadAdapter });
  const service = new OperationService({ store, registry, resolver, policy, approvalService, verification });
  const runner = new OperationRunner({
    store, registry, service, policy, approvalService, verification,
    providers: { codex, desktop, github_read: githubRead },
  });
  service.setRunner(runner);
  const recovery = new OperationRecovery({ store });

  const legacyProjectsFor = async (businessKey) => {
    const value = await getLegacyProjects(safeBusinessKey(businessKey));
    return Array.isArray(value) ? value : [];
  };

  const api = {
    store,
    registry,
    resolver,
    policy,
    service,
    runner,
    recovery,
    providers: { codex, desktop, githubRead },

    async initializeBusinesses(businessKeys = ['personal']) {
      const output = [];
      const discovered = await store.discoverBusinessKeys();
      const keys = [...new Set([...businessKeys, ...discovered].map((key) => safeBusinessKey(key)))];
      for (const rawKey of keys) {
        const businessKey = safeBusinessKey(rawKey);
        const legacyProjects = await legacyProjectsFor(businessKey);
        const migration = await registry.synchronizeLegacyProjects(businessKey, legacyProjects);
        const recovered = await recovery.recoverBusiness(businessKey);
        output.push({ businessKey, migration, recovered });
      }
      return output;
    },

    async resolveProject(businessKey, request, context = {}) {
      const key = safeBusinessKey(businessKey);
      const legacyProjects = await legacyProjectsFor(key);
      await registry.synchronizeLegacyProjects(key, legacyProjects);
      const recentOperations = await store.list(key, { limit: 25 });
      const desktop = await getDesktopContext().catch(() => ({}));
      return resolver.resolve({ businessKey: key, request, legacyProjects, context: { ...safeObject(context), recentOperations, desktop } });
    },

    async createFromRequest(businessKey, input = {}) {
      const key = safeBusinessKey(businessKey);
      const raw = safeObject(input);
      const originalRequest = safeString(raw.originalRequest || raw.request, 12_000);
      const objective = safeString(raw.objective, 8_000) || objectiveFromRequest(originalRequest);
      const resolution = await api.resolveProject(key, originalRequest || objective, {
        projectId: raw.projectId,
        registryId: raw.projectRegistryId,
        currentProjectId: raw.currentProjectId,
      });
      const record = resolution.registryRecord;
      let operation = await service.createOperation(key, {
        id: safeString(raw.id, 160) || makeOperationId(),
        businessKey: key,
        projectId: record?.projectId || safeString(raw.projectId, 160),
        projectName: record?.canonicalName || safeString(raw.projectName, 300),
        projectRegistryId: record?.id || safeString(raw.projectRegistryId, 160),
        title: safeString(raw.title, 300) || titleFromObjective(objective),
        objective,
        originalRequest,
        requestedBy: safeString(raw.requestedBy, 200) || 'mark',
        source: safeString(raw.source, 100) || 'api',
        riskLevel: safeString(raw.riskLevel, 100) || 'low',
        autonomyMode: safeString(raw.autonomyMode, 100) || 'supervised',
        acceptanceCriteria: Array.isArray(raw.acceptanceCriteria) && raw.acceptanceCriteria.length
          ? raw.acceptanceCriteria
          : acceptanceCriteriaFromRequest(originalRequest, objective),
        metadata: {
          ...safeObject(raw.metadata),
          projectResolution: {
            resolved: resolution.resolved,
            confidence: resolution.confidence,
            score: resolution.score,
            reason: resolution.reason,
            alternatives: resolution.alternatives.map((alternative) => ({ id: alternative.registryRecord?.id, name: alternative.registryRecord?.canonicalName, score: alternative.score })),
          },
          relevantMemory: Array.isArray(raw.relevantMemory) ? raw.relevantMemory.slice(0, 30) : [],
          currentArchitecture: safeString(raw.currentArchitecture, 12_000),
          projectSnapshot: record ? {
            id: record.id,
            canonicalName: record.canonicalName,
            owner: record.owner,
            teamMembers: record.teamMembers,
            repo: record.repo,
            localWorkspace: record.localWorkspace,
            deployments: record.deployments,
            services: record.services,
            documentation: record.documentation,
            stack: record.stack,
            commands: record.commands,
          } : {},
        },
      });
      if (resolution.confidence === 'low') {
        operation = await store.update(key, operation.id, (draft) => {
          draft.blockers.push({ id: makeOperationId('blocker'), operationId: draft.id, type: 'project_unresolved', status: 'active', message: resolution.reason, createdAt: nowIso() });
          draft.activityLog.push({ id: makeOperationId('evt'), operationId: draft.id, type: 'project_resolution_required', actor: 'resolver', message: 'Project confidence is too low for execution.', data: { reason: resolution.reason }, timestamp: nowIso() });
          return draft;
        });
        return { operation, resolution };
      }
      if (resolution.confidence === 'medium') return { operation, resolution };
      if (raw.autoPlan !== false) operation = await service.planOperation(key, operation.id, raw.plan || {});
      if (raw.autoStart === true && operation.status === 'planned') operation = await service.startOperation(key, operation.id, { actor: raw.requestedBy || 'mark', runCycle: true });
      return { operation, resolution };
    },

    async listOperations(businessKey, filters = {}) {
      const operations = await store.list(businessKey, filters);
      return operations.map((operation) => ({ ...operation, progress: summarizeOperationProgress(operation) }));
    },

    async getOperation(businessKey, operationId) {
      const operation = await store.get(businessKey, operationId);
      return operation ? { ...operation, progress: summarizeOperationProgress(operation) } : null;
    },

    createOperation: (businessKey, input) => service.createOperation(businessKey, input),
    updateOperation: (businessKey, id, patch, options) => service.updateOperation(businessKey, id, patch, options),
    planOperation: (businessKey, id, input) => service.planOperation(businessKey, id, input),
    startOperation: (businessKey, id, input) => service.startOperation(businessKey, id, input),
    pauseOperation: (businessKey, id, input) => service.pauseOperation(businessKey, id, input),
    resumeOperation: (businessKey, id, input) => service.resumeOperation(businessKey, id, input),
    cancelOperation: (businessKey, id, input) => service.cancelOperation(businessKey, id, input),
    retryOperation: (businessKey, id, input) => service.retryOperation(businessKey, id, input),
    completeOperation: (businessKey, id, input) => service.completeOperation(businessKey, id, input),
    failOperation: (businessKey, id, input) => service.failOperation(businessKey, id, input),
    appendOperationEvent: (businessKey, id, event) => service.appendOperationEvent(businessKey, id, event),
    approveOperationStep: (businessKey, id, approvalId, input) => service.approveOperationStep(businessKey, id, approvalId, input),
    rejectOperationStep: (businessKey, id, approvalId, input) => service.rejectOperationStep(businessKey, id, approvalId, input),
    registerExternalCodexJob: (businessKey, id, input) => service.registerExternalCodexJob(businessKey, id, input),
    registerVerificationResults: (businessKey, id, results, input) => service.registerVerificationResults(businessKey, id, results, input),
    waiveVerification: (businessKey, id, verificationId, input) => service.waiveVerification(businessKey, id, verificationId, input),
    tick: (businessKey, id) => runner.tick(businessKey, id),

    async listProjectRegistry(businessKey) {
      const key = safeBusinessKey(businessKey);
      const legacyProjects = await legacyProjectsFor(key);
      await registry.synchronizeLegacyProjects(key, legacyProjects);
      return registry.list(key);
    },
    createProjectRegistryRecord: (businessKey, input) => registry.create(businessKey, input),
    updateProjectRegistryRecord: (businessKey, id, patch) => registry.update(businessKey, id, patch),
  };

  return api;
}
