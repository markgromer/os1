import crypto from 'node:crypto';

import { ApprovalPolicy } from '../approvals/approval_policy.js';
import { ApprovalService } from '../approvals/approval_service.js';
import { CodexProvider } from '../providers/codex_provider.js';
import { DesktopProvider } from '../providers/desktop_provider.js';
import { GitHubReadProvider } from '../providers/github_provider.js';
import { ProjectRegistry } from '../projects/project_registry.js';
import { ProjectResolver } from '../projects/project_resolver.js';
import { OperationRecovery } from './operation_recovery.js';
import { OperationReconciliation } from './operation_reconciliation.js';
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

function deriveTrustedAuthorization({ request, businessKey, projectRegistryId }) {
  const text = safeString(request, 12_000).toLowerCase();
  const actionClasses = [];
  const readOnlyOpening = /^\s*(?:what|who|when|where|why|how|show|tell|explain|summarize|list|status\b|is\b|are\b|can\s+you\s+(?:tell|show|explain|summarize|list|check\s+(?:the\s+)?status)\b)/.test(text);
  const explicitMutationDirective = /(?:^|[.!?]\s+)(?:please\s+)?(?:use\s+codex\s+to\s+)?(?:implement|build|fix|change|update|redesign|repair|create|test|lint|typecheck|verify)\b/.test(text)
    || /\b(?:please|i\s+(?:want|need)\s+you\s+to|go\s+ahead(?:\s+and|\s+to)?|proceed(?:\s+and|\s+to)?|own\s+(?:the\s+)?problem\s+and)\b[^.!?]{0,100}\b(?:use\s+codex|implement|build|fix|change|update|redesign|repair|create|test|lint|typecheck|verify|get\s+codex\s+working)\b/.test(text);
  const permitsMutation = !readOnlyOpening || explicitMutationDirective;
  const codexDenied = /\b(?:do\s+not|don't|dont|never|no)\s+(?:\w+\s+){0,4}(?:use\s+codex|codex|implement|build|fix|change|update|redesign|repair|create)\b/.test(text);
  const scriptDenied = /\b(?:do\s+not|don't|dont|never|no)\s+(?:\w+\s+){0,4}(?:test|build|lint|typecheck|verify|verification)\b/.test(text);
  if (permitsMutation && !codexDenied && /\b(codex|implement|build|fix|change|update|redesign|repair|create)\b/.test(text)) actionClasses.push('codex_implementation');
  if (permitsMutation && !scriptDenied && /\b(test|build|lint|typecheck|verify|verification)\b/.test(text)) actionClasses.push('run-project-script');
  if (/\b(read|inspect|compare|github|repository|pull request|workflow)\b/.test(text)) {
    actionClasses.push('repository_metadata', 'default_branch', 'branch_metadata', 'commit_metadata', 'repository_file', 'compare_refs', 'pull_request_metadata', 'workflow_status');
  }
  return {
    source: 'authenticated_request', businessKey, projectRegistryId,
    environment: 'development', providers: [
      ...(actionClasses.includes('codex_implementation') ? ['codex'] : []),
      ...(actionClasses.includes('run-project-script') ? ['desktop'] : []),
      ...(actionClasses.some((item) => item.includes('repository') || item.includes('branch') || item.includes('commit') || item.includes('workflow') || item.includes('pull_request') || item === 'compare_refs' || item === 'default_branch') ? ['github_read'] : []),
    ],
    actionClasses: [...new Set(actionClasses)], requestDigest: crypto.createHash('sha256').update(text).digest('hex'), createdAt: nowIso(), revoked: false,
  };
}

export function createOperationsEngine({
  dataDir,
  getLegacyProjects = async () => [],
  getDesktopContext = async () => ({}),
  queueDesktopAction = null,
  githubReadAdapter = null,
  directCodexAdapter = null,
  providerTimeoutMs = 45_000,
  allowedWorkspaceRoots = [],
} = {}) {
  const store = new OperationStore({ dataDir });
  const registry = new ProjectRegistry({ dataDir, allowedWorkspaceRoots });
  const resolver = new ProjectResolver({ registry });
  const policy = new ApprovalPolicy();
  const approvalService = new ApprovalService({ store });
  const verification = new OperationVerification({ queueDesktopAction, store });
  const codex = new CodexProvider({ mode: directCodexAdapter ? 'direct' : 'external_handoff', directAdapter: directCodexAdapter });
  const desktop = new DesktopProvider({ queueAction: queueDesktopAction });
  const githubRead = new GitHubReadProvider({ readAdapter: githubReadAdapter });
  const service = new OperationService({ store, registry, resolver, policy, approvalService, verification });
  const runner = new OperationRunner({
    store, registry, service, policy, approvalService, verification,
    providers: { codex, desktop, github_read: githubRead },
    providerTimeoutMs,
  });
  service.setRunner(runner);
  const reconciliation = new OperationReconciliation({ store, runner });
  const recovery = new OperationRecovery({ store });

  const legacyProjectsFor = async (businessKey) => {
    const value = await getLegacyProjects(safeBusinessKey(businessKey));
    return Array.isArray(value) ? value : [];
  };

  const assertRegistryTargetMutable = async (businessKey, registryId, fields) => {
    const materialFields = new Set(['repo', 'localWorkspace', 'deployments', 'commands', 'projectId']);
    if (!fields.some((field) => materialFields.has(field))) return;
    const operations = await store.listAll(businessKey, { nonterminal: true });
    const bound = operations.find((operation) => operation.projectRegistryId === registryId && operation.status !== 'draft');
    if (!bound) return;
    await service.appendOperationEvent(businessKey, bound.id, {
      type: 'registry_target_change_rejected', actor: 'system',
      message: `Project registry target update rejected while operation ${bound.id} is ${bound.status}.`,
      data: { projectRegistryId: registryId, fields },
    });
    throw Object.assign(new Error('The project execution target is bound to a planned or active operation. Use an explicit operation recovery/re-plan workflow first.'), { code: 'REGISTRY_TARGET_IN_USE' });
  };

  const api = {
    store,
    registry,
    resolver,
    policy,
    service,
    runner,
    recovery,
    reconciliation,
    providers: { codex, desktop, githubRead },

    async initializeBusinesses(businessKeys = ['personal']) {
      const output = [];
      const discovered = await store.discoverBusinessKeys();
      const registryBusinesses = await registry.discoverBusinessKeys();
      const keys = [...new Set([...businessKeys, ...discovered, ...registryBusinesses].map((key) => safeBusinessKey(key)))];
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
      const authorizationProvenance = deriveTrustedAuthorization({ request: originalRequest, businessKey: key, projectRegistryId: record?.id || '' });
      let operation = await service.createOperation(key, {
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
        autonomyMode: 'supervised',
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
            confirmed: resolution.confidence === 'high',
          },
          authorizationProvenance,
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
      }, { authorizationProvenance });
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

    async listOperationSummaries(businessKey, filters = {}) {
      const operations = await store.list(businessKey, filters);
      return operations.map((operation) => ({
        id: operation.id, title: operation.title, projectName: operation.projectName, status: operation.status,
        riskLevel: operation.riskLevel, updatedAt: operation.updatedAt, progress: summarizeOperationProgress(operation),
        needsApproval: operation.approvals.some((approval) => approval.status === 'pending'),
        needsRecovery: operation.status === 'recovery_required',
      }));
    },

    async getOperation(businessKey, operationId) {
      const operation = await store.get(businessKey, operationId);
      return operation ? { ...operation, progress: summarizeOperationProgress(operation) } : null;
    },

    createOperation: (businessKey, input) => service.createOperation(businessKey, input),
    updateOperation: (businessKey, id, patch, options) => service.updateOperation(businessKey, id, patch, options),
    planOperation: (businessKey, id, input) => service.planOperation(businessKey, id, input),
    replanOperation: (businessKey, id, input) => service.replanOperation(businessKey, id, input),
    startOperation: (businessKey, id, input) => service.startOperation(businessKey, id, input),
    confirmProject: (businessKey, id, input) => service.confirmProject(businessKey, id, input),
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
    registerManualVerificationEvidence: (businessKey, id, results, input) => service.registerManualVerificationEvidence(businessKey, id, results, input),
    waiveVerification: (businessKey, id, verificationId, input) => service.waiveVerification(businessKey, id, verificationId, input),
    tick: (businessKey, id) => runner.tick(businessKey, id),
    reconcileDesktopResult: (input, options) => reconciliation.reconcileDesktopResult(input, options),

    async listProjectRegistry(businessKey) {
      const key = safeBusinessKey(businessKey);
      const legacyProjects = await legacyProjectsFor(key);
      await registry.synchronizeLegacyProjects(key, legacyProjects);
      return registry.list(key);
    },
    createProjectRegistryRecord: (businessKey, input) => registry.create(businessKey, input),
    async updateProjectRegistryRecord(businessKey, id, patch) {
      await assertRegistryTargetMutable(businessKey, id, Object.keys(safeObject(patch)));
      return registry.update(businessKey, id, patch);
    },
    async approveProjectWorkspace(businessKey, id, input) {
      await assertRegistryTargetMutable(businessKey, id, ['localWorkspace']);
      const project = await registry.approveWorkspace(businessKey, id, input);
      const challenge = safeObject(project.localWorkspace?.approvalChallenge);
      if (project.localWorkspace?.trustStatus === 'pending' && challenge.status === 'pending' && queueDesktopAction) {
        const action = await queueDesktopAction({
          id: challenge.id,
          idempotencyKey: challenge.id,
          type: 'validate-workspace',
          payload: {
            challengeId: challenge.id,
            path: challenge.registeredPath,
            registeredPath: challenge.registeredPath,
            businessKey: project.businessKey,
            projectRegistryId: project.id,
            desktopAgentId: challenge.desktopAgentId,
          },
          requestedBy: `workspace-approval:${project.id}`,
        });
        if (action?.id !== challenge.id) throw Object.assign(new Error('Desktop workspace validation queue returned a mismatched challenge id.'), { code: 'WORKSPACE_CHALLENGE_QUEUE_MISMATCH' });
      }
      return project;
    },
    attestProjectWorkspace: (businessKey, id, input) => registry.attestWorkspace(businessKey, id, input),
  };

  return api;
}
