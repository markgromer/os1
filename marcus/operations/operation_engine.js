import crypto from 'node:crypto';

import { ApprovalPolicy } from '../approvals/approval_policy.js';
import { ApprovalService } from '../approvals/approval_service.js';
import { CodexProvider } from '../providers/codex_provider.js';
import { CloudflareWriteProvider, CLOUDFLARE_WRITE_ACTIONS, workerMatchesRegisteredDeployment } from '../providers/cloudflare_provider.js';
import { DesktopProvider } from '../providers/desktop_provider.js';
import { GitHubReadProvider, GitHubWriteProvider, GITHUB_WRITE_ACTIONS } from '../providers/github_provider.js';
import { ProjectRegistry } from '../projects/project_registry.js';
import { ProjectResolver } from '../projects/project_resolver.js';
import { OperationRecovery } from './operation_recovery.js';
import { OperationReconciliation } from './operation_reconciliation.js';
import { OperationRunner } from './operation_runner.js';
import { OperationService } from './operation_service.js';
import { OperationStore } from './operation_store.js';
import { OperationVerification } from './operation_verification.js';
import { CodexResultReviewer } from './codex_result_reviewer.js';
import { makeOperationId, nowIso, safeBusinessKey, safeObject, safeString, sanitizeStructured, summarizeOperationProgress } from './operation_types.js';

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

const NONTERMINAL_STATUSES = new Set(['draft', 'planned', 'waiting_for_approval', 'queued', 'running', 'paused', 'blocked', 'verifying', 'awaiting_provider', 'recovery_required']);

function duplicateText(value) {
  const stop = new Set(['the', 'a', 'an', 'and', 'or', 'to', 'on', 'it', 'is', 'still', 'please', 'problem', 'own', 'working', 'work', 'get', 'with', 'for', 'of']);
  return safeString(value, 20_000)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .split(/\s+/)
    .filter((token) => token.length >= 3 && !stop.has(token))
    .sort()
    .join(' ');
}

function duplicateSimilarity(left, right) {
  const a = new Set(duplicateText(left).split(' ').filter(Boolean));
  const b = new Set(duplicateText(right).split(' ').filter(Boolean));
  if (!a.size || !b.size) return 0;
  let shared = 0;
  for (const token of a) if (b.has(token)) shared += 1;
  return shared / Math.max(a.size, b.size);
}

function findDuplicateOperation({ operations, projectRegistryId, objective, originalRequest }) {
  const now = Date.now();
  const targetText = `${objective}\n${originalRequest}`;
  return (Array.isArray(operations) ? operations : []).find((operation) => {
    if (!NONTERMINAL_STATUSES.has(operation.status)) return false;
    if (projectRegistryId && operation.projectRegistryId !== projectRegistryId) return false;
    if (Number.isFinite(Date.parse(operation.createdAt)) && now - Date.parse(operation.createdAt) > 7 * 24 * 60 * 60_000) return false;
    const candidateText = `${operation.objective}\n${operation.originalRequest}`;
    return duplicateSimilarity(targetText, candidateText) >= 0.45;
  }) || null;
}

function deriveTrustedAuthorization({ request, businessKey, projectRegistryId }) {
  const text = safeString(request, 12_000).toLowerCase();
  const actionClasses = [];
  const readOnlyOpening = /^\s*(?:what|who|when|where|why|how|show|tell|explain|summarize|list|status\b|is\b|are\b|can\s+you\s+(?:tell|show|explain|summarize|list|check\s+(?:the\s+)?status)\b)/.test(text);
  const implementationVerb = '(?:implement|build|fix|change|update|redesign|repair|create|install|replace|migrate|upgrade|swap|wire|integrate)';
  const codexDirective = '(?:use\\s+codex(?:\\s+to)?|start\\s+codex|launch\\s+codex|run\\s+codex|get\\s+codex\\s+(?:fixing|working|implementing)|get\\s+(?:it|this)\\s+going\\s+in\\s+codex|codex\\s+(?:fix|implement|build|change|update|repair|install|replace))';
  const implementationAction = `(?:${implementationVerb}|${codexDirective})`;
  const explicitMutationDirective = new RegExp(`(?:^|[.!?]\\s+)(?:please\\s+)?${implementationAction}\\b`).test(text)
    || new RegExp(`\\b(?:please|i\\s+(?:want|need)\\s+you\\s+to|go\\s+ahead(?:\\s+and|\\s+to)?|proceed(?:\\s+and|\\s+to)?|own\\s+(?:the\\s+)?problem\\s+and)\\b[^.!?]{0,100}\\b(?:${implementationAction}|test|lint|typecheck|verify)\\b`).test(text);
  const permitsMutation = !readOnlyOpening || explicitMutationDirective;
  const codexDenied = /\b(?:do\s+not|don't|dont|never|no)\s+(?:\w+\s+){0,4}(?:use\s+codex|codex|implement|build|fix|change|update|redesign|repair|create|install|replace|migrate|upgrade|swap|wire|integrate)\b/.test(text);
  const scriptDenied = /\b(?:do\s+not|don't|dont|never|no)\s+(?:\w+\s+){0,4}(?:test|build|lint|typecheck|verify|verification)\b/.test(text);
  if (permitsMutation && !codexDenied && new RegExp(`\\b${implementationAction}\\b`).test(text)) actionClasses.push('codex_implementation');
  if (permitsMutation && !scriptDenied && /\b(test|build|lint|typecheck|verify|verification)\b/.test(text)) actionClasses.push('run-project-script');
  const createsNewProject = permitsMutation && /\b(?:create|start|make|build)\b[^.!?]{0,100}\b(?:new project|project from scratch|empty project|new app|new application)\b/.test(text);
  if (createsNewProject) actionClasses.push('create-project-workspace', 'connect-github-repository');
  if (/\b(read|inspect|compare|github|repository|pull request|workflow)\b/.test(text)) {
    actionClasses.push('repository_metadata', 'default_branch', 'branch_metadata', 'commit_metadata', 'repository_file', 'compare_refs', 'pull_request_metadata', 'workflow_status');
  }
  return {
    source: 'authenticated_request', businessKey, projectRegistryId,
    environment: 'development', providers: [
      ...(actionClasses.includes('codex_implementation') ? ['codex'] : []),
      ...(actionClasses.includes('run-project-script') ? ['desktop'] : []),
      ...(actionClasses.some((item) => ['create-project-workspace', 'connect-github-repository'].includes(item)) ? ['desktop'] : []),
      ...(actionClasses.some((item) => item.includes('repository') || item.includes('branch') || item.includes('commit') || item.includes('workflow') || item.includes('pull_request') || item === 'compare_refs' || item === 'default_branch') ? ['github_read'] : []),
    ],
    actionClasses: [...new Set(actionClasses)], requestDigest: crypto.createHash('sha256').update(text).digest('hex'), createdAt: nowIso(), revoked: false,
  };
}

function providerActionApprovalTarget(provider, action, input, repository = '') {
  const raw = safeObject(input);
  if (provider === 'github_write' && action === 'merge_pull_request') {
    return `${repository}#${Number(raw.pullNumber) || 0}@${safeString(raw.expectedHeadSha, 40)}`;
  }
  if (provider === 'cloudflare_write' && action === 'deploy_worker_version') {
    return `${safeString(raw.scriptName, 200)}@${safeString(raw.versionId, 64)}`;
  }
  if (provider === 'cloudflare_write') {
    return `${safeString(raw.recordType || raw.type, 16).toUpperCase()} ${safeString(raw.name, 255)} -> ${safeString(raw.content, 160)}`;
  }
  return safeString(action, 200);
}

function providerActionTitle(provider, action, approvalTarget) {
  if (provider === 'github_write') return `Merge ${approvalTarget}`;
  if (action === 'deploy_worker_version') return `Deploy Cloudflare Worker ${approvalTarget}`;
  if (action === 'delete_dns_record') return `Delete Cloudflare DNS ${approvalTarget}`;
  return `Change Cloudflare DNS ${approvalTarget}`;
}

export function createOperationsEngine({
  dataDir,
  getLegacyProjects = async () => [],
  getDesktopContext = async () => ({}),
  queueDesktopAction = null,
  githubReadAdapter = null,
  githubWriteAdapter = null,
  cloudflareWriteAdapter = null,
  directCodexAdapter = null,
  reviewCodexResult = null,
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
  const githubWrite = new GitHubWriteProvider({ writeAdapter: githubWriteAdapter });
  const cloudflareWrite = new CloudflareWriteProvider({ writeAdapter: cloudflareWriteAdapter });
  const codexResultReviewer = typeof reviewCodexResult === 'function' ? new CodexResultReviewer({ complete: reviewCodexResult }) : null;
  const service = new OperationService({ store, registry, resolver, policy, approvalService, verification });
  const runner = new OperationRunner({
    store, registry, service, policy, approvalService, verification,
    providers: { codex, desktop, github_read: githubRead, github_write: githubWrite, cloudflare_write: cloudflareWrite },
    codexResultReviewer,
    providerTimeoutMs,
  });
  service.setRunner(runner);
  const reconciliation = new OperationReconciliation({ store, runner });
  const recovery = new OperationRecovery({ store, registry, queueDesktopAction });

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
    providers: { codex, desktop, githubRead, githubWrite, cloudflareWrite },

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
      if (record?.id && raw.allowDuplicate !== true) {
        const duplicate = findDuplicateOperation({
          operations: await store.listAll(key, { nonterminal: true }),
          projectRegistryId: record.id,
          objective,
          originalRequest,
        });
        if (duplicate) {
          const operation = await store.update(key, duplicate.id, (draft) => {
            if (!draft.activityLog.some((event) => event.type === 'duplicate_operation_reused' && event.data?.request === originalRequest)) {
              draft.activityLog.push({
                id: makeOperationId('evt'), operationId: draft.id, type: 'duplicate_operation_reused', actor: safeString(raw.requestedBy, 100) || 'mark',
                message: 'A likely duplicate durable operation request reused this active operation.',
                data: { request: originalRequest, similarity: duplicateSimilarity(`${objective}\n${originalRequest}`, `${draft.objective}\n${draft.originalRequest}`) },
                timestamp: nowIso(),
              });
            }
            return draft;
          });
          return { operation, resolution, reused: true };
        }
      }
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
          currentArchitecture: safeString(raw.currentArchitecture, 30_000),
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

    async createProviderActionFromRequest(businessKey, input = {}) {
      const key = safeBusinessKey(businessKey);
      const raw = safeObject(input);
      const provider = safeString(raw.provider, 100).toLowerCase();
      const action = safeString(raw.action, 100).toLowerCase();
      const allowed = provider === 'github_write' ? GITHUB_WRITE_ACTIONS
        : provider === 'cloudflare_write' ? CLOUDFLARE_WRITE_ACTIONS : null;
      if (!allowed?.has(action)) throw Object.assign(new Error('The requested provider action is not allowlisted.'), { code: 'PROVIDER_ACTION_NOT_ALLOWED' });
      const originalRequest = safeString(raw.originalRequest || raw.request, 12_000);
      if (!originalRequest) throw Object.assign(new Error('The authenticated user request is required.'), { code: 'AUTHENTICATED_REQUEST_REQUIRED' });
      const requestedInput = sanitizeStructured(raw.input ?? {}, 20_000);
      const repository = safeString(raw.repository || requestedInput.repository, 500).replace(/\.git$/i, '');
      const projectQuery = [safeString(raw.projectName, 300), repository, originalRequest].filter(Boolean).join(' ');
      const resolution = await api.resolveProject(key, projectQuery, {
        projectId: raw.projectId,
        registryId: raw.projectRegistryId,
        currentProjectId: raw.currentProjectId,
      });
      const record = resolution.registryRecord;
      if (!record || resolution.confidence !== 'high') {
        throw Object.assign(new Error(`An exact registered project is required before preparing ${action}. ${resolution.reason || ''}`.trim()), { code: 'PROJECT_CONFIRMATION_REQUIRED' });
      }
      if (provider === 'github_write') {
        if (!repository || repository.toLowerCase() !== safeString(record.repo?.fullName, 500).toLowerCase()) {
          throw Object.assign(new Error('The explicit GitHub repository does not match the resolved project registry target.'), { code: 'PROVIDER_TARGET_MISMATCH' });
        }
      }
      if (provider === 'cloudflare_write') {
        const deployments = safeObject(record.deployments);
        if (action === 'deploy_worker_version') {
          const accountId = safeString(requestedInput.accountId || deployments.cloudflareAccountId, 64);
          const scriptName = safeString(requestedInput.scriptName, 200).toLowerCase();
          if (deployments.cloudflareAccountId && deployments.cloudflareAccountId !== accountId) {
            throw Object.assign(new Error('The Cloudflare account does not match the resolved project registry target.'), { code: 'PROVIDER_TARGET_MISMATCH' });
          }
          if (!workerMatchesRegisteredDeployment(scriptName, deployments)) {
            throw Object.assign(new Error('The Worker script is not bound to the resolved project registry target.'), { code: 'PROVIDER_TARGET_MISMATCH' });
          }
        } else {
          const zoneId = safeString(requestedInput.zoneId || deployments.cloudflareZoneId, 64);
          if (deployments.cloudflareZoneId && deployments.cloudflareZoneId !== zoneId) {
            throw Object.assign(new Error('The Cloudflare zone does not match the resolved project registry target.'), { code: 'PROVIDER_TARGET_MISMATCH' });
          }
        }
      }
      const approvalTarget = providerActionApprovalTarget(provider, action, requestedInput, repository);
      const fingerprint = crypto.createHash('sha256').update(JSON.stringify({
        businessKey: key, projectRegistryId: record.id, provider, action, approvalTarget, input: requestedInput,
      })).digest('hex');
      const duplicate = (await store.listAll(key, { nonterminal: true })).find((candidate) =>
        candidate.metadata?.extra?.providerActionFingerprint === fingerprint);
      if (duplicate) return { operation: duplicate, resolution, reused: true };

      const authorizationProvenance = {
        source: 'authenticated_request', businessKey: key, projectRegistryId: record.id,
        environment: 'production', providers: [provider], actionClasses: [action],
        requestDigest: crypto.createHash('sha256').update(originalRequest).digest('hex'), createdAt: nowIso(), revoked: false,
      };
      const title = providerActionTitle(provider, action, approvalTarget);
      let operation = await service.createOperation(key, {
        projectId: record.projectId,
        projectName: record.canonicalName,
        projectRegistryId: record.id,
        title,
        objective: `${title} only after explicit approval, then verify the exact provider state.`,
        originalRequest,
        requestedBy: safeString(raw.requestedBy, 200) || 'mark',
        source: safeString(raw.source, 100) || 'marcus_chat',
        acceptanceCriteria: [
          `${title} is applied to the exact registered provider target.`,
          'The provider response and an authoritative read-back agree on the resulting state.',
          'No adjacent repository, zone, DNS record, Worker, or deployment is changed.',
        ],
        metadata: {
          projectResolution: { resolved: true, confidence: 'high', score: resolution.score, reason: resolution.reason, alternatives: [], confirmed: true },
          providerActionFingerprint: fingerprint,
        },
      }, { authorizationProvenance });
      operation = await service.planOperation(key, operation.id, {
        steps: [
          {
            id: 'context', title: 'Bind the immutable provider target', type: 'internal', provider: 'internal',
            toolName: 'prepare_operation_context', input: {},
          },
          {
            id: 'provider-action', title, description: operation.objective, type: provider, provider,
            toolName: action, dependsOn: ['context'], maxAttempts: 2,
            input: { ...requestedInput, environment: 'production', approvalTarget },
          },
        ],
      });
      operation = await service.startOperation(key, operation.id, { actor: safeString(raw.requestedBy, 100) || 'mark', runCycle: true });
      return { operation, resolution, reused: false };
    },

    async listOperations(businessKey, filters = {}) {
      const operations = await store.list(businessKey, filters);
      return operations.map((operation) => ({ ...operation, progress: summarizeOperationProgress(operation) }));
    },

    async listOperationSummaries(businessKey, filters = {}) {
      const operations = await store.list(businessKey, filters);
      return operations.map((operation) => {
        const currentStep = operation.steps.find((step) => step.id === operation.currentStepId)
          || operation.steps.find((step) => ['running', 'waiting_for_approval', 'blocked', 'ready', 'pending'].includes(step.status))
          || null;
        const pendingApproval = operation.approvals.find((approval) => approval.status === 'pending') || null;
        const required = operation.verification.filter((item) => item.required !== false);
        const passed = required.filter((item) => item.status === 'passed' || (item.waived === true && item.waiverApprovalId)).length;
        return {
          id: operation.id, title: operation.title, projectName: operation.projectName, status: operation.status,
          riskLevel: operation.riskLevel, updatedAt: operation.updatedAt, progress: summarizeOperationProgress(operation),
          needsApproval: operation.approvals.some((approval) => approval.status === 'pending'),
          pendingApproval: pendingApproval ? {
            id: safeString(pendingApproval.id, 160),
            action: safeString(pendingApproval.action, 200),
            riskLevel: safeString(pendingApproval.riskLevel, 40),
            reason: safeString(pendingApproval.reason, 1_000),
            expiresAt: safeString(pendingApproval.expiresAt, 64),
          } : null,
          needsRecovery: operation.status === 'recovery_required',
          activeBlockers: operation.blockers.filter((blocker) => blocker.status === 'active').length,
          currentStep: currentStep ? { title: currentStep.title, type: currentStep.type, status: currentStep.status } : null,
          verificationSummary: {
            required: required.length,
            passed,
            failed: required.filter((item) => item.status === 'failed').length,
            needsManualReview: required.filter((item) => item.status === 'needs_manual_review').length,
            pending: required.filter((item) => ['pending', 'running', 'skipped'].includes(item.status)).length,
          },
        };
      });
    },

    async readiness(businessKey) {
      const key = safeBusinessKey(businessKey);
      const operations = await store.listAll(key);
      const nonterminal = operations.filter((operation) => NONTERMINAL_STATUSES.has(operation.status));
      return {
        operationStoreAvailable: true,
        projectRegistryAvailable: true,
        operationEngineInitialized: true,
        runnerInitialized: Boolean(runner),
        desktopQueueInitialized: typeof queueDesktopAction === 'function',
        recoveryCompleted: true,
        codex: {
          mode: codex.mode,
          directAdapterConfigured: codex.mode === 'direct',
          provider: codex.providerName || codex.mode,
          authoritativeResultEvidence: typeof directCodexAdapter?.collectTargetEvidence === 'function',
          independentResultReviewerConfigured: Boolean(codexResultReviewer),
        },
        recoveryRequiredCount: operations.filter((operation) => operation.status === 'recovery_required').length,
        pendingApprovalCount: operations.reduce((count, operation) => count + operation.approvals.filter((approval) => approval.status === 'pending').length, 0),
        pendingExternalCodexCount: nonterminal.filter((operation) => operation.blockers.some((blocker) => blocker.status === 'active' && blocker.type === 'external_codex_required')).length,
      };
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
    setCodexLifecycleRecorder: (recorder) => codex.setLifecycleRecorder(recorder),
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
            idempotencyKey: challenge.id,
          },
          requestedBy: `workspace-approval:${project.id}`,
        });
        if (action?.id !== challenge.id) throw Object.assign(new Error('Desktop workspace validation queue returned a mismatched challenge id.'), { code: 'WORKSPACE_CHALLENGE_QUEUE_MISMATCH' });
      }
      return project;
    },
    attestProjectWorkspace: (businessKey, id, input) => registry.attestWorkspace(businessKey, id, input),
    prepareNewProjectWorkspace: (businessKey, id, input = {}) => registry.approveWorkspace(businessKey, id, {
      desktopAgentId: safeString(input.desktopAgentId, 200),
      remoteValidation: true,
      actor: 'mark_full_pc_authorization',
      message: 'Mark authorized Marcus to create this exact new project path on the bound desktop agent.',
    }),
    async approveCreatedProjectWorkspace(businessKey, id, input = {}) {
      const raw = safeObject(input);
      const project = await registry.approveWorkspace(businessKey, id, {
        desktopAgentId: raw.desktopAgentId,
        remoteValidation: true,
        actor: 'mark_full_pc_authorization',
        message: 'Workspace created by the exact durable desktop action authorized for this new project.',
      });
      const challenge = safeObject(project.localWorkspace?.approvalChallenge);
      return registry.attestWorkspace(businessKey, id, {
        ok: true,
        challengeId: challenge.id,
        businessKey,
        projectRegistryId: id,
        desktopAgentId: raw.desktopAgentId,
        registeredPath: raw.registeredPath,
        canonicalPath: raw.canonicalPath,
      });
    },
  };

  return api;
}
