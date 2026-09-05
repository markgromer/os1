import {
  assertOperationTransition,
  createActivityEvent,
  createApproval,
  createArtifact,
  createOperationRecord,
  createStep,
  createVerificationResult,
  makeOperationId,
  normalizeApproval,
  normalizeArtifact,
  normalizeStep,
  normalizeVerificationResult,
  nowIso,
  requiredVerificationPassed,
  safeObject,
  safeString,
  sanitizeStructured,
} from './operation_types.js';

function appendEvent(operation, input) {
  operation.activityLog.push(createActivityEvent(input, { operationId: operation.id }));
}

function transition(operation, status) {
  assertOperationTransition(operation.status, status);
  operation.status = status;
  const timestamp = nowIso();
  if (status === 'running' && !operation.startedAt) operation.startedAt = timestamp;
  if (status === 'paused') operation.pausedAt = timestamp;
  if (status === 'completed') operation.completedAt = timestamp;
  if (status === 'failed') operation.failedAt = timestamp;
  if (status === 'cancelled') operation.cancelledAt = timestamp;
  return operation;
}

function defaultAcceptanceCriteria(operation) {
  const objective = safeString(operation.objective, 2_000) || 'The requested outcome is implemented.';
  return [
    objective,
    'Existing project behavior and data remain intact outside the requested scope.',
    'Implementation evidence is attached to this operation.',
    'All configured required verification checks pass or have an explicit recorded waiver.',
  ];
}

function authorizationAfterProjectSelection(existing, projectRegistryId) {
  const authorization = safeObject(existing);
  if (authorization.projectRegistryId === projectRegistryId) return authorization;
  return {
    ...authorization,
    revoked: true,
    revokedAt: nowIso(),
    revocationReason: 'Project selection changed after the authenticated request was recorded; fresh action authorization is required.',
  };
}

function mergeVerificationResults(existing, incoming, operationId, stepId) {
  const map = new Map((Array.isArray(existing) ? existing : []).map((item) => [item.id, item]));
  const suppliedIds = (Array.isArray(incoming) ? incoming : []).map((item) => safeString(item?.id, 120)).filter(Boolean);
  if (new Set(suppliedIds).size !== suppliedIds.length) {
    throw Object.assign(new Error('Duplicate verification result IDs are not allowed.'), { code: 'DUPLICATE_VERIFICATION_ID' });
  }
  for (const raw of Array.isArray(incoming) ? incoming : []) {
    const candidate = createVerificationResult(raw, { operationId, stepId });
    const sameType = [...map.values()].find((item) => item.type === candidate.type && item.stepId === candidate.stepId);
    const normalized = sameType
      ? normalizeVerificationResult({ ...sameType, ...raw, id: sameType.id, operationId, stepId: sameType.stepId || stepId, completedAt: nowIso() }, { operationId, stepId })
      : candidate;
    if (sameType) map.delete(sameType.id);
    map.set(normalized.id, normalized);
  }
  return [...map.values()].slice(-100);
}

export class OperationService {
  constructor({ store, registry, resolver, policy, approvalService, verification, winningMethodStore = null }) {
    this.store = store;
    this.registry = registry;
    this.resolver = resolver;
    this.policy = policy;
    this.approvalService = approvalService;
    this.verification = verification;
    this.winningMethodStore = winningMethodStore;
    this.runner = null;
  }

  setRunner(runner) {
    this.runner = runner;
  }

  async createOperation(businessKey, input = {}, trusted = {}) {
    const raw = safeObject(input);
    const metadata = { ...safeObject(raw.metadata), authorizationProvenance: sanitizeStructured(trusted.authorizationProvenance ?? {}, 12_000) };
    const operation = createOperationRecord({
      ...raw,
      metadata,
      businessKey,
      status: 'draft',
      autonomyMode: 'supervised',
      riskLevel: 'low',
      acceptanceCriteria: raw.acceptanceCriteria,
      steps: [],
      plan: [],
      activityLog: [], providerActions: [], desktopCorrelations: [], approvals: [], artifacts: [], blockers: [], verification: [],
      revision: 1,
    }, { businessKey });
    operation.activityLog.push(createActivityEvent({
      operationId: operation.id,
      type: 'operation_created',
      actor: operation.requestedBy,
      message: 'Durable operation created.',
      data: { source: operation.source, projectRegistryId: operation.projectRegistryId },
    }));
    return this.store.create(businessKey, operation);
  }

  async updateOperation(businessKey, operationId, patch = {}, options = {}) {
    const rawPatch = safeObject(patch);
    const material = ['title', 'objective', 'originalRequest', 'acceptanceCriteria', 'projectRegistryId'].some((key) => Object.prototype.hasOwnProperty.call(rawPatch, key));
    const snapshot = await this.store.get(businessKey, operationId);
    if (!snapshot) throw Object.assign(new Error('Operation not found.'), { code: 'OPERATION_NOT_FOUND' });
    if (material && !Number.isFinite(Number(options.expectedRevision))) {
      throw Object.assign(new Error('Expected operation revision is required for material updates.'), { code: 'EXPECTED_REVISION_REQUIRED', currentRevision: snapshot.revision });
    }
    if (snapshot.status !== 'draft' && material) {
      await this.store.update(businessKey, operationId, (operation) => {
        appendEvent(operation, { type: 'execution_context_change_rejected', actor: safeString(options.actor, 100) || 'mark', message: `Material update rejected while operation was ${operation.status}.`, data: { requestedFields: Object.keys(rawPatch).slice(0, 20) } });
        return operation;
      });
      throw Object.assign(new Error(snapshot.status === 'planned'
        ? 'Planned operations must use the explicit re-plan workflow before material details can change.'
        : `Execution context is immutable after start; operation is ${snapshot.status}.`), { code: 'EXECUTION_CONTEXT_IMMUTABLE', currentRevision: snapshot.revision + 1 });
    }
    const requestedRegistryId = safeString(rawPatch.projectRegistryId, 160);
    let selectedRegistry = null;
    if (requestedRegistryId) {
      selectedRegistry = await this.registry.get(businessKey, requestedRegistryId);
      if (!selectedRegistry) throw Object.assign(new Error('Project registry record not found in the active business.'), { code: 'PROJECT_REGISTRY_NOT_FOUND' });
    }
    const allowed = ['title', 'objective', 'originalRequest', 'acceptanceCriteria', 'requestedBy'];
    return this.store.update(businessKey, operationId, (operation) => {
      const raw = rawPatch;
      for (const key of allowed) if (Object.prototype.hasOwnProperty.call(raw, key)) operation[key] = raw[key];
      if (selectedRegistry) {
        operation.projectRegistryId = selectedRegistry.id;
        operation.projectId = selectedRegistry.projectId;
        operation.projectName = selectedRegistry.canonicalName;
        operation.metadata = {
          ...safeObject(operation.metadata),
          projectResolution: { resolved: true, confidence: 'high', score: 100, reason: 'Explicitly selected by Mark.', alternatives: [] },
          authorizationProvenance: authorizationAfterProjectSelection(operation.metadata?.authorizationProvenance, selectedRegistry.id),
        };
        for (const blocker of operation.blockers) if (blocker.type === 'project_unresolved' && blocker.status === 'active') {
          blocker.status = 'resolved'; blocker.resolvedAt = nowIso(); blocker.resolution = 'Project explicitly selected.';
        }
      }
      const changedFields = allowed.filter((key) => Object.prototype.hasOwnProperty.call(raw, key));
      if (selectedRegistry) changedFields.push('projectRegistryId', 'projectId', 'projectName');
      appendEvent(operation, { type: 'operation_updated', actor: safeString(options.actor, 100) || 'mark', message: 'Operation details updated.', data: { fields: changedFields }, timestamp: nowIso() });
      return operation;
    }, { expectedOperationRevision: options.expectedRevision });
  }

  async planOperation(businessKey, operationId, input = {}) {
    const registryRecord = await this.getRegistryForOperation(businessKey, operationId);
    return this.store.update(businessKey, operationId, (operation) => {
      if (!['draft', 'planned'].includes(operation.status)) {
        throw Object.assign(new Error(`Operation cannot be planned from ${operation.status}.`), { code: 'INVALID_TRANSITION' });
      }
      const raw = safeObject(input);
      const resolution = safeObject(operation.metadata?.projectResolution);
      if (resolution.confidence === 'medium' && resolution.confirmed !== true) {
        throw Object.assign(new Error('The inferred project must be explicitly confirmed before planning.'), { code: 'PROJECT_CONFIRMATION_REQUIRED' });
      }
      const requestedSteps = Array.isArray(raw.steps) ? raw.steps : [];
      let steps;
      if (requestedSteps.length) {
        const suppliedIds = requestedSteps.map((step) => safeString(step?.id, 120)).filter(Boolean);
        if (new Set(suppliedIds).size !== suppliedIds.length) {
          throw Object.assign(new Error('Duplicate step IDs are not allowed.'), { code: 'DUPLICATE_STEP_ID' });
        }
        const idMap = new Map();
        steps = requestedSteps.map((step, index) => {
          const created = createStep({ ...safeObject(step), status: 'pending' }, index, { riskLevel: operation.riskLevel });
          const supplied = safeString(step?.id, 120);
          if (supplied) idMap.set(supplied, created.id);
          return created;
        });
        steps.forEach((step, index) => {
          const suppliedSelf = safeString(requestedSteps[index]?.id, 120);
          step.dependsOn = (Array.isArray(requestedSteps[index]?.dependsOn) ? requestedSteps[index].dependsOn : [])
            .map((id) => idMap.get(safeString(id, 120)) || '')
            .filter((id) => id && id !== step.id && id !== idMap.get(suppliedSelf));
        });
      } else {
        const contextStepId = makeOperationId('step');
        const codexStepId = makeOperationId('step');
        const verificationStepId = makeOperationId('step');
        const requirements = this.verification.buildRequirements(registryRecord, raw.verificationRequirements);
        steps = [
          createStep({
            title: 'Prepare durable project context',
            description: 'Confirm the resolved project, registry identities, repository, workspace, deployment references, constraints, and acceptance criteria.',
            type: 'internal', provider: 'internal', toolName: 'prepare_operation_context', status: 'pending', sequence: 1,
            riskLevel: 'low', maxAttempts: 2, input: {},
          }, 0),
          createStep({
            title: 'Implement with Codex',
            description: operation.objective,
            type: 'codex', provider: 'codex', toolName: 'codex_implementation', status: 'pending', sequence: 2,
            dependsOn: [contextStepId], riskLevel: 'medium', maxAttempts: 2,
            input: { providerMode: 'external_handoff' },
            verificationRequirements: requirements.map((item) => item.type),
          }, 1),
          createStep({
            title: 'Verify the implementation',
            description: 'Verify actual implementation evidence independently of the Codex completion claim.',
            type: 'verification', provider: 'verification', toolName: 'verify_operation', status: 'pending', sequence: 3,
            dependsOn: [codexStepId], riskLevel: 'low', maxAttempts: 3,
            input: { requirements }, verificationRequirements: requirements.map((item) => item.type),
          }, 2),
        ];
        steps[1].dependsOn = [steps[0].id];
        steps[2].dependsOn = [steps[1].id];
      }
      const stepIds = new Set(steps.map((step) => step.id));
      for (const step of steps) {
        step.dependsOn = step.dependsOn.filter((id) => stepIds.has(id) && id !== step.id);
        const classification = this.policy.classify({
          business: operation.businessKey, projectRegistryId: operation.projectRegistryId,
          environment: safeString(step.input?.environment, 100) || 'development', provider: step.provider,
          action: step.toolName || step.type, actionClass: step.toolName || step.type,
          authorization: operation.metadata?.authorizationProvenance,
        });
        step.riskLevel = classification.riskLevel;
        step.approvalRequired = classification.approvalRequired;
      }
      const visiting = new Set();
      const visited = new Set();
      const byId = new Map(steps.map((step) => [step.id, step]));
      const visit = (id) => {
        if (visiting.has(id)) throw Object.assign(new Error('Operation plan contains a dependency cycle.'), { code: 'DEPENDENCY_CYCLE' });
        if (visited.has(id)) return;
        visiting.add(id);
        for (const dependency of byId.get(id)?.dependsOn || []) visit(dependency);
        visiting.delete(id); visited.add(id);
      };
      for (const step of steps) visit(step.id);
      operation.steps = steps;
      operation.plan = steps.map((step) => step.title);
      operation.acceptanceCriteria = Array.isArray(raw.acceptanceCriteria) && raw.acceptanceCriteria.length
        ? raw.acceptanceCriteria
        : (operation.acceptanceCriteria.length ? operation.acceptanceCriteria : defaultAcceptanceCriteria(operation));
      operation.riskLevel = steps.some((step) => step.riskLevel === 'critical') ? 'critical'
        : steps.some((step) => step.riskLevel === 'high') ? 'high'
          : steps.some((step) => step.riskLevel === 'medium') ? 'medium' : 'low';
      if (operation.status === 'draft') transition(operation, 'planned');
      appendEvent(operation, { type: 'operation_planned', actor: safeString(raw.plannedBy, 100) || 'marcus', message: `Operation planned with ${steps.length} deterministic step(s).`, data: { stepIds: steps.map((step) => step.id) }, timestamp: nowIso() });
      return operation;
    });
  }

  async replanOperation(businessKey, operationId, input = {}) {
    const raw = safeObject(input);
    const expectedRevision = Number(raw.revision ?? raw.expectedRevision);
    if (!Number.isFinite(expectedRevision)) throw Object.assign(new Error('Expected operation revision is required for re-planning.'), { code: 'EXPECTED_REVISION_REQUIRED' });
    const snapshot = await this.store.get(businessKey, operationId);
    if (!snapshot) throw Object.assign(new Error('Operation not found.'), { code: 'OPERATION_NOT_FOUND' });
    if (snapshot.status !== 'planned') throw Object.assign(new Error(`Explicit re-planning is only allowed from planned, not ${snapshot.status}.`), { code: 'INVALID_TRANSITION' });
    const patch = safeObject(raw.patch);
    const requestedRegistryId = safeString(patch.projectRegistryId, 160);
    const registryRecord = requestedRegistryId ? await this.registry.get(businessKey, requestedRegistryId) : null;
    if (requestedRegistryId && !registryRecord) throw Object.assign(new Error('Project registry record not found in the active business.'), { code: 'PROJECT_REGISTRY_NOT_FOUND' });
    await this.store.update(businessKey, operationId, (operation) => {
      for (const key of ['title', 'objective', 'originalRequest', 'acceptanceCriteria']) if (Object.prototype.hasOwnProperty.call(patch, key)) operation[key] = patch[key];
      if (registryRecord) {
        operation.projectRegistryId = registryRecord.id; operation.projectId = registryRecord.projectId; operation.projectName = registryRecord.canonicalName;
        operation.metadata.projectResolution = { resolved: true, confidence: 'high', score: 100, reason: 'Explicitly selected during re-plan.', alternatives: [], confirmed: true };
        operation.metadata.authorizationProvenance = authorizationAfterProjectSelection(operation.metadata.authorizationProvenance, registryRecord.id);
      }
      operation.status = 'draft'; operation.steps = []; operation.plan = []; operation.approvals = []; operation.verification = [];
      operation.blockers = []; operation.providerActions = []; operation.desktopCorrelations = []; operation.currentStepId = '';
      operation.metadata.executionTarget = {};
      appendEvent(operation, { type: 'operation_plan_invalidated', actor: safeString(raw.actor, 100) || 'mark', message: 'The prior plan and verification state were explicitly invalidated before re-planning.', data: { fields: Object.keys(patch).slice(0, 20) } });
      return operation;
    }, { expectedOperationRevision: expectedRevision });
    return this.planOperation(businessKey, operationId, safeObject(raw.plan));
  }

  async startOperation(businessKey, operationId, { actor = 'mark', runCycle = true } = {}) {
    if (this.workGuard) await this.workGuard(businessKey, await this.store.get(businessKey, operationId));
    const registryRecord = await this.getRegistryForOperation(businessKey, operationId);
    const operation = await this.store.update(businessKey, operationId, (draft) => {
      if (draft.status !== 'planned') throw Object.assign(new Error(`Operation cannot start from ${draft.status}.`), { code: 'INVALID_TRANSITION' });
      const resolution = draft.metadata?.projectResolution;
      if (resolution?.confidence === 'low') throw Object.assign(new Error('Project resolution confidence is too low to execute. Resolve the project first.'), { code: 'PROJECT_UNRESOLVED' });
      if (resolution?.confidence === 'medium' && resolution?.confirmed !== true) throw Object.assign(new Error('The inferred project must be explicitly confirmed before execution.'), { code: 'PROJECT_CONFIRMATION_REQUIRED' });
      if (!draft.steps.length) throw Object.assign(new Error('Operation has no execution plan.'), { code: 'OPERATION_NOT_PLANNED' });
      draft.metadata.executionTarget = sanitizeStructured({
        businessKey: draft.businessKey,
        projectRegistryId: draft.projectRegistryId,
        projectId: draft.projectId,
        repository: registryRecord?.repo || {},
        localWorkspace: registryRecord?.localWorkspace || {},
        deployments: registryRecord?.deployments || {},
        commands: registryRecord?.commands || {},
        canonicalName: registryRecord?.canonicalName || draft.projectName,
        boundAt: nowIso(),
      }, 25_000);
      transition(draft, 'queued');
      appendEvent(draft, { type: 'operation_queued', actor, message: 'Operation queued for an explicit runner cycle.', data: {}, timestamp: nowIso() });
      return draft;
    });
    if (runCycle && this.runner) return this.runner.tick(businessKey, operation.id);
    return operation;
  }

  async confirmProject(businessKey, operationId, { actor = 'mark', projectRegistryId = '', expectedRevision } = {}) {
    const snapshot = await this.store.get(businessKey, operationId);
    if (!snapshot) throw Object.assign(new Error('Operation not found.'), { code: 'OPERATION_NOT_FOUND' });
    if (snapshot.status !== 'draft') throw Object.assign(new Error('Project confirmation is only allowed while the operation is draft.'), { code: 'INVALID_TRANSITION' });
    if (!Number.isFinite(Number(expectedRevision))) throw Object.assign(new Error('Expected operation revision is required.'), { code: 'EXPECTED_REVISION_REQUIRED', currentRevision: snapshot.revision });
    const selectedId = safeString(projectRegistryId, 160) || snapshot.projectRegistryId;
    const record = selectedId ? await this.registry.get(businessKey, selectedId) : null;
    if (!record) throw Object.assign(new Error('Project registry record not found in the active business.'), { code: 'PROJECT_REGISTRY_NOT_FOUND' });
    return this.store.update(businessKey, operationId, (operation) => {
      operation.projectRegistryId = record.id;
      operation.projectId = record.projectId;
      operation.projectName = record.canonicalName;
      operation.metadata.projectResolution = { ...safeObject(operation.metadata.projectResolution), resolved: true, confirmed: true, confirmedAt: nowIso(), confirmedBy: safeString(actor, 100) || 'mark' };
      operation.metadata.authorizationProvenance = authorizationAfterProjectSelection(operation.metadata.authorizationProvenance, record.id);
      appendEvent(operation, { type: 'project_resolution_confirmed', actor, message: `Confirmed inferred project ${record.canonicalName}.`, data: { projectRegistryId: record.id } });
      return operation;
    }, { expectedOperationRevision: expectedRevision });
  }

  async pauseOperation(businessKey, operationId, { actor = 'mark', reason = '' } = {}) {
    let operation = await this.store.update(businessKey, operationId, (draft) => {
      if (!['queued', 'running', 'awaiting_provider', 'waiting_for_approval', 'blocked', 'recovery_required', 'verifying'].includes(draft.status)) {
        throw Object.assign(new Error(`Operation cannot pause from ${draft.status}.`), { code: 'INVALID_TRANSITION' });
      }
      transition(draft, 'paused');
      const running = draft.steps.find((step) => step.status === 'running');
      if (running) running.error = '';
      appendEvent(draft, {
        type: 'operation_paused', actor,
        message: safeString(reason, 2_000) || 'Operation paused. Any in-flight provider attempt remains durably associated and cannot be relaunched.',
        data: { activeStepId: running?.id || '', providerJobRegistered: Boolean(running && safeObject(draft.metadata?.codexJobs)[running.id]) },
        timestamp: nowIso(),
      });
      return draft;
    });
    const running = operation.steps.find((step) => step.status === 'running' && step.type === 'codex');
    const activeJob = running ? safeObject(operation.metadata?.codexJobs)[running.id] : null;
    const provider = this.runner?.providers?.codex;
    if (!running || !activeJob || provider?.mode !== 'direct') return operation;
    if (!provider.supportsPause()) {
      return this.store.update(businessKey, operationId, (draft) => {
        if (draft.status !== 'paused') return draft;
        appendEvent(draft, {
          type: 'provider_pause_unsupported', actor, stepId: running.id,
          message: 'The provider does not support pause; its existing job remains bound to this attempt and may continue externally.',
          data: { jobId: activeJob.jobId || '', idempotencyKey: running.idempotencyKey },
        });
        return draft;
      });
    }
    let pausedJob;
    try { pausedJob = await provider.pauseJob(activeJob); }
    catch (error) {
      return this.store.update(businessKey, operationId, (draft) => {
        if (draft.status !== 'paused') return draft;
        appendEvent(draft, { type: 'provider_pause_failed', actor, stepId: running.id, message: `Provider pause was not confirmed: ${safeString(error?.message, 2_000) || 'unknown error'}` });
        return draft;
      });
    }
    operation = await this.store.update(businessKey, operationId, (draft) => {
      if (draft.status !== 'paused') return draft;
      const step = draft.steps.find((item) => item.id === running.id);
      if (!step || step.idempotencyKey !== running.idempotencyKey || step.attemptCount !== running.attemptCount) return draft;
      const safeJob = sanitizeStructured({ ...activeJob, ...safeObject(pausedJob) }, 30_000);
      delete safeJob.prompt;
      draft.metadata.codexJobs[step.id] = safeJob;
      const action = draft.providerActions.find((item) => item.stepId === step.id && item.idempotencyKey === step.idempotencyKey);
      if (action) { action.status = safeJob.status === 'paused' ? 'paused' : action.status; action.updatedAt = nowIso(); }
      appendEvent(draft, {
        type: 'provider_pause_result_recorded', actor, stepId: step.id,
        message: safeJob.status === 'paused' ? 'The external provider confirmed pause.' : 'The external provider did not confirm pause; the job remains associated and may continue.',
        data: { jobId: safeJob.jobId || '', confirmed: safeJob.status === 'paused' },
      });
      return draft;
    });
    return operation;
  }

  async resumeOperation(businessKey, operationId, { actor = 'mark', reason = '', runCycle = true } = {}) {
    const snapshot = await this.store.get(businessKey, operationId);
    if (!snapshot) throw Object.assign(new Error('Operation not found.'), { code: 'OPERATION_NOT_FOUND' });
    if (!['paused', 'blocked', 'recovery_required'].includes(snapshot.status)) throw Object.assign(new Error(`Operation cannot resume from ${snapshot.status}.`), { code: 'INVALID_TRANSITION' });
    const activeCodexStep = snapshot.steps.find((step) => step.type === 'codex' && ['running', 'blocked'].includes(step.status)
      && safeObject(snapshot.metadata?.codexJobs)[step.id]);
    const pendingCodexLaunch = snapshot.steps.find((step) => step.type === 'codex' && step.status === 'running'
      && !safeObject(snapshot.metadata?.codexJobs)[step.id]);
    if (snapshot.status === 'paused' && (activeCodexStep || pendingCodexLaunch) && this.runner?.providers?.codex?.mode === 'direct') {
      const stepId = (activeCodexStep || pendingCodexLaunch).id;
      const boundStep = activeCodexStep || pendingCodexLaunch;
      const boundJob = activeCodexStep ? safeObject(snapshot.metadata?.codexJobs)[stepId] : null;
      let resumedJob = boundJob;
      if (boundJob?.status === 'paused') {
        if (!this.runner.providers.codex.supportsResume()) {
          throw Object.assign(new Error('The Codex provider paused the job but does not support resuming it.'), { code: 'PROVIDER_RESUME_UNSUPPORTED' });
        }
        try { resumedJob = await this.runner.providers.codex.resumeJob(boundJob); }
        catch (error) { throw Object.assign(new Error(`Codex provider could not resume the paused job: ${error?.message || 'unknown error'}`), { code: 'PROVIDER_RESUME_FAILED' }); }
      }
      const resumed = await this.store.update(businessKey, operationId, (operation) => {
        if (operation.status !== 'paused') throw Object.assign(new Error(`Operation cannot resume from ${operation.status}.`), { code: 'INVALID_TRANSITION' });
        const step = operation.steps.find((item) => item.id === stepId);
        if (!step || step.idempotencyKey !== boundStep.idempotencyKey || step.attemptCount !== boundStep.attemptCount) {
          throw Object.assign(new Error('The provider attempt changed before resume.'), { code: 'STEP_REVISION_CHANGED' });
        }
        if (resumedJob) {
          const safeJob = sanitizeStructured({ ...boundJob, ...safeObject(resumedJob), updatedAt: nowIso() }, 30_000);
          delete safeJob.prompt;
          operation.metadata.codexJobs[stepId] = safeJob;
        }
        step.status = 'running'; step.error = '';
        operation.status = 'awaiting_provider'; operation.pausedAt = '';
        appendEvent(operation, {
          type: resumedJob !== boundJob ? 'provider_job_resumed' : 'provider_job_poll_resumed', actor, stepId,
          message: safeString(reason, 2_000) || (boundJob ? 'Resumed by polling the existing Codex job.' : 'Resumed while the original provider launch is still pending; no duplicate launch was issued.'),
          data: { jobId: resumedJob?.jobId || '', idempotencyKey: step.idempotencyKey, attempt: step.attemptCount },
        });
        return operation;
      });
      if (runCycle && this.runner) return this.runner.tick(businessKey, resumed.id);
      return resumed;
    }
    const operation = await this.store.update(businessKey, operationId, (draft) => {
      if (!['paused', 'blocked', 'recovery_required'].includes(draft.status)) throw Object.assign(new Error(`Operation cannot resume from ${draft.status}.`), { code: 'INVALID_TRANSITION' });
      const unreconciledProviderAttempt = draft.steps.find((step) => step.type === 'codex' && step.status === 'blocked'
        && step.attemptCount > 0 && !safeObject(draft.metadata?.codexJobs)[step.id]
        && draft.providerActions.some((action) => action.stepId === step.id && action.idempotencyKey === step.idempotencyKey
          && !['completed', 'failed', 'cancelled'].includes(action.status)));
      if (unreconciledProviderAttempt) {
        throw Object.assign(new Error('The original provider launch has no durable job ID after restart. Reconcile that exact attempt before resuming; it will not be relaunched.'), { code: 'OPERATION_STILL_BLOCKED' });
      }
      const unresolvedExternal = draft.blockers.some((blocker) => blocker.status === 'active' && ['external_codex_required', 'verification_required'].includes(blocker.type));
      if (unresolvedExternal) throw Object.assign(new Error('Required external Codex or verification evidence has not been registered.'), { code: 'OPERATION_STILL_BLOCKED' });
      const pendingApproval = draft.approvals.some((approval) => approval.status === 'pending');
      if (pendingApproval) {
        transition(draft, 'waiting_for_approval');
        appendEvent(draft, { type: 'operation_resumed_waiting_approval', actor, message: 'Operation resumed and remains safely waiting for its pending approval.', data: {}, timestamp: nowIso() });
        return draft;
      }
      for (const step of draft.steps) if (step.status === 'blocked' && step.attemptCount < step.maxAttempts) step.status = 'ready';
      for (const blocker of draft.blockers) if (blocker.type === 'recovery_required' && blocker.status === 'active') {
        blocker.status = 'resolved';
        blocker.resolvedAt = nowIso();
        blocker.resolution = safeString(reason, 2_000) || 'Mark explicitly resumed the interrupted operation.';
      }
      transition(draft, 'queued');
      appendEvent(draft, { type: 'operation_resumed', actor, message: 'Operation resumed and queued.', data: {}, timestamp: nowIso() });
      return draft;
    });
    if (runCycle && this.runner) return this.runner.tick(businessKey, operation.id);
    return operation;
  }

  async cancelOperation(businessKey, operationId, { actor = 'mark', reason = '' } = {}) {
    let activeJob = null;
    let activeStepId = '';
    let operation = await this.store.update(businessKey, operationId, (operation) => {
      if (['completed', 'failed', 'cancelled'].includes(operation.status)) throw Object.assign(new Error(`Operation cannot be cancelled from ${operation.status}.`), { code: 'INVALID_TRANSITION' });
      const activeEntry = Object.entries(safeObject(operation.metadata?.codexJobs)).find(([, job]) => ['started', 'queued', 'running', 'unknown', 'paused'].includes(job?.status));
      if (activeEntry) [activeStepId, activeJob] = activeEntry;
      transition(operation, 'cancelled');
      for (const step of operation.steps) if (!['completed', 'skipped'].includes(step.status)) step.status = 'cancelled';
      for (const approval of operation.approvals) if (approval.status === 'pending') approval.status = 'cancelled';
      const cancellationMessage = safeString(reason, 2_000) || 'Operation cancelled.';
      appendEvent(operation, {
        type: 'operation_cancelled', actor,
        message: activeJob ? `${cancellationMessage} External cancellation will be attempted without delaying the terminal transition.` : cancellationMessage,
        data: { externalCancellationConfirmed: false, externalCancellationPending: Boolean(activeJob) }, timestamp: nowIso(),
      });
      return operation;
    });
    if (!activeJob || this.runner?.providers?.codex?.mode !== 'direct') return operation;
    let cancelledJob = null;
    let cancellationError = '';
    try { cancelledJob = await this.runner.providers.codex.cancelJob(activeJob); }
    catch (error) { cancellationError = safeString(error?.message, 2_000) || 'Provider cancellation failed.'; }
    const cancellationConfirmed = cancelledJob?.status === 'cancelled';
    operation = await this.store.update(businessKey, operationId, (draft) => {
      if (draft.status !== 'cancelled') return draft;
      if (cancelledJob && activeStepId) {
        const safeJob = sanitizeStructured({ ...activeJob, ...safeObject(cancelledJob) }, 30_000);
        delete safeJob.prompt;
        draft.metadata.codexJobs[activeStepId] = safeJob;
      }
      const providerAction = draft.providerActions.find((item) => item.stepId === activeStepId
        && item.idempotencyKey === safeString(activeJob.idempotencyKey, 240));
      if (providerAction) {
        providerAction.status = cancellationConfirmed ? 'cancelled' : 'unknown';
        providerAction.updatedAt = nowIso();
        providerAction.completedAt = cancellationConfirmed ? nowIso() : '';
        providerAction.cancellationConfirmed = cancellationConfirmed;
      }
      appendEvent(draft, {
        type: 'provider_cancellation_result', actor, stepId: activeStepId,
        message: cancellationConfirmed ? 'External provider cancellation was confirmed.' : 'External provider cancellation was not confirmed; the operation remains cancelled.',
        data: { externalCancellationConfirmed: cancellationConfirmed, error: cancellationError },
      });
      return draft;
    });
    return operation;
  }

  async retryOperation(businessKey, operationId, { actor = 'mark', stepId = '', runCycle = true } = {}) {
    const operation = await this.store.update(businessKey, operationId, (draft) => {
      if (!['failed', 'blocked', 'paused', 'recovery_required'].includes(draft.status)) throw Object.assign(new Error(`Operation cannot retry from ${draft.status}.`), { code: 'INVALID_TRANSITION' });
      const step = stepId ? draft.steps.find((item) => item.id === stepId) : draft.steps.find((item) => ['failed', 'blocked'].includes(item.status));
      if (!step) throw Object.assign(new Error('No failed or blocked step is available to retry.'), { code: 'STEP_NOT_RETRYABLE' });
      if (step.attemptCount >= step.maxAttempts) throw Object.assign(new Error(`Retry limit reached for ${step.title}.`), { code: 'RETRY_LIMIT_REACHED' });
      step.status = 'ready';
      step.error = '';
      step.failedAt = '';
      for (const blocker of draft.blockers) if (blocker.stepId === step.id && blocker.status === 'active') {
        blocker.status = 'resolved'; blocker.resolvedAt = nowIso(); blocker.resolution = 'Retry requested.';
      }
      transition(draft, 'queued');
      appendEvent(draft, { type: 'operation_retry_queued', actor, stepId: step.id, message: `Retry queued for ${step.title}.`, data: { attemptCount: step.attemptCount, maxAttempts: step.maxAttempts }, timestamp: nowIso() });
      return draft;
    }, { allowTerminalTransition: true });
    if (runCycle && this.runner) return this.runner.tick(businessKey, operation.id);
    return operation;
  }

  async approveOperationStep(businessKey, operationId, approvalId, input = {}) {
    const operation = await this.approvalService.approve(businessKey, operationId, approvalId, input);
    if (input.runCycle !== false && this.runner) return this.runner.tick(businessKey, operation.id);
    return operation;
  }

  async rejectOperationStep(businessKey, operationId, approvalId, input = {}) {
    return this.approvalService.reject(businessKey, operationId, approvalId, input);
  }

  async appendOperationEvent(businessKey, operationId, event) {
    return this.store.update(businessKey, operationId, (operation) => {
      appendEvent(operation, event);
      return operation;
    });
  }

  async completeOperation(businessKey, operationId, { actor = 'system' } = {}) {
    const completed = await this.store.update(businessKey, operationId, (operation) => {
      if (operation.status !== 'verifying') throw Object.assign(new Error(`Operation cannot complete from ${operation.status}.`), { code: 'INVALID_TRANSITION' });
      const incompleteStep = operation.steps.find((step) => !['completed', 'skipped'].includes(step.status));
      if (incompleteStep) {
        throw Object.assign(new Error(`Operation cannot complete while step "${incompleteStep.title}" is ${incompleteStep.status}.`), { code: 'INCOMPLETE_STEPS' });
      }
      const pendingApproval = operation.approvals.find((approval) => approval.status === 'pending');
      if (pendingApproval) {
        throw Object.assign(new Error(`Operation cannot complete while approval "${pendingApproval.action}" is pending.`), { code: 'APPROVAL_PENDING' });
      }
      const activeBlocker = operation.blockers.find((blocker) => blocker.status === 'active');
      if (activeBlocker) {
        throw Object.assign(new Error(`Operation cannot complete while an active blocker remains: ${activeBlocker.message || activeBlocker.type}.`), { code: 'ACTIVE_BLOCKER' });
      }
      if (!requiredVerificationPassed(operation)) throw Object.assign(new Error('Required verification has not passed or been explicitly waived.'), { code: 'VERIFICATION_REQUIRED' });
      transition(operation, 'completed');
      operation.currentStepId = '';
      appendEvent(operation, { type: 'operation_completed', actor, message: 'Operation completed with required verification evidence.', data: {}, timestamp: nowIso() });
      return operation;
    });
    await this.winningMethodStore?.recordRecoveredOperation(businessKey, completed).catch(() => {});
    return completed;
  }

  async failOperation(businessKey, operationId, { actor = 'system', reason = 'Operation failed.' } = {}) {
    return this.store.update(businessKey, operationId, (operation) => {
      if (['completed', 'cancelled'].includes(operation.status)) throw Object.assign(new Error(`Operation cannot fail from ${operation.status}.`), { code: 'INVALID_TRANSITION' });
      if (operation.status !== 'failed') transition(operation, 'failed');
      appendEvent(operation, { type: 'operation_failed', actor, message: safeString(reason, 4_000), data: {}, timestamp: nowIso() });
      return operation;
    });
  }

  async registerExternalCodexJob(businessKey, operationId, input = {}) {
    const raw = safeObject(input);
    return this.store.update(businessKey, operationId, (operation) => {
      const stepId = safeString(raw.stepId, 160);
      const step = (stepId ? operation.steps.find((item) => item.id === stepId) : null)
        || operation.steps.find((item) => item.type === 'codex' && ['blocked', 'running', 'failed', 'waiting_for_approval'].includes(item.status))
        || operation.steps.find((item) => item.type === 'codex');
      if (!step) throw Object.assign(new Error('No Codex step is waiting for an external job or result.'), { code: 'CODEX_STEP_NOT_FOUND' });
      const timestamp = nowIso();
      const status = ['completed', 'failed', 'running', 'cancelled'].includes(raw.status) ? raw.status
        : (raw.result || raw.commit || raw.diffSummary ? 'completed' : 'running');
      const job = {
        recordId: makeOperationId('codexjob'), provider: 'external_handoff', jobId: safeString(raw.jobId, 300), operationId, stepId: step.id,
        projectRegistryId: operation.projectRegistryId, repository: safeString(raw.repository, 1_000), branch: safeString(raw.branch, 500),
        prompt: '', status, startedAt: timestamp, updatedAt: timestamp,
        completedAt: status === 'completed' ? timestamp : '', artifacts: Array.isArray(raw.artifacts) ? raw.artifacts.slice(0, 50) : [],
        diffSummary: safeString(raw.diffSummary, 20_000), rawMetadata: safeObject(raw.rawMetadata),
        idempotencyKey: safeString(safeObject(operation.metadata?.codexJobs)[step.id]?.idempotencyKey || step.idempotencyKey, 240),
      };
      const metadata = safeObject(operation.metadata);
      operation.metadata = { ...metadata, codexJobs: { ...safeObject(metadata.codexJobs), [step.id]: job } };
      operation.artifacts.push(createArtifact({
        operationId, stepId: step.id, type: 'external_job', name: `External Codex job ${job.jobId || 'result'}`,
        mimeType: 'application/json', content: JSON.stringify({ ...job, prompt: undefined }), createdAt: timestamp,
      }));
      if (job.branch) operation.artifacts.push(createArtifact({ operationId, stepId: step.id, type: 'branch', name: 'Codex branch', content: job.branch }));
      if (raw.commit) operation.artifacts.push(createArtifact({ operationId, stepId: step.id, type: 'commit', name: 'Codex commit', content: safeString(raw.commit, 500) }));
      if (job.diffSummary) operation.artifacts.push(createArtifact({ operationId, stepId: step.id, type: 'codex_diff', name: 'Codex diff summary', content: job.diffSummary }));
      if (raw.result) operation.artifacts.push(createArtifact({ operationId, stepId: step.id, type: 'codex_result', name: 'Codex completion result', content: raw.result }));
      if (Array.isArray(raw.artifacts)) for (const artifact of raw.artifacts) operation.artifacts.push(createArtifact(artifact, { operationId, stepId: step.id }));

      if (Array.isArray(raw.verificationResults) && raw.verificationResults.length) {
        operation.artifacts.push(createArtifact({
          operationId, stepId: step.id, type: 'untrusted_codex_verification_claim',
          name: 'Codex-provided verification claims (untrusted)', mimeType: 'application/json',
          content: JSON.stringify(sanitizeStructured(raw.verificationResults, 30_000)),
          metadata: { trustedForVerification: false },
        }));
        appendEvent(operation, {
          type: 'codex_verification_claims_quarantined', actor: safeString(raw.registeredBy, 100) || 'mark', stepId: step.id,
          message: 'Codex-provided verification claims were retained as untrusted evidence and did not change verification status.',
          data: { claimCount: raw.verificationResults.length }, timestamp,
        });
      }

      if (['completed', 'failed', 'cancelled'].includes(operation.status)) {
        appendEvent(operation, {
          type: 'late_external_codex_result_recorded', actor: safeString(raw.registeredBy, 100) || 'mark', stepId: step.id,
          message: `External Codex evidence arrived after the operation became ${operation.status}; terminal state was retained.`,
          data: { jobId: job.jobId, providerStatus: status }, timestamp,
        });
        return operation;
      }

      if (status === 'completed') {
        step.status = 'completed'; step.completedAt = timestamp; step.error = ''; step.output = `External Codex result registered${job.jobId ? ` for job ${job.jobId}` : ''}.`;
        for (const blocker of operation.blockers) if (blocker.stepId === step.id && blocker.status === 'active') {
          blocker.status = 'resolved'; blocker.resolvedAt = timestamp; blocker.resolution = 'External Codex completion evidence registered.';
        }
        const verificationStep = operation.steps.find((item) => item.type === 'verification');
        if (verificationStep && ['blocked', 'failed'].includes(verificationStep.status) && verificationStep.attemptCount < verificationStep.maxAttempts) {
          verificationStep.status = 'ready'; verificationStep.error = '';
          for (const blocker of operation.blockers) if (blocker.stepId === verificationStep.id && blocker.status === 'active') {
            blocker.status = 'resolved'; blocker.resolvedAt = timestamp; blocker.resolution = 'New verification evidence registered.';
          }
        }
        if (['blocked', 'failed'].includes(operation.status)) operation.status = 'queued';
      } else if (status === 'failed') {
        step.status = 'failed'; step.failedAt = timestamp; step.error = safeString(raw.error, 8_000) || 'External Codex job failed.';
        if (operation.status !== 'paused') { operation.status = 'failed'; operation.failedAt = timestamp; }
      } else if (status === 'cancelled') {
        step.status = 'cancelled'; step.error = 'External Codex job was cancelled.';
        operation.status = 'cancelled'; operation.cancelledAt = timestamp;
      } else {
        step.status = 'blocked';
        if (operation.status !== 'paused') operation.status = 'blocked';
      }
      appendEvent(operation, { type: 'external_codex_job_registered', actor: safeString(raw.registeredBy, 100) || 'mark', stepId: step.id, message: `External Codex job/result registered with status ${status}.`, data: { jobId: job.jobId, branch: job.branch, hasCommit: Boolean(raw.commit), hasDiff: Boolean(job.diffSummary) }, timestamp });
      return operation;
    });
  }

  async registerManualVerificationEvidence(businessKey, operationId, results, { actor = 'authenticated_operator' } = {}) {
    const suppliedBy = safeString(actor, 120) || 'authenticated_operator';
    return this.store.update(businessKey, operationId, (operation) => {
      if (['completed', 'failed', 'cancelled'].includes(operation.status)) {
        throw Object.assign(new Error(`Manual verification evidence cannot change a terminal operation from ${operation.status}.`), { code: 'INVALID_TRANSITION' });
      }
      const step = operation.steps.find((item) => item.type === 'verification');
      if (!step) throw Object.assign(new Error('Operation has no verification step.'), { code: 'VERIFICATION_STEP_NOT_FOUND' });
      const timestamp = nowIso();
      const normalized = [];
      for (const rawResult of Array.isArray(results) ? results : []) {
        const raw = safeObject(rawResult);
        const note = safeString(raw.note || raw.evidenceNote, 4_000);
        const artifactInput = safeObject(raw.artifact);
        const meaningfulArtifact = Boolean(safeString(artifactInput.content, 100_000) || safeString(artifactInput.path, 1_000) || safeString(artifactInput.url, 2_000));
        if (note.length < 10 && !meaningfulArtifact) {
          throw Object.assign(new Error('Manual verification requires a meaningful evidence note or artifact.'), { code: 'MANUAL_EVIDENCE_REQUIRED' });
        }
        const status = raw.status === 'failed' ? 'failed' : 'passed';
        let artifactId = '';
        if (meaningfulArtifact) {
          const artifact = createArtifact({ ...artifactInput, type: 'manual_verification_evidence', name: artifactInput.name || `Manual ${safeString(raw.type, 100) || 'verification'} evidence`, metadata: { ...safeObject(artifactInput.metadata), suppliedBy, suppliedAt: timestamp } }, { operationId, stepId: step.id });
          operation.artifacts.push(artifact);
          artifactId = artifact.id;
        }
        normalized.push({
          ...raw,
          stepId: step.id,
          status,
          output: status === 'passed' ? note : '',
          error: status === 'failed' ? note : '',
          evidence: sanitizeStructured({
            source: 'authenticated_operator_manual', suppliedBy, suppliedAt: timestamp, note, artifactId,
          }, 20_000),
        });
      }
      if (!normalized.length) throw Object.assign(new Error('At least one manual verification result is required.'), { code: 'MANUAL_EVIDENCE_REQUIRED' });
      operation.verification = mergeVerificationResults(operation.verification, normalized, operationId, step.id);
      if (['blocked', 'failed'].includes(step.status) && step.attemptCount < step.maxAttempts) step.status = 'ready';
      for (const blocker of operation.blockers) if (blocker.stepId === step.id && blocker.status === 'active') {
        blocker.status = 'resolved'; blocker.resolvedAt = nowIso(); blocker.resolution = 'Verification evidence registered.';
      }
      if (['blocked', 'failed'].includes(operation.status)) operation.status = 'queued';
      appendEvent(operation, { type: 'manual_verification_evidence_registered', actor: suppliedBy, stepId: step.id, message: `${normalized.length} authenticated manual verification result(s) registered.`, data: { suppliedBy }, timestamp });
      return operation;
    });
  }

  async waiveVerification(businessKey, operationId, verificationId, { approvalId, actor = 'mark', reason = '' } = {}) {
    return this.store.update(businessKey, operationId, (operation) => {
      const result = operation.verification.find((item) => item.id === verificationId);
      if (!result) throw Object.assign(new Error('Verification result not found.'), { code: 'VERIFICATION_NOT_FOUND' });
      const waiverReason = safeString(reason, 2_000);
      if (!waiverReason) throw Object.assign(new Error('A reason is required to waive verification.'), { code: 'WAIVER_REASON_REQUIRED' });
      const approval = createApproval({
        operationId,
        stepId: result.stepId,
        action: `waive_verification:${result.type}`,
        riskLevel: 'medium',
        reason: waiverReason,
        status: 'approved',
        approvedBy: actor,
        approvalMessage: waiverReason,
      });
      operation.approvals.push(approval);
      result.waived = true; result.waiverApprovalId = approval.id; result.status = 'skipped'; result.completedAt = nowIso();
      const step = operation.steps.find((item) => item.id === result.stepId);
      if (step && ['blocked', 'failed'].includes(step.status) && step.attemptCount < step.maxAttempts) step.status = 'ready';
      for (const blocker of operation.blockers) if (blocker.stepId === result.stepId && blocker.status === 'active') {
        blocker.status = 'resolved'; blocker.resolvedAt = nowIso(); blocker.resolution = `Verification ${result.type} explicitly waived.`;
      }
      if (['blocked', 'paused'].includes(operation.status)) operation.status = 'queued';
      appendEvent(operation, { type: 'verification_waived', actor, stepId: result.stepId, message: `Verification ${result.type} explicitly waived: ${waiverReason}`, data: { verificationId, approvalId: approval.id }, timestamp: nowIso() });
      return operation;
    });
  }

  async getRegistryForOperation(businessKey, operationId) {
    const operation = await this.store.get(businessKey, operationId);
    if (!operation) throw Object.assign(new Error('Operation not found.'), { code: 'OPERATION_NOT_FOUND' });
    if (!operation.projectRegistryId) return null;
    return this.registry.get(businessKey, operation.projectRegistryId);
  }
}

export { appendEvent, transition, mergeVerificationResults };
