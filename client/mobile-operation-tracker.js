const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled']);
const ACTIVE_STATUSES = new Set(['draft', 'planned', 'queued', 'running', 'awaiting_provider', 'verifying']);
const STORAGE_KEY = 'marcus_mobile_active_operation';

function bounded(value, max = 300) {
  return String(value || '').trim().slice(0, max);
}

export function toMobileOperationSummary(operation = {}) {
  const steps = Array.isArray(operation.steps) ? operation.steps : [];
  const currentStep = operation.currentStep && typeof operation.currentStep === 'object'
    ? operation.currentStep
    : steps.find((step) => step.id === operation.currentStepId)
      || steps.find((step) => ['running', 'waiting_for_approval', 'blocked', 'ready', 'pending'].includes(step.status))
      || null;
  const completedSteps = Number(operation.progress?.completed ?? steps.filter((step) => ['completed', 'skipped'].includes(step.status)).length) || 0;
  const totalSteps = Number(operation.progress?.total ?? steps.length) || 0;
  const required = Array.isArray(operation.verification)
    ? operation.verification.filter((item) => item.required !== false)
    : [];
  const passed = Number(operation.verificationSummary?.passed
    ?? required.filter((item) => item.status === 'passed' || (item.waived === true && item.waiverApprovalId)).length) || 0;
  const failed = Number(operation.verificationSummary?.failed
    ?? required.filter((item) => item.status === 'failed').length) || 0;
  const manual = Number(operation.verificationSummary?.needsManualReview
    ?? required.filter((item) => item.status === 'needs_manual_review').length) || 0;
  const requiredCount = Number(operation.verificationSummary?.required ?? required.length) || 0;
  return {
    id: bounded(operation.id, 160),
    title: bounded(operation.title, 500),
    projectName: bounded(operation.projectName, 300),
    status: bounded(operation.status, 80),
    updatedAt: bounded(operation.updatedAt, 50),
    riskLevel: bounded(operation.riskLevel, 40),
    needsApproval: operation.needsApproval === true || (Array.isArray(operation.approvals) && operation.approvals.some((item) => item.status === 'pending')),
    needsRecovery: operation.needsRecovery === true || operation.status === 'recovery_required',
    activeBlockers: Number(operation.activeBlockers ?? (Array.isArray(operation.blockers) ? operation.blockers.filter((item) => item.status === 'active').length : 0)) || 0,
    progress: {
      completed: completedSteps,
      total: totalSteps,
      percent: totalSteps ? Math.round((completedSteps / totalSteps) * 100) : 0,
    },
    currentStep: currentStep ? {
      title: bounded(currentStep.title, 300),
      type: bounded(currentStep.type, 80),
      status: bounded(currentStep.status, 80),
    } : null,
    verificationSummary: {
      required: requiredCount,
      passed,
      failed,
      needsManualReview: manual,
      pending: Math.max(0, requiredCount - passed - failed),
    },
  };
}

export function operationSignature(operation) {
  const item = toMobileOperationSummary(operation);
  return [
    item.status,
    item.progress.completed,
    item.progress.total,
    item.currentStep?.title || '',
    item.currentStep?.status || '',
    item.verificationSummary.passed,
    item.verificationSummary.failed,
    item.verificationSummary.needsManualReview,
    Number(item.needsApproval),
    item.activeBlockers,
  ].join('|');
}

export function selectTrackedOperation(operations, trackedId = '') {
  const normalized = (Array.isArray(operations) ? operations : []).map(toMobileOperationSummary).filter((item) => item.id);
  const exact = normalized.find((item) => item.id === trackedId);
  if (exact && !TERMINAL_STATUSES.has(exact.status)) return exact;
  return normalized.find((item) => !TERMINAL_STATUSES.has(item.status)) || exact || null;
}

export function formatOperationTransition(operation) {
  const item = toMobileOperationSummary(operation);
  const label = item.projectName || item.title || 'The operation';
  const progress = item.progress.total ? `${item.progress.completed}/${item.progress.total} steps` : 'step progress unavailable';
  const checks = item.verificationSummary.required
    ? `${item.verificationSummary.passed}/${item.verificationSummary.required} required checks passed`
    : 'verification evidence pending';
  if (item.status === 'completed') return `${label} completed with persisted verification: ${progress}; ${checks}. Operation: ${item.id}.`;
  if (item.status === 'failed') return `${label} failed at ${item.currentStep?.title || 'the current step'}. Marcus has not claimed completion. Operation: ${item.id}.`;
  if (item.status === 'cancelled') return `${label} was cancelled. Operation: ${item.id}.`;
  if (item.status === 'waiting_for_approval' || item.needsApproval) return `${label} is waiting for explicit approval at ${item.currentStep?.title || 'the current step'}. Operation: ${item.id}.`;
  if (item.status === 'recovery_required' || item.needsRecovery) return `${label} needs provider reconciliation before Marcus can continue. Operation: ${item.id}.`;
  if (item.status === 'blocked') return `${label} is blocked at ${item.currentStep?.title || 'the current step'}; evidence or operator action is still required. Operation: ${item.id}.`;
  return `${label} advanced to ${item.currentStep?.title || item.status}. ${progress}. Operation: ${item.id}.`;
}

export function shouldAnnounceOperationTransition(operation) {
  const item = toMobileOperationSummary(operation);
  return TERMINAL_STATUSES.has(item.status)
    || item.needsApproval
    || item.needsRecovery
    || ['waiting_for_approval', 'blocked', 'recovery_required'].includes(item.status);
}

function readStoredTracking(storage) {
  try {
    const value = JSON.parse(storage?.getItem(STORAGE_KEY) || '{}');
    return { id: bounded(value.id, 160), signature: bounded(value.signature, 1_000) };
  } catch {
    return { id: '', signature: '' };
  }
}

export function createMarcusMobileOperationTracker({
  loadSummaries,
  onRender = () => {},
  onTransition = () => {},
  storage = globalThis.localStorage,
  timers = globalThis,
  activePollMs = 5_000,
  idlePollMs = 30_000,
} = {}) {
  if (typeof loadSummaries !== 'function') throw new Error('Mobile operation tracker requires loadSummaries.');
  let state = readStoredTracking(storage);
  let timer = null;
  let stopped = true;
  let refreshInFlight = null;

  const persist = () => {
    try { storage?.setItem(STORAGE_KEY, JSON.stringify(state)); } catch {}
  };
  const schedule = (operation) => {
    if (stopped) return;
    if (timer !== null) timers.clearTimeout(timer);
    const active = operation && !TERMINAL_STATUSES.has(operation.status);
    timer = timers.setTimeout(() => {
      timer = null;
      void refresh();
    }, active ? activePollMs : idlePollMs);
  };
  const refresh = async () => {
    if (refreshInFlight) return refreshInFlight;
    refreshInFlight = (async () => {
      const payload = await loadSummaries();
      const operation = selectTrackedOperation(payload?.operations || payload || [], state.id);
      if (!operation) {
        onRender(null);
        schedule(null);
        return null;
      }
      const signature = operationSignature(operation);
      const previousSignature = state.id === operation.id ? state.signature : '';
      state = { id: operation.id, signature };
      persist();
      onRender(operation);
      if (previousSignature && previousSignature !== signature) onTransition(operation);
      schedule(operation);
      return operation;
    })().finally(() => { refreshInFlight = null; });
    return refreshInFlight;
  };
  const track = (rawOperation) => {
    const operation = toMobileOperationSummary(rawOperation);
    if (!operation.id) return null;
    state = { id: operation.id, signature: operationSignature(operation) };
    persist();
    onRender(operation);
    schedule(operation);
    return operation;
  };
  return {
    track,
    refresh,
    start() {
      stopped = false;
      return refresh();
    },
    stop() {
      stopped = true;
      if (timer !== null) timers.clearTimeout(timer);
      timer = null;
    },
    getTrackedId: () => state.id,
  };
}

if (typeof window !== 'undefined') {
  window.createMarcusMobileOperationTracker = createMarcusMobileOperationTracker;
  window.formatOperationTransition = formatOperationTransition;
  window.shouldAnnounceOperationTransition = shouldAnnounceOperationTransition;
}

export { ACTIVE_STATUSES, TERMINAL_STATUSES };
