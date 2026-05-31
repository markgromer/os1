import {
  OperationalTypes,
  makeOperationalObject,
  normalizeStoreToOperationalState,
  nowIso,
  safeText,
} from './domain.js';
import { scoreOperationalSignals } from './attention_scoring.js';

function iconForType(type) {
  return {
    [OperationalTypes.CONVERSATION]: 'fa-comments',
    [OperationalTypes.TASK]: 'fa-list-check',
    [OperationalTypes.PROJECT]: 'fa-diagram-project',
    [OperationalTypes.RISK]: 'fa-triangle-exclamation',
    [OperationalTypes.OPPORTUNITY]: 'fa-arrow-trend-up',
    [OperationalTypes.ACTION_DRAFT]: 'fa-wand-magic-sparkles',
    [OperationalTypes.SYSTEM_SIGNAL]: 'fa-wave-square',
  }[type] || 'fa-circle-dot';
}

function priorityForScore(score) {
  if (score >= 78) return 'critical';
  if (score >= 55) return 'warning';
  if (score >= 35) return 'active';
  return 'normal';
}

function asHudItem(scored) {
  const signal = scored.signal;
  return {
    id: signal.id,
    kind: String(signal.type || '').toLowerCase(),
    priority: priorityForScore(scored.score),
    icon: iconForType(signal.type),
    title: signal.title,
    detail: signal.summary || signal.nextAction,
    businessKey: signal.businessKey,
    targetType: signal.type === OperationalTypes.PROJECT ? 'project' : signal.type === OperationalTypes.CONVERSATION ? 'conversation' : signal.type === OperationalTypes.ACTION_DRAFT ? 'draft' : '',
    target: signal.relatedProjectId || signal.id,
    score: scored.score,
    bucket: scored.bucket,
    confidence: scored.confidence,
    reasons: scored.reasons,
    prompt: `Brief me on this ${signal.type}: ${signal.title}. Explain what matters, why, and the next action.`,
  };
}

function createPreparedAction(signal, scored) {
  const projectPhrase = signal.relatedProjectId ? ` for ${signal.relatedProjectId}` : '';
  const isConversation = signal.type === OperationalTypes.CONVERSATION;
  const isTask = signal.type === OperationalTypes.TASK;
  const type = isConversation ? 'draft_client_reply' : isTask ? 'create_internal_task' : 'open_project_workspace';
  const title = isConversation
    ? `Draft reply to ${signal.title}`
    : isTask
      ? `Package next step: ${signal.title}`
      : `Open workspace: ${signal.title}`;
  const approvalReason = isConversation
    ? 'External communication needs Mark approval before sending.'
    : 'MARCUS can prepare the work, but execution remains under Mark control.';

  return makeOperationalObject({
    id: `action:${signal.id}`,
    type: OperationalTypes.ACTION_DRAFT,
    title,
    summary: signal.nextAction || signal.summary || `Prepare action${projectPhrase}.`,
    businessKey: signal.businessKey,
    relatedProjectId: signal.relatedProjectId,
    source: 'active-brief',
    status: 'prepared',
    priority: signal.priority,
    urgency: signal.urgency,
    confidence: scored.confidence,
    requiresMark: true,
    canAutonomouslyPrepare: true,
    requiresApproval: true,
    evidence: signal.evidence,
    raw: {
      title,
      type,
      target: signal.relatedProjectId || signal.relatedClientId || signal.id,
      body: isConversation
        ? `Mark, I can draft a concise reply to ${signal.title} based on: ${safeText(signal.summary, 260)}`
        : `Prepare the next move for ${signal.title}: ${safeText(signal.nextAction || signal.summary, 260)}`,
      riskLevel: scored.score >= 78 ? 'high' : scored.score >= 55 ? 'medium' : 'low',
      requiresApproval: true,
      approvalReason,
      relatedSignalIds: [signal.id],
      suggestedButtonLabel: isConversation ? 'Draft reply' : isTask ? 'Prepare task' : 'Open workspace',
    },
  });
}

function compatibilityProject(signal, scored) {
  const evidence = Object.fromEntries((signal.evidence || []).map((e) => [e.label, e.value]));
  return {
    id: signal.relatedProjectId || signal.id,
    name: signal.title,
    status: signal.status,
    activityStatus: signal.status,
    dueDate: signal.dueAt || '',
    businessKey: signal.businessKey,
    score: scored.score,
    reason: scored.reasons[0] || evidence.activityStatus || 'Operational signal',
    openTasks: Number(evidence.openTasks) || 0,
    urgentTasks: 0,
    inboxCount: Number(evidence.inboxItems) || 0,
    lastActivityAt: signal.lastSeenAt || '',
  };
}

function compatibilityConversation(signal, scored) {
  return {
    id: signal.id.replace(/^conversation:[^:]+:/, ''),
    projectId: signal.relatedProjectId || '',
    projectName: '',
    businessKey: signal.businessKey,
    who: signal.title,
    source: signal.source,
    preview: safeText(signal.summary, 180),
    score: scored.score,
    needsAction: Boolean(signal.requiresMark),
    updatedAt: signal.lastSeenAt || signal.updatedAt || '',
  };
}

function compatibilityTask(signal) {
  return {
    id: signal.id.replace(/^task:[^:]+:/, ''),
    title: signal.title,
    project: '',
    owner: signal.owner,
    priority: signal.priority,
    dueDate: signal.dueAt || '',
    status: signal.status,
    businessKey: signal.businessKey,
  };
}

function narrativeForBrief({ topPriorities, urgentInterrupts, waitingOnMark, preparedActions, lowSignalSuppressedCount }) {
  if (!topPriorities.length && !urgentInterrupts.length) {
    const suppressed = lowSignalSuppressedCount ? ` I suppressed ${lowSignalSuppressedCount} low-signal items.` : '';
    return `Mark, nothing deserves an interruption right now.${suppressed} I am watching for client risk, blocked work, and approval points.`;
  }
  const count = urgentInterrupts.length || topPriorities.length;
  const lead = count === 1 ? 'one thing actually needs attention' : `${Math.min(3, count)} things actually need attention`;
  const first = urgentInterrupts[0] || topPriorities[0];
  const second = topPriorities.find((item) => item.id !== first?.id);
  const prepared = preparedActions.length ? ` I prepared ${preparedActions.length === 1 ? 'an action' : `${preparedActions.length} actions`}, but need approval before anything external happens.` : '';
  const waiting = waitingOnMark.length ? ` ${waitingOnMark.length} item${waitingOnMark.length === 1 ? '' : 's'} are waiting on you.` : '';
  return `Mark, ${lead}. ${first?.title || 'The top signal'} is surfaced because ${first?.reasons?.[0] || 'it affects current execution'}.${second ? ` ${second.title} can stay behind it.` : ''}${waiting}${prepared}`;
}

export function buildActiveBrief({ stores = [], settings = {}, desktopData = null, nowMs = Date.now() } = {}) {
  const historicalDays = Number(settings?.marcusHistoricalProjectDays || settings?.historicalProjectDays || 75);
  const activeProjectId = String(desktopData?.activeProjectId || '').trim();
  const signals = [];

  for (const entry of stores) {
    signals.push(...normalizeStoreToOperationalState({
      store: entry.store,
      businessKey: entry.businessKey,
      businessName: entry.businessName,
      desktopData,
      activeProjectId: entry.activeProjectId || activeProjectId,
      nowMs,
      historicalDays: Number.isFinite(historicalDays) ? historicalDays : 75,
    }));
  }

  const scored = scoreOperationalSignals(signals, { nowMs });
  const visible = scored.filter((item) => item.bucket !== 'archive/noise');
  const suppressed = scored.length - visible.length;
  const urgentInterrupts = visible.filter((item) => item.bucket === 'interrupt_now').slice(0, 5).map(asHudItem);
  const topPriorities = visible.filter((item) => ['interrupt_now', 'today', 'soon'].includes(item.bucket)).slice(0, 8).map(asHudItem);
  const waitingOnMark = visible.filter((item) => item.signal.requiresMark).slice(0, 8).map(asHudItem);
  const waitingOnClients = visible.filter((item) => /client|customer|waiting/i.test(`${item.signal.owner} ${item.signal.status} ${item.signal.summary}`) && !item.signal.requiresMark).slice(0, 8).map(asHudItem);
  const waitingOnTeam = visible.filter((item) => item.bucket === 'delegated').slice(0, 8).map(asHudItem);
  const stalledProjects = visible.filter((item) => item.signal.type === OperationalTypes.PROJECT && ['parked', 'historical'].includes(item.signal.status)).slice(0, 8).map(asHudItem);
  const risks = visible.filter((item) => /risk|blocked|overdue|relationship|financial|revision|delayed/i.test(`${item.reasons.join(' ')} ${item.signal.summary}`)).slice(0, 8).map(asHudItem);
  const opportunities = visible.filter((item) => /opportunity|upgrade|upsell|proposal|referral/i.test(`${item.reasons.join(' ')} ${item.signal.summary}`)).slice(0, 6).map(asHudItem);
  const preparedActionSignals = visible
    .filter((item) => item.signal.canAutonomouslyPrepare && ['interrupt_now', 'today', 'soon'].includes(item.bucket))
    .slice(0, 5)
    .map((item) => createPreparedAction(item.signal, item));
  const preparedActions = preparedActionSignals.map((action) => ({
    id: action.id,
    title: action.raw.title,
    type: action.raw.type,
    summary: action.summary,
    target: action.raw.target,
    body: action.raw.body,
    riskLevel: action.raw.riskLevel,
    requiresApproval: action.raw.requiresApproval,
    approvalReason: action.raw.approvalReason,
    relatedSignalIds: action.raw.relatedSignalIds,
    suggestedButtonLabel: action.raw.suggestedButtonLabel,
    businessKey: action.businessKey,
    confidence: action.confidence,
  }));
  const suggestedDelegations = waitingOnTeam.slice(0, 5);
  const projectSignals = scored.filter((item) => item.signal.type === OperationalTypes.PROJECT);
  const activeProjects = projectSignals.filter((item) => ['active', 'warming', 'waiting'].includes(item.signal.status)).map((item) => compatibilityProject(item.signal, item));
  const activeProject = activeProjects[0] ? {
    id: activeProjects[0].id,
    name: activeProjects[0].name,
    businessKey: activeProjects[0].businessKey,
    businessName: stores.find((s) => s.businessKey === activeProjects[0].businessKey)?.businessName || activeProjects[0].businessKey,
    activityStatus: activeProjects[0].activityStatus,
  } : null;
  const confidence = visible.length
    ? Math.round((visible.reduce((sum, item) => sum + item.confidence, 0) / visible.length) * 100) / 100
    : 0.7;

  const brief = {
    ok: true,
    type: OperationalTypes.ACTIVE_BRIEF,
    timestamp: nowIso(new Date(nowMs)),
    generatedAt: nowIso(new Date(nowMs)),
    currentFocus: activeProject || topPriorities[0] || null,
    activeProject,
    topPriorities,
    urgentInterrupts,
    waitingOnMark,
    waitingOnClients,
    waitingOnTeam,
    stalledProjects,
    risks,
    opportunities,
    preparedActions,
    suggestedDelegations,
    lowSignalSuppressedCount: suppressed,
    confidence,
    narrativeSummary: '',
    operationalSignals: visible.slice(0, 25).map((item) => ({ ...item.signal, score: item.score, bucket: item.bucket, scoreReasons: item.reasons })),
    projectActivity: projectSignals.map((item) => compatibilityProject(item.signal, item)),
    projects: activeProjects.slice(0, 3),
    conversations: scored.filter((item) => item.signal.type === OperationalTypes.CONVERSATION && item.bucket !== 'archive/noise').slice(0, 6).map((item) => compatibilityConversation(item.signal, item)),
    tasks: scored.filter((item) => item.signal.type === OperationalTypes.TASK && item.bucket !== 'archive/noise').slice(0, 4).map((item) => compatibilityTask(item.signal)),
    messageDrafts: preparedActions.filter((item) => item.type === 'draft_client_reply').slice(0, 2).map((item) => ({
      id: item.id,
      conversationId: item.relatedSignalIds?.[0] || '',
      businessKey: item.businessKey,
      to: item.target,
      reason: item.approvalReason,
      body: item.body,
    })),
    stats: {
      openTasks: scored.filter((item) => item.signal.type === OperationalTypes.TASK).length,
      relevantTasks: scored.filter((item) => item.signal.type === OperationalTypes.TASK && item.bucket !== 'archive/noise').length,
      overdueTasks: scored.filter((item) => item.signal.type === OperationalTypes.TASK && item.dueDays !== null && item.dueDays < 0).length,
      dueTodayTasks: scored.filter((item) => item.signal.type === OperationalTypes.TASK && item.dueDays === 0).length,
      inboxActionable: scored.filter((item) => item.signal.type === OperationalTypes.CONVERSATION && item.signal.requiresMark && item.bucket !== 'archive/noise').length,
    },
  };
  brief.narrativeSummary = narrativeForBrief(brief);
  return brief;
}

export function validateActiveBrief(brief) {
  const errors = [];
  if (!brief || typeof brief !== 'object') errors.push('brief must be an object');
  if (brief?.type !== OperationalTypes.ACTIVE_BRIEF) errors.push('type must be ActiveBrief');
  for (const key of ['topPriorities', 'urgentInterrupts', 'waitingOnMark', 'preparedActions']) {
    if (!Array.isArray(brief?.[key])) errors.push(`${key} must be an array`);
  }
  if (typeof brief?.narrativeSummary !== 'string') errors.push('narrativeSummary must be a string');
  return { ok: errors.length === 0, errors };
}
