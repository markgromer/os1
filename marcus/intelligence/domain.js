const MS_PER_DAY = 24 * 60 * 60 * 1000;

export const OperationalTypes = Object.freeze({
  PROJECT: 'Project',
  CLIENT: 'Client',
  BUSINESS: 'Business',
  CONVERSATION: 'Conversation',
  TASK: 'Task',
  BLOCKER: 'Blocker',
  RISK: 'Risk',
  OPPORTUNITY: 'Opportunity',
  DECISION: 'Decision',
  ACTION_DRAFT: 'ActionDraft',
  SYSTEM_SIGNAL: 'SystemSignal',
  ACTIVE_BRIEF: 'ActiveBrief',
});

export function nowIso(date = new Date()) {
  return date.toISOString();
}

export function safeText(value, max = 2000) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

export function parseTime(value) {
  if (!value) return 0;
  if (value instanceof Date) {
    const ms = value.getTime();
    return Number.isFinite(ms) ? ms : 0;
  }
  const raw = String(value || '').trim();
  if (!raw) return 0;
  const ms = Date.parse(raw);
  return Number.isFinite(ms) ? ms : 0;
}

export function daysBetween(nowMs, thenMs) {
  if (!thenMs) return 999;
  return Math.max(0, Math.floor((nowMs - thenMs) / MS_PER_DAY));
}

export function ymd(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const direct = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (direct) return direct[0];
  const ms = Date.parse(raw);
  return Number.isFinite(ms) ? new Date(ms).toISOString().slice(0, 10) : '';
}

export function makeOperationalObject(input = {}) {
  const now = nowIso();
  return {
    id: safeText(input.id || `${input.type || 'Signal'}:${Math.random().toString(16).slice(2)}`, 160),
    type: input.type || 'SystemSignal',
    title: safeText(input.title, 240),
    summary: safeText(input.summary, 1200),
    businessKey: safeText(input.businessKey, 80),
    relatedProjectId: safeText(input.relatedProjectId, 160),
    relatedClientId: safeText(input.relatedClientId, 160),
    source: safeText(input.source, 80),
    status: safeText(input.status || 'open', 80),
    owner: safeText(input.owner, 160),
    priority: Number.isFinite(Number(input.priority)) ? Number(input.priority) : 2,
    urgency: Number.isFinite(Number(input.urgency)) ? Number(input.urgency) : 0.35,
    confidence: Number.isFinite(Number(input.confidence)) ? Math.max(0, Math.min(1, Number(input.confidence))) : 0.72,
    createdAt: input.createdAt || now,
    updatedAt: input.updatedAt || input.createdAt || now,
    lastSeenAt: input.lastSeenAt || input.updatedAt || input.createdAt || now,
    dueAt: input.dueAt || '',
    nextAction: safeText(input.nextAction, 500),
    requiresMark: Boolean(input.requiresMark),
    canAutonomouslyPrepare: input.canAutonomouslyPrepare !== false,
    requiresApproval: Boolean(input.requiresApproval),
    evidence: Array.isArray(input.evidence) ? input.evidence.slice(0, 8) : [],
    sourceRefs: Array.isArray(input.sourceRefs) ? input.sourceRefs.slice(0, 8) : [],
    raw: input.raw || undefined,
  };
}

export function isClosedStatus(status) {
  return ['done', 'complete', 'completed', 'closed', 'archived', 'cancelled', 'canceled'].includes(String(status || '').trim().toLowerCase());
}

export function isMarkOwner(owner) {
  const raw = String(owner || '').trim().toLowerCase();
  return !raw || ['mark', 'me', 'owner', 'client'].includes(raw);
}

function linkedTasksForProject(store, project) {
  const projectId = String(project?.id || '').trim();
  const name = String(project?.name || '').trim();
  return (Array.isArray(store?.tasks) ? store.tasks : []).filter((task) => {
    return String(task?.projectId || '').trim() === projectId || String(task?.project || '').trim() === name || String(task?.project || '').trim() === projectId;
  });
}

function linkedInboxForProject(store, project) {
  const projectId = String(project?.id || '').trim();
  const name = String(project?.name || '').trim().toLowerCase();
  return (Array.isArray(store?.inboxItems) ? store.inboxItems : []).filter((item) => {
    const itemProjectId = String(item?.projectId || '').trim();
    const itemProject = String(item?.projectName || item?.project || '').trim().toLowerCase();
    return (projectId && itemProjectId === projectId) || (name && itemProject === name);
  });
}

export function projectLastActivityMs(store, project, tasks = linkedTasksForProject(store, project), inbox = linkedInboxForProject(store, project)) {
  const marks = [];
  const push = (value) => {
    const ms = parseTime(value);
    if (ms > 0) marks.push(ms);
  };
  push(project?.updatedAt);
  push(project?.createdAt);
  push(store?.projectScratchpads?.[project?.id]?.updatedAt);
  push(store?.projectChats?.[project?.id]?.updatedAt);
  for (const task of tasks) {
    push(task?.updatedAt);
    push(task?.createdAt);
    push(task?.dueDate);
  }
  for (const item of inbox) {
    push(item?.updatedAt);
    push(item?.createdAt);
    push(item?.date);
  }
  return marks.length ? Math.max(...marks) : 0;
}

export function classifyProjectActivity({ project, tasks = [], inbox = [], nowMs = Date.now(), activeProjectId = '', historicalDays = 75 }) {
  const explicit = String(project?.operationalStatus || project?.activityStatus || '').trim().toLowerCase();
  if (explicit) return explicit;
  if (isClosedStatus(project?.status)) return 'archived';
  const projectId = String(project?.id || '').trim();
  const openTasks = tasks.filter((task) => !isClosedStatus(task?.status));
  const dueSoon = openTasks.some((task) => {
    const due = ymd(task?.dueDate);
    if (!due) return false;
    const dueMs = Date.parse(`${due}T23:59:59Z`);
    return Number.isFinite(dueMs) && dueMs <= nowMs + (7 * MS_PER_DAY);
  });
  const unreadOrNew = inbox.some((item) => ['', 'new', 'triaged', 'unread'].includes(String(item?.status || '').trim().toLowerCase()));
  const waitingOnExternal = openTasks.some((task) => /client|customer|team|va|designer|developer|vendor|waiting/i.test(`${task?.owner || ''} ${task?.status || ''} ${task?.title || ''}`));
  const lastActivity = projectLastActivityMs({ tasks, inbox }, project, tasks, inbox);
  const staleDays = daysBetween(nowMs, lastActivity);
  const pinned = Boolean(project?.pinned || project?.isPinned || project?.starred);

  if (activeProjectId && projectId === activeProjectId) return 'active';
  if (pinned || dueSoon || unreadOrNew || openTasks.some((t) => isMarkOwner(t?.owner))) return waitingOnExternal ? 'waiting' : 'active';
  if (waitingOnExternal) return 'waiting';
  if (openTasks.length && staleDays <= 30) return 'warming';
  if (openTasks.length) return 'parked';
  if (staleDays >= historicalDays) return 'historical';
  return 'parked';
}

export function normalizeStoreToOperationalState({ store, businessKey = 'personal', businessName = 'Personal', desktopData = null, nowMs = Date.now(), activeProjectId = '', historicalDays = 75 }) {
  const s = store && typeof store === 'object' ? store : {};
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const tasks = Array.isArray(s.tasks) ? s.tasks : [];
  const inbox = Array.isArray(s.inboxItems) ? s.inboxItems : [];
  const normalized = [];

  normalized.push(makeOperationalObject({
    id: `business:${businessKey}`,
    type: OperationalTypes.BUSINESS,
    title: businessName,
    summary: `Operational context for ${businessName}.`,
    businessKey,
    source: 'settings',
    confidence: 1,
  }));

  for (const project of projects) {
    const projectTasks = linkedTasksForProject(s, project);
    const projectInbox = linkedInboxForProject(s, project);
    const lastActivity = projectLastActivityMs(s, project, projectTasks, projectInbox);
    const activityStatus = classifyProjectActivity({ project, tasks: projectTasks, inbox: projectInbox, nowMs, activeProjectId, historicalDays });
    normalized.push(makeOperationalObject({
      id: `project:${businessKey}:${project?.id || project?.name}`,
      type: OperationalTypes.PROJECT,
      title: project?.name || 'Untitled project',
      summary: safeText(project?.description || project?.summary || project?.notes || ''),
      businessKey,
      relatedProjectId: project?.id || '',
      source: 'project-store',
      status: activityStatus,
      owner: project?.owner || project?.accountManagerName || '',
      priority: project?.priority || 2,
      urgency: activityStatus === 'active' ? 0.75 : activityStatus === 'waiting' ? 0.45 : activityStatus === 'historical' ? 0.05 : 0.25,
      confidence: 0.82,
      createdAt: project?.createdAt || '',
      updatedAt: project?.updatedAt || (lastActivity ? new Date(lastActivity).toISOString() : ''),
      lastSeenAt: lastActivity ? new Date(lastActivity).toISOString() : '',
      dueAt: ymd(project?.dueDate),
      nextAction: project?.nextAction || '',
      requiresMark: activityStatus === 'active',
      evidence: [
        { label: 'activityStatus', value: activityStatus },
        { label: 'openTasks', value: projectTasks.filter((t) => !isClosedStatus(t?.status)).length },
        { label: 'inboxItems', value: projectInbox.length },
      ],
      raw: project,
    }));
  }

  for (const task of tasks) {
    if (isClosedStatus(task?.status)) continue;
    normalized.push(makeOperationalObject({
      id: `task:${businessKey}:${task?.id || task?.title}`,
      type: OperationalTypes.TASK,
      title: task?.title || 'Untitled task',
      summary: task?.notes || task?.summary || '',
      businessKey,
      relatedProjectId: task?.projectId || '',
      source: 'task-store',
      status: task?.status || 'open',
      owner: task?.owner || '',
      priority: task?.priority || 2,
      urgency: Number(task?.priority) === 1 ? 0.85 : 0.45,
      confidence: 0.84,
      createdAt: task?.createdAt || '',
      updatedAt: task?.updatedAt || task?.createdAt || '',
      dueAt: ymd(task?.dueDate),
      nextAction: task?.nextAction || task?.title || '',
      requiresMark: isMarkOwner(task?.owner),
      evidence: [{ label: 'project', value: task?.project || task?.projectId || '' }],
      raw: task,
    }));
  }

  for (const item of inbox) {
    const text = safeText(item?.text || item?.body || item?.summary || item?.subject || '', 1200);
    if (!text || /smoke|inboxcreate/i.test(text)) continue;
    const source = safeText(item?.source || item?.channel || 'inbox', 80);
    const status = safeText(item?.status || 'new', 80);
    const sender = safeText(item?.contactName || item?.fromName || item?.sender || item?.fromNumber || source, 160);
    const needsMark = /\b(approve|approval|need you|can you|please|urgent|blocked|stuck|reply|respond|confirm|decision)\b/i.test(text);
    normalized.push(makeOperationalObject({
      id: `conversation:${businessKey}:${item?.id || item?.externalId || text.slice(0, 24)}`,
      type: OperationalTypes.CONVERSATION,
      title: sender,
      summary: text,
      businessKey,
      relatedProjectId: item?.projectId || '',
      relatedClientId: sender,
      source,
      status,
      owner: needsMark ? 'Mark' : '',
      priority: needsMark ? 1 : 2,
      urgency: needsMark ? 0.78 : 0.36,
      confidence: source === 'inbox' ? 0.62 : 0.76,
      createdAt: item?.createdAt || item?.date || '',
      updatedAt: item?.updatedAt || item?.createdAt || item?.date || '',
      lastSeenAt: item?.updatedAt || item?.createdAt || item?.date || '',
      nextAction: needsMark ? 'Prepare a reply or delegation path.' : 'Monitor unless it becomes actionable.',
      requiresMark: needsMark,
      canAutonomouslyPrepare: true,
      requiresApproval: needsMark,
      evidence: [{ label: 'preview', value: text.slice(0, 220) }],
      raw: item,
    }));
  }

  if (desktopData?.workspace || desktopData?.windowTitle) {
    normalized.push(makeOperationalObject({
      id: `system:${businessKey}:desktop-context`,
      type: OperationalTypes.SYSTEM_SIGNAL,
      title: desktopData?.workspace?.folderName || desktopData?.windowTitle || 'Desktop context',
      summary: safeText(desktopData?.windowTitle || desktopData?.workspace?.workspacePath || ''),
      businessKey,
      source: 'desktop-agent',
      status: 'active',
      urgency: 0.35,
      confidence: 0.7,
      raw: desktopData,
    }));
  }

  return normalized;
}
