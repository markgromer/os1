const MS_PER_DAY = 24 * 60 * 60 * 1000;

export const OperationalTypes = Object.freeze({
  PROJECT: 'Project',
  PERSON: 'Person',
  COMPANY: 'Company',
  CLIENT: 'Client',
  BUSINESS: 'Business',
  CONVERSATION: 'Conversation',
  TASK: 'Task',
  BLOCKER: 'Blocker',
  RISK: 'Risk',
  OPPORTUNITY: 'Opportunity',
  DECISION: 'Decision',
  ACTION: 'Action',
  ACTION_DRAFT: 'ActionDraft',
  MEMORY: 'Memory',
  SYSTEM: 'System',
  TOOL: 'Tool',
  WEBSITE: 'Website',
  PAYMENT: 'Payment',
  SYSTEM_SIGNAL: 'SystemSignal',
  ACTIVE_BRIEF: 'ActiveBrief',
});

export function nowIso(date = new Date()) {
  return date.toISOString();
}

export function safeText(value, max = 2000) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function normalizeBoundedNumber(value, fallback = 0, { min = 0, max = 1 } = {}) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function normalizeImportance(value, priority = 2) {
  const raw = String(value || '').trim().toLowerCase();
  if (['critical', 'high', 'important'].includes(raw)) return 1;
  if (['low', 'minor', 'background'].includes(raw)) return 3;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return Math.max(1, Math.min(3, Math.round(numeric)));
  return Number(priority) === 1 ? 1 : Number(priority) === 3 ? 3 : 2;
}

function normalizeRelatedEntities(input = []) {
  const list = Array.isArray(input) ? input : [input];
  const out = [];
  const seen = new Set();
  for (const value of list) {
    const entity = typeof value === 'string'
      ? { id: safeText(value, 220), type: '', name: '' }
      : {
          id: safeText(value?.id || value?.target || value?.name || value?.title, 220),
          type: safeText(value?.type || value?.kind || '', 80),
          name: safeText(value?.name || value?.title || '', 180),
        };
    if (!entity.id || seen.has(entity.id)) continue;
    seen.add(entity.id);
    out.push(entity);
  }
  return out.slice(0, 12);
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
  const priority = Number.isFinite(Number(input.priority)) ? Number(input.priority) : 2;
  const importance = normalizeImportance(input.importance, priority);
  const dueAt = input.dueAt || '';
  const expiresAt = input.expiresAt || dueAt || '';
  const relatedEntities = normalizeRelatedEntities([
    ...(Array.isArray(input.relatedEntities) ? input.relatedEntities : []),
    input.relatedProjectId ? `project:${input.businessKey || ''}:${input.relatedProjectId}` : '',
    input.relatedClientId ? `client:${input.businessKey || ''}:${input.relatedClientId}` : '',
  ].filter(Boolean));
  const nextAction = safeText(input.nextAction || input.recommendedAction, 500);
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
    projectState: safeText(input.projectState || input.state, 80),
    owner: safeText(input.owner, 160),
    priority,
    importance,
    urgency: normalizeBoundedNumber(input.urgency, 0.35),
    moneyImpact: normalizeBoundedNumber(input.moneyImpact, 0),
    relationshipImpact: normalizeBoundedNumber(input.relationshipImpact, 0),
    riskImpact: normalizeBoundedNumber(input.riskImpact, 0),
    confidence: normalizeBoundedNumber(input.confidence, 0.72),
    createdAt: input.createdAt || now,
    updatedAt: input.updatedAt || input.createdAt || now,
    lastSeenAt: input.lastSeenAt || input.updatedAt || input.createdAt || now,
    dueAt,
    expiresAt,
    nextAction,
    recommendedAction: nextAction,
    relatedEntities,
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

function slugId(value, fallback = 'unknown') {
  const raw = safeText(value || fallback, 180).toLowerCase();
  return raw.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || fallback;
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const text = safeText(value, 500);
    if (text) return text;
  }
  return '';
}

function clientRowsForStore(store) {
  return [
    ...(Array.isArray(store?.clients) ? store.clients : []),
    ...(Array.isArray(store?.clientList) ? store.clientList : []),
    ...(Array.isArray(store?.contacts) ? store.contacts : []),
  ].filter(Boolean);
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

export function classifyProjectActivity({ project, tasks = [], inbox = [], nowMs = Date.now(), activeProjectId = '', historicalDays = 14 }) {
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

  const markOwnedOpenTask = openTasks.some((t) => isMarkOwner(t?.owner));

  if (activeProjectId && projectId === activeProjectId) return 'active';
  if (pinned || dueSoon || unreadOrNew || (markOwnedOpenTask && staleDays <= historicalDays)) return waitingOnExternal ? 'waiting' : 'active';
  if (waitingOnExternal) return 'waiting';
  if (openTasks.length && staleDays <= historicalDays) return 'warming';
  if (openTasks.length) return 'parked';
  if (staleDays >= historicalDays) return 'historical';
  return 'parked';
}

export function deriveProjectState({ project, tasks = [], inbox = [], activityStatus = '', nowMs = Date.now(), historicalDays = 14 }) {
  const explicit = String(project?.projectState || project?.state || project?.operationalState || '').trim().toLowerCase();
  const allowed = new Set(['idea', 'active', 'waiting_on_mark', 'waiting_on_client', 'waiting_on_team', 'blocked', 'review', 'launched', 'complete', 'dormant', 'archived']);
  if (allowed.has(explicit)) return explicit;

  const rawStatus = String(project?.status || project?.activityStatus || '').trim().toLowerCase();
  if (['idea', 'backlog', 'proposal', 'draft'].includes(rawStatus)) return 'idea';
  if (['review', 'qa', 'approval'].includes(rawStatus)) return 'review';
  if (['launched', 'live'].includes(rawStatus)) return 'launched';
  if (['done', 'complete', 'completed', 'closed'].includes(rawStatus)) return 'complete';
  if (['archived', 'cancelled', 'canceled'].includes(rawStatus)) return 'archived';

  const openTasks = (Array.isArray(tasks) ? tasks : []).filter((task) => !isClosedStatus(task?.status));
  const haystack = `${project?.status || ''} ${project?.notes || ''} ${project?.summary || ''} ${project?.risk || ''} ${openTasks.map((task) => `${task?.title || ''} ${task?.status || ''} ${task?.owner || ''}`).join(' ')}`.toLowerCase();
  if (/\b(blocked|stuck|cannot proceed|waiting for mark|needs mark|need mark|mark needs|mark must)\b/.test(haystack)) return /mark/.test(haystack) ? 'waiting_on_mark' : 'blocked';
  if (/\b(waiting on client|client waiting|customer waiting|needs client|waiting for client|waiting on customer)\b/.test(haystack)) return 'waiting_on_client';
  if (/\b(waiting on team|waiting on va|waiting on designer|waiting on developer|waiting on vendor|needs team|needs vendor)\b/.test(haystack)) return 'waiting_on_team';

  const unreadOrNew = (Array.isArray(inbox) ? inbox : []).some((item) => ['', 'new', 'triaged', 'unread'].includes(String(item?.status || '').trim().toLowerCase()));
  const lastActivity = projectLastActivityMs({ tasks, inbox }, project, tasks, inbox);
  const staleDays = daysBetween(nowMs, lastActivity);
  const activity = String(activityStatus || '').trim().toLowerCase();

  if (activity === 'active' || unreadOrNew) return 'active';
  if (activity === 'waiting') return 'waiting_on_client';
  if (activity === 'warming') return 'active';
  if (activity === 'archived') return 'archived';
  if (activity === 'historical' || activity === 'parked' || staleDays >= historicalDays) return 'dormant';
  return openTasks.length ? 'active' : 'dormant';
}

export function normalizeStoreToOperationalState({ store, businessKey = 'personal', businessName = 'Personal', desktopData = null, nowMs = Date.now(), activeProjectId = '', historicalDays = 14 }) {
  const s = store && typeof store === 'object' ? store : {};
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const tasks = Array.isArray(s.tasks) ? s.tasks : [];
  const inbox = Array.isArray(s.inboxItems) ? s.inboxItems : [];
  const clients = clientRowsForStore(s);
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
    const clientName = safeText(project?.clientName || project?.client || project?.companyName, 180);
    const lastActivity = projectLastActivityMs(s, project, projectTasks, projectInbox);
    const activityStatus = classifyProjectActivity({ project, tasks: projectTasks, inbox: projectInbox, nowMs, activeProjectId, historicalDays });
    const projectState = deriveProjectState({ project, tasks: projectTasks, inbox: projectInbox, activityStatus, nowMs, historicalDays });
    normalized.push(makeOperationalObject({
      id: `project:${businessKey}:${project?.id || project?.name}`,
      type: OperationalTypes.PROJECT,
      title: project?.name || 'Untitled project',
      summary: safeText(project?.description || project?.summary || project?.notes || ''),
      businessKey,
      relatedProjectId: project?.id || '',
      source: 'project-store',
      status: activityStatus,
      projectState,
      owner: project?.owner || project?.accountManagerName || '',
      priority: project?.priority || 2,
      importance: project?.importance || project?.priority || 2,
      urgency: activityStatus === 'active' ? 0.75 : activityStatus === 'waiting' ? 0.45 : activityStatus === 'historical' ? 0.05 : 0.25,
      moneyImpact: normalizeBoundedNumber(project?.moneyImpact || project?.projectValue || project?.value || project?.budget, 0, { max: 100000 }) ? 0.75 : 0,
      relationshipImpact: clientName || projectInbox.length ? 0.65 : 0.25,
      riskImpact: /blocked|risk|overdue|late|stuck/i.test(`${project?.status || ''} ${project?.risk || ''} ${project?.notes || ''}`) ? 0.8 : 0,
      confidence: 0.82,
      createdAt: project?.createdAt || '',
      updatedAt: project?.updatedAt || (lastActivity ? new Date(lastActivity).toISOString() : ''),
      lastSeenAt: lastActivity ? new Date(lastActivity).toISOString() : '',
      dueAt: ymd(project?.dueDate),
      expiresAt: ymd(project?.dueDate),
      nextAction: project?.nextAction || '',
      requiresMark: activityStatus === 'active',
      relatedEntities: [
        `business:${businessKey}`,
        project?.id || project?.name ? `project:${businessKey}:${project?.id || project?.name}` : '',
        clientName ? `client:${businessKey}:${clientName.toLowerCase()}` : '',
      ].filter(Boolean),
      evidence: [
        { label: 'activityStatus', value: activityStatus },
        { label: 'projectState', value: projectState },
        { label: 'openTasks', value: projectTasks.filter((t) => !isClosedStatus(t?.status)).length },
        { label: 'inboxItems', value: projectInbox.length },
      ],
      raw: project,
    }));
  }

  for (const task of tasks) {
    if (isClosedStatus(task?.status)) continue;
    const taskText = `${task?.title || ''} ${task?.notes || ''} ${task?.summary || ''} ${task?.status || ''}`;
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
      importance: task?.importance || task?.priority || 2,
      urgency: Number(task?.priority) === 1 ? 0.85 : 0.45,
      relationshipImpact: task?.projectId || task?.project ? 0.35 : 0,
      riskImpact: /blocked|stuck|overdue|urgent/i.test(`${task?.status || ''} ${task?.title || ''} ${task?.notes || ''}`) ? 0.75 : 0,
      confidence: 0.84,
      createdAt: task?.createdAt || '',
      updatedAt: task?.updatedAt || task?.createdAt || '',
      dueAt: ymd(task?.dueDate),
      expiresAt: ymd(task?.dueDate),
      nextAction: task?.nextAction || task?.title || '',
      requiresMark: isMarkOwner(task?.owner),
      relatedEntities: [
        `business:${businessKey}`,
        task?.projectId ? `project:${businessKey}:${task.projectId}` : '',
      ].filter(Boolean),
      evidence: [{ label: 'project', value: task?.project || task?.projectId || '' }],
      raw: task,
    }));
    if (/\b(site|website|domain|wordpress|elementor|hosting|dns|ssl|deploy|deployment|api|integration|automation|webhook|zapier|make\.com|stripe|invoice|payment|billing)\b/i.test(taskText)) {
      const isPayment = /\b(stripe|invoice|payment|billing|charge|subscription|refund)\b/i.test(taskText);
      const isWebsite = /\b(site|website|domain|wordpress|elementor|hosting|dns|ssl|deploy|deployment)\b/i.test(taskText);
      const isAutomation = /\b(api|integration|automation|webhook|zapier|make\.com)\b/i.test(taskText);
      normalized.push(makeOperationalObject({
        id: `system-task:${businessKey}:${task?.id || slugId(task?.title)}`,
        type: isPayment ? OperationalTypes.PAYMENT : isWebsite ? OperationalTypes.WEBSITE : isAutomation ? OperationalTypes.TOOL : OperationalTypes.SYSTEM_SIGNAL,
        title: task?.title || 'Operational system task',
        summary: safeText(task?.notes || task?.summary || task?.title || ''),
        businessKey,
        relatedProjectId: task?.projectId || '',
        source: 'task-store',
        status: task?.status || 'open',
        owner: task?.owner || '',
        priority: task?.priority || 2,
        importance: Number(task?.priority) === 1 ? 1 : 2,
        urgency: Number(task?.priority) === 1 ? 0.72 : 0.42,
        moneyImpact: isPayment ? 0.82 : 0.2,
        relationshipImpact: task?.projectId || task?.project ? 0.45 : 0.2,
        riskImpact: /\b(broken|down|failed|error|blocked|urgent|overdue|expired|disconnect|missing)\b/i.test(taskText) ? 0.82 : 0.45,
        confidence: 0.74,
        createdAt: task?.createdAt || '',
        updatedAt: task?.updatedAt || task?.createdAt || '',
        dueAt: ymd(task?.dueDate),
        expiresAt: ymd(task?.dueDate),
        nextAction: isPayment ? 'Review payment or billing item.' : isWebsite ? 'Review website/domain/hosting operational state.' : 'Review system or automation item.',
        requiresMark: isMarkOwner(task?.owner) || /\b(approve|confirm|decide|need|blocked)\b/i.test(taskText),
        relatedEntities: [
          `business:${businessKey}`,
          task?.projectId ? `project:${businessKey}:${task.projectId}` : '',
          isPayment ? `payment:${businessKey}:${task?.id || slugId(task?.title)}` : '',
          isWebsite ? `website:${businessKey}:${task?.projectId || slugId(task?.project || task?.title)}` : '',
          isAutomation ? `tool:${businessKey}:${task?.projectId || slugId(task?.project || task?.title)}` : '',
        ].filter(Boolean),
        evidence: [{ label: 'taskText', value: safeText(taskText, 240) }],
        raw: task,
      }));
    }
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
      importance: needsMark ? 1 : 2,
      urgency: needsMark ? 0.78 : 0.36,
      moneyImpact: /\b(price|pricing|quote|estimate|invoice|payment|billing|budget|contract)\b/i.test(text) ? 0.8 : 0,
      relationshipImpact: sender && sender !== source ? 0.72 : 0.35,
      riskImpact: /\b(angry|upset|frustrated|blocked|stuck|urgent|deadline|cancel|cancellation|escalat)\b/i.test(text) ? 0.85 : 0,
      confidence: source === 'inbox' ? 0.62 : 0.76,
      createdAt: item?.createdAt || item?.date || '',
      updatedAt: item?.updatedAt || item?.createdAt || item?.date || '',
      lastSeenAt: item?.updatedAt || item?.createdAt || item?.date || '',
      nextAction: needsMark ? 'Prepare a reply or delegation path.' : 'Monitor unless it becomes actionable.',
      requiresMark: needsMark,
      canAutonomouslyPrepare: true,
      requiresApproval: needsMark,
      relatedEntities: [
        `business:${businessKey}`,
        item?.projectId ? `project:${businessKey}:${item.projectId}` : '',
        sender ? `person:${businessKey}:${sender.toLowerCase()}` : '',
        sender ? `client:${businessKey}:${sender.toLowerCase()}` : '',
      ].filter(Boolean),
      evidence: [{ label: 'preview', value: text.slice(0, 220) }],
      raw: item,
    }));
    if (/\b(site|website|domain|wordpress|elementor|hosting|dns|ssl|deploy|deployment|api|integration|automation|webhook|stripe|invoice|payment|billing|subscription)\b/i.test(text)) {
      const isPayment = /\b(stripe|invoice|payment|billing|subscription|charge|refund)\b/i.test(text);
      const isWebsite = /\b(site|website|domain|wordpress|elementor|hosting|dns|ssl|deploy|deployment)\b/i.test(text);
      normalized.push(makeOperationalObject({
        id: `system-message:${businessKey}:${item?.id || item?.externalId || slugId(text.slice(0, 40))}`,
        type: isPayment ? OperationalTypes.PAYMENT : isWebsite ? OperationalTypes.WEBSITE : OperationalTypes.SYSTEM_SIGNAL,
        title: isPayment ? `Payment or billing mentioned by ${sender}` : isWebsite ? `Website/system mentioned by ${sender}` : `Integration/system mentioned by ${sender}`,
        summary: text,
        businessKey,
        relatedProjectId: item?.projectId || '',
        relatedClientId: sender,
        source,
        status,
        owner: needsMark ? 'Mark' : '',
        priority: needsMark ? 1 : 2,
        importance: needsMark ? 1 : 2,
        urgency: needsMark ? 0.74 : 0.4,
        moneyImpact: isPayment ? 0.86 : 0.25,
        relationshipImpact: sender && sender !== source ? 0.68 : 0.3,
        riskImpact: /\b(broken|down|failed|error|blocked|urgent|overdue|expired|disconnect|missing|cancel)\b/i.test(text) ? 0.84 : 0.48,
        confidence: 0.68,
        createdAt: item?.createdAt || item?.date || '',
        updatedAt: item?.updatedAt || item?.createdAt || item?.date || '',
        lastSeenAt: item?.updatedAt || item?.createdAt || item?.date || '',
        nextAction: isPayment ? 'Review billing/payment impact and decide next step.' : 'Review system impact and prepare a response if needed.',
        requiresMark: needsMark,
        canAutonomouslyPrepare: true,
        requiresApproval: needsMark,
        relatedEntities: [
          `business:${businessKey}`,
          item?.projectId ? `project:${businessKey}:${item.projectId}` : '',
          sender ? `person:${businessKey}:${sender.toLowerCase()}` : '',
          isPayment ? `payment:${businessKey}:${slugId(sender)}` : '',
          isWebsite ? `website:${businessKey}:${item?.projectId || slugId(sender)}` : '',
        ].filter(Boolean),
        evidence: [{ label: 'message', value: text.slice(0, 220) }],
        raw: item,
      }));
    }
  }

  for (const client of clients) {
    const name = firstNonEmpty(client?.name, client?.clientName, client?.companyName, client?.contactName, client?.email, 'Unknown client');
    const website = firstNonEmpty(client?.website, client?.site, client?.url, client?.domain);
    const invoiceUrl = firstNonEmpty(client?.stripeInvoiceUrl, client?.invoiceUrl, client?.paymentUrl, client?.billingUrl);
    const clientStatus = safeText(client?.status || client?.state || '', 80);
    if (website) {
      normalized.push(makeOperationalObject({
        id: `website:${businessKey}:${slugId(website || name)}`,
        type: OperationalTypes.WEBSITE,
        title: `${name} website`,
        summary: website,
        businessKey,
        relatedClientId: name,
        source: 'client-store',
        status: clientStatus || 'known',
        urgency: /broken|down|error|review|launch|urgent/i.test(`${clientStatus} ${client?.notes || ''}`) ? 0.62 : 0.22,
        importance: /launch|urgent|review/i.test(`${clientStatus} ${client?.notes || ''}`) ? 1 : 2,
        moneyImpact: 0.25,
        relationshipImpact: 0.55,
        riskImpact: /broken|down|error|expired|ssl|dns|hosting/i.test(`${clientStatus} ${client?.notes || ''}`) ? 0.78 : 0.2,
        confidence: 0.7,
        createdAt: client?.createdAt || '',
        updatedAt: client?.updatedAt || client?.createdAt || '',
        nextAction: 'Monitor as client website context; review if a related signal appears.',
        relatedEntities: [`business:${businessKey}`, `client:${businessKey}:${name.toLowerCase()}`, `website:${businessKey}:${slugId(website)}`],
        evidence: [{ label: 'website', value: website }],
        raw: client,
      }));
    }
    if (invoiceUrl || /\b(invoice|payment|billing|subscription|stripe|past due|unpaid)\b/i.test(`${clientStatus} ${client?.notes || ''} ${client?.tags || ''}`)) {
      normalized.push(makeOperationalObject({
        id: `payment:${businessKey}:${slugId(invoiceUrl || name)}`,
        type: OperationalTypes.PAYMENT,
        title: `${name} billing context`,
        summary: invoiceUrl || safeText(client?.notes || clientStatus || 'Billing/payment context is present.'),
        businessKey,
        relatedClientId: name,
        source: 'client-store',
        status: clientStatus || 'known',
        urgency: /past due|unpaid|overdue|failed|urgent/i.test(`${clientStatus} ${client?.notes || ''}`) ? 0.78 : 0.28,
        importance: /past due|unpaid|overdue|failed/i.test(`${clientStatus} ${client?.notes || ''}`) ? 1 : 2,
        moneyImpact: 0.86,
        relationshipImpact: 0.42,
        riskImpact: /past due|unpaid|overdue|failed|cancel/i.test(`${clientStatus} ${client?.notes || ''}`) ? 0.82 : 0.32,
        confidence: 0.72,
        createdAt: client?.createdAt || '',
        updatedAt: client?.updatedAt || client?.createdAt || '',
        nextAction: 'Review billing/payment status before it becomes relationship or cash-flow risk.',
        relatedEntities: [`business:${businessKey}`, `client:${businessKey}:${name.toLowerCase()}`, `payment:${businessKey}:${slugId(name)}`],
        evidence: [{ label: 'billing', value: invoiceUrl || safeText(client?.notes || clientStatus, 220) }],
        raw: client,
      }));
    }
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
      importance: 2,
      confidence: 0.7,
      expiresAt: new Date(nowMs + MS_PER_DAY).toISOString(),
      relatedEntities: [`business:${businessKey}`],
      raw: desktopData,
    }));
  }

  return normalized;
}
