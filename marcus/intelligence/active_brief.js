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
    importance: signal.importance,
    urgency: signal.urgency,
    moneyImpact: signal.moneyImpact,
    relationshipImpact: signal.relationshipImpact,
    riskImpact: signal.riskImpact,
    relatedEntities: Array.isArray(signal.relatedEntities) ? signal.relatedEntities : [],
    duplicateCount: Number(signal.duplicateCount || scored.duplicateCount || 1),
    duplicateIds: Array.isArray(signal.duplicateIds) ? signal.duplicateIds : Array.isArray(scored.duplicateIds) ? scored.duplicateIds : [],
    expiresAt: signal.expiresAt || '',
    recommendedAction: signal.recommendedAction || signal.nextAction || '',
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
    projectState: signal.projectState || signal.raw?.projectState || signal.raw?.state || '',
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

function uniqueById(rows) {
  const out = [];
  const seen = new Set();
  for (const row of Array.isArray(rows) ? rows : []) {
    const id = safeText(row?.id || row?.name || row?.title, 180);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(row);
  }
  return out;
}

function semanticSignalKey(item) {
  const signal = item?.signal && typeof item.signal === 'object' ? item.signal : {};
  const title = safeText(signal.title || '', 180).toLowerCase().replace(/\s+/g, ' ').trim();
  if (!title) return safeText(signal.id || '', 180);
  const businessKey = safeText(signal.businessKey || '', 80).toLowerCase();
  const target = safeText(signal.relatedProjectId || signal.relatedClientId || signal.relatedCompanyId || signal.relatedPersonId || '', 180).toLowerCase();
  return [businessKey, target, title].join('|');
}

function mergeEvidenceRows(...groups) {
  const out = [];
  const seen = new Set();
  for (const group of groups) {
    for (const row of Array.isArray(group) ? group : []) {
      const key = `${safeText(row?.label || row?.source || row?.why || row?.title, 120)}|${safeText(row?.value || row?.id || row?.summary, 180)}`;
      if (!key.trim() || seen.has(key)) continue;
      seen.add(key);
      out.push(row);
      if (out.length >= 8) return out;
    }
  }
  return out;
}

function compressScoredSignals(scored = []) {
  const byKey = new Map();
  for (const item of Array.isArray(scored) ? scored : []) {
    const key = semanticSignalKey(item);
    if (!key) continue;
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, {
        ...item,
        signal: { ...(item.signal || {}) },
        duplicateIds: [],
        duplicateCount: 1,
      });
      continue;
    }
    existing.duplicateCount = Number(existing.duplicateCount || 1) + 1;
    existing.duplicateIds = [
      ...(Array.isArray(existing.duplicateIds) ? existing.duplicateIds : []),
      safeText(item?.signal?.id || item?.id || '', 180),
    ].filter(Boolean).slice(0, 12);
    existing.score = Math.max(Number(existing.score || 0), Number(item?.score || 0));
    existing.confidence = Math.max(Number(existing.confidence || 0), Number(item?.confidence || 0));
    existing.reasons = Array.from(new Set([...(existing.reasons || []), ...(item?.reasons || [])])).slice(0, 6);
    existing.signal = {
      ...existing.signal,
      urgency: Math.max(Number(existing.signal?.urgency || 0), Number(item?.signal?.urgency || 0)),
      confidence: Math.max(Number(existing.signal?.confidence || 0), Number(item?.signal?.confidence || 0)),
      evidence: mergeEvidenceRows(existing.signal?.evidence, item?.signal?.evidence),
      sourceRefs: mergeEvidenceRows(existing.signal?.sourceRefs, item?.signal?.sourceRefs),
      relatedEntities: Array.from(new Set([...(existing.signal?.relatedEntities || []), ...(item?.signal?.relatedEntities || [])])).slice(0, 12),
      duplicateCount: existing.duplicateCount,
      duplicateIds: existing.duplicateIds,
    };
  }
  return Array.from(byKey.values())
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
}

function buildWorldModel({ stores = [], scored = [] }) {
  const people = [];
  const clients = [];
  const businesses = [];
  const projects = [];
  const tasks = [];
  const messages = [];
  const relationships = [];

  for (const entry of stores) {
    const store = entry?.store && typeof entry.store === 'object' ? entry.store : {};
    const businessKey = safeText(entry?.businessKey || 'personal', 80);
    const businessName = safeText(entry?.businessName || businessKey, 160);
    businesses.push({
      id: `business:${businessKey}`,
      name: businessName,
      type: OperationalTypes.BUSINESS,
      status: businessKey === safeText(entry?.activeBusinessKey) ? 'active' : 'known',
      source: 'settings',
      confidence: 1,
    });

    for (const project of Array.isArray(store.projects) ? store.projects : []) {
      const projectId = safeText(project?.id || project?.name, 180);
      if (!projectId) continue;
      const clientName = safeText(project?.clientName || project?.client || project?.companyName, 180);
      projects.push({
        id: `project:${businessKey}:${projectId}`,
        name: safeText(project?.name || 'Untitled project', 240),
        type: OperationalTypes.PROJECT,
        status: safeText(project?.status || 'known', 80),
        businessKey,
        source: 'project-store',
        confidence: 0.82,
        lastActivityAt: safeText(project?.updatedAt || project?.createdAt, 80),
      });
      if (clientName) {
        const clientId = `client:${businessKey}:${clientName.toLowerCase()}`;
        clients.push({
          id: clientId,
          name: clientName,
          type: OperationalTypes.CLIENT,
          status: 'known',
          businessKey,
          source: 'project-store',
          confidence: 0.74,
        });
        people.push({
          id: `person:${businessKey}:${clientName.toLowerCase()}`,
          name: clientName,
          type: OperationalTypes.PERSON,
          status: 'known',
          businessKey,
          source: 'project-store',
          confidence: 0.62,
        });
        relationships.push({
          from: clientId,
          to: `project:${businessKey}:${projectId}`,
          type: 'client_of_project',
          source: 'project-store',
          confidence: 0.8,
        });
      }
    }

    for (const task of Array.isArray(store.tasks) ? store.tasks : []) {
      const taskId = safeText(task?.id || task?.title, 180);
      if (!taskId) continue;
      tasks.push({
        id: `task:${businessKey}:${taskId}`,
        name: safeText(task?.title || 'Untitled task', 240),
        type: OperationalTypes.TASK,
        status: safeText(task?.status || 'open', 80),
        businessKey,
        source: 'task-store',
        confidence: 0.82,
      });
      const projectRef = safeText(task?.projectId || task?.project, 180);
      if (projectRef) {
        relationships.push({
          from: `task:${businessKey}:${taskId}`,
          to: `project:${businessKey}:${projectRef}`,
          type: 'task_relates_to_project',
          source: 'task-store',
          confidence: 0.7,
        });
      }
    }

    for (const item of Array.isArray(store.inboxItems) ? store.inboxItems : []) {
      const sender = safeText(item?.contactName || item?.fromName || item?.sender || item?.fromNumber || item?.source, 180);
      const messageId = safeText(item?.id || item?.externalId || item?.text || item?.subject, 180);
      if (sender) {
        people.push({
          id: `person:${businessKey}:${sender.toLowerCase()}`,
          name: sender,
          type: OperationalTypes.PERSON,
          status: 'known',
          businessKey,
          source: safeText(item?.source || 'inbox', 80),
          confidence: 0.7,
        });
      }
      if (messageId) {
        messages.push({
          id: `message:${businessKey}:${messageId}`,
          name: safeText(item?.subject || item?.title || item?.text || 'Message', 240),
          type: OperationalTypes.CONVERSATION,
          status: safeText(item?.status || 'new', 80),
          businessKey,
          source: safeText(item?.source || 'inbox', 80),
          confidence: 0.68,
        });
      }
      if (sender && messageId) {
        relationships.push({
          from: `person:${businessKey}:${sender.toLowerCase()}`,
          to: `message:${businessKey}:${messageId}`,
          type: 'person_sent_message',
          source: safeText(item?.source || 'inbox', 80),
          confidence: 0.7,
        });
      }
    }
  }

  const activeWork = scored
    .filter((item) => item.signal?.type === OperationalTypes.PROJECT && ['active', 'waiting', 'warming'].includes(item.signal.status))
    .slice(0, 12)
    .map((item) => compatibilityProject(item.signal, item));
  const knownHistory = scored
    .filter((item) => item.signal?.type === OperationalTypes.PROJECT && ['parked', 'historical', 'archived'].includes(item.signal.status))
    .slice(0, 24)
    .map((item) => compatibilityProject(item.signal, item));

  return {
    entities: {
      people: uniqueById(people).slice(0, 60),
      clients: uniqueById(clients).slice(0, 60),
      businesses: uniqueById(businesses),
      projects: uniqueById(projects).slice(0, 120),
      tasks: uniqueById(tasks).slice(0, 120),
      messages: uniqueById(messages).slice(0, 120),
    },
    relationships: uniqueById(relationships.map((r, idx) => ({ id: `${r.type}:${r.from}:${r.to}:${idx}`, ...r }))).slice(0, 160),
    activeWork,
    knownHistory,
    counts: {
      people: uniqueById(people).length,
      clients: uniqueById(clients).length,
      businesses: uniqueById(businesses).length,
      projects: uniqueById(projects).length,
      tasks: uniqueById(tasks).length,
      messages: uniqueById(messages).length,
      relationships: relationships.length,
    },
  };
}

function addRelationship(relationships, from, to, type, source = 'active-brief', confidence = 0.7) {
  const safeFrom = safeText(from, 220);
  const safeTo = safeText(to, 220);
  const safeType = safeText(type, 120);
  if (!safeFrom || !safeTo || !safeType) return;
  relationships.push({
    from: safeFrom,
    to: safeTo,
    type: safeType,
    source: safeText(source, 80),
    confidence: Number.isFinite(Number(confidence)) ? Number(confidence) : 0.7,
  });
}

function relatedEntityIds(item) {
  const out = [];
  const related = Array.isArray(item?.relatedEntities) ? item.relatedEntities : [];
  for (const entity of related) {
    const id = typeof entity === 'string' ? entity : entity?.id;
    const clean = safeText(id, 220);
    if (clean) out.push(clean);
  }
  for (const value of [item?.target, item?.targetId, item?.relatedProjectId, item?.relatedClientId, item?.businessKey ? `business:${item.businessKey}` : '']) {
    const clean = safeText(value, 220);
    if (clean && !out.includes(clean)) out.push(clean);
  }
  return out.slice(0, 12);
}

function augmentWorldModelWithOperationalContext(worldModel, { visible = [], actionQueue = [], decisionQueue = [], systemHealth = {}, memoryPulse = {} } = {}) {
  const base = worldModel && typeof worldModel === 'object' ? worldModel : {};
  const entities = base.entities && typeof base.entities === 'object' ? base.entities : {};
  const relationships = Array.isArray(base.relationships) ? [...base.relationships] : [];
  const signals = [];
  const decisions = [];
  const actions = [];
  const systems = [];
  const memory = [];

  for (const item of Array.isArray(visible) ? visible : []) {
    const signal = item?.signal || {};
    if (!signal.id) continue;
    signals.push({
      id: signal.id,
      name: signal.title || signal.id,
      type: signal.type || OperationalTypes.SYSTEM_SIGNAL,
      status: signal.status || item.bucket || 'open',
      businessKey: signal.businessKey || '',
      source: signal.source || 'active-brief',
      confidence: item.confidence ?? signal.confidence ?? 0.7,
      priority: signal.priority,
      importance: signal.importance,
      score: item.score,
      lastActivityAt: signal.lastSeenAt || signal.updatedAt || signal.createdAt || '',
      nextAction: signal.recommendedAction || signal.nextAction || '',
      relatedEntities: signal.relatedEntities || [],
    });
    for (const relatedId of relatedEntityIds(signal)) addRelationship(relationships, signal.id, relatedId, 'signal_relates_to_entity', signal.source, signal.confidence);
    if (signal.type === OperationalTypes.SYSTEM_SIGNAL || signal.type === OperationalTypes.SYSTEM || signal.type === OperationalTypes.TOOL || signal.type === OperationalTypes.WEBSITE || signal.type === OperationalTypes.PAYMENT) {
      systems.push({
        id: signal.id,
        name: signal.title || signal.id,
        type: signal.type,
        status: signal.status || 'active',
        businessKey: signal.businessKey || '',
        source: signal.source || 'active-brief',
        confidence: signal.confidence ?? 0.7,
        lastActivityAt: signal.lastSeenAt || signal.updatedAt || signal.createdAt || '',
      });
    }
  }

  for (const decision of Array.isArray(decisionQueue) ? decisionQueue : []) {
    if (!decision?.id) continue;
    decisions.push({
      id: decision.id,
      name: decision.question || decision.title || decision.id,
      type: OperationalTypes.DECISION,
      status: decision.approvalRequired ? 'approval_required' : 'needs_decision',
      businessKey: decision.businessKey || '',
      source: decision.source || 'decision-queue',
      confidence: decision.confidence ?? 0.72,
      score: decision.score,
      nextAction: decision.recommendedAction || '',
      relatedEntities: decision.relatedEntities || [],
    });
    if (decision.sourceSignalId) addRelationship(relationships, decision.id, decision.sourceSignalId, 'decision_from_signal', decision.source, decision.confidence);
    if (decision.sourceActionId) addRelationship(relationships, decision.id, decision.sourceActionId, 'decision_controls_action', decision.source, decision.confidence);
    for (const relatedId of relatedEntityIds(decision)) addRelationship(relationships, decision.id, relatedId, 'decision_relates_to_entity', decision.source, decision.confidence);
  }

  for (const action of Array.isArray(actionQueue) ? actionQueue : []) {
    if (!action?.id) continue;
    actions.push({
      id: action.id,
      name: action.title || action.summary || action.id,
      type: OperationalTypes.ACTION_DRAFT,
      status: action.lifecycle || (action.approvalRequired || action.requiresApproval ? 'draft_action' : 'suggested_action'),
      businessKey: action.businessKey || '',
      source: action.source || 'action-queue',
      confidence: action.confidence ?? 0.72,
      nextAction: action.suggestedButtonLabel || action.summary || '',
      relatedEntities: action.relatedEntities || [],
    });
    const signalIds = Array.isArray(action.relatedSignalIds) ? action.relatedSignalIds : [];
    for (const signalId of signalIds) addRelationship(relationships, action.id, signalId, 'action_prepared_from_signal', action.source, action.confidence);
    for (const relatedId of relatedEntityIds(action)) addRelationship(relationships, action.id, relatedId, 'action_relates_to_entity', action.source, action.confidence);
  }

  for (const system of Array.isArray(systemHealth?.items) ? systemHealth.items : []) {
    const id = safeText(system?.id || `system:${system?.title || system?.name || system?.type || 'unknown'}`, 220);
    if (!id) continue;
    systems.push({
      id,
      name: safeText(system?.title || system?.name || id, 240),
      type: system?.type || OperationalTypes.SYSTEM,
      status: safeText(system?.status || 'unknown', 80),
      source: safeText(system?.source || 'system-health', 80),
      confidence: Number.isFinite(Number(system?.confidence)) ? Number(system.confidence) : 0.72,
      nextAction: safeText(system?.recommendedAction || system?.summary || '', 500),
    });
  }

  for (const record of Array.isArray(memoryPulse?.records) ? memoryPulse.records : []) {
    if (!record?.id) continue;
    memory.push({
      id: record.id,
      name: record.title || record.id,
      type: OperationalTypes.MEMORY,
      status: record.status || 'active',
      source: record.source || 'memory-pulse',
      confidence: record.confidence ?? 0.72,
      lastActivityAt: record.lastConfirmedAt || record.createdAt || '',
      summary: record.summary || '',
      relatedEntities: record.relatedEntities || [],
    });
    for (const relatedId of relatedEntityIds(record)) addRelationship(relationships, record.id, relatedId, 'memory_relates_to_entity', record.source, record.confidence);
  }

  const nextEntities = {
    ...entities,
    signals: uniqueById([...(entities.signals || []), ...signals]).slice(0, 160),
    decisions: uniqueById([...(entities.decisions || []), ...decisions]).slice(0, 80),
    actions: uniqueById([...(entities.actions || []), ...actions]).slice(0, 80),
    systems: uniqueById([...(entities.systems || []), ...systems]).slice(0, 80),
    memory: uniqueById([...(entities.memory || []), ...memory]).slice(0, 80),
  };
  const uniqueRelationships = uniqueById(relationships.map((r, idx) => ({
    id: r.id || `${r.type}:${r.from}:${r.to}:${idx}`,
    ...r,
  }))).slice(0, 260);
  const counts = { ...(base.counts || {}) };
  for (const [key, list] of Object.entries(nextEntities)) counts[key] = Array.isArray(list) ? list.length : 0;
  counts.relationships = uniqueRelationships.length;
  return {
    ...base,
    entities: nextEntities,
    relationships: uniqueRelationships,
    counts,
  };
}

function buildMemoryPulse({ stores = [], settings = {}, visible = [], scored = [], lowSignalSuppressedCount = 0 }) {
  const records = [];
  const now = nowIso();
  const globalMemory = safeText(settings?.agentMemory || settings?.globalMemory || '', 1200);
  if (globalMemory) {
    records.push({
      id: 'memory:settings:agent-memory',
      title: 'Global operator memory',
      summary: globalMemory,
      source: 'settings.agentMemory',
      confidence: 0.86,
      status: 'active',
      createdAt: '',
      lastConfirmedAt: '',
      relatedEntities: [],
      controls: ['correct', 'important', 'archive', 'forget', 'pin'],
    });
  }

  for (const entry of stores) {
    const store = entry?.store && typeof entry.store === 'object' ? entry.store : {};
    const businessKey = safeText(entry?.businessKey || 'personal', 80);
    const scratchpads = store.projectScratchpads && typeof store.projectScratchpads === 'object' ? store.projectScratchpads : {};
    for (const [projectId, pad] of Object.entries(scratchpads).slice(0, 10)) {
      const text = safeText(pad?.text || pad?.content || pad?.summary || '', 500);
      if (!text) continue;
      records.push({
        id: `memory:${businessKey}:scratchpad:${projectId}`,
        title: `Project scratchpad: ${projectId}`,
        summary: text,
        source: 'projectScratchpads',
        confidence: 0.76,
        status: 'active',
        createdAt: safeText(pad?.createdAt || '', 80),
        lastConfirmedAt: safeText(pad?.updatedAt || '', 80),
        relatedEntities: [`project:${businessKey}:${projectId}`],
      controls: ['important', 'archive', 'outdated', 'pin'],
      });
    }
    const marcusNotes = store.marcusNotes && typeof store.marcusNotes === 'object' ? store.marcusNotes : {};
    for (const [projectId, notes] of Object.entries(marcusNotes).slice(0, 10)) {
      const noteList = Array.isArray(notes) ? notes : [];
      const latest = noteList[noteList.length - 1];
      const text = safeText(latest?.text || latest?.content || latest?.summary || '', 500);
      if (!text) continue;
      records.push({
        id: `memory:${businessKey}:marcus-note:${projectId}`,
        title: `MARCUS note: ${projectId}`,
        summary: text,
        source: 'marcusNotes',
        confidence: 0.74,
        status: 'active',
        createdAt: safeText(latest?.createdAt || '', 80),
        lastConfirmedAt: safeText(latest?.updatedAt || latest?.createdAt || '', 80),
        relatedEntities: [`project:${businessKey}:${projectId}`],
      controls: ['important', 'archive', 'outdated', 'pin'],
      });
    }
  }

  const lowConfidence = visible.filter((item) => Number(item.confidence) < 0.55).slice(0, 5);
  const staleProjects = scored.filter((item) => item.signal?.type === OperationalTypes.PROJECT && ['parked', 'historical'].includes(item.signal.status)).slice(0, 8);
  return {
    newFacts: records.slice(0, 4).map((r) => ({ title: r.title, summary: r.summary, source: r.source, confidence: r.confidence })),
    staleAssumptions: staleProjects.map((item) => ({
      title: item.signal.title,
      summary: `${item.signal.title} is ${item.signal.status}; keep it in known history unless Mark pins or reactivates it.`,
      source: item.signal.source,
      confidence: item.confidence,
    })),
    conflicts: [],
    uncertain: lowConfidence.map((item) => ({
      title: item.signal.title,
      summary: item.signal.summary || item.signal.nextAction,
      source: item.signal.source,
      confidence: item.confidence,
    })),
    records: records.slice(0, 30),
    lowSignalSuppressedCount,
    generatedAt: now,
  };
}

function buildSystemHealth({ settings = {}, desktopData = null, systemSignals = [] }) {
  const openAiReady = settings?.aiEnabled !== false && Boolean(settings?.aiEnabled || settings?.openaiKeyHint || settings?.openrouterConfigured || settings?.openrouterKeyHint);
  const googleConfigured = Boolean(settings?.googleConfigured || settings?.googleClientId);
  const googleConnected = Boolean(settings?.googleConnected || (settings?.googleTokens && typeof settings.googleTokens === 'object' && settings.googleTokens.refresh_token));
  const googleNeedsConnection = googleConfigured && !googleConnected;
  const signalItems = (Array.isArray(systemSignals) ? systemSignals : []).slice(0, 12).map((item) => {
    const signal = item?.signal || item || {};
    const status = safeText(signal?.status || item?.bucket || 'active');
    const score = Number(item?.score || signal?.score || 0);
    return {
      id: signal.id,
      title: signal.title,
      status: status === 'needs_credentials' || status === 'approval_required'
        ? status
        : score >= 78 || Number(signal?.riskImpact || 0) >= 0.8
          ? 'warning'
          : status || 'active',
      summary: signal.summary || signal.nextAction || signal.recommendedAction || '',
      source: signal.source || 'active-brief',
      confidence: item?.confidence ?? signal.confidence ?? 0.72,
      recommendedAction: signal.recommendedAction || signal.nextAction || 'Monitor.',
      type: signal.type,
      score,
    };
  });
  const items = [
    {
      id: 'system:openai',
      title: 'OpenAI routing',
      status: openAiReady ? 'ok' : 'warning',
      summary: openAiReady ? 'AI routes are available for command responses.' : 'AI routes are missing provider credentials.',
      source: 'settings',
      confidence: 0.78,
      recommendedAction: openAiReady ? 'Monitor.' : 'Review AI provider settings.',
    },
    {
      id: 'system:google',
      title: 'Google integrations',
      status: googleConnected ? 'ok' : googleNeedsConnection ? 'needs_connection' : 'needs_credentials',
      summary: googleConnected
        ? 'Google connection is available.'
        : googleNeedsConnection
          ? 'Google OAuth app is configured, but the account connection needs refresh.'
          : 'Calendar/Drive context may be incomplete until Google OAuth is configured.',
      source: 'settings.googleConnected',
      confidence: 0.74,
      recommendedAction: googleConnected ? 'Monitor.' : googleNeedsConnection ? 'Reconnect Google account in Settings.' : 'Configure Google OAuth in Settings.',
    },
    {
      id: 'system:desktop-context',
      title: 'Desktop context',
      status: desktopData?.workspace || desktopData?.windowTitle ? 'active' : 'quiet',
      summary: desktopData?.workspace?.workspacePath || desktopData?.windowTitle || 'No current desktop context in this brief.',
      source: desktopData ? 'desktop-agent' : 'desktop-agent-cache',
      confidence: desktopData ? 0.72 : 0.5,
      recommendedAction: desktopData ? 'Use for focus inference.' : 'Enable desktop awareness when local context matters.',
    },
    ...signalItems.filter((item) => item.id && !['system:openai', 'system:google', 'system:desktop-context'].includes(item.id)),
  ];
  return {
    items,
    criticalCount: items.filter((item) => item.status === 'critical').length,
    warningCount: items.filter((item) => ['warning', 'needs_credentials', 'needs_connection'].includes(item.status)).length,
    okCount: items.filter((item) => ['ok', 'active', 'quiet'].includes(item.status)).length,
  };
}

function briefingCanIgnoreRow(item) {
  if (!item || typeof item !== 'object') {
    return {
      id: `ignore:note:${String(item || 'can-wait').slice(0, 48)}`,
      type: 'CanIgnore',
      title: String(item || 'Can wait'),
      summary: 'Can wait.',
      reason: 'Not active attention right now.',
      source: 'session-briefing',
      confidence: 0.7,
      recommendedAction: 'No action needed right now.',
    };
  }
  return {
    id: item.id || `ignore:${item.title || item.sourceSignalId || 'can-wait'}`,
    type: item.type || 'CanIgnore',
    title: item.title || item.summary || 'Can wait',
    summary: item.summary || item.reason || 'Can wait.',
    reason: item.reason || item.summary || 'Not active attention right now.',
    source: item.source || 'attention-scoring',
    sourceSignalId: item.sourceSignalId || '',
    target: item.target || '',
    targetType: item.targetType || '',
    score: item.score,
    confidence: item.confidence,
    relatedEntities: Array.isArray(item.relatedEntities) ? item.relatedEntities : [],
    recommendedAction: item.recommendedAction || 'Ignore unless new urgency, a deadline, or a relationship signal appears.',
  };
}

function buildSessionBriefing({ topPriorities, waitingOnMark, waitingOnClients, waitingOnTeam, risks, opportunities, preparedActions, systemHealth, lowSignalSuppressedCount, ignoreQueue = [] }) {
  const canIgnore = [
    ...ignoreQueue.slice(0, 3).map(briefingCanIgnoreRow),
    lowSignalSuppressedCount && !ignoreQueue.length ? briefingCanIgnoreRow({
      id: 'ignore:low-signal-suppressed',
      title: `${lowSignalSuppressedCount} low-signal items suppressed`,
      summary: 'MARCUS kept these out of active attention because they lacked urgency, Mark ownership, or current relevance.',
      reason: 'Below attention threshold.',
      source: 'attention-scoring',
      confidence: 0.7,
      recommendedAction: 'No action needed right now.',
    }) : null,
    briefingCanIgnoreRow({
      id: 'ignore:dormant-project-history',
      title: 'Dormant projects remain searchable',
      summary: 'Dormant projects stay in memory/search but are not treated as current work unless Mark pins or reactivates them.',
      reason: 'Known history should not compete with active attention.',
      source: 'project-activity-policy',
      confidence: 0.82,
      recommendedAction: 'Leave in known history unless it needs to become active again.',
    }),
  ].filter(Boolean);

  return {
    changedSinceLastTime: [],
    changedSummary: 'No prior check-in delta is available yet.',
    needsAttention: topPriorities.slice(0, 5),
    waitingOnMark: waitingOnMark.slice(0, 5),
    waitingOnOthers: [...waitingOnClients, ...waitingOnTeam].slice(0, 5),
    topActions: preparedActions.slice(0, 3),
    systems: systemHealth.items.filter((item) => item.status !== 'ok').slice(0, 4),
    opportunities: opportunities.slice(0, 4),
    risks: risks.slice(0, 4),
    canIgnore,
    ignoreQueue: ignoreQueue.slice(0, 5),
  };
}

function buildActionQueue(preparedActions) {
  return (Array.isArray(preparedActions) ? preparedActions : []).map((action) => ({
    ...action,
    lifecycle: action.requiresApproval ? 'draft_action' : 'suggested_action',
    executionStatus: action.executionStatus || 'not_executable',
    executionDeferred: true,
    states: ['suggested_action', 'draft_action', 'approved_action', 'completed_action', 'dismissed_action'],
    approvalRequired: Boolean(action.requiresApproval),
  }));
}

function decisionQuestionForSignal(signal) {
  const text = `${signal?.title || ''} ${signal?.summary || ''} ${signal?.nextAction || ''}`.toLowerCase();
  if (/\b(approve|approval|send|reply|respond|confirm)\b/.test(text)) return 'Should Mark approve, respond, or confirm the next step?';
  if (/\b(price|pricing|quote|invoice|payment|billing|money|budget)\b/.test(text)) return 'What commercial decision is needed before this can move?';
  if (/\b(blocked|stuck|risk|deadline|urgent|overdue)\b/.test(text)) return 'What decision removes the blocker or reduces the risk?';
  if (/\b(archive|complete|done|stale|dormant|historical)\b/.test(text)) return 'Should this stay active or move to known history?';
  return 'What should happen next?';
}

function buildDecisionQueue({ visible = [], actionQueue = [] } = {}) {
  const decisionSignals = (Array.isArray(visible) ? visible : [])
    .filter((item) => {
      const signal = item?.signal || {};
      const text = `${signal.title || ''} ${signal.summary || ''} ${signal.nextAction || ''} ${signal.status || ''}`.toLowerCase();
      return signal.requiresMark || /\b(decision|decide|approve|approval|confirm|choose|blocked|stuck|waiting on mark|need you)\b/.test(text);
    })
    .slice(0, 8)
    .map((item) => {
      const signal = item.signal;
      return {
        id: `decision:${signal.id}`,
        type: OperationalTypes.DECISION,
        title: signal.title,
        summary: signal.nextAction || signal.summary || 'Decision needed.',
        question: decisionQuestionForSignal(signal),
        source: signal.source || 'active-brief',
        sourceSignalId: signal.id,
        businessKey: signal.businessKey,
        target: signal.relatedProjectId || signal.relatedClientId || signal.id,
        targetType: signal.type,
        score: item.score,
        urgency: signal.urgency,
        importance: signal.importance,
        moneyImpact: signal.moneyImpact,
        relationshipImpact: signal.relationshipImpact,
        riskImpact: signal.riskImpact,
        confidence: item.confidence,
        reasons: item.reasons,
        relatedEntities: Array.isArray(signal.relatedEntities) ? signal.relatedEntities : [],
        expiresAt: signal.expiresAt || '',
        recommendedAction: signal.nextAction || 'Decide the next operational move.',
        createdAt: signal.createdAt,
        updatedAt: signal.updatedAt,
        requiresMark: true,
        approvalRequired: Boolean(signal.requiresApproval),
      };
    });

  const approvalDecisions = (Array.isArray(actionQueue) ? actionQueue : [])
    .filter((action) => action?.approvalRequired || action?.requiresApproval)
    .slice(0, 6)
    .map((action) => ({
      id: `decision:${action.id}`,
      type: OperationalTypes.DECISION,
      title: action.title || 'Approve prepared action',
      summary: action.summary || action.body || 'Prepared action is waiting for Mark approval.',
      question: 'Should this prepared action be approved, revised, or dismissed?',
      source: action.source || 'action-queue',
      sourceActionId: action.id,
      businessKey: action.businessKey || '',
      target: action.target || action.id,
      targetType: OperationalTypes.ACTION_DRAFT,
      score: Number(action.score || 62),
      urgency: Number(action.urgency || 0.5),
      confidence: Number(action.confidence || 0.72),
      reasons: [action.approvalReason || 'External execution remains approval-gated.'],
      recommendedAction: action.suggestedButtonLabel || 'Review and approve or dismiss.',
      createdAt: action.createdAt || '',
      updatedAt: action.updatedAt || '',
      requiresMark: true,
      approvalRequired: true,
    }));

  return uniqueById([...decisionSignals, ...approvalDecisions])
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, 8);
}

function buildIgnoreQueue({ scored = [], lowSignalSuppressedCount = 0 } = {}) {
  const rows = (Array.isArray(scored) ? scored : [])
    .filter((item) => {
      const status = String(item?.signal?.status || '').toLowerCase();
      return item?.bucket === 'archive/noise'
        || ['historical', 'archived', 'parked'].includes(status)
        || (Number(item?.score || 0) < 22 && !item?.signal?.requiresMark);
    })
    .map((item) => {
      const signal = item.signal || {};
      const status = String(signal.status || '').toLowerCase();
      const reason = item.bucket === 'archive/noise'
        ? 'Suppressed as archive/noise.'
        : ['historical', 'archived', 'parked'].includes(status)
          ? 'Not current active work.'
          : 'Below the active attention threshold.';
      return {
        id: `ignore:${signal.id}`,
        sourceSignalId: signal.id,
        type: 'CanIgnore',
        title: signal.title || 'Low-signal item',
        summary: signal.summary || signal.nextAction || reason,
        reason,
        source: signal.source || 'active-brief',
        businessKey: signal.businessKey || '',
        target: signal.relatedProjectId || signal.relatedClientId || signal.id,
        targetType: signal.type || '',
        status: signal.status || item.bucket || '',
        score: item.score,
        bucket: item.bucket,
        confidence: item.confidence,
        relatedEntities: Array.isArray(signal.relatedEntities) ? signal.relatedEntities : [],
        recommendedAction: status === 'historical' || status === 'archived' || status === 'parked'
          ? 'Leave in known history unless Mark pins or reactivates it.'
          : 'Ignore unless new urgency, a deadline, or a relationship signal appears.',
      };
    });

  const out = uniqueById(rows)
    .sort((a, b) => Number(a.score || 0) - Number(b.score || 0))
    .slice(0, 10);
  if (!out.length && lowSignalSuppressedCount) {
    out.push({
      id: 'ignore:low-signal-suppressed',
      type: 'CanIgnore',
      title: `${lowSignalSuppressedCount} low-signal items suppressed`,
      summary: 'MARCUS kept these out of active attention because they lacked urgency, Mark ownership, or current relevance.',
      reason: 'Below attention threshold.',
      source: 'attention-scoring',
      score: 0,
      confidence: 0.7,
      recommendedAction: 'No action needed right now.',
    });
  }
  return out;
}

function dateMs(value) {
  const ms = Date.parse(String(value || ''));
  return Number.isFinite(ms) ? ms : 0;
}

function communicationStamp(item) {
  return dateMs(item?.lastSeenAt || item?.updatedAt || item?.createdAt || item?.date);
}

function buildCommunicationIntelligence({ stores = [], visible = [], preparedActions = [], nowMs = Date.now() }) {
  const MS_PER_DAY_LOCAL = 24 * 60 * 60 * 1000;
  const conversationSignals = visible.filter((item) => item.signal?.type === OperationalTypes.CONVERSATION);
  const waitingOnMark = conversationSignals
    .filter((item) => item.signal.requiresMark)
    .slice(0, 10)
    .map(asHudItem);
  const waitingOnOthers = conversationSignals
    .filter((item) => !item.signal.requiresMark && /waiting|awaiting|client|team|vendor|reply/i.test(`${item.signal.status} ${item.signal.owner} ${item.signal.summary}`))
    .slice(0, 10)
    .map(asHudItem);
  const draftableReplies = (Array.isArray(preparedActions) ? preparedActions : [])
    .filter((action) => /reply|client|message/i.test(`${action.title || ''} ${action.type || ''} ${action.summary || ''}`))
    .slice(0, 8);
  const followUpsDue = [];
  const unusualSilence = [];
  const highValueMissedOpportunities = [];
  const bySender = new Map();

  for (const entry of stores) {
    const store = entry?.store && typeof entry.store === 'object' ? entry.store : {};
    const businessKey = safeText(entry?.businessKey || 'personal', 80);
    for (const item of Array.isArray(store.inboxItems) ? store.inboxItems : []) {
      const text = safeText(item?.text || item?.body || item?.summary || item?.subject || item?.title || '', 1200);
      if (!text) continue;
      const sender = safeText(item?.contactName || item?.fromName || item?.sender || item?.fromNumber || item?.source || 'Unknown', 180);
      const status = safeText(item?.status || 'new', 80).toLowerCase();
      const ts = communicationStamp(item);
      const ageDays = ts ? Math.floor((nowMs - ts) / MS_PER_DAY_LOCAL) : 999;
      const base = {
        id: `comm:${businessKey}:${safeText(item?.id || item?.externalId || text.slice(0, 32), 120)}`,
        title: sender,
        summary: text,
        source: safeText(item?.source || 'inbox', 80),
        businessKey,
        status: status || 'new',
        updatedAt: ts ? new Date(ts).toISOString() : '',
        ageDays,
        confidence: 0.68,
      };
      const actionable = /\b(need|needs|please|can you|could you|follow up|send|call|schedule|review|fix|update|quote|invoice|confirm|ship|deploy|publish|prepare|asap|urgent|today|tomorrow|deadline|due|decision|approval)\b/i.test(text);
      const opportunity = /\b(opportunity|proposal|quote|estimate|upgrade|upsell|referral|new project|lead|interested|pricing)\b/i.test(text);
      if (actionable && (status === 'new' || status === 'triaged' || ageDays >= 2)) {
        followUpsDue.push({
          ...base,
          reason: ageDays >= 2 ? 'actionable message aging' : 'actionable message',
          recommendedAction: 'Draft a reply or create a follow-up action.',
        });
      }
      if (opportunity) {
        highValueMissedOpportunities.push({
          ...base,
          reason: 'possible revenue or relationship opportunity',
          recommendedAction: 'Review and decide whether to respond or qualify.',
        });
      }
      const key = `${businessKey}:${sender.toLowerCase()}`;
      const existing = bySender.get(key);
      if (!existing || ts > existing.ts) bySender.set(key, { sender, businessKey, ts, item: base });
    }
  }

  for (const row of bySender.values()) {
    if (!row.ts) continue;
    const ageDays = Math.floor((nowMs - row.ts) / MS_PER_DAY_LOCAL);
    if (ageDays >= 14) {
      unusualSilence.push({
        ...row.item,
        ageDays,
        reason: 'no recent communication in 14+ days',
        recommendedAction: 'Review whether this relationship or lead needs a follow-up.',
      });
    }
  }

  return {
    waitingOnMark,
    waitingOnOthers,
    draftableReplies,
    followUpsDue: followUpsDue.sort((a, b) => b.ageDays - a.ageDays).slice(0, 10),
    unusualSilence: unusualSilence.sort((a, b) => b.ageDays - a.ageDays).slice(0, 10),
    highValueMissedOpportunities: highValueMissedOpportunities.slice(0, 10),
    counts: {
      waitingOnMark: waitingOnMark.length,
      waitingOnOthers: waitingOnOthers.length,
      draftableReplies: draftableReplies.length,
      followUpsDue: followUpsDue.length,
      unusualSilence: unusualSilence.length,
      highValueMissedOpportunities: highValueMissedOpportunities.length,
    },
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

function buildSettingsOperationalSignals(settings = {}, nowMs = Date.now()) {
  const s = settings && typeof settings === 'object' ? settings : {};
  const out = [];
  const now = nowIso(new Date(nowMs));
  const hasAiProvider = Boolean(s.openaiApiKey || s.openaiKeyHint || s.openrouterConfigured || s.openrouterKeyHint || s.aiEnabled);
  const googleTokens = s.googleTokens && typeof s.googleTokens === 'object' ? s.googleTokens : null;
  const googleConnected = Boolean(s.googleConnected || googleTokens?.refresh_token);
  const googleConfigured = Boolean(s.googleConfigured || s.googleClientId || s.googleClientSecret);
  const automationQueue = Array.isArray(s.automationDigestQueue) ? s.automationDigestQueue : [];
  const automationPending = automationQueue.filter((item) => !item?.decision && !item?.decidedAt && !item?.approvedAt && !item?.rejectedAt);
  const integrationRows = [
    {
      id: 'openai',
      title: 'OpenAI routing',
      configured: hasAiProvider && s.aiEnabled !== false,
      source: 'settings.openai',
      summary: hasAiProvider && s.aiEnabled !== false ? 'AI routing appears configured.' : 'AI routing may be disabled or missing provider credentials.',
      recommendedAction: hasAiProvider && s.aiEnabled !== false ? 'Monitor.' : 'Review AI provider settings.',
      missingTitle: 'OpenAI routing needs provider credentials',
      missingStatus: 'needs_credentials',
      evidence: [{ label: 'aiProviderConfigured', value: 'false' }],
    },
    {
      id: 'google',
      title: 'Google integrations',
      configured: googleConnected,
      source: 'settings.google',
      summary: googleConnected ? 'Google OAuth context is connected.' : 'Calendar, Drive, or Analytics context may be incomplete until Google is connected.',
      recommendedAction: googleConnected ? 'Monitor.' : googleConfigured ? 'Reconnect Google account in Settings.' : 'Configure Google OAuth in Settings.',
      missingTitle: googleConfigured ? 'Google account connection needs refresh' : 'Google integrations need OAuth setup',
      missingStatus: googleConfigured ? 'needs_connection' : 'needs_credentials',
      evidence: [{ label: googleConfigured ? 'accountConnected' : 'googleOAuthConfigured', value: 'false' }],
    },
  ];

  for (const row of integrationRows) {
    if (row.configured) {
      out.push(makeOperationalObject({
        id: `tool:settings:${row.id}`,
        type: OperationalTypes.TOOL,
        title: row.title,
        summary: row.summary,
        source: row.source,
        status: 'connected',
        urgency: 0.16,
        importance: 3,
        confidence: 0.72,
        createdAt: now,
        updatedAt: now,
        nextAction: row.recommendedAction,
        relatedEntities: [`tool:${row.id}`],
      }));
    } else {
      out.push(makeOperationalObject({
        id: `system:settings:${row.id}:needs-credentials`,
        type: OperationalTypes.SYSTEM_SIGNAL,
        title: row.missingTitle || `${row.title} needs credentials`,
        summary: row.summary,
        source: row.source,
        status: row.missingStatus || 'needs_credentials',
        priority: 2,
        importance: 2,
        urgency: 0.58,
        riskImpact: 0.62,
        confidence: 0.76,
        createdAt: now,
        updatedAt: now,
        nextAction: row.recommendedAction,
        requiresMark: true,
        relatedEntities: [`tool:${row.id}`],
        evidence: row.evidence || [{ label: 'configured', value: 'false' }],
      }));
    }
  }

  if (automationPending.length) {
    out.push(makeOperationalObject({
      id: 'tool:settings:automation-digest-queue',
      type: OperationalTypes.TOOL,
      title: 'Automation recommendations need review',
      summary: `${automationPending.length} automation recommendation${automationPending.length === 1 ? '' : 's'} are waiting for approval or dismissal.`,
      source: 'settings.automationDigestQueue',
      status: 'approval_required',
      priority: 1,
      importance: 1,
      urgency: 0.66,
      riskImpact: 0.35,
      confidence: 0.82,
      createdAt: now,
      updatedAt: now,
      nextAction: 'Review queued automation recommendations before external execution.',
      requiresMark: true,
      requiresApproval: true,
      relatedEntities: ['tool:automation'],
      evidence: [{ label: 'pendingRecommendations', value: automationPending.length }],
    }));
  }

  return out;
}

export function buildActiveBrief({ stores = [], settings = {}, desktopData = null, nowMs = Date.now() } = {}) {
  const historicalDays = Number(settings?.marcusHistoricalProjectDays || settings?.historicalProjectDays || 14);
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
      historicalDays: Number.isFinite(historicalDays) ? historicalDays : 14,
    }));
  }
  signals.push(...buildSettingsOperationalSignals(settings, nowMs));

  const scoredRaw = scoreOperationalSignals(signals, { nowMs });
  const scored = compressScoredSignals(scoredRaw);
  const visible = scored.filter((item) => item.bucket !== 'archive/noise');
  const suppressed = scoredRaw.filter((item) => item.bucket === 'archive/noise').length;
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
  const memoryPulse = buildMemoryPulse({ stores, settings, visible, scored, lowSignalSuppressedCount: suppressed });
  const systemHealth = buildSystemHealth({
    settings,
    desktopData,
    systemSignals: visible.filter((item) => [
      OperationalTypes.SYSTEM_SIGNAL,
      OperationalTypes.SYSTEM,
      OperationalTypes.TOOL,
      OperationalTypes.WEBSITE,
      OperationalTypes.PAYMENT,
    ].includes(item?.signal?.type)),
  });
  const actionQueue = buildActionQueue(preparedActions);
  const decisionQueue = buildDecisionQueue({ visible, actionQueue });
  const ignoreQueue = buildIgnoreQueue({ scored, lowSignalSuppressedCount: suppressed });
  const worldModel = augmentWorldModelWithOperationalContext(buildWorldModel({ stores, scored }), {
    visible,
    actionQueue,
    decisionQueue,
    systemHealth,
    memoryPulse,
  });
  const communicationIntelligence = buildCommunicationIntelligence({ stores, visible, preparedActions: actionQueue, nowMs });
  const sessionBriefing = buildSessionBriefing({
    topPriorities,
    waitingOnMark: communicationIntelligence.waitingOnMark.length ? communicationIntelligence.waitingOnMark : waitingOnMark,
    waitingOnClients,
    waitingOnTeam: communicationIntelligence.waitingOnOthers.length ? communicationIntelligence.waitingOnOthers : waitingOnTeam,
    risks,
    opportunities,
    preparedActions: actionQueue,
    systemHealth,
    lowSignalSuppressedCount: suppressed,
    ignoreQueue,
  });
  sessionBriefing.decisions = decisionQueue.slice(0, 3);
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
    actionQueue,
    decisionQueue,
    ignoreQueue,
    suggestedDelegations,
    sessionBriefing,
    systemHealth,
    memoryPulse,
    worldModel,
    communicationIntelligence,
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
