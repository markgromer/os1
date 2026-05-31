import { OperationalTypes, daysBetween, isMarkOwner, parseTime, ymd } from './domain.js';

const MS_PER_DAY = 24 * 60 * 60 * 1000;

function dueDistanceDays(dueAt, nowMs) {
  const due = ymd(dueAt);
  if (!due) return null;
  const ms = Date.parse(`${due}T23:59:59Z`);
  if (!Number.isFinite(ms)) return null;
  return Math.floor((ms - nowMs) / MS_PER_DAY);
}

function hasAny(text, terms) {
  const raw = String(text || '').toLowerCase();
  return terms.some((term) => raw.includes(term));
}

export function scoreOperationalSignal(signal, { nowMs = Date.now(), projectStatusById = new Map() } = {}) {
  const text = `${signal?.title || ''} ${signal?.summary || ''} ${signal?.status || ''} ${signal?.nextAction || ''}`;
  const status = String(signal?.status || '').toLowerCase();
  const type = signal?.type || OperationalTypes.SYSTEM_SIGNAL;
  const confidence = Number.isFinite(Number(signal?.confidence)) ? Math.max(0, Math.min(1, Number(signal.confidence))) : 0.65;
  const projectStatus = projectStatusById.get(String(signal?.relatedProjectId || '')) || '';
  const ageDays = daysBetween(nowMs, parseTime(signal?.lastSeenAt || signal?.updatedAt || signal?.createdAt));
  const dueDays = dueDistanceDays(signal?.dueAt, nowMs);
  let score = 0;
  const reasons = [];

  score += Math.max(0, Math.min(30, Number(signal?.urgency || 0) * 30));
  score += Math.max(0, 18 - ageDays);

  if (Number(signal?.priority) === 1) {
    score += 20;
    reasons.push('high priority');
  } else if (Number(signal?.priority) === 3) {
    score -= 5;
  }

  if (dueDays !== null) {
    if (dueDays < 0) {
      score += 32;
      reasons.push('overdue');
    } else if (dueDays === 0) {
      score += 28;
      reasons.push('due today');
    } else if (dueDays <= 3) {
      score += 20;
      reasons.push('deadline proximity');
    } else if (dueDays <= 7) {
      score += 10;
    }
  }

  if (signal?.requiresMark) {
    score += 22;
    reasons.push('Mark required');
  }
  if (!isMarkOwner(signal?.owner) && signal?.owner) {
    score -= 12;
    reasons.push('delegable');
  }
  if (signal?.canAutonomouslyPrepare) score += 4;
  if (signal?.requiresApproval) score += 8;

  if (type === OperationalTypes.CONVERSATION) {
    score += 10;
    if (hasAny(text, ['approve', 'approval', 'decision', 'confirm'])) score += 16;
    if (hasAny(text, ['reply', 'respond', 'can you', 'please', 'need you'])) score += 12;
    if (hasAny(text, ['revision', 'revisions', 'again', 'still', 'same issue'])) {
      score += 12;
      reasons.push('possible revision loop');
    }
    if (ageDays >= 2 && signal?.requiresMark) {
      score += 10;
      reasons.push('delayed reply risk');
    }
  }

  if (type === OperationalTypes.TASK) {
    if (hasAny(text, ['blocked', 'stuck', 'waiting'])) {
      score += 18;
      reasons.push('blocked work');
    }
    if (!isMarkOwner(signal?.owner) && signal?.owner) score -= 8;
  }

  if (type === OperationalTypes.PROJECT) {
    if (status === 'active') score += 18;
    if (status === 'waiting') score += 6;
    if (status === 'warming') score -= 2;
    if (status === 'parked') score -= 20;
    if (status === 'historical' || status === 'archived') score -= 55;
    if (ageDays >= 21 && status !== 'historical' && status !== 'archived') {
      score += 8;
      reasons.push('stale project');
    }
  } else if (projectStatus === 'historical' || projectStatus === 'archived') {
    score -= 35;
    reasons.push('historical project suppressed');
  } else if (projectStatus === 'parked') {
    score -= 14;
  }

  if (hasAny(text, ['invoice', 'payment', 'billing', 'refund', 'chargeback', 'contract'])) {
    score += 18;
    reasons.push('financial risk');
  }
  if (hasAny(text, ['angry', 'upset', 'frustrated', 'unhappy', 'cancel', 'cancellation', 'escalat'])) {
    score += 20;
    reasons.push('client relationship risk');
  }
  if (hasAny(text, ['opportunity', 'upgrade', 'upsell', 'proposal', 'new project', 'referral'])) {
    score += 10;
    reasons.push('opportunity');
  }

  if (confidence < 0.45) {
    score -= 22;
    reasons.push('low confidence');
  } else if (confidence > 0.85) {
    score += 4;
  }

  if (ageDays > 45 && !signal?.requiresMark && dueDays === null) {
    score -= 40;
    reasons.push('old low-signal item');
  }

  let bucket = 'monitor';
  if (score >= 78 && signal?.requiresMark && confidence >= 0.5) bucket = 'interrupt_now';
  else if (score >= 55) bucket = 'today';
  else if (score >= 38) bucket = 'soon';
  else if (!isMarkOwner(signal?.owner) && signal?.owner && score >= 20) bucket = 'delegated';
  else if (status.includes('waiting') || hasAny(text, ['waiting on', 'awaiting'])) bucket = signal?.requiresMark ? 'waiting' : 'delegated';
  else if (score < 12 || ['historical', 'archived'].includes(status)) bucket = 'archive/noise';

  return {
    signal,
    score: Math.round(score),
    bucket,
    confidence,
    reasons,
    dueDays,
    ageDays,
  };
}

export function scoreOperationalSignals(signals, options = {}) {
  const projectStatusById = new Map();
  for (const signal of signals || []) {
    if (signal?.type === OperationalTypes.PROJECT) {
      projectStatusById.set(String(signal.relatedProjectId || ''), signal.status || '');
    }
  }
  return (signals || [])
    .map((signal) => scoreOperationalSignal(signal, { ...options, projectStatusById }))
    .sort((a, b) => (b.score - a.score) || String(b.signal?.lastSeenAt || '').localeCompare(String(a.signal?.lastSeenAt || '')));
}
