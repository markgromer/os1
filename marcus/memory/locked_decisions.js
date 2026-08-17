function tokens(value) {
  return new Set((String(value || '').toLowerCase().match(/[a-z0-9]{4,}/g) || []).filter((item) => !['that', 'this', 'with', 'from', 'should', 'must', 'locked', 'decision'].includes(item)));
}

export function lockedDecisionRecords(memories = []) {
  return (Array.isArray(memories) ? memories : []).filter((item) => item?.status === 'active' && item?.kind === 'decision'
    && (/\blocked\b/i.test(item.title || '') || item.locked === true || /\[locked\]/i.test(item.content || '')));
}

export function assessLockedDecisionConflict(request, memories = []) {
  const changeIntent = /\b(change|replace|remove|disable|bypass|ignore|stop requiring|no longer|instead|direct access|skip)\b/i.test(String(request || ''));
  if (!changeIntent) return null;
  const requestTokens = tokens(request);
  const match = lockedDecisionRecords(memories).find((decision) => {
    const decisionTokens = tokens(`${decision.title} ${decision.content}`);
    return [...decisionTokens].filter((token) => requestTokens.has(token)).length >= 2;
  });
  if (!match) return null;
  return {
    status: 'locked_decision_conflict',
    decisionId: match.id,
    title: match.title,
    reply: `That conflicts with the locked decision “${match.title.replace(/^\s*\[?locked\]?[: -]*/i, '')}.” Is this a permanent change or a one-time exception?`,
  };
}
