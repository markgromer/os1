const NEGATED_CLAUSE_RE = /\b(?:do not|don't|dont|never)\b.*?(?=\b(?:but|instead)\b|[.!?;\n]|$)/gi;

export function withoutExplicitlyNegatedClauses(message) {
  return String(message || '').replace(NEGATED_CLAUSE_RE, ' ').replace(/\s+/g, ' ').trim();
}

export function withoutProjectExecutionDeferrals(message) {
  return String(message || '').replace(NEGATED_CLAUSE_RE, (clause) => (
    explicitlyDefersProjectAudit(clause) || explicitlyDefersCodexStart(clause) ? ' ' : clause
  )).replace(/\s+/g, ' ').trim();
}

export function explicitlyDefersProjectAudit(message) {
  const text = String(message || '');
  return /\b(?:do not|don't|dont|never)\b[^.!?;\n]{0,100}\b(?:audit|inspect|check|review)\b/i.test(text)
    || /\bwithout\b[^.!?;\n]{0,60}\b(?:auditing|inspecting|checking|reviewing)\b/i.test(text);
}

export function explicitlyDefersCodexStart(message) {
  const text = String(message || '');
  return /\b(?:do not|don't|dont|never)\b[^.!?;\n]{0,100}\b(?:start|launch|run|open|execute|begin)\b[^.!?;\n]{0,100}\bcodex\b/i.test(text)
    || /\bwithout\b[^.!?;\n]{0,60}\b(?:starting|launching|running|opening|executing)\b[^.!?;\n]{0,60}\bcodex\b/i.test(text);
}
