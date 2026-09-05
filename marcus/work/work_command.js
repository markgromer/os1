// Exact read-only intents: mixed requests and action instructions retain their
// existing handlers and authority checks. No model or dispatcher is involved.
export function isWorkStatusCommand(message) {
  const normalized = String(message || '').trim().toLowerCase().replace(/[?.!]+$/, '');
  return /^(?:(?:show|summarize|inspect) (?:my |the )?(?:tracked|durable) work(?: status)?|what needs (?:me|mark)|what (?:work )?(?:is blocked|can continue)|what changed while i was away)$/.test(normalized);
}

export function buildWorkStatusResponse(summary, state) {
  const work = new Map(state.items.map((item) => [item.id, item]));
  const groups = [
    ['Needs you', summary.needsMark], ['Can continue', summary.canContinue],
    ['Needs investigation', summary.anomalies], ['Ready, not authorized to advance', summary.opportunities],
  ];
  const lines = [`Tracked work: ${summary.trackedWorkCount}.`, ...summary.uncertainty];
  const cards = [];
  for (const [label, entries] of groups) {
    lines.push(`${label}: ${entries.length}.`);
    for (const entry of entries.slice(0, 3)) {
      const title = work.get(entry.id)?.objective || label;
      lines.push(`- ${title}: ${entry.reason}`);
      cards.push({ id: entry.id, title, summary: entry.reason, source: 'work-graph', evidence: entry.evidence, readOnly: true });
    }
  }
  lines.push(`Recent tracked changes: ${summary.away.changes.length}${summary.away.truncated ? ' (limited to the latest 100)' : ''}.`);
  lines.push('This is a read-only report. Existing tasks are not automatically imported; execution still requires project setup and approval.');
  return { ok: true, handled: true, intent: 'work_status', plainText: true, title: 'Durable work status', reply: lines.join('\n'),
    cards: cards.slice(0, 8), suggestedActions: [], workSummary: summary, generatedAt: summary.observedAt };
}
