function clean(value, max) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

export function buildCommunitySourceLedger(observations, { limit = 20 } = {}) {
  return (Array.isArray(observations) ? observations : []).slice(0, limit).map((observation) => ({
    id: clean(observation?.id, 120),
    member: clean(observation?.member?.displayName, 200),
    title: clean(observation?.sourceTitle, 300),
    summary: clean(observation?.contentSummary, 360),
    sourceUrl: clean(observation?.sourceUrl, 2_000),
  })).filter((observation) => observation.id && observation.sourceUrl);
}

