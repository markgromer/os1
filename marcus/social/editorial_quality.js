const GENERIC_PATTERNS = [
  { pattern: /\bwhat(?:'s| is) your biggest (?:challenge|problem|bottleneck)\b/i, issue: 'generic-biggest-challenge' },
  { pattern: /\bwhere does your (?:business|operation|workflow) (?:lose|waste)\b/i, issue: 'generic-friction-question' },
  { pattern: /\bwhich .{0,50} costs? you the most (?:time|money)\b/i, issue: 'generic-ranking-question' },
  { pattern: /\bvote for (?:the|which|your)\b/i, issue: 'engagement-bait-vote' },
  { pattern: /\bpick the .{0,60}(?:time|money|hardest|most)\b/i, issue: 'engagement-bait-pick' },
  { pattern: /\bi(?:'| wi)ll (?:take|use) the top (?:answer|choice|response)\b/i, issue: 'engagement-bait-followup' },
  { pattern: /\bshare (?:your|a) (?:wins?|challenges?|thoughts?)\b/i, issue: 'generic-share-prompt' },
  { pattern: /\blet(?:'s| us) (?:talk|explore|dive in|share)\b/i, issue: 'generic-conversation-opener' },
  { pattern: /\blooking forward to (?:learning|hearing|connecting)\b/i, issue: 'generic-assistant-closer' },
  { pattern: /\b(?:unlock|supercharge|level up|game[- ]changer|elevate)\b/i, issue: 'marketing-cliche' },
  { pattern: /\bin today(?:'s)? (?:fast-paced|digital)\b/i, issue: 'generic-ai-preface' },
  { pattern: /\bi(?:'ve| have) noticed (?:a )?(?:recurring )?(?:tension|pattern)\b/i, issue: 'generic-observation-opener' },
  { pattern: /\b(?:ai|automation)(?: tools?)? (?:is|are) only as good as\b/i, issue: 'generic-ai-maxim' },
  { pattern: /\b(?:not|isn't) (?:a )?magic wand\b/i, issue: 'generic-ai-metaphor' },
  { pattern: /\bthink of (?:ai|automation|technology) as\b/i, issue: 'generic-ai-framing' },
  { pattern: /\bstrategic enabler\b/i, issue: 'generic-strategy-language' },
  { pattern: /\b(?:surface[- ]level|polished surface|shiny apps?|under the hood)\b/i, issue: 'generic-surface-metaphor' },
  { pattern: /\b(?:leverage|leveraging) ai\b/i, issue: 'generic-ai-leverage' },
  { pattern: /\bscalable (?:business|operations?|systems?)\b/i, issue: 'generic-scale-claim' },
  { pattern: /\bthis post isn(?:'|’)t about\b/i, issue: 'generic-post-disclaimer' },
  { pattern: /\bi(?:'| wi)ll continue sharing\b/i, issue: 'generic-content-promise' },
];

const SOURCE_STOP_WORDS = new Set([
  'about', 'after', 'again', 'also', 'been', 'being', 'between', 'business', 'community', 'could', 'from',
  'have', 'into', 'just', 'like', 'more', 'only', 'other', 'people', 'really', 'should', 'some', 'system',
  'systems', 'their', 'there', 'these', 'they', 'this', 'those', 'through', 'tools', 'using', 'very', 'what',
  'when', 'where', 'which', 'with', 'would', 'your', 'automation', 'automate', 'automated', 'workflow',
  'workflows', 'operations', 'operational', 'artificial', 'intelligence', 'mark', 'marcus',
]);

function clean(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function sentenceWordCounts(text) {
  return String(text || '')
    .replace(/\n+/g, ' ')
    .split(/[.!?]+(?:\s+|$)/)
    .map((sentence) => clean(sentence).split(/\s+/).filter(Boolean).length)
    .filter(Boolean);
}

function meaningfulTokens(value) {
  return new Set(clean(value).toLowerCase().match(/[a-z0-9][a-z0-9'-]{2,}/g)
    ?.filter((token) => !SOURCE_STOP_WORDS.has(token)) || []);
}

function groundedSourceCount(text, observations) {
  const bodyTokens = meaningfulTokens(text);
  return (Array.isArray(observations) ? observations : []).filter((observation) => {
    const sourceTokens = meaningfulTokens([
      observation?.member?.displayName,
      observation?.sourceTitle,
      observation?.contentSummary,
    ].filter(Boolean).join(' '));
    const overlap = [...sourceTokens].filter((token) => bodyTokens.has(token));
    return overlap.length >= 3;
  }).length;
}

export function analyzeMarcusSocialDraft(input = {}) {
  const title = clean(input.title);
  const text = String(input.text || '').trim();
  const combined = `${title}\n${text}`;
  const editorialAngle = clean(input.editorialAngle);
  const readerValue = clean(input.readerValue);
  const sourceObservationIds = [...new Set((Array.isArray(input.sourceObservationIds) ? input.sourceObservationIds : [])
    .map((value) => clean(value)).filter(Boolean))].slice(0, 8);
  const issues = GENERIC_PATTERNS.filter(({ pattern }) => pattern.test(combined)).map(({ issue }) => issue);
  const paragraphs = text.split(/\n\s*\n/).map(clean).filter(Boolean);
  const sentenceLengths = sentenceWordCounts(text);
  const maxSentenceWords = sentenceLengths.length ? Math.max(...sentenceLengths) : 0;
  const questionCount = (combined.match(/\?/g) || []).length;
  const sourceObservations = Array.isArray(input.sourceObservations) ? input.sourceObservations : [];
  const groundedSources = groundedSourceCount(combined, sourceObservations);

  if (sourceObservationIds.length < 1) issues.push('missing-source-observation');
  if (editorialAngle.length < 40) issues.push('missing-editorial-angle');
  if (readerValue.length < 30) issues.push('missing-reader-value');
  if (paragraphs.length < 2) issues.push('wall-of-text');
  if (paragraphs.length > 5) issues.push('too-many-paragraphs');
  if (questionCount > 1) issues.push('too-many-questions');
  if (maxSentenceWords > 36) issues.push('sentence-too-long');
  if (text.length > 900) issues.push('draft-too-long');
  if (sourceObservations.length && groundedSources < 1) issues.push('weak-source-grounding');

  return {
    ok: issues.length === 0,
    issues: [...new Set(issues)],
    metrics: {
      paragraphs: paragraphs.length,
      questions: questionCount,
      maxSentenceWords,
      sourceObservations: sourceObservationIds.length,
      groundedSources,
    },
  };
}

export const MARCUS_SOCIAL_GENERIC_PATTERNS = Object.freeze(GENERIC_PATTERNS.map(({ issue }) => issue));
