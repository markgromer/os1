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
];

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

  if (sourceObservationIds.length < 1) issues.push('missing-source-observation');
  if (editorialAngle.length < 40) issues.push('missing-editorial-angle');
  if (readerValue.length < 30) issues.push('missing-reader-value');
  if (paragraphs.length < 2) issues.push('wall-of-text');
  if (questionCount > 1) issues.push('too-many-questions');
  if (maxSentenceWords > 36) issues.push('sentence-too-long');

  return {
    ok: issues.length === 0,
    issues: [...new Set(issues)],
    metrics: {
      paragraphs: paragraphs.length,
      questions: questionCount,
      maxSentenceWords,
      sourceObservations: sourceObservationIds.length,
    },
  };
}

export const MARCUS_SOCIAL_GENERIC_PATTERNS = Object.freeze(GENERIC_PATTERNS.map(({ issue }) => issue));
