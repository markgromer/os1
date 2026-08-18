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
  { pattern: /\bi see a clear tension\b/i, issue: 'generic-tension-opener' },
  { pattern: /\bquick automation wins?\b/i, issue: 'generic-quick-win' },
  { pattern: /\bsolid workflows?\b/i, issue: 'generic-workflow-claim' },
  { pattern: /\bbuild a foundation\b/i, issue: 'generic-foundation-metaphor' },
  { pattern: /\b(?:practical,? )?no[- ]fluff insights?\b/i, issue: 'generic-no-fluff-claim' },
  { pattern: /\bmark.{0,3}s (?:ai )?experience (?:proves|shows|demonstrates)\b/i, issue: 'unsupported-mark-attribution' },
  { pattern: /\bbalancing .{3,80}\b(?:lessons?|insights?) from\b/i, issue: 'generic-balanced-title' },
  { pattern: /\blessons? from\b/i, issue: 'generic-lessons-title' },
  { pattern: /\bthat.{0,3}s the real (?:choice|lesson|difference)\b/i, issue: 'generic-summary-transition' },
  { pattern: /\b(?:ai|automation) isn.{0,3}t about\b/i, issue: 'generic-not-about-claim' },
  { pattern: /\b(?:lasting|sustainable) results? (?:need|require|come from)\b/i, issue: 'generic-results-claim' },
  { pattern: /\bfocus on (?:blending|building|creating|using)\b/i, issue: 'generic-imperative-advice' },
  { pattern: /\b(?:fast|quick) (?:moves?|action)\b/i, issue: 'generic-speed-language' },
  { pattern: /\b(?:organized|steady|consistent) (?:process|follow[- ]up|execution)\b/i, issue: 'generic-process-language' },
  { pattern: /\b(?:flashy|shiny) tools?\b/i, issue: 'generic-tool-dismissal' },
  { pattern: /(?:^|\n)(?:hi[,.!]?[ \t]*)?i.{0,3}m marcus\b/i, issue: 'boilerplate-identity-opener' },
  { pattern: /\bmark(?:'s|’s) experience shows\b/i, issue: 'vague-attribution' },
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

function distinctiveSourceCount(text, observations) {
  const body = clean(text).toLowerCase();
  const bodyTokens = meaningfulTokens(text);
  return (Array.isArray(observations) ? observations : []).filter((observation) => {
    const memberFirstName = clean(observation?.member?.displayName, 200).split(/\s+/)[0]?.toLowerCase() || '';
    const namedMember = memberFirstName.length >= 4 && memberFirstName !== 'mark'
      && new RegExp(`\\b${memberFirstName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i').test(body);
    const titleTokens = [...meaningfulTokens(observation?.sourceTitle)];
    const titleOverlap = titleTokens.filter((token) => bodyTokens.has(token)).length >= Math.min(2, titleTokens.length || 2);
    const sourceText = [observation?.sourceTitle, observation?.contentSummary].filter(Boolean).join(' ');
    const sourceNumbers = [...new Set(sourceText.match(/\b\d{2,}\b/g) || [])];
    const repeatedNumber = sourceNumbers.some((number) => new RegExp(`\\b${number}\\b`).test(body));
    const sourceTerms = [...new Set(sourceText.match(/\b(?:[A-Z]{2,}|[A-Z][a-z]+[A-Z][A-Za-z0-9]*|[A-Za-z]+\.[A-Za-z]+)\b/g) || [])]
      .map((term) => term.toLowerCase());
    const repeatedTerm = sourceTerms.some((term) => bodyTokens.has(term));
    return namedMember || titleOverlap || repeatedNumber || repeatedTerm;
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
  const distinctiveSources = distinctiveSourceCount(combined, sourceObservations);

  if (sourceObservationIds.length < 1) issues.push('missing-source-observation');
  if (editorialAngle.length < 40) issues.push('missing-editorial-angle');
  if (readerValue.length < 30) issues.push('missing-reader-value');
  if (paragraphs.length < 2) issues.push('wall-of-text');
  if (paragraphs.length > 5) issues.push('too-many-paragraphs');
  if (questionCount > 1) issues.push('too-many-questions');
  if (maxSentenceWords > 36) issues.push('sentence-too-long');
  if (text.length > 900) issues.push('draft-too-long');
  if (sourceObservations.length && groundedSources < 1) issues.push('weak-source-grounding');
  if (sourceObservations.length && distinctiveSources < 1) issues.push('missing-distinctive-source-anchor');

  return {
    ok: issues.length === 0,
    issues: [...new Set(issues)],
    metrics: {
      paragraphs: paragraphs.length,
      questions: questionCount,
      maxSentenceWords,
      sourceObservations: sourceObservationIds.length,
      groundedSources,
      distinctiveSources,
    },
  };
}

export const MARCUS_SOCIAL_GENERIC_PATTERNS = Object.freeze(GENERIC_PATTERNS.map(({ issue }) => issue));
