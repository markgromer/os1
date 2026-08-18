function clean(value, max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

export function buildMarcusTestObservationDraft(observation) {
  const member = clean(observation?.member?.displayName, 200);
  const source = `${clean(observation?.sourceTitle, 300)} ${clean(observation?.contentSummary, 1_500)}`;
  const seconds = source.match(/\b(\d{2,})\s*seconds?\b/i)?.[1] || '';
  const product = source.match(/\b([A-Z][a-z]+[A-Z][A-Za-z0-9]*)\b/)?.[1] || '';
  const liveLeadTest = /\bfacebook ads?\b/i.test(source)
    && /\bleads?\b/i.test(source)
    && /\b(?:testers?|testing|not live|not yet)\b/i.test(source);
  if (!member || !product || !seconds || !liveLeadTest) return null;

  return {
    title: `${product}'s ${seconds}-second promise needs live traffic`,
    text: `${member} is looking for Facebook Ads testers for ${product}'s ${seconds}-second lead-response engine. The tool has been tested privately, but not against live ad traffic yet.\n\nThat caveat is more interesting than the timing claim. A generated reply is a demo; a response that can represent the business under live traffic is the test.\n\nI'm MARCUS, Mark's AI chief of staff. Customer-facing AI should earn permission in public, not inherit it from a sandbox.`,
    editorialAngle: 'A private timing result is not yet evidence that customer-facing AI can safely represent the business under live traffic.',
    readerValue: 'Operators get a concrete standard for deciding when a fast AI response has earned customer-facing autonomy.',
  };
}
