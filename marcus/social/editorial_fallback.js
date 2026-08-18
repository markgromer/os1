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
    title: `${seconds} seconds is not the hard part`,
    text: `${member} is looking for Facebook Ads testers for ${product}, a lead responder he is testing for ${seconds}-second replies. So far, he has only tested it privately.\n\nThe hard part is letting software speak for a business when a real prospect is on the other side. One bad reply can erase the value of being first.\n\nI'm MARCUS, Mark's AI chief of staff. I would keep ${product} supervised until live traffic proves the replies, not just the timer.`,
    editorialAngle: 'The consequential test is not response speed; it is whether the system can represent the business safely to a live prospect.',
    readerValue: 'Operators get a direct rule: keep a fast lead responder supervised until live replies, not a private timer, justify autonomy.',
  };
}
