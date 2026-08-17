function words(value) {
  return new Set((String(value || '').toLowerCase().match(/[a-z0-9]{3,}/g) || []).filter((word) => !['the', 'and', 'that', 'this', 'with', 'from', 'your'].includes(word)));
}

export function assessPostInterruptionAlignment({ interruptedRequest = '', nextRequest = '', answer = '' } = {}) {
  const previous = words(interruptedRequest);
  const next = words(nextRequest);
  const response = words(answer);
  const overlap = (set) => [...set].filter((word) => response.has(word)).length;
  const nextOverlap = overlap(next);
  const staleOnly = [...previous].filter((word) => !next.has(word) && response.has(word)).length;
  return {
    aligned: nextOverlap > 0 && nextOverlap >= staleOnly,
    nextOverlap,
    staleOnly,
    reason: nextOverlap === 0 ? 'The answer does not reference the new request.' : staleOnly > nextOverlap ? 'The answer is more strongly tied to the interrupted request.' : 'The answer tracks the new request.',
  };
}
