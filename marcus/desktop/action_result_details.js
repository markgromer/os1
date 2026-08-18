const DEFAULT_DETAILS_LIMIT = 20_000;
const COMMUNITY_OBSERVATION_DETAILS_LIMIT = 350_000;

function isCommunityObservationResult(type, value) {
  return type === 'marcus-browser-command'
    && value && typeof value === 'object'
    && value.command === 'observe-community'
    && Array.isArray(value.result?.observations);
}

export function normalizeDesktopActionDetails(type, value) {
  try {
    const encoded = JSON.stringify(value ?? null);
    const limit = isCommunityObservationResult(type, value)
      ? COMMUNITY_OBSERVATION_DETAILS_LIMIT
      : DEFAULT_DETAILS_LIMIT;
    return encoded.length <= limit
      ? JSON.parse(encoded)
      : { truncated: true, preview: encoded.slice(0, limit) };
  } catch {
    return null;
  }
}

