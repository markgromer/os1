export function messageHasExplicitPublishApproval(messageText) {
  const text = String(messageText || '').toLowerCase();
  const hasApproval = /\b(approve|approved|approval granted|go ahead|ship it|publish it|push it|send it|do it)\b/.test(text);
  const hasPublishIntent = /\b(publish|push|commit|deploy|ship|release)\b/.test(text);
  return hasApproval && hasPublishIntent;
}
