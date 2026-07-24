const ACTION_ALIASES = Object.freeze({
  commit: ['commit'],
  push: ['push'],
  deploy: ['deploy'],
  publish: ['publish', 'release', 'ship'],
  merge: ['merge'],
});

function escapePattern(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function actionPattern(action) {
  return ACTION_ALIASES[action].map(escapePattern).join('|');
}

function hasNegation(text, action) {
  const verb = actionPattern(action);
  return new RegExp(`\\b(?:do\\s+not|don't|dont|never|no)\\s+(?:\\w+\\s+){0,4}(?:${verb})\\b|\\b(?:${verb})\\b(?:\\s+\\w+){0,3}\\s+not\\s+yet\\b`, 'i').test(text);
}

function isPreparationOnly(clause, action) {
  const verb = actionPattern(action);
  return new RegExp(`\\b(?:prepare|preparing|plan|planning|review|reviewing)\\b[^.;!?]{0,50}\\b(?:${verb})\\b|\\b(?:${verb})\\s+(?:plan|review)\\b`, 'i').test(clause);
}

function hasPositiveAuthorization(text, action) {
  const verb = actionPattern(action);
  const clauses = text.split(/[.;!?\n]+|\bbut\b/i).map((item) => item.trim()).filter(Boolean);
  return clauses.some((clause) => {
    if (!new RegExp(`\\b(?:${verb})\\b`, 'i').test(clause) || isPreparationOnly(clause, action)) return false;
    return new RegExp(`\\b(?:approve|approved|authorize|authorized|approval\\s+granted)\\b[^.;!?]{0,80}\\b(?:${verb})\\b`, 'i').test(clause)
      || new RegExp(`^(?:please\\s+)?(?:${verb})\\b`, 'i').test(clause)
      || new RegExp(`\\b(?:you\\s+may|you\\s+can|i\\s+want\\s+you\\s+to|go\\s+ahead(?:\\s+and|\\s+to)?|proceed(?:\\s+and|\\s+to)?)\\s+(?:${verb})\\b`, 'i').test(clause)
      || new RegExp(`\\b(?:${verb})\\s+(?:it|this|these|the\\b)`, 'i').test(clause);
  });
}

export function getExplicitActionAuthorizations(messageText) {
  const text = String(messageText || '').replace(/[\u2019]/g, "'").trim().toLowerCase();
  const output = {};
  for (const action of Object.keys(ACTION_ALIASES)) {
    const denied = hasNegation(text, action);
    output[action] = {
      authorized: !denied && hasPositiveAuthorization(text, action),
      denied,
    };
  }
  return output;
}

export function messageHasExplicitPublishApproval(messageText, action = 'push') {
  const decision = getExplicitActionAuthorizations(messageText)[action];
  return decision?.authorized === true;
}

export function authorizedPublishActions(messageText) {
  const decisions = getExplicitActionAuthorizations(messageText);
  return Object.entries(decisions).filter(([, value]) => value.authorized).map(([action]) => action);
}

export function scopeAuthorizedPublishActions(messageText, requested = {}) {
  const decisions = getExplicitActionAuthorizations(messageText);
  const requestedActions = Object.keys(ACTION_ALIASES).filter((action) => requested?.[action] === true);
  const unauthorizedActions = requestedActions.filter((action) => decisions[action]?.authorized !== true);
  return {
    ok: unauthorizedActions.length === 0,
    requestedActions,
    authorizedActions: requestedActions.filter((action) => decisions[action]?.authorized === true),
    unauthorizedActions,
    decisions,
  };
}

export { ACTION_ALIASES };
