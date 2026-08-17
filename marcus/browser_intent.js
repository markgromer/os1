const BROWSER_SURFACE_PATTERN = /\b(browser|chrome|web\s?page|website|skool|gmail|google mail|zoom|youtube|tik\s?tok)\b/i;
const COMPOSITION_PATTERN = /\b(post|comment|reply|response|message|caption)\b/i;
const APPROVAL_PATTERN = /\b(approve|approved|go ahead|do it|post it|publish it|send it|submit it|reply now|comment now)\b/i;
const THREAD_NAVIGATION_PATTERN = /\b(head to|go to|find|open|visit|navigate to|(?:in(?:side)?|on|to) (?:the )?(?:thread|post|tab)|thread)\b/i;
const SUBMISSION_NEGATION_PATTERN = /\b(do not|don't|never|not yet|without)\b[^.!?\n]{0,60}\b(post|publish|send|submit|reply|comment)\b/i;

export function classifyMarcusBrowserIntent(message, { pendingDraft = false, contextKind = '' } = {}) {
  const text = String(message || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';

  const approvedSubmit = !SUBMISSION_NEGATION_PATTERN.test(text)
    && APPROVAL_PATTERN.test(text)
    && /\b(post|publish|send|submit|reply|comment)\b/i.test(text);
  if (pendingDraft && approvedSubmit && !/\b(email|e-mail|text|sms)\b/i.test(text)) return 'marcus_browser_submit';
  const liveBrowserContext = ['gmail', 'zoom', 'skool', 'google-meet', 'teams', 'youtube', 'tiktok']
    .includes(String(contextKind || '').trim().toLowerCase());
  const implicitCurrentSurface = liveBrowserContext
    && /\b(?:(?:this|that|the|current|open|visible)\s+(?:thread|post|page|message|comment)|thread)\b/i.test(text);
  if (!BROWSER_SURFACE_PATTERN.test(text) && !implicitCurrentSurface) return '';
  if (approvedSubmit) return 'marcus_browser_submit';

  if (/\b(read|review|inspect|analy[sz]e|browse|browsing|scan|summari[sz]e|feedback|look(?:ing)? at)\b|\blook through\b/i.test(text)) {
    return 'marcus_browser_read';
  }
  if (COMPOSITION_PATTERN.test(text) && /\b(write|draft|compose|type|fill|prepare|create|make|respond)\b/i.test(text)) {
    if (THREAD_NAVIGATION_PATTERN.test(text)) return 'marcus_browser_prepare_reply';
    return 'marcus_browser_fill';
  }
  if (/\b(click|press|activate|choose|select|follow)\b/i.test(text)) return 'marcus_browser_activate';
  if (/\bhttps?:\/\//i.test(text) && /\b(open|show|navigate|go to|pull up|visit)\b/i.test(text)) {
    return 'marcus_browser_open';
  }
  if (/\b(can(?:not|'t)?|unable|access|connected|control|see)\b/i.test(text)) return 'marcus_browser_status';
  return '';
}

export function validateMarcusIntroductionDraft(text, { requestMessage = '' } = {}) {
  const request = String(requestMessage || '').replace(/\s+/g, ' ').trim();
  const draft = String(text || '').replace(/\s+/g, ' ').trim();
  const marcusIntroduction = /\bmarcus\b/i.test(request)
    || /\b(?:your|yourself|himself)\b[^.!?\n]{0,60}\bintro(?:duce|duction)?\b/i.test(request)
    || /\bintro(?:duce|duction)?\b[^.!?\n]{0,60}\b(?:yourself|himself)\b/i.test(request);
  if (!marcusIntroduction) return { ok: true };
  if (/\b(?:i am|i'm)\s+mark\b/i.test(draft)) {
    return { ok: false, error: 'MARCUS cannot introduce himself as Mark.' };
  }
  if (!/\bmarcus\b/i.test(draft)) {
    return { ok: false, error: 'MARCUS must identify himself by name in his introduction.' };
  }
  if (!/\bAI\b/i.test(draft)) {
    return { ok: false, error: 'MARCUS must identify himself openly as AI in his introduction.' };
  }
  return { ok: true };
}
