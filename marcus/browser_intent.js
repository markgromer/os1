const BROWSER_SURFACE_PATTERN = /\b(browser|chrome|web\s?page|website|skool|gmail|google mail|zoom|youtube|tik\s?tok)\b/i;
const COMPOSITION_PATTERN = /\b(post|comment|reply|response|message|caption)\b/i;
const APPROVAL_PATTERN = /\b(approve|approved|go ahead|do it|post it|publish it|send it|submit it|reply now|comment now)\b/i;
const THREAD_NAVIGATION_PATTERN = /\b(head to|go to|find|open|visit|navigate to|(?:in(?:side)?|on|to) (?:the )?(?:thread|post|tab)|thread)\b/i;
const SUBMISSION_NEGATION_PATTERN = /\b(do not|don't|never|not yet|without)\b[^.!?\n]{0,60}\b(post|publish|send|submit|reply|comment)\b/i;
const FEED_READING_PATTERN = /\b(?:main\s+feed|feed|posts?|comments?|community|group|timeline|latest|browse|read|review|inspect|scan|look\s+through|check\s+out)\b/i;
const COMPOSITION_VERB_PATTERN = /\b(write|draft|compose|type|fill|prepare|create|make|respond)\b/i;
const STANDALONE_POST_PATTERN = /\b(?:(?:new|standalone|own|first)\s+post|(?:create|draft|write|make)\s+(?:a|the)\s+(?:new\s+)?(?:standalone\s+)?post|post\s+(?:of|from)\s+(?:your|marcus))\b/i;
const BROWSER_FOLLOWUP_CONFIRMATION_PATTERN = /^(?:yes|yeah|yep|yup|ok|okay|do it|go ahead|please do|proceed|confirm(?:ed)?|approv(?:e|ed)|i approve|sure)(?:[.!\s,]*(?:i approve|approve it|do it|please|now|with (?:your|the) account|it'?s? (?:marcus|your) account|you are logged in with|you'?re logged in with))*[.!]?$/i;
const BROWSER_PROMPT_PATTERN = /\b(browser|chrome|skool|feed|page|post|posts|comments|thread|visible content|dedicated profile|marcus account)\b/i;
const BROWSER_READ_PROMPT_PATTERN = /\b(read|inspect|review|summari[sz]e|scan|browse|look through|check out|visible content|posts?|comments?)\b/i;
const BROWSER_OPEN_PROMPT_PATTERN = /\b(open|navigate|go to|pull up|visit|browse)\b/i;
const COMMUNITY_MEMORY_PATTERN = /\b(learn|remember|take notes?|build (?:a )?profile|profiles?|members?|engagement|content trends?)\b/i;
const COMMUNITY_NOTIFICATION_PATTERN = /\b(notifications?|mentions?|replies|inbox)\b/i;
const LIVE_BROWSER_CONTEXTS = ['gmail', 'zoom', 'skool', 'google-meet', 'teams', 'youtube', 'tiktok'];
const BROWSER_CONTROL_RETURN_PATTERN = /^(?:(?:browser\s+)?control(?:\s+is|\s+has\s+been)?\s+(?:back|returned|released)(?:\s+(?:back\s+)?to\s+(?:you|marcus))?|(?:i(?:'ve|\s+have)?\s+)?(?:returned|released|gave|given|handed)\s+(?:the\s+)?(?:browser\s+)?control(?:\s+back)?\s+to\s+(?:you|marcus)|you(?:'ve|\s+have|\s+got)\s+(?:the\s+)?(?:browser\s+)?control(?:\s+back)?|it'?s\s+yours(?:\s+again)?)[.!\s]*$/i;
const RESUMABLE_BROWSER_MISSION_SKILLS = new Set([
  'marcus_browser_open',
  'marcus_browser_activate',
  'marcus_browser_read',
  'marcus_browser_observe_community',
  'marcus_browser_inspect_notifications',
  'marcus_browser_fill',
  'marcus_browser_prepare_post',
  'marcus_browser_prepare_reply',
]);

export function isMarcusBrowserFollowupConfirmation(message) {
  const text = String(message || '').replace(/\s+/g, ' ').trim();
  return Boolean(text && BROWSER_FOLLOWUP_CONFIRMATION_PATTERN.test(text));
}

export function isMarcusBrowserControlReturn(message) {
  const text = String(message || '').replace(/\s+/g, ' ').trim();
  return Boolean(text && BROWSER_CONTROL_RETURN_PATTERN.test(text));
}

export function browserMissionSkillForControlReturn(message, mission) {
  if (!isMarcusBrowserControlReturn(message) || !mission || typeof mission !== 'object') return '';
  const status = String(mission.status || '').trim().toLowerCase();
  let skill = String(mission.currentSkill || '').trim();
  if (skill === 'marcus_browser_fill') {
    const retainedIntent = classifyMarcusBrowserIntent(mission.objective || mission.currentInstruction, {
      contextKind: mission.platform,
    });
    if (retainedIntent === 'marcus_browser_prepare_post') skill = retainedIntent;
  }
  if (!['active', 'recovering'].includes(status) || !RESUMABLE_BROWSER_MISSION_SKILLS.has(skill)) return '';
  return skill;
}

export function isMarcusBrowserMissionResume({ message = '', mission = null, toolName = '' } = {}) {
  const resumableSkill = browserMissionSkillForControlReturn(message, mission);
  return Boolean(resumableSkill && resumableSkill === String(toolName || '').trim());
}

export function classifyMarcusBrowserIntent(message, { pendingDraft = false, contextKind = '' } = {}) {
  const text = String(message || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';

  const approvedSubmit = !SUBMISSION_NEGATION_PATTERN.test(text)
    && APPROVAL_PATTERN.test(text)
    && /\b(post|publish|send|submit|reply|comment)\b/i.test(text);
  if (pendingDraft && approvedSubmit && !/\b(email|e-mail|text|sms)\b/i.test(text)) return 'marcus_browser_submit';
  const liveBrowserContext = LIVE_BROWSER_CONTEXTS.includes(String(contextKind || '').trim().toLowerCase());
  const implicitCurrentSurface = liveBrowserContext
    && /\b(?:(?:this|that|the|current|open|visible)\s+(?:thread|post|page|message|comment)|thread)\b/i.test(text);
  const implicitFeedRead = liveBrowserContext && FEED_READING_PATTERN.test(text);
  const implicitBrowserComposition = liveBrowserContext && (COMPOSITION_PATTERN.test(text) || STANDALONE_POST_PATTERN.test(text)) && COMPOSITION_VERB_PATTERN.test(text);
  const explicitRead = /\b(read|review|inspect|analy[sz]e|browse|browsing|scan|summari[sz]e|feedback|look(?:ing)? at|check(?:ing)? out)\b|\blook through\b/i.test(text);
  if (!BROWSER_SURFACE_PATTERN.test(text) && !implicitCurrentSurface && !implicitFeedRead && !implicitBrowserComposition) return '';
  if (approvedSubmit) return 'marcus_browser_submit';

  const skoolContext = String(contextKind || '').trim().toLowerCase() === 'skool' || /\bskool\b/i.test(text);
  if (skoolContext && COMMUNITY_NOTIFICATION_PATTERN.test(text)
    && /\b(check|inspect|read|review|scan|triage|clear|respond|handle|look)\b/i.test(text)) {
    return 'marcus_browser_inspect_notifications';
  }
  if (skoolContext && COMMUNITY_MEMORY_PATTERN.test(text)
    && /\b(read|review|inspect|analy[sz]e|browse|scan|learn|remember|study|take)\b/i.test(text)) {
    return 'marcus_browser_observe_community';
  }

  if (explicitRead) {
    return 'marcus_browser_read';
  }
  if ((COMPOSITION_PATTERN.test(text) || STANDALONE_POST_PATTERN.test(text)) && COMPOSITION_VERB_PATTERN.test(text)) {
    if (!STANDALONE_POST_PATTERN.test(text) && THREAD_NAVIGATION_PATTERN.test(text)) return 'marcus_browser_prepare_reply';
    if (STANDALONE_POST_PATTERN.test(text)
      && (String(contextKind || '').trim().toLowerCase() === 'skool' || /\bskool\b/i.test(text))) {
      return 'marcus_browser_prepare_post';
    }
    return 'marcus_browser_fill';
  }
  if (implicitFeedRead) return 'marcus_browser_read';
  if (FEED_READING_PATTERN.test(text)) return 'marcus_browser_read';
  if (/\b(click|press|activate|choose|select|follow)\b/i.test(text)) return 'marcus_browser_activate';
  if (/\bhttps?:\/\//i.test(text) && /\b(open|show|navigate|go to|pull up|visit)\b/i.test(text)) {
    return 'marcus_browser_open';
  }
  if (/\b(can(?:not|'t)?|unable|access|connected|control|see)\b/i.test(text)) return 'marcus_browser_status';
  return '';
}

export function resolveMarcusBrowserFollowupIntent(message, recentMessages = [], {
  pendingDraft = false, contextKind = '', activeMission = null,
} = {}) {
  const resumedMissionSkill = browserMissionSkillForControlReturn(message, activeMission);
  if (resumedMissionSkill) return resumedMissionSkill;
  const directIntent = classifyMarcusBrowserIntent(message, { pendingDraft, contextKind });
  if (directIntent) return directIntent;
  if (!isMarcusBrowserFollowupConfirmation(message)) return '';

  const liveBrowserContext = LIVE_BROWSER_CONTEXTS.includes(String(contextKind || '').trim().toLowerCase());
  const recentAssistantPrompts = (Array.isArray(recentMessages) ? recentMessages : [])
    .slice(-8)
    .filter((item) => ['assistant', 'ai', 'marcus'].includes(String(item?.role || '').toLowerCase()))
    .map((item) => String(item?.content || '').replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .reverse();

  const browserPrompt = recentAssistantPrompts.find((content) => BROWSER_PROMPT_PATTERN.test(content));
  if (browserPrompt) {
    if (BROWSER_READ_PROMPT_PATTERN.test(browserPrompt)) return 'marcus_browser_read';
    if (BROWSER_OPEN_PROMPT_PATTERN.test(browserPrompt)) return 'marcus_browser_open';
    if (liveBrowserContext) return 'marcus_browser_read';
  }

  if (liveBrowserContext && String(contextKind || '').trim().toLowerCase() === 'skool') {
    return 'marcus_browser_read';
  }
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
