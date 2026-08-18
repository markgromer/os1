import { defineMarcusSkill, verifyMarcusSkillResult } from './skill_contract.js';

function actionResult(result) {
  return result?.details?.result || {};
}

function exactInsertedText(result, input) {
  const observed = actionResult(result);
  const expectedChars = String(input?.text || '').length;
  return expectedChars > 0 && observed.insertedChars === expectedChars;
}

const BROWSER_SKILLS = [
  defineMarcusSkill({
    id: 'browser.observe-status', version: 1, toolName: 'marcus_browser_status', authority: 'observe',
    purpose: 'Report the live browser relay, current approved surface, and control owner without changing browser state.',
    contexts: ['gmail', 'skool', 'zoom', 'google-meet', 'teams', 'youtube', 'tiktok'],
    preconditions: ['authenticated MARCUS session'],
    evidence: ['relay freshness', 'connection state', 'control owner', 'bounded current URL and title'],
    recovery: ['distinguish Chrome offline from relay stale', 'report the exact recovery boundary'],
    verify(result) {
      return typeof result?.connected === 'boolean' && result?.control?.owner
        ? { ok: true, evidence: { connected: result.connected, owner: result.control.owner, contextKind: result.contextKind || '' } }
        : { ok: false, error: 'Browser status omitted connection or control-owner evidence.' };
    },
  }),
  defineMarcusSkill({
    id: 'browser.open-url', version: 1, toolName: 'marcus_browser_open', authority: 'prepare',
    purpose: 'Open an exact approved HTTP(S) URL in a new MARCUS Chrome tab.',
    contexts: ['web'],
    preconditions: ['direct navigation request', 'valid HTTP(S) URL', 'MARCUS has control'],
    evidence: ['new Chrome target id'],
    recovery: ['preserve the current tab', 'report invalid or blocked URL', 'refresh relay status'],
    verify(result) {
      return actionResult(result).targetId
        ? { ok: true, evidence: { targetId: actionResult(result).targetId } }
        : { ok: false, error: 'Chrome did not confirm creation of the requested browser tab.' };
    },
  }),
  defineMarcusSkill({
    id: 'browser.activate-visible-control', version: 1, toolName: 'marcus_browser_activate', authority: 'prepare',
    purpose: 'Activate one exact visible non-consequential browser control.',
    contexts: ['web'],
    preconditions: ['direct click request', 'exact visible label', 'non-consequential action', 'MARCUS has control'],
    evidence: ['matched visible control', 'activation confirmation'],
    recovery: ['do not choose a partial unrelated match', 'report the missing control'],
    verify(result) {
      const observed = actionResult(result);
      return observed.activated === true
        ? { ok: true, evidence: { text: observed.text || observed.label || '', href: observed.href || '' } }
        : { ok: false, error: 'The requested visible control was not confirmed as activated.' };
    },
  }),
  defineMarcusSkill({
    id: 'browser.inspect-visible-page', version: 1, toolName: 'marcus_browser_read', authority: 'observe',
    purpose: 'Read bounded rendered content from an approved visible browser surface.',
    contexts: ['gmail', 'skool', 'zoom', 'google-meet', 'teams', 'youtube', 'tiktok'],
    preconditions: ['browser connected', 'MARCUS has control', 'approved context', 'no password field focused'],
    evidence: ['approved context kind', 'one or more viewports read', 'bounded visible text result'],
    recovery: ['refresh relay status', 'retry the same bounded read', 'report the exact unavailable surface'],
    verify(result) {
      const observed = actionResult(result);
      return observed.contextKind && Number(observed.viewportsRead) >= 1
        ? { ok: true, evidence: { contextKind: observed.contextKind, viewportsRead: observed.viewportsRead } }
        : { ok: false, error: 'The browser read returned no verified viewport evidence.' };
    },
  }),
  defineMarcusSkill({
    id: 'skool.observe-community', version: 1, toolName: 'marcus_browser_observe_community', authority: 'observe',
    purpose: 'Collect bounded source-linked member activity from visible Skool community surfaces for durable profiles.',
    contexts: ['skool'],
    preconditions: ['browser connected', 'MARCUS has control', 'approved Skool surface', 'no password field focused'],
    evidence: ['community identifier', 'structured rendered observations', 'source URLs', 'bounded viewport count'],
    recovery: ['return to the requested community surface', 'retry a bounded scan', 'never infer hidden activity'],
    verify(result) {
      const observed = actionResult(result);
      return observed.contextKind === 'skool' && Array.isArray(observed.observations)
        ? { ok: true, evidence: { community: observed.community, observedCount: observed.observations.length } }
        : { ok: false, error: 'The community scan returned no structured Skool observation evidence.' };
    },
  }),
  defineMarcusSkill({
    id: 'browser.research-social', version: 1, toolName: 'marcus_browser_research_social', authority: 'observe',
    purpose: 'Traverse visible social posts and their rendered comment threads, expand available content, and return source-level coverage evidence.',
    contexts: ['web', 'skool', 'youtube', 'tiktok'],
    preconditions: ['browser connected', 'MARCUS has control', 'direct research request', 'signed-in page already accessible', 'no password field focused'],
    evidence: ['source URLs', 'post text', 'bounded comments', 'posts and comments read counts', 'coverage limits'],
    recovery: ['retain the research mission', 'return to the starting page', 'report inaccessible or unrendered content precisely'],
    verify(result) {
      const observed = actionResult(result);
      return Array.isArray(observed.sources) && observed.coverage && Number(observed.postsRead) >= 0
        ? { ok: true, evidence: {
          contextKind: observed.contextKind,
          postsRead: observed.postsRead,
          commentsRead: observed.commentsRead,
          allVisibleCommentEndsReached: observed.coverage.allVisibleCommentEndsReached === true,
          platformComplete: false,
        } }
        : { ok: false, error: 'Social research returned no verifiable source and coverage ledger.' };
    },
  }),
  defineMarcusSkill({
    id: 'skool.inspect-notifications', version: 1, toolName: 'marcus_browser_inspect_notifications', authority: 'observe',
    purpose: 'Inspect visible Skool notifications and classify them without clearing or answering them.',
    contexts: ['skool'],
    preconditions: ['browser connected', 'MARCUS has control', 'approved Skool surface', 'no password field focused'],
    evidence: ['community identifier', 'structured visible notifications', 'source URLs'],
    recovery: ['open the visible notification control', 'report when the notification surface is unavailable'],
    verify(result) {
      const observed = actionResult(result);
      return observed.contextKind === 'skool' && Array.isArray(observed.notifications)
        ? { ok: true, evidence: { community: observed.community, notificationCount: observed.notifications.length } }
        : { ok: false, error: 'The notification scan returned no structured Skool evidence.' };
    },
  }),
  defineMarcusSkill({
    id: 'browser.prepare-visible-draft', version: 1, toolName: 'marcus_browser_fill', authority: 'prepare',
    purpose: 'Fill an explicitly targeted visible editor without submitting it.',
    contexts: ['gmail', 'skool', 'zoom', 'google-meet', 'teams', 'youtube', 'tiktok'],
    preconditions: ['direct drafting request', 'visible non-password editor', 'MARCUS has control'],
    evidence: ['target editor label', 'exact inserted character count'],
    recovery: ['do not choose an arbitrary editor', 'report the missing editor label'],
    verify(result, input) {
      return exactInsertedText(result, input)
        ? { ok: true, evidence: { insertedChars: actionResult(result).insertedChars } }
        : { ok: false, error: 'The requested visible editor did not verify the exact inserted draft.' };
    },
  }),
  defineMarcusSkill({
    id: 'skool.prepare-standalone-post', version: 1, toolName: 'marcus_browser_prepare_post', authority: 'prepare',
    purpose: 'Prepare a new standalone post in the Skool community feed, never in a thread comment editor.',
    contexts: ['skool'],
    preconditions: ['direct standalone-post request', 'Skool community tab', 'MARCUS has control'],
    evidence: ['community-root URL', 'feed composer opened', 'comment surface rejected', 'exact draft read-back'],
    recovery: ['return to community root', 'dismiss thread surface', 'reopen feed composer', 'fail closed if proof is incomplete'],
    verify(result, input) {
      const observed = actionResult(result);
      const expectedTitle = String(input?.title || '').trim();
      const expectedCategory = String(input?.category || '').trim();
      const expectedPoll = Array.isArray(input?.pollOptions) ? input.pollOptions.filter(Boolean) : [];
      const richCompositionRequired = Boolean(expectedTitle || expectedCategory || expectedPoll.length);
      const richCompositionVerified = !richCompositionRequired || (
        observed.completeDraft === true
        && observed.title === expectedTitle
        && observed.category === expectedCategory
        && JSON.stringify(observed.pollOptions || []) === JSON.stringify(expectedPoll.slice(0, 3))
      );
      const ok = observed.surface === 'standalone-feed-composer'
        && observed.verified === true
        && observed.communityRoot === true
        && exactInsertedText(result, input)
        && richCompositionVerified;
      return ok
        ? { ok: true, evidence: {
          surface: observed.surface,
          href: observed.href,
          insertedChars: observed.insertedChars,
          completeDraft: observed.completeDraft === true,
          title: observed.title || '',
          category: observed.category || '',
          pollOptions: observed.pollOptions || [],
        } }
        : { ok: false, error: 'A complete standalone post was not proven in the Skool main-feed composer.' };
    },
  }),
  defineMarcusSkill({
    id: 'skool.prepare-thread-reply', version: 1, toolName: 'marcus_browser_prepare_reply', authority: 'prepare',
    purpose: 'Open an exact Skool thread and prepare an unsubmitted reply in its current comment editor.',
    contexts: ['skool'],
    preconditions: ['direct thread-reply request', 'distinctive thread title', 'MARCUS has control'],
    evidence: ['matched thread', 'thread URL', 'reply editor', 'exact inserted character count'],
    recovery: ['return to community root', 'relocate the exact thread', 'jump to latest comment', 'fail closed'],
    verify(result, input) {
      const observed = actionResult(result);
      return observed.thread && observed.href && exactInsertedText(result, input)
        ? { ok: true, evidence: { thread: observed.thread, href: observed.href, insertedChars: observed.insertedChars } }
        : { ok: false, error: 'The Skool reply was not verified in the requested thread editor.' };
    },
  }),
  defineMarcusSkill({
    id: 'browser.publish-approved-draft', version: 1, toolName: 'marcus_browser_submit', authority: 'consequential',
    purpose: 'Publish only the exact recent draft Mark approved and retain browser read-back evidence.',
    contexts: ['gmail', 'skool'],
    preconditions: ['durable admin authentication', 'exact recent draft', 'explicit publish approval', 'MARCUS has control'],
    evidence: ['publication id', 'exact text match', 'approved submit control', 'published read-back'],
    recovery: ['never use a generic click', 'retain failed draft', 'require a fresh exact approval after edits'],
    verify(result) {
      const observed = actionResult(result);
      return observed.published === true && observed.publicationId
        ? { ok: true, evidence: { publicationId: observed.publicationId, submitLabel: observed.submitLabel } }
        : { ok: false, error: 'The browser did not prove publication of the exact approved draft.' };
    },
  }),
];

const BY_TOOL = new Map(BROWSER_SKILLS.map((skill) => [skill.toolName, skill]));

export function listMarcusBrowserSkills() {
  return [...BROWSER_SKILLS];
}

export function describeMarcusBrowserSkills() {
  return BROWSER_SKILLS.map(({ verify: _verify, ...skill }) => ({ ...skill }));
}

export function getMarcusBrowserSkill(toolName) {
  return BY_TOOL.get(String(toolName || '').trim()) || null;
}

export function verifyMarcusBrowserSkillResult(toolName, result, input = {}) {
  const skill = getMarcusBrowserSkill(toolName);
  if (!skill) return result?.ok ? { ok: true, skillId: '', version: 0 } : { ok: false, error: String(result?.error || 'Browser action failed.') };
  return verifyMarcusSkillResult(skill, result, input);
}
