import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import {
  nowIso,
  redactSecrets,
  safeBusinessKey,
  safeHttpUrl,
  safeInteger,
  safeObject,
  safeString,
} from '../operations/operation_types.js';

const STORE_VERSION = 2;
const MAX_MEMBERS = 2_000;
const MAX_OBSERVATIONS = 5_000;
const MAX_NOTIFICATIONS = 2_000;
const MAX_THREADS = 5_000;
const MAX_RESEARCH_RUNS = 500;
const MAX_ANNOTATIONS = 5_000;
const OBSERVATION_KINDS = new Set(['post', 'comment', 'reply', 'reaction', 'mention', 'profile', 'other']);
const NOTIFICATION_STATES = new Set(['unread', 'triaged', 'drafted', 'cleared', 'responded', 'ignored']);
const KNOWLEDGE_KINDS = new Set([
  'goal', 'struggle', 'expertise', 'language', 'notable_moment', 'open_loop', 'relationship',
  'cultural_reference', 'recurring_topic', 'ongoing_thread',
]);
const SENSITIVE_INFERENCE_PATTERN = /\b(?:age(?:d)?|racial|race|ethnic|religio|politic|sexual|gender|disab|medical|health|income|financ|citizen|nationalit)/i;
const TOPIC_STOP_WORDS = new Set([
  'about', 'after', 'again', 'also', 'because', 'been', 'before', 'being', 'between', 'business',
  'community', 'could', 'does', 'from', 'have', 'here', 'into', 'just', 'like', 'more', 'only',
  'other', 'people', 'really', 'should', 'some', 'than', 'that', 'their', 'there', 'these', 'they',
  'this', 'those', 'through', 'using', 'very', 'want', 'what', 'when', 'where', 'which', 'with',
  'would', 'your', 'youre', 'skool', 'scoopos',
]);

function normalizeText(value, maxChars = 1_000) {
  return redactSecrets(safeString(value, maxChars), maxChars).replace(/\s+/g, ' ').trim();
}

function normalizeList(value, { maxItems = 20, maxChars = 160 } = {}) {
  const output = [];
  const seen = new Set();
  for (const item of Array.isArray(value) ? value : []) {
    const text = normalizeText(item, maxChars);
    const key = text.toLowerCase();
    if (!text || seen.has(key)) continue;
    seen.add(key);
    output.push(text);
    if (output.length >= maxItems) break;
  }
  return output;
}

function normalizedIso(value) {
  const parsed = Date.parse(String(value || ''));
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : nowIso();
}

function stableId(prefix, value) {
  return `${prefix}_${crypto.createHash('sha256').update(String(value || '')).digest('base64url').slice(0, 18)}`;
}

function sourceFingerprint(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('base64url').slice(0, 24);
}

function sentenceFragments(value) {
  return String(value || '').replace(/\s+/g, ' ').trim().split(/(?<=[.!?])\s+/).map((item) => item.trim()).filter(Boolean);
}

function explicitSignals(text) {
  const sentences = sentenceFragments(text);
  return {
    struggles: sentences.filter((sentence) => /\b(?:struggl|stuck|hard(?:est)?|difficult|problem|issue|pain|frustrat|fail|los(?:e|ing)|can(?:not|'t)|need help|keeps? getting)\b/i.test(sentence)).slice(0, 8),
    goals: sentences.filter((sentence) => /\b(?:my goal|our goal|trying to|want to|need to|plan to|hoping to|working toward|looking to)\b/i.test(sentence)).slice(0, 8),
    questions: sentences.filter((sentence) => sentence.endsWith('?')).slice(0, 8),
  };
}

function extractTopicTerms(value) {
  const words = normalizeText(value, 3_000).toLowerCase().match(/[a-z][a-z0-9'-]{3,}/g) || [];
  return [...new Set(words.filter((word) => !TOPIC_STOP_WORDS.has(word)))].slice(0, 20);
}

function normalizePlatform(value) {
  return normalizeText(value, 80).toLowerCase() || 'unknown';
}

function normalizeCommunity(value) {
  return normalizeText(value, 160) || 'Unknown community';
}

function memberIdentity(raw, platform, community) {
  const profileUrl = safeHttpUrl(raw.profileUrl || raw.authorUrl || '');
  const platformUserId = normalizeText(raw.platformUserId || raw.userId, 160);
  const displayName = normalizeText(raw.displayName || raw.author || raw.name, 200) || 'Unknown member';
  const identity = platformUserId || profileUrl.toLowerCase() || displayName.toLowerCase();
  return {
    id: stableId('person', `${platform}\n${community.toLowerCase()}\n${identity}`),
    displayName,
    platformUserId,
    profileUrl,
  };
}

function normalizeEngagement(value) {
  const raw = safeObject(value);
  return {
    reactions: safeInteger(raw.reactions, 0, 0, 10_000_000),
    comments: safeInteger(raw.comments, 0, 0, 10_000_000),
    replies: safeInteger(raw.replies, 0, 0, 10_000_000),
  };
}

function normalizeInference(value, observationId) {
  const raw = safeObject(value);
  const label = normalizeText(raw.label, 160);
  const summary = normalizeText(raw.summary || raw.value, 500);
  if (!label || !summary || SENSITIVE_INFERENCE_PATTERN.test(`${label} ${summary}`)) return null;
  return {
    id: stableId('inf', `${observationId}\n${label.toLowerCase()}\n${summary.toLowerCase()}`),
    label,
    summary,
    confidence: Math.max(0, Math.min(1, Number(raw.confidence) || 0)),
    sourceObservationIds: [observationId],
    updatedAt: nowIso(),
  };
}

function normalizeObservation(input) {
  const raw = safeObject(input);
  const platform = normalizePlatform(raw.platform);
  const community = normalizeCommunity(raw.community);
  const member = memberIdentity(safeObject(raw.member || raw.author), platform, community);
  const sourceUrl = safeHttpUrl(raw.sourceUrl || raw.url || '');
  const sourceTitle = normalizeText(raw.sourceTitle || raw.title, 300);
  const contentSummary = normalizeText(raw.contentSummary || raw.content || raw.text, 1_500);
  if (!contentSummary && !sourceTitle) {
    throw Object.assign(new Error('Community observation content or title is required.'), { code: 'COMMUNITY_OBSERVATION_CONTENT_REQUIRED' });
  }
  const kindValue = normalizeText(raw.kind, 80).toLowerCase();
  const observedAt = normalizedIso(raw.observedAt || raw.timestamp);
  const sourceKey = normalizeText(raw.sourceKey, 500)
    || `${sourceUrl}\n${member.id}\n${kindValue}\n${contentSummary}`;
  const id = /^obs_[A-Za-z0-9_-]{8,}$/.test(safeString(raw.id, 120))
    ? safeString(raw.id, 120)
    : stableId('obs', sourceKey);
  return {
    id,
    platform,
    community,
    memberId: member.id,
    member,
    kind: OBSERVATION_KINDS.has(kindValue) ? kindValue : 'other',
    sourceUrl,
    sourceTitle,
    contentSummary,
    threadId: safeString(raw.threadId, 120),
    parentObservationId: safeString(raw.parentObservationId, 120),
    position: safeInteger(raw.position, 0, 0, 100_000),
    sourceFingerprint: normalizeText(raw.sourceFingerprint, 120) || sourceFingerprint(`${sourceUrl}\n${contentSummary}`),
    topics: normalizeList(raw.topics, { maxItems: 20, maxChars: 100 }),
    engagement: normalizeEngagement(raw.engagement),
    observedAt,
    capturedAt: nowIso(),
    evidenceLevel: 'observed',
    inferences: (Array.isArray(raw.inferences) ? raw.inferences : [])
      .map((item) => normalizeInference(item, id)).filter(Boolean).slice(0, 12),
  };
}

function notificationRecommendation(raw, summary) {
  const text = `${normalizeText(raw.kind, 80)} ${summary}`.toLowerCase();
  if (/\b(threat|legal|refund|chargeback|angry|harass|payment|security|account locked)\b/.test(text)) {
    return { action: 'escalate', priority: 'high', reason: 'Potential reputation, money, security, or conflict risk.' };
  }
  if (/\b(asked|question|mentioned|mention|reply|replied|comment|direct message|dm)\b/.test(text) || /\?$/.test(summary)) {
    return { action: 'draft_response', priority: 'normal', reason: 'A person may be expecting a response.' };
  }
  if (/\b(like|liked|reaction|reacted|follow|followed)\b/.test(text)) {
    return { action: 'clear', priority: 'low', reason: 'Acknowledgement-only activity normally does not need a response.' };
  }
  return { action: 'review', priority: 'normal', reason: 'The notification needs human-readable context before action.' };
}

function normalizeNotification(input) {
  const raw = safeObject(input);
  const platform = normalizePlatform(raw.platform);
  const community = normalizeCommunity(raw.community);
  const actorInput = safeObject(raw.actor || raw.member);
  const actor = normalizeText(actorInput.displayName || actorInput.name || raw.actorName, 200)
    ? memberIdentity(actorInput.displayName ? actorInput : { ...actorInput, displayName: raw.actorName }, platform, community)
    : null;
  const sourceUrl = safeHttpUrl(raw.sourceUrl || raw.url || '');
  const summary = normalizeText(raw.summary || raw.text || raw.title, 1_000);
  if (!summary) throw Object.assign(new Error('Notification summary is required.'), { code: 'COMMUNITY_NOTIFICATION_SUMMARY_REQUIRED' });
  const sourceKey = normalizeText(raw.sourceKey, 500) || `${sourceUrl}\n${actor?.id || ''}\n${summary}`;
  const recommendation = notificationRecommendation(raw, summary);
  return {
    id: /^notif_[A-Za-z0-9_-]{8,}$/.test(safeString(raw.id, 120))
      ? safeString(raw.id, 120)
      : stableId('notif', sourceKey),
    platform,
    community,
    actorId: actor?.id || '',
    actor,
    kind: normalizeText(raw.kind, 80).toLowerCase() || 'other',
    summary,
    targetTitle: normalizeText(raw.targetTitle || raw.target, 300),
    sourceUrl,
    observedAt: normalizedIso(raw.observedAt || raw.timestamp),
    state: 'unread',
    recommendation,
    draftId: '',
    transitionEvidence: [],
    updatedAt: nowIso(),
  };
}

function emptyDocument(businessKey) {
  return {
    version: STORE_VERSION,
    businessKey: safeBusinessKey(businessKey),
    revision: 1,
    updatedAt: new Date(0).toISOString(),
    members: [],
    observations: [],
    threads: [],
    communities: [],
    researchRuns: [],
    annotations: [],
    notifications: [],
  };
}

function normalizeKnowledgeSignal(input) {
  const raw = safeObject(input);
  const kind = normalizeText(raw.kind, 80).toLowerCase();
  const summary = normalizeText(raw.summary || raw.value, 800);
  if (!KNOWLEDGE_KINDS.has(kind) || !summary || SENSITIVE_INFERENCE_PATTERN.test(`${kind} ${summary}`)) return null;
  const sourceObservationIds = normalizeList(raw.sourceObservationIds, { maxItems: 30, maxChars: 120 });
  return {
    id: safeString(raw.id, 120) || stableId('knowledge', `${kind}\n${summary.toLowerCase()}\n${sourceObservationIds.join('\n')}`),
    kind,
    summary,
    scope: ['member', 'thread', 'community'].includes(normalizeText(raw.scope, 40).toLowerCase())
      ? normalizeText(raw.scope, 40).toLowerCase() : 'community',
    memberId: safeString(raw.memberId, 120),
    threadId: safeString(raw.threadId, 120),
    platform: normalizePlatform(raw.platform),
    community: normalizeCommunity(raw.community),
    relatedMemberIds: normalizeList(raw.relatedMemberIds, { maxItems: 20, maxChars: 120 }),
    sourceObservationIds,
    confidence: Math.max(0, Math.min(1, Number(raw.confidence) || 0)),
    firstObservedAt: normalizedIso(raw.firstObservedAt || raw.observedAt),
    lastObservedAt: normalizedIso(raw.lastObservedAt || raw.observedAt),
    updatedAt: normalizedIso(raw.updatedAt),
  };
}

function normalizeMember(input) {
  const raw = safeObject(input);
  return {
    id: safeString(raw.id, 120),
    platform: normalizePlatform(raw.platform),
    community: normalizeCommunity(raw.community),
    displayName: normalizeText(raw.displayName, 200) || 'Unknown member',
    platformUserId: normalizeText(raw.platformUserId, 160),
    profileUrl: safeHttpUrl(raw.profileUrl || ''),
    aliases: normalizeList(raw.aliases, { maxItems: 20, maxChars: 200 }),
    firstSeenAt: normalizedIso(raw.firstSeenAt),
    lastSeenAt: normalizedIso(raw.lastSeenAt),
    observationCount: safeInteger(raw.observationCount, 0, 0, MAX_OBSERVATIONS),
    activityCounts: {
      post: safeInteger(raw.activityCounts?.post, 0, 0, MAX_OBSERVATIONS),
      comment: safeInteger(raw.activityCounts?.comment, 0, 0, MAX_OBSERVATIONS),
      reply: safeInteger(raw.activityCounts?.reply, 0, 0, MAX_OBSERVATIONS),
      reaction: safeInteger(raw.activityCounts?.reaction, 0, 0, MAX_OBSERVATIONS),
      mention: safeInteger(raw.activityCounts?.mention, 0, 0, MAX_OBSERVATIONS),
      profile: safeInteger(raw.activityCounts?.profile, 0, 0, MAX_OBSERVATIONS),
      other: safeInteger(raw.activityCounts?.other, 0, 0, MAX_OBSERVATIONS),
    },
    topics: (Array.isArray(raw.topics) ? raw.topics : []).map((item) => ({
      name: normalizeText(item?.name, 100), count: safeInteger(item?.count, 0, 0, MAX_OBSERVATIONS),
    })).filter((item) => item.name).slice(0, 40),
    verifiedFacts: (Array.isArray(raw.verifiedFacts) ? raw.verifiedFacts : []).map((item) => ({
      key: normalizeText(item?.key, 120), value: normalizeText(item?.value, 500),
      sourceObservationId: safeString(item?.sourceObservationId, 120), observedAt: normalizedIso(item?.observedAt),
    })).filter((item) => item.key && item.value).slice(-100),
    inferences: (Array.isArray(raw.inferences) ? raw.inferences : []).map((item) => ({
      id: safeString(item?.id, 120), label: normalizeText(item?.label, 160), summary: normalizeText(item?.summary, 500),
      confidence: Math.max(0, Math.min(1, Number(item?.confidence) || 0)),
      sourceObservationIds: normalizeList(item?.sourceObservationIds, { maxItems: 20, maxChars: 120 }),
      updatedAt: normalizedIso(item?.updatedAt),
    })).filter((item) => item.id && item.label && item.summary && !SENSITIVE_INFERENCE_PATTERN.test(`${item.label} ${item.summary}`)).slice(-100),
    openLoops: normalizeList(raw.openLoops, { maxItems: 30, maxChars: 500 }),
    signals: (Array.isArray(raw.signals) ? raw.signals : []).map(normalizeKnowledgeSignal).filter(Boolean).slice(-200),
    relationships: (Array.isArray(raw.relationships) ? raw.relationships : []).map((item) => ({
      memberId: safeString(item?.memberId, 120),
      displayName: normalizeText(item?.displayName, 200),
      sharedThreadCount: safeInteger(item?.sharedThreadCount, 0, 0, MAX_THREADS),
      lastSeenAt: normalizedIso(item?.lastSeenAt),
      sourceObservationIds: normalizeList(item?.sourceObservationIds, { maxItems: 30, maxChars: 120 }),
    })).filter((item) => item.memberId).slice(0, 100),
    noteFilename: normalizeText(raw.noteFilename, 160),
  };
}

function normalizeThread(input) {
  const raw = safeObject(input);
  const sourceUrl = safeHttpUrl(raw.sourceUrl || raw.url || '');
  const id = safeString(raw.id, 120) || stableId('thread', sourceUrl || `${raw.platform}\n${raw.community}\n${raw.title}`);
  return {
    id,
    platform: normalizePlatform(raw.platform),
    community: normalizeCommunity(raw.community),
    sourceUrl,
    title: normalizeText(raw.title, 300),
    postSummary: normalizeText(raw.postSummary || raw.postText, 2_000),
    authorMemberId: safeString(raw.authorMemberId, 120),
    participantMemberIds: normalizeList(raw.participantMemberIds, { maxItems: 500, maxChars: 120 }),
    observationIds: normalizeList(raw.observationIds, { maxItems: 500, maxChars: 120 }),
    commentCountObserved: safeInteger(raw.commentCountObserved, 0, 0, 100_000),
    reachedVisibleEnd: raw.reachedVisibleEnd === true,
    firstObservedAt: normalizedIso(raw.firstObservedAt),
    lastObservedAt: normalizedIso(raw.lastObservedAt),
    lastFingerprint: normalizeText(raw.lastFingerprint, 120),
    revisionCount: safeInteger(raw.revisionCount, 1, 1, 100_000),
    openQuestions: normalizeList(raw.openQuestions, { maxItems: 50, maxChars: 500 }),
    explicitStruggles: normalizeList(raw.explicitStruggles, { maxItems: 50, maxChars: 500 }),
    explicitGoals: normalizeList(raw.explicitGoals, { maxItems: 50, maxChars: 500 }),
    topicTerms: normalizeList(raw.topicTerms, { maxItems: 50, maxChars: 100 }),
  };
}

function normalizeCommunityModel(input) {
  const raw = safeObject(input);
  const platform = normalizePlatform(raw.platform);
  const name = normalizeCommunity(raw.name || raw.community);
  return {
    id: safeString(raw.id, 120) || stableId('community', `${platform}\n${name.toLowerCase()}`),
    platform,
    name,
    sourceUrl: safeHttpUrl(raw.sourceUrl || ''),
    firstObservedAt: normalizedIso(raw.firstObservedAt),
    lastObservedAt: normalizedIso(raw.lastObservedAt),
    threadCount: safeInteger(raw.threadCount, 0, 0, MAX_THREADS),
    observationCount: safeInteger(raw.observationCount, 0, 0, MAX_OBSERVATIONS),
    memberCount: safeInteger(raw.memberCount, 0, 0, MAX_MEMBERS),
    recurringTopics: (Array.isArray(raw.recurringTopics) ? raw.recurringTopics : []).map((item) => ({
      name: normalizeText(item?.name, 100), count: safeInteger(item?.count, 0, 0, MAX_OBSERVATIONS),
      memberCount: safeInteger(item?.memberCount, 0, 0, MAX_MEMBERS),
      sourceObservationIds: normalizeList(item?.sourceObservationIds, { maxItems: 20, maxChars: 120 }),
    })).filter((item) => item.name).slice(0, 80),
    commonStruggles: (Array.isArray(raw.commonStruggles) ? raw.commonStruggles : []).map((item) => ({
      summary: normalizeText(item?.summary, 500), memberId: safeString(item?.memberId, 120),
      sourceObservationId: safeString(item?.sourceObservationId, 120), observedAt: normalizedIso(item?.observedAt),
    })).filter((item) => item.summary && item.sourceObservationId).slice(-100),
    openQuestions: (Array.isArray(raw.openQuestions) ? raw.openQuestions : []).map((item) => ({
      summary: normalizeText(item?.summary, 500), memberId: safeString(item?.memberId, 120),
      sourceObservationId: safeString(item?.sourceObservationId, 120), observedAt: normalizedIso(item?.observedAt),
    })).filter((item) => item.summary && item.sourceObservationId).slice(-100),
    recurringLanguage: (Array.isArray(raw.recurringLanguage) ? raw.recurringLanguage : []).map((item) => ({
      phrase: normalizeText(item?.phrase, 160), observationCount: safeInteger(item?.observationCount, 0, 0, MAX_OBSERVATIONS),
      memberCount: safeInteger(item?.memberCount, 0, 0, MAX_MEMBERS),
      sourceObservationIds: normalizeList(item?.sourceObservationIds, { maxItems: 20, maxChars: 120 }),
    })).filter((item) => item.phrase).slice(0, 60),
    annotations: (Array.isArray(raw.annotations) ? raw.annotations : []).map(normalizeKnowledgeSignal).filter(Boolean).slice(-500),
    coverage: {
      feedViewportsRead: safeInteger(raw.coverage?.feedViewportsRead, 0, 0, 100_000),
      postsDiscovered: safeInteger(raw.coverage?.postsDiscovered, 0, 0, 100_000),
      postsRead: safeInteger(raw.coverage?.postsRead, 0, 0, 100_000),
      commentsRead: safeInteger(raw.coverage?.commentsRead, 0, 0, 1_000_000),
      feedEndReached: raw.coverage?.feedEndReached === true,
      allVisibleCommentEndsReached: raw.coverage?.allVisibleCommentEndsReached === true,
      lastRunAt: normalizedIso(raw.coverage?.lastRunAt),
      resumeToken: normalizeText(raw.coverage?.resumeToken, 200),
      limitation: normalizeText(raw.coverage?.limitation, 600),
    },
  };
}

function normalizeResearchRun(input) {
  const raw = safeObject(input);
  const sourceUrls = normalizeList(raw.sourceUrls, { maxItems: 500, maxChars: 2_000 }).map(safeHttpUrl).filter(Boolean);
  const startedAt = normalizedIso(raw.startedAt || raw.observedAt);
  return {
    id: safeString(raw.id, 120) || stableId('research', `${startedAt}\n${sourceUrls.join('\n')}`),
    platform: normalizePlatform(raw.platform),
    community: normalizeCommunity(raw.community),
    sourceUrl: safeHttpUrl(raw.sourceUrl || raw.startingUrl || ''),
    query: normalizeText(raw.query, 300),
    startedAt,
    completedAt: normalizedIso(raw.completedAt || raw.observedAt),
    sourceUrls,
    postsDiscovered: safeInteger(raw.postsDiscovered, 0, 0, 100_000),
    postsRead: safeInteger(raw.postsRead, 0, 0, 100_000),
    commentsRead: safeInteger(raw.commentsRead, 0, 0, 1_000_000),
    coverage: safeObject(raw.coverage),
  };
}

function normalizeDocument(input, businessKey) {
  const raw = safeObject(input);
  const key = safeBusinessKey(businessKey || raw.businessKey);
  const observations = [];
  const observationIds = new Set();
  for (const item of (Array.isArray(raw.observations) ? raw.observations : []).slice(-MAX_OBSERVATIONS)) {
    try {
      const normalized = normalizeObservation(item);
      if (observationIds.has(normalized.id)) continue;
      observationIds.add(normalized.id);
      observations.push(normalized);
    } catch {}
  }
  const members = (Array.isArray(raw.members) ? raw.members : []).slice(-MAX_MEMBERS)
    .map(normalizeMember).filter((item) => item.id);
  const threads = (Array.isArray(raw.threads) ? raw.threads : []).slice(-MAX_THREADS)
    .map(normalizeThread).filter((item) => item.id);
  const communities = (Array.isArray(raw.communities) ? raw.communities : [])
    .map(normalizeCommunityModel).filter((item) => item.id);
  const researchRuns = (Array.isArray(raw.researchRuns) ? raw.researchRuns : []).slice(-MAX_RESEARCH_RUNS)
    .map(normalizeResearchRun).filter((item) => item.id);
  const annotations = (Array.isArray(raw.annotations) ? raw.annotations : []).slice(-MAX_ANNOTATIONS)
    .map(normalizeKnowledgeSignal).filter(Boolean);
  const notifications = (Array.isArray(raw.notifications) ? raw.notifications : []).slice(-MAX_NOTIFICATIONS)
    .map((item) => {
      try {
        const normalized = normalizeNotification(item);
        const state = normalizeText(item?.state, 80).toLowerCase();
        normalized.state = NOTIFICATION_STATES.has(state) ? state : 'unread';
        normalized.draftId = safeString(item?.draftId, 120);
        normalized.transitionEvidence = (Array.isArray(item?.transitionEvidence) ? item.transitionEvidence : []).slice(-20).map((entry) => ({
          state: NOTIFICATION_STATES.has(normalizeText(entry?.state, 80).toLowerCase())
            ? normalizeText(entry?.state, 80).toLowerCase() : 'triaged',
          at: normalizedIso(entry?.at),
          actor: normalizeText(entry?.actor, 120) || 'mark',
          details: normalizeText(entry?.details, 1_000),
          sourceUrl: safeHttpUrl(entry?.sourceUrl || ''),
        }));
        normalized.updatedAt = normalizedIso(item?.updatedAt);
        return normalized;
      } catch { return null; }
    }).filter(Boolean);
  return {
    version: STORE_VERSION,
    businessKey: key,
    revision: safeInteger(raw.revision, 1, 1),
    updatedAt: normalizedIso(raw.updatedAt),
    members,
    observations,
    threads,
    communities,
    researchRuns,
    annotations,
    notifications,
  };
}

function noteFilename(member) {
  const slug = member.displayName.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 70) || 'member';
  return `community-${slug}-${member.id.slice(-6).toLowerCase()}.md`;
}

function applyObservation(document, observation) {
  if (document.observations.some((item) => item.id === observation.id)) return { created: false, observation };
  let existingIndex = document.members.findIndex((item) => item.id === observation.memberId);
  if (existingIndex < 0) {
    existingIndex = document.members.findIndex((item) => item.platform === observation.platform
      && item.community.toLowerCase() === observation.community.toLowerCase()
      && ((observation.member.platformUserId && item.platformUserId === observation.member.platformUserId)
        || (observation.member.profileUrl && item.profileUrl === observation.member.profileUrl)));
  }
  if (existingIndex < 0) {
    existingIndex = document.members.findIndex((item) => item.platform === observation.platform
      && item.community.toLowerCase() === observation.community.toLowerCase()
      && item.displayName.toLowerCase() === observation.member.displayName.toLowerCase()
      && (!(item.platformUserId || item.profileUrl)
        || !(observation.member.platformUserId || observation.member.profileUrl)));
  }
  if (existingIndex >= 0) {
    observation.memberId = document.members[existingIndex].id;
    observation.member.id = document.members[existingIndex].id;
  }
  document.observations.push(observation);
  const member = existingIndex >= 0 ? document.members[existingIndex] : normalizeMember({
    ...observation.member,
    id: observation.memberId,
    platform: observation.platform,
    community: observation.community,
    firstSeenAt: observation.observedAt,
    lastSeenAt: observation.observedAt,
  });
  member.displayName = observation.member.displayName || member.displayName;
  member.platformUserId ||= observation.member.platformUserId;
  member.profileUrl ||= observation.member.profileUrl;
  member.firstSeenAt = member.firstSeenAt < observation.observedAt ? member.firstSeenAt : observation.observedAt;
  member.lastSeenAt = member.lastSeenAt > observation.observedAt ? member.lastSeenAt : observation.observedAt;
  member.observationCount += 1;
  member.activityCounts[observation.kind] = (member.activityCounts[observation.kind] || 0) + 1;
  const observationTopics = observation.topics.length ? observation.topics : extractTopicTerms(`${observation.sourceTitle} ${observation.contentSummary}`).slice(0, 8);
  observation.topics = observationTopics;
  for (const topic of observationTopics) {
    const current = member.topics.find((item) => item.name.toLowerCase() === topic.toLowerCase());
    if (current) current.count += 1;
    else member.topics.push({ name: topic, count: 1 });
  }
  member.topics.sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  for (const inference of observation.inferences) {
    const current = member.inferences.find((item) => item.id === inference.id);
    if (!current) member.inferences.push(inference);
  }
  const signals = explicitSignals(observation.contentSummary);
  for (const [kind, values] of [['struggle', signals.struggles], ['goal', signals.goals], ['open_loop', signals.questions]]) {
    for (const summary of values) {
      const signal = normalizeKnowledgeSignal({
        kind, summary, scope: 'member', memberId: member.id, platform: member.platform,
        community: member.community, sourceObservationIds: [observation.id], confidence: 1,
        firstObservedAt: observation.observedAt, lastObservedAt: observation.observedAt,
      });
      if (signal && !member.signals.some((item) => item.id === signal.id)) member.signals.push(signal);
      if (kind === 'open_loop' && !member.openLoops.includes(summary)) member.openLoops.push(summary);
    }
  }
  member.signals = member.signals.slice(-200);
  member.openLoops = member.openLoops.slice(-30);
  if (!member.verifiedFacts.some((item) => item.key === 'community_membership' && item.value === observation.community)) {
    member.verifiedFacts.push({
      key: 'community_membership', value: observation.community,
      sourceObservationId: observation.id, observedAt: observation.observedAt,
    });
  }
  member.noteFilename = noteFilename(member);
  if (existingIndex >= 0) document.members[existingIndex] = member;
  else document.members.push(member);
  return { created: true, observation, member: structuredClone(member) };
}

function phraseCandidates(value) {
  const words = normalizeText(value, 3_000).toLowerCase().match(/[a-z][a-z0-9'-]{2,}/g) || [];
  const output = [];
  for (let size = 3; size <= 5; size += 1) {
    for (let index = 0; index <= words.length - size; index += 1) {
      const phraseWords = words.slice(index, index + size);
      if (phraseWords.filter((word) => !TOPIC_STOP_WORDS.has(word)).length < 2) continue;
      output.push(phraseWords.join(' '));
    }
  }
  return [...new Set(output)].slice(0, 300);
}

function rebuildCommunityModel(document, platform, community, sourceUrl = '') {
  const relevant = document.observations.filter((item) => item.platform === platform
    && item.community.toLowerCase() === community.toLowerCase());
  const members = new Set(relevant.map((item) => item.memberId).filter(Boolean));
  const threads = document.threads.filter((item) => item.platform === platform
    && item.community.toLowerCase() === community.toLowerCase());
  const topicMap = new Map();
  const phraseMap = new Map();
  const struggles = [];
  const questions = [];
  for (const observation of relevant) {
    for (const topic of observation.topics) {
      const key = topic.toLowerCase();
      const current = topicMap.get(key) || { name: topic, count: 0, members: new Set(), sourceObservationIds: [] };
      current.count += 1;
      current.members.add(observation.memberId);
      if (current.sourceObservationIds.length < 20) current.sourceObservationIds.push(observation.id);
      topicMap.set(key, current);
    }
    const signals = explicitSignals(observation.contentSummary);
    for (const summary of signals.struggles) struggles.push({ summary, memberId: observation.memberId, sourceObservationId: observation.id, observedAt: observation.observedAt });
    for (const summary of signals.questions) questions.push({ summary, memberId: observation.memberId, sourceObservationId: observation.id, observedAt: observation.observedAt });
    for (const phrase of phraseCandidates(observation.contentSummary)) {
      const current = phraseMap.get(phrase) || { phrase, observations: new Set(), members: new Set(), sourceObservationIds: [] };
      current.observations.add(observation.id);
      current.members.add(observation.memberId);
      if (current.sourceObservationIds.length < 20) current.sourceObservationIds.push(observation.id);
      phraseMap.set(phrase, current);
    }
  }
  const existingIndex = document.communities.findIndex((item) => item.platform === platform
    && item.name.toLowerCase() === community.toLowerCase());
  const existing = existingIndex >= 0 ? document.communities[existingIndex] : normalizeCommunityModel({ platform, name: community, sourceUrl });
  const model = normalizeCommunityModel({
    ...existing,
    sourceUrl: sourceUrl || existing.sourceUrl,
    firstObservedAt: relevant.reduce((value, item) => value < item.observedAt ? value : item.observedAt, existing.firstObservedAt),
    lastObservedAt: relevant.reduce((value, item) => value > item.observedAt ? value : item.observedAt, existing.lastObservedAt),
    threadCount: threads.length,
    observationCount: relevant.length,
    memberCount: members.size,
    recurringTopics: [...topicMap.values()].map((item) => ({
      name: item.name, count: item.count, memberCount: item.members.size, sourceObservationIds: item.sourceObservationIds,
    })).filter((item) => item.count >= 2).sort((a, b) => b.memberCount - a.memberCount || b.count - a.count).slice(0, 80),
    commonStruggles: struggles.slice(-100),
    openQuestions: questions.slice(-100),
    recurringLanguage: [...phraseMap.values()].map((item) => ({
      phrase: item.phrase, observationCount: item.observations.size, memberCount: item.members.size,
      sourceObservationIds: item.sourceObservationIds,
    })).filter((item) => item.observationCount >= 3 && item.memberCount >= 2)
      .sort((a, b) => b.memberCount - a.memberCount || b.observationCount - a.observationCount).slice(0, 60),
  });
  if (existingIndex >= 0) document.communities[existingIndex] = model;
  else document.communities.push(model);
  return model;
}

export function buildCommunityProfileMarkdown(memberInput, observationsInput = []) {
  const member = normalizeMember(memberInput);
  const observations = (Array.isArray(observationsInput) ? observationsInput : [])
    .filter((item) => item?.memberId === member.id)
    .sort((a, b) => b.observedAt.localeCompare(a.observedAt)).slice(0, 25);
  const countLines = Object.entries(member.activityCounts).filter(([, count]) => count > 0)
    .map(([kind, count]) => `- ${kind}: ${count}`);
  const lines = [
    `# ${member.displayName}`,
    '',
    'Status: active',
    'Tags: #person #community-profile',
    '',
    `Platform: ${member.platform}`,
    `Community: ${member.community}`,
    `First observed: ${member.firstSeenAt}`,
    `Last observed: ${member.lastSeenAt}`,
    member.profileUrl ? `Profile: ${member.profileUrl}` : 'Profile: not captured',
    '',
    '## Verified Facts',
    '',
    `- Display name: ${member.displayName}`,
    `- Observed participating in ${member.community} on ${member.platform}.`,
    '',
    '## Engagement',
    '',
    ...(countLines.length ? countLines : ['- No categorized activity yet']),
    `- Total source observations: ${member.observationCount}`,
    '',
    '## Recurring Topics',
    '',
    ...(member.topics.length ? member.topics.slice(0, 15).map((item) => `- ${item.name} (${item.count} observations)`) : ['- No source-grounded topics identified yet']),
    '',
    '## Evidence-Based Inferences',
    '',
    ...(member.inferences.length ? member.inferences.slice(-15).map((item) => `- ${item.label}: ${item.summary} (confidence ${Math.round(item.confidence * 100)}%)`) : ['- None recorded']),
    '',
    '## Open Loops',
    '',
    ...(member.openLoops.length ? member.openLoops.map((item) => `- ${item}`) : ['- None recorded']),
    '',
    '## Durable Social Context',
    '',
    ...(member.signals.length ? member.signals.slice(-30).map((item) => `- ${item.kind}: ${item.summary} (sources: ${item.sourceObservationIds.join(', ') || 'none'})`) : ['- No source-backed social signals recorded yet']),
    '',
    '## Conversation Network',
    '',
    ...(member.relationships.length ? member.relationships.slice(0, 25).map((item) => `- ${item.displayName || item.memberId}: ${item.sharedThreadCount} shared threads`) : ['- No shared-thread relationships observed yet']),
    '',
    '## Recent Activity',
    '',
    ...(observations.length ? observations.map((item) => `- ${item.observedAt.slice(0, 10)} | ${item.kind} | ${item.contentSummary || item.sourceTitle}${item.sourceUrl ? ` | [source](${item.sourceUrl})` : ''}`) : ['- No retained activity summaries']),
    '',
    '## Privacy And Confidence',
    '',
    '- Facts above come from visible community activity and are source-linked where available.',
    '- Inferences are kept separate from facts. Sensitive-trait inference is not stored.',
    '- This note stores bounded summaries, not full post or comment transcripts.',
    '',
    '## Links',
    '',
    '- [[people-index]]',
    '- [[conversation-index]]',
    '',
  ];
  return `${lines.join('\n')}\n`;
}

export function buildCommunityBriefMarkdown(communityInput, threadsInput = [], membersInput = []) {
  const community = normalizeCommunityModel(communityInput);
  const threads = (Array.isArray(threadsInput) ? threadsInput : []).map(normalizeThread)
    .filter((item) => item.platform === community.platform && item.community.toLowerCase() === community.name.toLowerCase())
    .sort((a, b) => b.lastObservedAt.localeCompare(a.lastObservedAt)).slice(0, 40);
  const members = (Array.isArray(membersInput) ? membersInput : []).map(normalizeMember)
    .filter((item) => item.platform === community.platform && item.community.toLowerCase() === community.name.toLowerCase())
    .sort((a, b) => b.observationCount - a.observationCount).slice(0, 40);
  const lines = [
    `# ${community.name} Community Intelligence`,
    '',
    'Status: active',
    'Tags: #community #social-intelligence #marcus-memory',
    '',
    `Platform: ${community.platform}`,
    community.sourceUrl ? `Community: ${community.sourceUrl}` : 'Community: source URL not captured',
    `Last learned: ${community.coverage.lastRunAt}`,
    '',
    '## Coverage',
    '',
    `- ${community.threadCount} conversation threads retained`,
    `- ${community.observationCount} source observations retained`,
    `- ${community.memberCount} members observed`,
    `- Feed end reached in latest pass: ${community.coverage.feedEndReached ? 'yes' : 'no'}`,
    `- All rendered comment ends reached in latest pass: ${community.coverage.allVisibleCommentEndsReached ? 'yes' : 'no'}`,
    `- Limitation: ${community.coverage.limitation || 'Only content rendered to the signed-in MARCUS browser can be claimed.'}`,
    '',
    '## Recurring Topics',
    '',
    ...(community.recurringTopics.length ? community.recurringTopics.slice(0, 30).map((item) => `- ${item.name}: ${item.count} observations across ${item.memberCount} members`) : ['- Not established yet']),
    '',
    '## Explicit Struggles',
    '',
    ...(community.commonStruggles.length ? community.commonStruggles.slice(-30).map((item) => `- ${item.summary} (source: ${item.sourceObservationId})`) : ['- None captured yet']),
    '',
    '## Open Questions And Threads',
    '',
    ...(community.openQuestions.length ? community.openQuestions.slice(-30).map((item) => `- ${item.summary} (source: ${item.sourceObservationId})`) : ['- None captured yet']),
    '',
    '## Shared Language And Culture',
    '',
    ...(community.recurringLanguage.length ? community.recurringLanguage.slice(0, 25).map((item) => `- "${item.phrase}" (${item.memberCount} members)`) : ['- No recurring phrase has enough cross-member evidence yet']),
    ...(community.annotations.filter((item) => ['cultural_reference', 'language'].includes(item.kind)).length
      ? community.annotations.filter((item) => ['cultural_reference', 'language'].includes(item.kind)).slice(-25).map((item) => `- ${item.kind}: ${item.summary}`) : []),
    '',
    '## People In The Conversation',
    '',
    ...(members.length ? members.map((member) => `- [[${(member.noteFilename || noteFilename(member)).replace(/\.md$/i, '')}|${member.displayName}]]: ${member.observationCount} observations; ${member.topics.slice(0, 5).map((item) => item.name).join(', ') || 'topics not established'}`) : ['- No member profiles yet']),
    '',
    '## Recent Conversations',
    '',
    ...(threads.length ? threads.map((thread) => `- ${thread.lastObservedAt.slice(0, 10)} | ${thread.title || 'Untitled thread'} | ${thread.participantMemberIds.length} participants | ${thread.commentCountObserved} comments observed${thread.sourceUrl ? ` | [source](${thread.sourceUrl})` : ''}`) : ['- No conversation threads retained yet']),
    '',
    '## MARCUS Rule',
    '',
    '- Use this history to recognize people and continue conversations. Do not turn it into imitation content.',
    '- Recheck source evidence before asserting that an old fact is still current.',
    '- Never infer sensitive personal traits.',
    '',
    '## Links',
    '',
    '- [[people-index]]',
    '- [[conversation-index]]',
    '',
  ];
  return `${lines.join('\n')}\n`;
}

export class CommunityIntelligenceStore {
  constructor({ dataDir } = {}) {
    if (!dataDir) throw new Error('CommunityIntelligenceStore requires dataDir.');
    this.dataDir = path.resolve(String(dataDir));
    this.writeQueues = new Map();
  }

  fileForBusiness(businessKey) {
    return path.join(this.dataDir, 'businesses', safeBusinessKey(businessKey), 'marcus-community-intelligence.json');
  }

  async readDocument(businessKey) {
    const key = safeBusinessKey(businessKey);
    const file = this.fileForBusiness(key);
    try {
      return normalizeDocument(JSON.parse(await fs.readFile(file, 'utf8')), key);
    } catch (primaryError) {
      if (primaryError?.code === 'ENOENT') return emptyDocument(key);
      try {
        const recovered = normalizeDocument(JSON.parse(await fs.readFile(`${file}.bak`, 'utf8')), key);
        await fs.rename(file, `${file}.corrupt-${Date.now()}`).catch(() => {});
        await this.atomicWrite(file, recovered, { createBackup: false });
        return recovered;
      } catch {
        throw Object.assign(new Error(`Community intelligence store is corrupt for business ${key}; the original file was preserved.`), {
          code: 'CORRUPT_COMMUNITY_INTELLIGENCE_STORE', cause: primaryError,
        });
      }
    }
  }

  async ingestObservations(businessKey, values) {
    const observations = (Array.isArray(values) ? values : [values]).slice(0, 200).map(normalizeObservation);
    return this.mutate(businessKey, (document) => {
      const results = observations.map((item) => applyObservation(document, item));
      document.observations = document.observations.slice(-MAX_OBSERVATIONS);
      document.members = document.members.slice(-MAX_MEMBERS);
      return {
        created: results.filter((item) => item.created).length,
        duplicate: results.filter((item) => !item.created).length,
        observations: results.map((item) => structuredClone(item.observation)),
        members: [...new Map(results.filter((item) => item.member).map((item) => [item.member.id, item.member])).values()],
      };
    });
  }

  async ingestResearch(businessKey, input = {}) {
    const raw = safeObject(input);
    const platform = normalizePlatform(raw.platform || raw.contextKind);
    const community = normalizeCommunity(raw.community);
    const observedAt = nowIso();
    const sourceUrl = safeHttpUrl(raw.startingUrl || raw.sourceUrl || '');
    const sourceValues = (Array.isArray(raw.sources) ? raw.sources : []).slice(0, 100);
    return this.mutate(businessKey, (document) => {
      let created = 0;
      let duplicate = 0;
      const affectedMembers = new Map();
      const affectedThreads = [];
      for (const source of sourceValues) {
        const threadUrl = safeHttpUrl(source?.sourceUrl || '');
        if (!threadUrl) continue;
        const threadId = stableId('thread', threadUrl);
        const postAuthor = safeObject(source?.author || source?.member);
        const postObservation = normalizeObservation({
          platform, community, member: {
            displayName: postAuthor.displayName || postAuthor.name || source?.authorName || 'Unknown author',
            profileUrl: postAuthor.profileUrl || source?.authorUrl || '',
          },
          kind: 'post', sourceUrl: threadUrl, sourceTitle: source?.title,
          contentSummary: source?.postText || source?.title, threadId, position: 0,
          sourceKey: `${threadUrl}\npost\n${sourceFingerprint(source?.postText || source?.title)}`,
          observedAt,
        });
        const postApplied = applyObservation(document, postObservation);
        if (postApplied.created) created += 1; else duplicate += 1;
        if (postApplied.member) affectedMembers.set(postApplied.member.id, postApplied.member);
        const observationIds = [postObservation.id];
        const participantIds = new Set([postObservation.memberId]);
        let commentPosition = 0;
        for (const comment of (Array.isArray(source?.comments) ? source.comments : []).slice(0, 500)) {
          commentPosition += 1;
          const commentObservation = normalizeObservation({
            platform, community, member: {
              displayName: comment?.author || comment?.displayName || 'Unknown commenter',
              profileUrl: comment?.authorUrl || comment?.profileUrl || '',
            },
            kind: 'comment', sourceUrl: threadUrl, sourceTitle: source?.title,
            contentSummary: comment?.text, threadId, parentObservationId: postObservation.id,
            position: commentPosition,
            sourceKey: `${threadUrl}\ncomment\n${comment?.author || ''}\n${sourceFingerprint(comment?.text)}`,
            observedAt,
          });
          const commentApplied = applyObservation(document, commentObservation);
          if (commentApplied.created) created += 1; else duplicate += 1;
          if (commentApplied.member) affectedMembers.set(commentApplied.member.id, commentApplied.member);
          observationIds.push(commentObservation.id);
          participantIds.add(commentObservation.memberId);
        }
        const signals = explicitSignals(`${source?.postText || ''} ${(source?.comments || []).map((item) => item?.text || '').join(' ')}`);
        const fingerprint = sourceFingerprint(`${source?.postText || ''}\n${(source?.comments || []).map((item) => `${item?.author || ''}:${item?.text || ''}`).join('\n')}`);
        const existingIndex = document.threads.findIndex((item) => item.id === threadId);
        const existing = existingIndex >= 0 ? document.threads[existingIndex] : null;
        const thread = normalizeThread({
          ...existing,
          id: threadId, platform, community, sourceUrl: threadUrl, title: source?.title,
          postSummary: source?.postText, authorMemberId: postObservation.memberId,
          participantMemberIds: [...participantIds], observationIds,
          commentCountObserved: Math.max(0, Number(source?.commentsRead) || commentPosition),
          reachedVisibleEnd: source?.reachedVisibleEnd === true,
          firstObservedAt: existing?.firstObservedAt || observedAt, lastObservedAt: observedAt,
          lastFingerprint: fingerprint,
          revisionCount: existing ? existing.revisionCount + (existing.lastFingerprint === fingerprint ? 0 : 1) : 1,
          openQuestions: signals.questions,
          explicitStruggles: signals.struggles,
          explicitGoals: signals.goals,
          topicTerms: extractTopicTerms(`${source?.title || ''} ${source?.postText || ''}`),
        });
        if (existingIndex >= 0) document.threads[existingIndex] = thread;
        else document.threads.push(thread);
        affectedThreads.push(thread);

        const participantList = [...participantIds].filter(Boolean);
        for (const memberId of participantList) {
          const member = document.members.find((item) => item.id === memberId);
          if (!member) continue;
          for (const relatedId of participantList.filter((id) => id !== memberId)) {
            const related = document.members.find((item) => item.id === relatedId);
            const relationship = member.relationships.find((item) => item.memberId === relatedId);
            if (relationship) {
              if (!relationship.sourceObservationIds.includes(postObservation.id)) relationship.sharedThreadCount += 1;
              relationship.lastSeenAt = observedAt;
              relationship.sourceObservationIds = [...new Set([...relationship.sourceObservationIds, postObservation.id])].slice(-30);
            } else {
              member.relationships.push({
                memberId: relatedId, displayName: related?.displayName || '', sharedThreadCount: 1,
                lastSeenAt: observedAt, sourceObservationIds: [postObservation.id],
              });
            }
          }
          member.relationships.sort((a, b) => b.sharedThreadCount - a.sharedThreadCount || b.lastSeenAt.localeCompare(a.lastSeenAt));
          member.relationships = member.relationships.slice(0, 100);
          affectedMembers.set(member.id, structuredClone(member));
        }
      }
      document.observations = document.observations.slice(-MAX_OBSERVATIONS);
      document.members = document.members.slice(-MAX_MEMBERS);
      document.threads = document.threads.slice(-MAX_THREADS);
      const communityModel = rebuildCommunityModel(document, platform, community, sourceUrl);
      const run = normalizeResearchRun({
        platform, community, sourceUrl, query: raw.query, startedAt: raw.startedAt || observedAt,
        completedAt: observedAt, sourceUrls: affectedThreads.map((item) => item.sourceUrl),
        postsDiscovered: raw.postsDiscovered, postsRead: raw.postsRead, commentsRead: raw.commentsRead,
        coverage: raw.coverage,
      });
      document.researchRuns.push(run);
      document.researchRuns = document.researchRuns.slice(-MAX_RESEARCH_RUNS);
      communityModel.coverage = normalizeCommunityModel({
        ...communityModel,
        coverage: {
          ...communityModel.coverage,
          ...(safeObject(raw.coverage)),
          postsDiscovered: raw.postsDiscovered,
          postsRead: raw.postsRead,
          commentsRead: raw.commentsRead,
          lastRunAt: observedAt,
        },
      }).coverage;
      const affectedObservationIds = new Set(affectedThreads.flatMap((thread) => thread.observationIds));
      return {
        created, duplicate, researchRun: structuredClone(run),
        members: [...affectedMembers.values()], threads: structuredClone(affectedThreads),
        observations: structuredClone(document.observations.filter((item) => affectedObservationIds.has(item.id))),
        community: structuredClone(communityModel),
      };
    });
  }

  async rememberKnowledge(businessKey, input = {}) {
    const signal = normalizeKnowledgeSignal(input);
    if (!signal || !signal.sourceObservationIds.length) {
      throw Object.assign(new Error('Durable community knowledge requires a supported kind, summary, and source observation IDs.'), { code: 'COMMUNITY_KNOWLEDGE_EVIDENCE_REQUIRED' });
    }
    return this.mutate(businessKey, (document) => {
      const validIds = new Set(document.observations.map((item) => item.id));
      if (signal.sourceObservationIds.some((id) => !validIds.has(id))) {
        throw Object.assign(new Error('Community knowledge cited an observation that is not in the evidence ledger.'), { code: 'COMMUNITY_KNOWLEDGE_SOURCE_INVALID' });
      }
      const sourceObservations = document.observations.filter((item) => signal.sourceObservationIds.includes(item.id));
      if (signal.platform === 'unknown') signal.platform = sourceObservations[0]?.platform || signal.platform;
      if (signal.community === 'Unknown community') signal.community = sourceObservations[0]?.community || signal.community;
      if (signal.scope === 'member' && !document.members.some((item) => item.id === signal.memberId)) {
        throw Object.assign(new Error('Community member for this knowledge note was not found.'), { code: 'COMMUNITY_MEMBER_NOT_FOUND' });
      }
      if (signal.scope === 'thread' && !document.threads.some((item) => item.id === signal.threadId)) {
        throw Object.assign(new Error('Community thread for this knowledge note was not found.'), { code: 'COMMUNITY_THREAD_NOT_FOUND' });
      }
      const existing = document.annotations.find((item) => item.id === signal.id);
      if (!existing) document.annotations.push(signal);
      if (signal.scope === 'member') {
        const member = document.members.find((item) => item.id === signal.memberId);
        if (!member.signals.some((item) => item.id === signal.id)) member.signals.push(signal);
        if (signal.kind === 'open_loop' && !member.openLoops.includes(signal.summary)) member.openLoops.push(signal.summary);
      }
      if (signal.scope === 'community') {
        const community = document.communities.find((item) => item.platform === signal.platform
          && item.name.toLowerCase() === signal.community.toLowerCase());
        if (community && !community.annotations.some((item) => item.id === signal.id)) community.annotations.push(signal);
      }
      document.annotations = document.annotations.slice(-MAX_ANNOTATIONS);
      return { created: !existing, knowledge: structuredClone(existing || signal) };
    });
  }

  async getCommunityContext(businessKey, filters = {}) {
    const document = await this.readDocument(businessKey);
    const platform = normalizeText(filters.platform, 80).toLowerCase();
    const communityQuery = normalizeText(filters.community, 160).toLowerCase();
    const community = document.communities.find((item) => (!platform || item.platform === platform)
      && (!communityQuery || item.name.toLowerCase().includes(communityQuery)));
    const relevantThreads = document.threads.filter((item) => (!community || (item.platform === community.platform
      && item.community.toLowerCase() === community.name.toLowerCase())))
      .sort((a, b) => b.lastObservedAt.localeCompare(a.lastObservedAt));
    const relevantMembers = document.members.filter((item) => (!community || (item.platform === community.platform
      && item.community.toLowerCase() === community.name.toLowerCase())))
      .sort((a, b) => b.lastSeenAt.localeCompare(a.lastSeenAt));
    const relevantRuns = document.researchRuns.filter((item) => !community || (item.platform === community.platform
      && item.community.toLowerCase() === community.name.toLowerCase()))
      .sort((a, b) => b.completedAt.localeCompare(a.completedAt));
    return {
      version: document.version, businessKey: document.businessKey, revision: document.revision,
      updatedAt: document.updatedAt, community: community ? structuredClone(community) : null,
      members: structuredClone(relevantMembers.slice(0, safeInteger(filters.memberLimit, 30, 1, 200))),
      threads: structuredClone(relevantThreads.slice(0, safeInteger(filters.threadLimit, 30, 1, 200))),
      recentResearchRuns: structuredClone(relevantRuns.slice(0, 10)),
      knownSourceUrls: [...new Set(relevantThreads.map((item) => item.sourceUrl).filter(Boolean))].slice(0, 500),
    };
  }

  async listMembers(businessKey, filters = {}) {
    const document = await this.readDocument(businessKey);
    const query = normalizeText(filters.query, 200).toLowerCase();
    const platform = normalizeText(filters.platform, 80).toLowerCase();
    const community = normalizeText(filters.community, 160).toLowerCase();
    let members = document.members.filter((item) => (!query || `${item.displayName} ${item.topics.map((topic) => topic.name).join(' ')}`.toLowerCase().includes(query))
      && (!platform || item.platform === platform)
      && (!community || item.community.toLowerCase().includes(community)));
    members.sort((a, b) => b.lastSeenAt.localeCompare(a.lastSeenAt) || a.displayName.localeCompare(b.displayName));
    return { ...document, observations: undefined, notifications: undefined, members: members.slice(0, safeInteger(filters.limit, 100, 1, 500)) };
  }

  async getMember(businessKey, memberId) {
    const document = await this.readDocument(businessKey);
    const member = document.members.find((item) => item.id === safeString(memberId, 120));
    if (!member) throw Object.assign(new Error('Community member not found.'), { code: 'COMMUNITY_MEMBER_NOT_FOUND' });
    const observations = document.observations.filter((item) => item.memberId === member.id)
      .sort((a, b) => b.observedAt.localeCompare(a.observedAt));
    return { member, observations };
  }

  async ingestNotifications(businessKey, values) {
    const notifications = (Array.isArray(values) ? values : [values]).slice(0, 200).map(normalizeNotification);
    return this.mutate(businessKey, (document) => {
      let created = 0;
      for (const notification of notifications) {
        if (document.notifications.some((item) => item.id === notification.id)) continue;
        document.notifications.push(notification);
        created += 1;
      }
      document.notifications = document.notifications.slice(-MAX_NOTIFICATIONS);
      return { created, duplicate: notifications.length - created, notifications: structuredClone(notifications) };
    });
  }

  async listNotifications(businessKey, filters = {}) {
    const document = await this.readDocument(businessKey);
    const state = normalizeText(filters.state, 80).toLowerCase();
    let notifications = document.notifications.filter((item) => !state || item.state === state);
    notifications.sort((a, b) => b.observedAt.localeCompare(a.observedAt));
    return {
      version: document.version, businessKey: document.businessKey, revision: document.revision,
      updatedAt: document.updatedAt,
      notifications: notifications.slice(0, safeInteger(filters.limit, 100, 1, 500)),
    };
  }

  async transitionNotification(businessKey, notificationId, input = {}) {
    const requestedState = normalizeText(input.state, 80).toLowerCase();
    if (!NOTIFICATION_STATES.has(requestedState)) {
      throw Object.assign(new Error(`Unsupported notification state: ${requestedState}.`), { code: 'COMMUNITY_NOTIFICATION_STATE_INVALID' });
    }
    return this.mutate(businessKey, (document) => {
      const notification = document.notifications.find((item) => item.id === safeString(notificationId, 120));
      if (!notification) throw Object.assign(new Error('Community notification not found.'), { code: 'COMMUNITY_NOTIFICATION_NOT_FOUND' });
      const evidence = {
        state: requestedState,
        at: nowIso(),
        actor: normalizeText(input.actor, 120) || 'mark',
        details: normalizeText(input.evidence || input.details, 1_000),
        sourceUrl: safeHttpUrl(input.sourceUrl || ''),
      };
      if (['cleared', 'responded'].includes(requestedState) && !evidence.details && !evidence.sourceUrl) {
        throw Object.assign(new Error(`Notification state ${requestedState} requires browser or publication evidence.`), { code: 'COMMUNITY_NOTIFICATION_EVIDENCE_REQUIRED' });
      }
      notification.state = requestedState;
      notification.draftId = safeString(input.draftId, 120) || notification.draftId;
      notification.transitionEvidence.push(evidence);
      notification.transitionEvidence = notification.transitionEvidence.slice(-20);
      notification.updatedAt = evidence.at;
      return structuredClone(notification);
    });
  }

  async profileProjection(businessKey, memberId) {
    const { member, observations } = await this.getMember(businessKey, memberId);
    return { filename: member.noteFilename || noteFilename(member), content: buildCommunityProfileMarkdown(member, observations), member };
  }

  async communityProjection(businessKey, { platform = 'skool', community = 'localgiants' } = {}) {
    const context = await this.getCommunityContext(businessKey, { platform, community, memberLimit: 200, threadLimit: 200 });
    if (!context.community) throw Object.assign(new Error('Community model not found.'), { code: 'COMMUNITY_NOT_FOUND' });
    const slug = `${context.community.platform}-${context.community.name}`.toLowerCase()
      .replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 100);
    return {
      filename: `community-${slug}.md`,
      content: buildCommunityBriefMarkdown(context.community, context.threads, context.members),
      community: context.community,
    };
  }

  async mutate(businessKey, mutator) {
    const key = safeBusinessKey(businessKey);
    const previous = this.writeQueues.get(key) || Promise.resolve();
    const run = previous.catch(() => {}).then(async () => {
      const document = await this.readDocument(key);
      const result = await mutator(document);
      document.revision += 1;
      document.updatedAt = nowIso();
      await this.atomicWrite(this.fileForBusiness(key), normalizeDocument(document, key));
      return result;
    });
    this.writeQueues.set(key, run);
    try { return await run; } finally {
      if (this.writeQueues.get(key) === run) this.writeQueues.delete(key);
    }
  }

  async atomicWrite(file, value, { createBackup = true } = {}) {
    await fs.mkdir(path.dirname(file), { recursive: true });
    const temporary = `${file}.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    if (createBackup) await fs.copyFile(file, `${file}.bak`).catch(() => {});
    await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    try { await fs.rename(temporary, file); } catch (error) {
      await fs.unlink(temporary).catch(() => {});
      throw error;
    }
  }
}

export {
  emptyDocument as createEmptyCommunityIntelligenceDocument,
  normalizeDocument as normalizeCommunityIntelligenceDocument,
  notificationRecommendation as recommendCommunityNotificationAction,
};
