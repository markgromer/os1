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

const STORE_VERSION = 1;
const MAX_MEMBERS = 2_000;
const MAX_OBSERVATIONS = 5_000;
const MAX_NOTIFICATIONS = 2_000;
const OBSERVATION_KINDS = new Set(['post', 'comment', 'reply', 'reaction', 'mention', 'profile', 'other']);
const NOTIFICATION_STATES = new Set(['unread', 'triaged', 'drafted', 'cleared', 'responded', 'ignored']);
const SENSITIVE_INFERENCE_PATTERN = /\b(?:age(?:d)?|racial|race|ethnic|religio|politic|sexual|gender|disab|medical|health|income|financ|citizen|nationalit)/i;

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
    notifications: [],
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
    noteFilename: normalizeText(raw.noteFilename, 160),
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
  for (const topic of observation.topics) {
    const current = member.topics.find((item) => item.name.toLowerCase() === topic.toLowerCase());
    if (current) current.count += 1;
    else member.topics.push({ name: topic, count: 1 });
  }
  member.topics.sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  for (const inference of observation.inferences) {
    const current = member.inferences.find((item) => item.id === inference.id);
    if (!current) member.inferences.push(inference);
  }
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
