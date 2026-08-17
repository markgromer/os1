export const LIVE_PRESENCE_MODES = Object.freeze([
  'silent_shadow',
  'private_copilot',
  'public_push_to_talk',
  'public_auto_reply',
  'show_mode',
]);

export const LIVE_PRESENCE_SETUP_ITEMS = Object.freeze([
  {
    id: 'browser_profile',
    label: 'Dedicated Marcus browser profile',
    owner: 'mark',
    required: true,
    description: 'Use scripts/launch-marcus-browser.ps1 to open the isolated Marcus browser profile and keep all Marcus sessions separate from Mark personal browsing.',
  },
  {
    id: 'google_account',
    label: 'Marcus Google account',
    owner: 'mark',
    required: true,
    description: 'Sign into markgromermarcus@gmail.com in the isolated Marcus Chrome window and complete recovery, MFA, and trusted-device prompts manually.',
  },
  {
    id: 'gmail_access',
    label: 'Gmail access verified',
    owner: 'mark',
    required: true,
    description: 'Open Gmail in the Marcus browser profile and confirm Marcus can read, compose, and send test email from the signed-in account.',
  },
  {
    id: 'assistant_identity',
    label: 'Visible assistant identity',
    owner: 'mark',
    required: true,
    description: 'Use display name Marcus - Mark\'s AI Assistant wherever the platform allows it.',
  },
  {
    id: 'platform_login',
    label: 'Platform logins complete',
    owner: 'mark',
    required: true,
    description: 'Log into Skool, Zoom, YouTube, TikTok, and other target platforms in the Marcus browser profile. Mark handles MFA, captcha, recovery, and identity-sensitive prompts directly.',
  },
  {
    id: 'audio_router',
    label: 'Virtual audio routing installed',
    owner: 'mark',
    required: true,
    description: 'Install and configure a virtual audio route such as VB-CABLE or VoiceMeeter so Marcus can hear meeting audio without feedback.',
  },
  {
    id: 'marcus_mic',
    label: 'Marcus microphone route selected',
    owner: 'mark',
    required: true,
    description: 'Zoom or the live platform can receive Marcus voice output on a separate input device from Mark\'s microphone.',
  },
  {
    id: 'mark_mic',
    label: 'Mark microphone isolated',
    owner: 'mark',
    required: true,
    description: 'Mark\'s microphone remains separate from Marcus output to prevent echo and doubled speech.',
  },
  {
    id: 'obs_scene',
    label: 'OBS Marcus scene ready',
    owner: 'mark',
    required: false,
    description: 'OBS has a Marcus scene or browser source for visual presence, demos, and live-stream staging.',
  },
  {
    id: 'realtime_voice',
    label: 'OpenAI Realtime voice configured',
    owner: 'marcus',
    required: true,
    description: 'The app can mint a Realtime client secret and stream live speech in the Marcus voice layer.',
  },
  {
    id: 'emergency_controls',
    label: 'Emergency controls rehearsed',
    owner: 'mark',
    required: true,
    description: 'Mark can immediately mute Marcus, stop speaking, switch private-only, or leave the meeting.',
  },
  {
    id: 'notes_memory',
    label: 'Meeting memory capture ready',
    owner: 'marcus',
    required: true,
    description: 'Live sessions write durable notes, decisions, action items, and follow-up drafts.',
  },
]);

const DEFAULT_PLATFORM_TARGETS = Object.freeze([
  {
    id: 'zoom',
    label: 'Zoom',
    joinModel: 'local_browser',
    requiredCapabilities: ['hear_audio', 'speak_audio', 'read_chat', 'write_notes', 'visible_identity'],
  },
  {
    id: 'skool',
    label: 'Skool',
    joinModel: 'local_browser',
    requiredCapabilities: ['read_page', 'read_chat', 'draft_replies', 'visible_identity'],
  },
  {
    id: 'youtube_live',
    label: 'YouTube Live',
    joinModel: 'browser_plus_official_api_when_available',
    requiredCapabilities: ['hear_audio', 'read_chat', 'speak_audio', 'obs_presence'],
  },
  {
    id: 'tiktok_live',
    label: 'TikTok Live',
    joinModel: 'local_browser_until_approved_api_exists',
    requiredCapabilities: ['hear_audio', 'read_visible_chat', 'speak_audio', 'obs_presence'],
  },
]);

function cleanId(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 80);
}

function cleanText(value, max = 500) {
  return String(value || '').trim().replace(/\s+/g, ' ').slice(0, max);
}

export function normalizeLivePresenceSettings(input = {}) {
  const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const setup = raw.livePresenceSetup && typeof raw.livePresenceSetup === 'object' && !Array.isArray(raw.livePresenceSetup)
    ? raw.livePresenceSetup
    : {};
  const completed = setup.completed && typeof setup.completed === 'object' && !Array.isArray(setup.completed)
    ? setup.completed
    : {};
  const notes = setup.notes && typeof setup.notes === 'object' && !Array.isArray(setup.notes)
    ? setup.notes
    : {};
  const cleanCompleted = {};
  const cleanNotes = {};
  for (const item of LIVE_PRESENCE_SETUP_ITEMS) {
    cleanCompleted[item.id] = completed[item.id] === true;
    cleanNotes[item.id] = cleanText(notes[item.id] || '', 400);
  }
  const mode = LIVE_PRESENCE_MODES.includes(String(setup.defaultMode || '').trim())
    ? String(setup.defaultMode).trim()
    : 'public_push_to_talk';
  return {
    ...raw,
    livePresenceSetup: {
      defaultMode: mode,
      completed: cleanCompleted,
      notes: cleanNotes,
      updatedAt: typeof setup.updatedAt === 'string' ? setup.updatedAt : '',
    },
  };
}

export function patchLivePresenceSetup(settings = {}, patch = {}, now = new Date().toISOString()) {
  const normalized = normalizeLivePresenceSettings(settings);
  const setup = normalized.livePresenceSetup;
  const raw = patch && typeof patch === 'object' && !Array.isArray(patch) ? patch : {};
  const nextCompleted = { ...setup.completed };
  const nextNotes = { ...setup.notes };

  if (LIVE_PRESENCE_MODES.includes(String(raw.defaultMode || '').trim())) {
    setup.defaultMode = String(raw.defaultMode).trim();
  }

  const completedPatch = raw.completed && typeof raw.completed === 'object' && !Array.isArray(raw.completed)
    ? raw.completed
    : {};
  for (const [key, value] of Object.entries(completedPatch)) {
    const id = cleanId(key);
    if (Object.prototype.hasOwnProperty.call(nextCompleted, id)) nextCompleted[id] = value === true;
  }

  const notesPatch = raw.notes && typeof raw.notes === 'object' && !Array.isArray(raw.notes)
    ? raw.notes
    : {};
  for (const [key, value] of Object.entries(notesPatch)) {
    const id = cleanId(key);
    if (Object.prototype.hasOwnProperty.call(nextNotes, id)) nextNotes[id] = cleanText(value, 400);
  }

  return {
    ...normalized,
    livePresenceSetup: {
      defaultMode: setup.defaultMode,
      completed: nextCompleted,
      notes: nextNotes,
      updatedAt: now,
    },
  };
}

export function buildLivePresenceStatus({ settings = {}, voice = {}, desktop = null } = {}) {
  const normalized = normalizeLivePresenceSettings(settings);
  const setup = normalized.livePresenceSetup;
  const items = LIVE_PRESENCE_SETUP_ITEMS.map((item) => {
    const done = setup.completed[item.id] === true
      || (item.id === 'realtime_voice' && voice?.configured === true)
      || (item.id === 'notes_memory' && voice?.telemetryReady !== false);
    return {
      ...item,
      done,
      note: setup.notes[item.id] || '',
      source: item.id === 'realtime_voice' && voice?.configured === true ? 'system' : 'manual',
    };
  });
  const required = items.filter((item) => item.required);
  const requiredDone = required.filter((item) => item.done);
  const optionalDone = items.filter((item) => !item.required && item.done);
  const desktopOnline = Boolean(desktop?.ok || desktop?.agentId || desktop?.windowTitle || desktop?.observedAt);
  const readyForPrivateCopilot = requiredDone.length >= Math.max(1, required.length - 2) && desktopOnline;
  const readyForPublicVoice = required.every((item) => item.done) && desktopOnline;

  return {
    ok: true,
    model: 'local_browser_presence',
    defaultMode: setup.defaultMode,
    modes: LIVE_PRESENCE_MODES,
    desktopOnline,
    readyForPrivateCopilot,
    readyForPublicVoice,
    requiredCompleted: requiredDone.length,
    requiredTotal: required.length,
    optionalCompleted: optionalDone.length,
    updatedAt: setup.updatedAt || '',
    items,
    platformTargets: DEFAULT_PLATFORM_TARGETS,
    nextHumanSteps: items
      .filter((item) => item.owner === 'mark' && item.required && !item.done)
      .map((item) => ({ id: item.id, label: item.label, description: item.description })),
    blockedReasons: [
      ...(!desktopOnline ? ['Desktop agent is not reporting a current local PC context.'] : []),
      ...required.filter((item) => !item.done).map((item) => `${item.label} is not complete.`),
    ],
  };
}
