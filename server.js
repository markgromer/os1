import crypto from 'node:crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import fs from 'node:fs/promises';
import fsSync from 'node:fs';
import { exec } from 'node:child_process';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import tls from 'node:tls';

import express from 'express';
import { google } from 'googleapis';
import { ImapFlow } from 'imapflow';
import { simpleParser } from 'mailparser';
import nodemailer from 'nodemailer';

import { mcpCallTool, mcpListTools } from './mcpClient.js';
import { buildMarcusSystemPrompt } from './marcus/core/build_system_prompt.js';

const app = express();
// When running behind SiteGround / reverse proxies, trust forwarded headers.
app.set('trust proxy', true);
const PORT = process.env.PORT ? Number(process.env.PORT) : 3030;

const DEFAULT_BUSINESS_KEY = 'personal';
const requestContext = new AsyncLocalStorage();

let cachedActiveBusinessKey = DEFAULT_BUSINESS_KEY;
let cachedBusinesses = [{ key: DEFAULT_BUSINESS_KEY, name: 'Personal', phoneNumbers: [] }];

const lastRevisionCollapseByKey = new Map();

// Cache cross-business rollups so chat doesn't re-scan every store on every message.
let crossBizRollupCache = { at: 0, text: '' };

const DEBUG_WEBHOOKS = String(process.env.DEBUG_WEBHOOKS || '').trim().toLowerCase() === 'true';

// Capture the raw request bytes so we can verify webhook signatures (Slack/Twilio/etc).
app.use(express.json({
  limit: '512kb',
  verify: (req, res, buf) => {
    // Buffer may be empty for requests with no body.
    req.rawBody = buf;
  },
}));

app.use(express.urlencoded({
  extended: false,
  verify: (req, res, buf) => {
    req.rawBody = buf;
  },
}));

function normalizeBusinessKey(input) {
  const raw = typeof input === 'string' ? input.trim().toLowerCase() : '';
  if (!raw) return '';
  // allow already-sanitized keys; convert label-like strings to slugs
  const key = raw.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 64);
  return key;
}

function getBusinessNameForKey(key) {
  const k = normalizeBusinessKey(key) || DEFAULT_BUSINESS_KEY;
  const list = Array.isArray(cachedBusinesses) ? cachedBusinesses : [];
  const match = list.find((b) => normalizeBusinessKey(b?.key || '') === k);
  if (typeof match?.name === 'string' && match.name.trim()) return match.name.trim();
  if (k === DEFAULT_BUSINESS_KEY) return 'Personal';
  return k;
}

function normalizeBusinessName(input) {
  const raw = typeof input === 'string' ? input.trim() : '';
  return raw.slice(0, 80);
}

function normalizeBusinessPhoneNumbers(input) {
  const out = [];
  const seen = new Set();
  const list = Array.isArray(input)
    ? input
    : (typeof input === 'string' ? input.split(/[\n,;]+/g) : []);

  for (const item of list) {
    const raw = String(item || '').trim();
    if (!raw) continue;
    const val = raw.slice(0, 32);
    if (seen.has(val)) continue;
    seen.add(val);
    out.push(val);
  }
  return out.slice(0, 20);
}

function normalizeBusinessesList(input) {
  const list = Array.isArray(input) ? input : [];
  const out = [];
  const seen = new Set();

  for (const row of list) {
    const r = row && typeof row === 'object' ? row : {};
    const name = normalizeBusinessName(r.name || r.label || r.business || '');
    const key = normalizeBusinessKey(r.key || r.businessKey || '') || normalizeBusinessKey(name);
    const phoneNumbers = normalizeBusinessPhoneNumbers(r.phoneNumbers || r.phones || r.phoneNumbersRaw || r.phoneRouting || []);
    if (!name || !key) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ key, name, phoneNumbers });
  }

  if (!seen.has(DEFAULT_BUSINESS_KEY)) {
    out.unshift({ key: DEFAULT_BUSINESS_KEY, name: 'Personal', phoneNumbers: [] });
  }

  return out;
}

function getBusinessConfigFromSettings(settings) {
  const s = settings && typeof settings === 'object' ? settings : {};
  const businesses = normalizeBusinessesList(s.businesses);
  const activeBusinessKey = normalizeBusinessKey(s.activeBusinessKey || s.activeBusiness || '') || DEFAULT_BUSINESS_KEY;

  const keys = new Set(businesses.map((b) => b.key));
  const finalActive = keys.has(activeBusinessKey) ? activeBusinessKey : DEFAULT_BUSINESS_KEY;

  return { businesses, activeBusinessKey: finalActive };
}

async function refreshBusinessCacheFromSettings() {
  try {
    const saved = await readSettings();
    const cfg = getBusinessConfigFromSettings(saved);
    cachedActiveBusinessKey = cfg.activeBusinessKey;
    cachedBusinesses = cfg.businesses;
  } catch {
    // best-effort cache
  }
}

function getBusinessKeyFromContext() {
  const store = requestContext.getStore();
  const key = normalizeBusinessKey(store?.businessKey || '');
  return key || cachedActiveBusinessKey || DEFAULT_BUSINESS_KEY;
}

function withBusinessKey(businessKey, fn) {
  const key = normalizeBusinessKey(businessKey) || cachedActiveBusinessKey || DEFAULT_BUSINESS_KEY;
  return requestContext.run({ businessKey: key }, fn);
}

function getBusinessKeyFromRequest(req) {
  const headerKey = typeof req?.get === 'function' ? req.get('x-business-key') : '';
  const queryKey = typeof req?.query?.businessKey === 'string' ? req.query.businessKey : '';
  const bodyKey = typeof req?.body?.businessKey === 'string' ? req.body.businessKey : '';
  return normalizeBusinessKey(headerKey || queryKey || bodyKey);
}

// Attach a per-request business context.
// - If client sends X-Business-Key, we honor it.
// - Otherwise we fall back to the server's saved active business key.
app.use((req, res, next) => {
  const incoming = getBusinessKeyFromRequest(req);
  const key = incoming || cachedActiveBusinessKey || DEFAULT_BUSINESS_KEY;
  requestContext.run({ businessKey: key }, () => {
    try {
      res.setHeader('X-Business-Key', key);
    } catch {
      // ignore
    }
    next();
  });
});

function resolveDirFromEnv(envValue) {
  const raw = typeof envValue === 'string' ? envValue.trim() : '';
  if (!raw) return '';
  try {
    return path.isAbsolute(raw) ? raw : path.resolve(process.cwd(), raw);
  } catch {
    return '';
  }
}

const DATA_DIR = resolveDirFromEnv(process.env.TASK_TRACKER_DATA_DIR || process.env.DATA_DIR) || path.join(process.cwd(), 'data');
const DATA_FILE = path.join(DATA_DIR, 'tasks.json');

const BUSINESS_DATA_DIR = path.join(DATA_DIR, 'businesses');

function getStoreFileForBusiness(businessKey) {
  const key = normalizeBusinessKey(businessKey) || DEFAULT_BUSINESS_KEY;
  // Keep backwards-compat: Personal uses the legacy data/tasks.json file.
  if (key === DEFAULT_BUSINESS_KEY) return DATA_FILE;
  return path.join(BUSINESS_DATA_DIR, key, 'tasks.json');
}

// Branding: app is called M.A.R.C.U.S., but keep backward compatibility with existing
// settings directories that were created under the old name.
const APP_NAME = 'M.A.R.C.U.S.';
const LEGACY_APP_NAME = 'Task Tracker';

function getDefaultSettingsDir() {
  const home = os.homedir();
  if (process.platform === 'win32') {
    const appData = typeof process.env.APPDATA === 'string' ? process.env.APPDATA.trim() : '';
    const base = appData || path.join(home, 'AppData', 'Roaming');
    const next = path.join(base, APP_NAME);
    const legacy = path.join(base, LEGACY_APP_NAME);
    // Prefer legacy folder if it already exists to avoid “losing” saved settings.
    try {
      if (fs.existsSync(legacy) && !fs.existsSync(next)) return legacy;
    } catch {
      // ignore
    }
    return next;
  }
  if (process.platform === 'darwin') {
    const base = path.join(home, 'Library', 'Application Support');
    const next = path.join(base, APP_NAME);
    const legacy = path.join(base, LEGACY_APP_NAME);
    try {
      if (fs.existsSync(legacy) && !fs.existsSync(next)) return legacy;
    } catch {
      // ignore
    }
    return next;
  }
  const xdg = typeof process.env.XDG_CONFIG_HOME === 'string' ? process.env.XDG_CONFIG_HOME.trim() : '';
  return path.join(xdg || path.join(home, '.config'), 'task-tracker');
}

const SETTINGS_DIR = resolveDirFromEnv(process.env.TASK_TRACKER_SETTINGS_DIR || process.env.SETTINGS_DIR) || getDefaultSettingsDir();
const SETTINGS_FILE = resolveDirFromEnv(process.env.TASK_TRACKER_SETTINGS_FILE || process.env.SETTINGS_FILE) || path.join(SETTINGS_DIR, 'settings.json');

function parsePositiveIntEnv(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

const BACKUP_DIR = resolveDirFromEnv(process.env.TASK_TRACKER_BACKUP_DIR || process.env.BACKUP_DIR) || path.join(DATA_DIR, 'backups');
const BACKUP_MIRROR_DIR = resolveDirFromEnv(process.env.TASK_TRACKER_BACKUP_MIRROR_DIR || process.env.BACKUP_MIRROR_DIR);
const BACKUP_INTERVAL_MINUTES = parsePositiveIntEnv(process.env.TASK_TRACKER_BACKUP_INTERVAL_MINUTES || process.env.BACKUP_INTERVAL_MINUTES, 60);
const BACKUP_INTERVAL_MS = BACKUP_INTERVAL_MINUTES * 60 * 1000;
const BACKUP_RETENTION_DAYS = parsePositiveIntEnv(process.env.TASK_TRACKER_BACKUP_RETENTION_DAYS || process.env.BACKUP_RETENTION_DAYS, 14);
const BACKUP_RETENTION_MS = BACKUP_RETENTION_DAYS * 24 * 60 * 60 * 1000;
const lastBackupAtByKey = new Map();

const GA4_PULL_INTERVAL_MINUTES = parsePositiveIntEnv(process.env.TASK_TRACKER_GA4_PULL_INTERVAL_MINUTES || process.env.GA4_PULL_INTERVAL_MINUTES, 60);
const GA4_PULL_INTERVAL_MS = GA4_PULL_INTERVAL_MINUTES * 60 * 1000;

const AIRTABLE_REQUESTS_WINDOW_DAYS = parsePositiveIntEnv(process.env.TASK_TRACKER_AIRTABLE_REQUESTS_WINDOW_DAYS || process.env.AIRTABLE_REQUESTS_WINDOW_DAYS, 30);
const AIRTABLE_AUTO_SYNC_ENABLED = String(process.env.TASK_TRACKER_AIRTABLE_AUTO_SYNC || process.env.AIRTABLE_AUTO_SYNC || 'true').trim().toLowerCase() !== 'false';
const AIRTABLE_AUTO_SYNC_MINUTES = parsePositiveIntEnv(process.env.TASK_TRACKER_AIRTABLE_AUTO_SYNC_MINUTES || process.env.AIRTABLE_AUTO_SYNC_MINUTES, 5);
const AIRTABLE_AUTO_SYNC_INTERVAL_MS = AIRTABLE_AUTO_SYNC_MINUTES * 60 * 1000;

function shouldMaterializeAirtableRevisionRequests(settings) {
  const s = settings && typeof settings === 'object' ? settings : {};
  return s.airtableMaterializeRevisionRequests === true;
}

function backupTimestamp(d = new Date()) {
  return d.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
}

function shouldCreateBackupForKey(key) {
  const k = String(key || '').trim();
  if (!k) return false;
  const last = Number(lastBackupAtByKey.get(k) || 0);
  if (!Number.isFinite(last) || last <= 0) return true;
  return (Date.now() - last) >= BACKUP_INTERVAL_MS;
}

function markBackupForKey(key) {
  const k = String(key || '').trim();
  if (!k) return;
  lastBackupAtByKey.set(k, Date.now());
}

async function pruneBackupsInDir({ dirPath, prefix }) {
  const dir = String(dirPath || '').trim();
  const pfx = String(prefix || '').trim();
  if (!dir || !pfx) return;
  if (!BACKUP_RETENTION_MS) return;
  const now = Date.now();
  let entries = [];
  try {
    entries = await fs.readdir(dir, { withFileTypes: true });
  } catch {
    return;
  }
  await Promise.all(entries
    .filter((entry) => entry && entry.isFile() && String(entry.name || '').startsWith(`${pfx}-`) && String(entry.name || '').endsWith('.json'))
    .map(async (entry) => {
      const filePath = path.join(dir, entry.name);
      try {
        const stat = await fs.stat(filePath);
        if ((now - Number(stat.mtimeMs || 0)) > BACKUP_RETENTION_MS) {
          await fs.unlink(filePath);
        }
      } catch {
        // ignore cleanup errors
      }
    }));
}

async function writeBackupSnapshot({ sourceFile, prefix }) {
  const src = String(sourceFile || '').trim();
  const pfx = String(prefix || '').trim();
  if (!src || !pfx) return false;
  try {
    await fs.access(src);
  } catch {
    return false;
  }

  const stamp = backupTimestamp();
  const fileName = `${pfx}-${stamp}.json`;

  await fs.mkdir(BACKUP_DIR, { recursive: true });
  await fs.copyFile(src, path.join(BACKUP_DIR, fileName));
  await pruneBackupsInDir({ dirPath: BACKUP_DIR, prefix: pfx });

  if (BACKUP_MIRROR_DIR) {
    try {
      await fs.mkdir(BACKUP_MIRROR_DIR, { recursive: true });
      await fs.copyFile(src, path.join(BACKUP_MIRROR_DIR, fileName));
      await pruneBackupsInDir({ dirPath: BACKUP_MIRROR_DIR, prefix: pfx });
    } catch {
      // mirror is best-effort
    }
  }

  return true;
}

async function backupCriticalFiles({ force = false } = {}) {
  const shouldTasks = force || shouldCreateBackupForKey('tasks');
  const shouldSettings = force || shouldCreateBackupForKey('settings');

  // Personal/legacy store
  if (shouldTasks) {
    const ok = await writeBackupSnapshot({ sourceFile: DATA_FILE, prefix: 'tasks' });
    if (ok) markBackupForKey('tasks');
  }

  // Per-business stores (best-effort)
  try {
    const settings = await readSettings();
    const cfg = getBusinessConfigFromSettings(settings);
    const extra = (Array.isArray(cfg.businesses) ? cfg.businesses : []).map((b) => b.key).filter((k) => k && k !== DEFAULT_BUSINESS_KEY);
    for (const key of extra) {
      const cacheKey = `tasks:${key}`;
      const should = force || shouldCreateBackupForKey(cacheKey);
      if (!should) continue;
      const file = getStoreFileForBusiness(key);
      const ok = await writeBackupSnapshot({ sourceFile: file, prefix: `tasks-${key}` });
      if (ok) markBackupForKey(cacheKey);
    }
  } catch {
    // ignore extra backup errors
  }

  if (shouldSettings) {
    const ok = await writeBackupSnapshot({ sourceFile: SETTINGS_FILE, prefix: 'settings' });
    if (ok) markBackupForKey('settings');
  }
}

function startBackupScheduler() {
  const timer = setInterval(() => {
    backupCriticalFiles().catch(() => {
      // ignore periodic backup errors
    });
  }, BACKUP_INTERVAL_MS);
  if (typeof timer.unref === 'function') timer.unref();
}

const ADMIN_TOKEN = typeof process.env.ADMIN_TOKEN === 'string' ? process.env.ADMIN_TOKEN.trim() : '';
const AUTH_COOKIE_NAME = 'ops_admin_token';
const AUTH_COOKIE_MAX_AGE_SEC = 60 * 60 * 24 * 30;

function parseCookies(req) {
  const raw = typeof req.headers?.cookie === 'string' ? req.headers.cookie : '';
  const out = {};
  if (!raw) return out;
  for (const chunk of raw.split(';')) {
    const part = String(chunk || '').trim();
    if (!part) continue;
    const eq = part.indexOf('=');
    if (eq <= 0) continue;
    const key = part.slice(0, eq).trim();
    const val = part.slice(eq + 1).trim();
    if (!key) continue;
    try {
      out[key] = decodeURIComponent(val);
    } catch {
      out[key] = val;
    }
  }
  return out;
}

function buildAuthCookie({ req, token, clear = false, remember = true }) {
  const proto = req?.headers?.['x-forwarded-proto'] ? String(req.headers['x-forwarded-proto']).split(',')[0].trim() : req?.protocol;
  const secure = String(proto || '').toLowerCase() === 'https';
  const val = clear ? '' : encodeURIComponent(String(token || ''));
  const parts = [`${AUTH_COOKIE_NAME}=${val}`, 'Path=/', 'HttpOnly', 'SameSite=Lax'];
  if (clear) {
    parts.push('Max-Age=0');
  } else if (remember) {
    parts.push(`Max-Age=${AUTH_COOKIE_MAX_AGE_SEC}`);
  }
  if (secure) parts.push('Secure');
  return parts.join('; ');
}

function isPublicApiRoute(req) {
  const method = String(req.method || '').toUpperCase();
  const p = String(req.path || '');
  if (method === 'POST' && p === '/api/auth/login') return true;
  if (method === 'POST' && p === '/api/auth/logout') return true;
  if (method === 'GET' && p === '/api/auth/status') return true;
  if (method === 'GET' && p === '/api/health') return true;
  if (method === 'POST' && p === '/api/integrations/slack/events') return true;
  if (method === 'POST' && p === '/api/integrations/crm/webhook') return true;
  if (method === 'POST' && p === '/api/integrations/quo/sms') return true;
  if (method === 'POST' && p === '/api/integrations/quo/calls') return true;
  if (method === 'POST' && p === '/api/integrations/fireflies/ingest') return true;
  if (method === 'GET' && p === '/api/integrations/slack/oauth/callback') return true;
  if (method === 'GET' && p === '/api/integrations/google/callback') return true;
  return false;
}

function extractBearerToken(req) {
  const auth = typeof req.headers.authorization === 'string' ? req.headers.authorization.trim() : '';
  if (auth.toLowerCase().startsWith('bearer ')) return auth.slice(7).trim();
  const headerToken = typeof req.headers['x-admin-token'] === 'string' ? req.headers['x-admin-token'].trim() : '';
  if (headerToken) return headerToken;
  const cookieToken = String(parseCookies(req)[AUTH_COOKIE_NAME] || '').trim();
  if (cookieToken) return cookieToken;
  const liveToken = typeof req.query?.liveToken === 'string' ? req.query.liveToken.trim() : '';
  if (liveToken) return liveToken;
  const queryToken = typeof req.query?.token === 'string' ? req.query.token.trim() : '';
  return queryToken;
}

const MARCUS_LIVE_SESSION_TTL_MS = 10 * 60 * 1000;
const marcusLiveSessionTokens = new Map();

function pruneMarcusLiveSessionTokens() {
  const now = Date.now();
  for (const [token, expiresAt] of marcusLiveSessionTokens.entries()) {
    if (!Number.isFinite(expiresAt) || expiresAt <= now) marcusLiveSessionTokens.delete(token);
  }
}

function createMarcusLiveSessionToken() {
  pruneMarcusLiveSessionTokens();
  const token = crypto.randomBytes(24).toString('base64url');
  const expiresAt = Date.now() + MARCUS_LIVE_SESSION_TTL_MS;
  marcusLiveSessionTokens.set(token, expiresAt);
  return { token, expiresAt };
}

function isValidMarcusLiveSessionToken(token) {
  const t = typeof token === 'string' ? token.trim() : '';
  if (!t) return false;
  pruneMarcusLiveSessionTokens();
  return Boolean(marcusLiveSessionTokens.has(t));
}

function isMarcusLiveSessionRoute(req) {
  const p = String(req?.path || '');
  return p === '/api/marcus/live'
    || p === '/api/marcus/live/chat'
    || p === '/api/marcus/live/session-status'
    || p === '/api/desktop-context/health';
}

// Optional auth for internet-hosting. If ADMIN_TOKEN is set, all /api/* routes require it
// except inbound webhooks + OAuth callbacks.
app.use((req, res, next) => {
  try {
    if (!ADMIN_TOKEN) return next();
    const p = String(req.path || '');
    if (!p.startsWith('/api/')) return next();
    if (isPublicApiRoute(req)) return next();

    const token = extractBearerToken(req);
    if (token && safeTimingEqual(token, ADMIN_TOKEN)) return next();
    if (isMarcusLiveSessionRoute(req) && isValidMarcusLiveSessionToken(token)) return next();
    res.status(401).json({ error: 'Unauthorized' });
  } catch {
    res.status(401).json({ error: 'Unauthorized' });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

app.get('/api/auth/status', (req, res) => {
  if (!ADMIN_TOKEN) {
    res.json({ ok: true, authRequired: false, authenticated: true });
    return;
  }
  const token = extractBearerToken(req);
  const authenticated = Boolean(token && (safeTimingEqual(token, ADMIN_TOKEN) || isValidMarcusLiveSessionToken(token)));
  res.json({ ok: true, authRequired: true, authenticated });
});

app.post('/api/auth/login', (req, res) => {
  if (!ADMIN_TOKEN) {
    res.json({ ok: true, authRequired: false });
    return;
  }
  const token = typeof req.body?.token === 'string' ? req.body.token.trim() : '';
  const remember = req.body?.remember !== false;
  if (!token || !safeTimingEqual(token, ADMIN_TOKEN)) {
    res.status(401).json({ ok: false, error: 'Invalid admin token' });
    return;
  }
  res.setHeader('Set-Cookie', buildAuthCookie({ req, token, remember }));
  res.json({ ok: true, authRequired: true, authenticated: true });
});

app.post('/api/auth/logout', (req, res) => {
  res.setHeader('Set-Cookie', buildAuthCookie({ req, token: '', clear: true }));
  res.json({ ok: true });
});

/**
 * File format:
 * {
 *   revision: number,
 *   updatedAt: string,
 *   projects?: Array<Project>,
 *   tasks: Array<Task>
 *   senderProjectMap?: Record<string, string | { projectId: string, projectName?: string }>,
 *   projectNotes?: Record<string, { notes: string, updatedAt: string } | string>, // legacy
 *   projectScratchpads?: Record<projectId, { text: string, updatedAt: string }>,
 *   projectNoteEntries?: Record<projectId, Array<NoteEntry>>,
 *   projectChats?: Record<projectId, { messages: Array<ChatMessage>, updatedAt: string }>,
 *   projectCommunications?: Record<projectId, Array<Communication>>, // { id, source: 'email'|'quo'|'other', date, from, to, subject, body }
 *   team?: Array<TeamMember>
 * }
 */
const EMPTY_STORE = {
  revision: 1,
  updatedAt: new Date(0).toISOString(),
  projects: [],
  clients: [],
  tasks: [],
  senderProjectMap: {},
  team: [],
  projectNotes: {},
  projectScratchpads: {},
  projectNoteEntries: {},
  projectChats: {},
  projectCommunications: {},
  marcusNotes: {},
  inboxItems: [],
  projectTranscriptUndo: {},
};

let writeLock = Promise.resolve();

const OPENAI_MODEL_FALLBACKS = [
  'gpt-5',
  'gpt-5-mini',
  'gpt-5-nano',
  'gpt-4.1',
  'gpt-4.1-mini',
  'gpt-4o',
  'gpt-4o-mini',
];
const OPENAI_MODELS_CACHE_TTL_MS = 5 * 60 * 1000;
let openAiModelsCache = {
  fetchedAt: 0,
  keyHint: '',
  models: OPENAI_MODEL_FALLBACKS.slice(),
};

const LEGACY_AGENT_SYSTEM_PROMPT_EXACT = new Set([
  'You are my ops agent. Be concise. End with Next steps.',
]);

const MARCUS_RECENT_ACTIVITY_DAYS = 21;
const MARCUS_UPCOMING_WINDOW_DAYS = 14;
const MARCUS_HARD_STALE_TASK_DAYS = 45;
const MARCUS_OVERDUE_GRACE_DAYS = 7;
const MS_PER_DAY = 24 * 60 * 60 * 1000;

function normalizeAgentSystemPrompt(input) {
  const value = typeof input === 'string' ? input.trim() : '';
  if (!value) return '';
  if (LEGACY_AGENT_SYSTEM_PROMPT_EXACT.has(value)) return '';

  const lower = value.toLowerCase();
  const looksLikeLegacyMartyPrompt = (
    lower.includes('management assistant for routing tasks and yield') ||
    lower.includes('you are marty') ||
    lower.includes('m.a.r.t.y')
  );

  return looksLikeLegacyMartyPrompt ? '' : value;
}

function normalizeOperatorVoice(input) {
  const value = typeof input === 'string' ? input.trim().toLowerCase() : '';
  if (!value) return '';
  if (value === 'take_control') return '';
  return value;
}

function normalizeSettingsShape(settings) {
  const parsed = settings && typeof settings === 'object' ? settings : {};
  return {
    ...parsed,
    agentSystemPrompt: normalizeAgentSystemPrompt(parsed.agentSystemPrompt),
    operatorVoice: normalizeOperatorVoice(parsed.operatorVoice),
    automationConfig: normalizeAutomationConfig(parsed.automationConfig),
    automationDigestQueue: normalizeAutomationDigestQueue(parsed.automationDigestQueue),
  };
}

async function readSettings() {
  try {
    await fs.mkdir(SETTINGS_DIR, { recursive: true });
    const raw = await fs.readFile(SETTINGS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return normalizeSettingsShape({});
    return normalizeSettingsShape(parsed);
  } catch {
    return normalizeSettingsShape({});
  }
}

async function writeSettings(next) {
  const normalized = normalizeSettingsShape(next);
  await fs.mkdir(SETTINGS_DIR, { recursive: true });
  const tmpFile = `${SETTINGS_FILE}.tmp-${crypto.randomBytes(6).toString('hex')}`;
  await fs.writeFile(tmpFile, JSON.stringify(normalized, null, 2) + '\n', 'utf8');
  await fs.rename(tmpFile, SETTINGS_FILE);
  refreshBusinessCacheFromSettings().catch(() => {
    // best-effort
  });
  backupCriticalFiles().catch(() => {
    // backup is best-effort
  });
}

async function getAiConfig() {
  const saved = await readSettings();
  const envKey = typeof process.env.OPENAI_API_KEY === 'string' ? process.env.OPENAI_API_KEY.trim() : '';
  const savedKey = typeof saved.openaiApiKey === 'string' ? saved.openaiApiKey.trim() : '';
  const apiKey = envKey || savedKey;

  const envModel = typeof process.env.OPENAI_MODEL === 'string' ? process.env.OPENAI_MODEL.trim() : '';
  const savedModel = typeof saved.openaiModel === 'string' ? saved.openaiModel.trim() : '';
  // Prefer the user-selected/saved model when present; env acts as a default.
  // This makes the model picker in the UI actually take effect on hosted envs.
  const model = savedModel || envModel || 'gpt-4o-mini';

  const source = envKey ? 'env' : savedKey ? 'saved' : 'none';
  const last4 = apiKey && apiKey.length >= 4 ? apiKey.slice(-4) : '';
  const keyHint = last4 ? `����${last4}` : '';
  const settingsUpdatedAt = typeof saved.updatedAt === 'string' ? saved.updatedAt : '';

  return {
    apiKey,
    model,
    source,
    keyHint,
    settingsUpdatedAt,
  };
}

function normalizeAiProvider(input) {
  const s = typeof input === 'string' ? input.trim().toLowerCase() : '';
  if (s === 'openrouter') return 'openrouter';
  return 'openai';
}

function pickObject(input) {
  return input && typeof input === 'object' && !Array.isArray(input) ? input : {};
}

function normalizeAiRoutes(input) {
  const raw = pickObject(input);
  const keys = ['marcusChat', 'operatorBio', 'projectAssistant', 'dashboardPreview'];
  const out = {};
  for (const k of keys) {
    const entry = pickObject(raw[k]);
    const provider = normalizeAiProvider(entry.provider);
    const model = typeof entry.model === 'string' ? entry.model.trim() : '';
    out[k] = { provider, model };
  }
  return out;
}

function clampUnit(input, fallback) {
  const n = Number(input);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(1, n));
}

function normalizeAutomationConfig(input) {
  const raw = pickObject(input);
  const inbox = pickObject(raw.inboxAutoConvert);
  const delegation = pickObject(raw.autoDelegation);
  const overdue = pickObject(raw.workloadRebalance);

  const approvalModeRaw = typeof raw.approvalMode === 'string' ? raw.approvalMode.trim().toLowerCase() : '';
  const approvalMode = ['manual', 'dailydigest', 'auto'].includes(approvalModeRaw)
    ? (approvalModeRaw === 'dailydigest' ? 'dailyDigest' : approvalModeRaw)
    : 'dailyDigest';

  const maxTasksPerItemRaw = Number(inbox.maxTasksPerItem);
  const maxTasksPerItem = Number.isFinite(maxTasksPerItemRaw)
    ? Math.max(1, Math.min(5, Math.floor(maxTasksPerItemRaw)))
    : 3;

  const limitRaw = Number(inbox.limit);
  const limit = Number.isFinite(limitRaw)
    ? Math.max(1, Math.min(300, Math.floor(limitRaw)))
    : 120;

  const overdueDaysRaw = Number(overdue.overdueDays);
  const overdueDays = Number.isFinite(overdueDaysRaw)
    ? Math.max(1, Math.min(30, Math.floor(overdueDaysRaw)))
    : 5;

  return {
    enabled: raw.enabled !== false,
    approvalMode,
    inboxAutoConvert: {
      enabled: inbox.enabled !== false,
      onlyNew: inbox.onlyNew !== false,
      includeArchived: inbox.includeArchived === true,
      limit,
      minProjectConfidence: clampUnit(inbox.minProjectConfidence, 0.8),
      minDelegateConfidence: clampUnit(inbox.minDelegateConfidence, 0.85),
      autoLinkProject: inbox.autoLinkProject !== false,
      autoDelegate: inbox.autoDelegate !== false,
      markInboxDoneOnApply: inbox.markInboxDoneOnApply !== false,
      maxTasksPerItem,
    },
    autoDelegation: {
      enabled: delegation.enabled !== false,
      skipConfirmIfConfidence: clampUnit(delegation.skipConfirmIfConfidence, 0.85),
      respectWipLimits: delegation.respectWipLimits !== false,
      skillMatchRequired: delegation.skillMatchRequired !== false,
    },
    commsDraft: {
      enabled: pickObject(raw.commsDraft).enabled === true,
    },
    autoArchiveLinkedInbox: raw.autoArchiveLinkedInbox === true,
    workloadRebalance: {
      enabled: overdue.enabled === true,
      overdueDays,
      escalateToSlack: overdue.escalateToSlack === true,
    },
    auditLog: raw.auditLog !== false,
    notificationBatching: raw.notificationBatching !== false,
  };
}

function normalizeAutomationDigestQueue(input) {
  if (!Array.isArray(input)) return [];
  return input
    .map((entry) => {
      const e = entry && typeof entry === 'object' ? entry : {};
      const tasks = Array.isArray(e.tasks)
        ? e.tasks.map((t) => ({
          title: String(t?.title || '').trim(),
          priority: [1, 2, 3].includes(Number(t?.priority)) ? Number(t.priority) : 2,
        })).filter((t) => t.title).slice(0, 5)
        : [];
      return {
        id: String(e.id || '').trim() || makeId(),
        itemId: String(e.itemId || '').trim(),
        status: ['pending', 'applied', 'rejected'].includes(String(e.status || '').trim().toLowerCase())
          ? String(e.status || '').trim().toLowerCase()
          : 'pending',
        createdAt: safeIsoMaybe(String(e.createdAt || '').trim()) || nowIso(),
        decidedAt: safeIsoMaybe(String(e.decidedAt || '').trim()) || '',
        runId: String(e.runId || '').trim(),
        source: String(e.source || '').trim() || 'marcus-automation',
        signalPreview: previewTextServer(String(e.signalPreview || '').trim(), 220),
        projectId: String(e.projectId || '').trim(),
        projectName: String(e.projectName || '').trim(),
        projectConfidence: clampUnit(e.projectConfidence, 0),
        delegateName: String(e.delegateName || '').trim(),
        delegateConfidence: clampUnit(e.delegateConfidence, 0),
        appliedTaskIds: Array.isArray(e.appliedTaskIds)
          ? e.appliedTaskIds.map((x) => String(x || '').trim()).filter(Boolean).slice(0, 20)
          : [],
        decision: {
          acceptProjectLink: Boolean(e?.decision?.acceptProjectLink),
          acceptDelegate: Boolean(e?.decision?.acceptDelegate),
          acceptTaskIndexes: Array.isArray(e?.decision?.acceptTaskIndexes)
            ? e.decision.acceptTaskIndexes
              .map((x) => Number(x))
              .filter((x) => Number.isInteger(x) && x >= 0 && x <= 20)
              .slice(0, 20)
            : [],
        },
        tasks,
      };
    })
    .filter((e) => e.itemId)
    .slice(0, 500);
}

function getOpenAiSecrets(saved) {
  const envKey = typeof process.env.OPENAI_API_KEY === 'string' ? process.env.OPENAI_API_KEY.trim() : '';
  const savedKey = typeof saved?.openaiApiKey === 'string' ? saved.openaiApiKey.trim() : '';
  const apiKey = envKey || savedKey;
  const source = envKey ? 'env' : savedKey ? 'saved' : 'none';
  const last4 = apiKey && apiKey.length >= 4 ? apiKey.slice(-4) : '';
  const keyHint = last4 ? `••••${last4}` : '';
  const envModel = typeof process.env.OPENAI_MODEL === 'string' ? process.env.OPENAI_MODEL.trim() : '';
  const savedModel = typeof saved?.openaiModel === 'string' ? saved.openaiModel.trim() : '';
  const model = savedModel || envModel || 'gpt-4o-mini';
  return { apiKey, source, keyHint, model };
}

function normalizeOpenAiModelList(input) {
  const rows = Array.isArray(input) ? input : [];
  const ids = [];
  for (const row of rows) {
    const id = typeof row?.id === 'string' ? row.id.trim() : '';
    if (!id) continue;
    const lower = id.toLowerCase();
    const looksLikeChatModel = lower.startsWith('gpt-') || lower.startsWith('o1') || lower.startsWith('o3') || lower.startsWith('o4');
    if (!looksLikeChatModel) continue;
    ids.push(id);
  }
  const uniq = Array.from(new Set(ids));
  uniq.sort((a, b) => a.localeCompare(b));
  return uniq;
}

async function fetchOpenAiModelsCatalog({ apiKey, force = false } = {}) {
  const token = typeof apiKey === 'string' ? apiKey.trim() : '';
  if (!token) {
    return { ok: false, error: 'OpenAI API key is not configured.', models: OPENAI_MODEL_FALLBACKS.slice(), source: 'fallback' };
  }

  const keyHint = token.length >= 6 ? token.slice(-6) : token;
  const now = Date.now();
  const isFresh = Number(openAiModelsCache.fetchedAt) > 0 && (now - Number(openAiModelsCache.fetchedAt)) < OPENAI_MODELS_CACHE_TTL_MS;
  if (!force && isFresh && openAiModelsCache.keyHint === keyHint && Array.isArray(openAiModelsCache.models) && openAiModelsCache.models.length) {
    return {
      ok: true,
      models: openAiModelsCache.models.slice(),
      source: 'cache',
      fetchedAt: Number(openAiModelsCache.fetchedAt) || now,
    };
  }

  const { resp, data } = await fetchJsonWithTimeout('https://api.openai.com/v1/models', {
    timeoutMs: 20_000,
    method: 'GET',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
  });

  if (!resp.ok) {
    const detail = typeof data?.error?.message === 'string' ? data.error.message : '';
    const msg = `OpenAI model discovery failed (${resp.status})${detail ? `: ${detail}` : ''}`;
    return {
      ok: false,
      error: msg,
      models: OPENAI_MODEL_FALLBACKS.slice(),
      source: 'fallback',
    };
  }

  const discovered = normalizeOpenAiModelList(data?.data);
  const merged = Array.from(new Set([...discovered, ...OPENAI_MODEL_FALLBACKS]));
  merged.sort((a, b) => a.localeCompare(b));

  openAiModelsCache = {
    fetchedAt: now,
    keyHint,
    models: merged,
  };

  return {
    ok: true,
    models: merged,
    source: 'live',
    fetchedAt: now,
  };
}

function getOpenRouterSecrets(saved) {
  const envKey = typeof process.env.OPENROUTER_API_KEY === 'string' ? process.env.OPENROUTER_API_KEY.trim() : '';
  const savedKey = typeof saved?.openrouterApiKey === 'string' ? saved.openrouterApiKey.trim() : '';
  const apiKey = envKey || savedKey;
  const source = envKey ? 'env' : savedKey ? 'saved' : 'none';
  const last4 = apiKey && apiKey.length >= 4 ? apiKey.slice(-4) : '';
  const keyHint = last4 ? `••••${last4}` : '';
  const savedModel = typeof saved?.openrouterModel === 'string' ? saved.openrouterModel.trim() : '';
  const envModel = typeof process.env.OPENROUTER_MODEL === 'string' ? process.env.OPENROUTER_MODEL.trim() : '';
  const model = savedModel || envModel || 'openai/gpt-4o-mini';
  return { apiKey, source, keyHint, model };
}

function guessEmbeddingVectorSize(model) {
  const name = typeof model === 'string' ? model.trim().toLowerCase() : '';
  if (name === 'text-embedding-3-large') return 3072;
  if (name === 'text-embedding-ada-002') return 1536;
  return 1536;
}

function normalizeBaseUrl(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return '';
  return raw.replace(/\/+$/g, '');
}

function maskSecretHint(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw || raw.length < 4) return '';
  return `••••${raw.slice(-4)}`;
}

function normalizeNetworkPort(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.min(65535, Math.floor(n)));
}

function normalizeBooleanFlag(value, fallback = false) {
  if (typeof value === 'boolean') return value;
  const s = typeof value === 'string' ? value.trim().toLowerCase() : '';
  if (s === '1' || s === 'true' || s === 'yes' || s === 'on') return true;
  if (s === '0' || s === 'false' || s === 'no' || s === 'off') return false;
  return fallback;
}

function normalizeTimeoutMs(value, fallback, max = 60_000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1_000, Math.min(max, Math.floor(n)));
}

async function withOperationTimeout(factory, timeoutMs, label) {
  const waitMs = normalizeTimeoutMs(timeoutMs, 5_000, 30_000);
  let timer = null;
  try {
    return await Promise.race([
      Promise.resolve().then(factory),
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label} timed out after ${waitMs}ms`));
        }, waitMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function probeEmailTransportProfile(profile, timeoutMs) {
  const waitMs = normalizeTimeoutMs(timeoutMs, 2_500, 15_000);
  const secure = profile?.secure === true;
  const label = String(profile?.label || `${profile?.host || ''}:${profile?.port || ''}`).trim();
  return new Promise((resolve) => {
    let settled = false;
    const done = (result) => {
      if (settled) return;
      settled = true;
      try {
        socket.destroy();
      } catch {
        // ignore destroy failures
      }
      resolve(result);
    };

    const socket = secure
      ? tls.connect({
        host: profile.host,
        port: profile.port,
        servername: profile.host,
        rejectUnauthorized: false,
      })
      : net.connect({
        host: profile.host,
        port: profile.port,
      });

    socket.setTimeout(waitMs);
    socket.once(secure ? 'secureConnect' : 'connect', () => {
      done({ ok: true, profile: label });
    });
    socket.once('timeout', () => {
      done({ ok: false, profile: label, error: `Timed out after ${waitMs}ms` });
    });
    socket.once('error', (err) => {
      done({ ok: false, profile: label, error: String(err?.message || 'Connection failed') });
    });
  });
}

async function probeEmailTransportProfiles(protocol, profiles, timeoutMs) {
  const attempts = [];
  for (const profile of profiles) {
    const label = protocol === 'imap' ? describeImapProfile(profile) : describeSmtpProfile(profile);
    const result = await probeEmailTransportProfile({ ...profile, label }, timeoutMs);
    attempts.push({
      ok: result.ok,
      profile: label,
      ...(result.ok ? {} : { error: result.error || 'Connection failed' }),
    });
    if (result.ok) {
      return { ok: true, profile: label, attempts };
    }
  }
  return { ok: false, attempts };
}

function normalizeEmailFolderList(input, fallback = []) {
  const raw = Array.isArray(input)
    ? input
    : (typeof input === 'string' ? input.split(/[\n,;]+/g) : []);
  const folders = raw
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .slice(0, 20);
  if (folders.length) return Array.from(new Set(folders));
  return Array.isArray(fallback) ? Array.from(new Set(fallback.map((item) => String(item || '').trim()).filter(Boolean))).slice(0, 20) : [];
}

function normalizeEmailAddress(value) {
  return String(value || '').trim().toLowerCase();
}

function makeEmailTransportAttemptKey(prefix, profile) {
  const host = String(profile?.host || '').trim().toLowerCase();
  const port = Number(profile?.port) || 0;
  const secure = profile?.secure === true ? 'secure' : 'plain';
  const starttls = profile?.doSTARTTLS === true
    ? 'starttls-required'
    : (profile?.doSTARTTLS === false ? 'starttls-disabled' : 'starttls-auto');
  const smtpTls = profile?.requireTLS === true
    ? 'requiretls'
    : (profile?.ignoreTLS === true ? 'ignoretls' : 'tls-auto');
  return [prefix, host, port, secure, starttls, smtpTls].join(':');
}

function pushUniqueEmailTransportProfile(list, seen, prefix, profile) {
  const key = makeEmailTransportAttemptKey(prefix, profile);
  if (seen.has(key)) return;
  seen.add(key);
  list.push(profile);
}

function describeImapProfile(profile) {
  const parts = [`${profile.host}:${profile.port}`];
  if (profile.secure) parts.push('direct TLS');
  else if (profile.doSTARTTLS === true) parts.push('STARTTLS');
  else if (profile.doSTARTTLS === false) parts.push('cleartext');
  else parts.push('opportunistic STARTTLS');
  return parts.join(' / ');
}

function describeSmtpProfile(profile) {
  const parts = [`${profile.host}:${profile.port}`];
  if (profile.secure) parts.push('direct TLS');
  else if (profile.requireTLS === true) parts.push('STARTTLS required');
  else if (profile.ignoreTLS === true) parts.push('cleartext');
  else parts.push('STARTTLS if available');
  return parts.join(' / ');
}

function buildImapConnectionProfiles(emailCfg) {
  const profiles = [];
  const seen = new Set();
  const host = String(emailCfg?.imap?.host || '').trim();
  const port = normalizeNetworkPort(emailCfg?.imap?.port, 993);
  const secure = emailCfg?.imap?.secure === true;
  const auth = {
    user: String(emailCfg?.imap?.username || '').trim(),
    pass: String(emailCfg?.imap?.password || ''),
  };

  pushUniqueEmailTransportProfile(profiles, seen, 'imap', {
    host,
    port,
    secure,
    ...(secure ? {} : { doSTARTTLS: port === 143 ? true : undefined }),
    auth,
    label: 'configured',
  });

  if (!(secure && port === 993)) {
    pushUniqueEmailTransportProfile(profiles, seen, 'imap', {
      host,
      port: 993,
      secure: true,
      auth,
      label: 'direct-tls-993',
    });
  }

  if (!(port === 143 && secure === false)) {
    pushUniqueEmailTransportProfile(profiles, seen, 'imap', {
      host,
      port: 143,
      secure: false,
      doSTARTTLS: true,
      auth,
      label: 'starttls-143',
    });
  }

  if (secure === false || port === 143) {
    pushUniqueEmailTransportProfile(profiles, seen, 'imap', {
      host,
      port: 143,
      secure: false,
      doSTARTTLS: false,
      auth,
      label: 'cleartext-143',
    });
  }

  return profiles;
}

function buildSmtpConnectionProfiles(emailCfg) {
  const profiles = [];
  const seen = new Set();
  const host = String(emailCfg?.smtp?.host || '').trim();
  const port = normalizeNetworkPort(emailCfg?.smtp?.port, 465);
  const secure = emailCfg?.smtp?.secure === true;
  const auth = {
    user: String(emailCfg?.smtp?.username || '').trim(),
    pass: String(emailCfg?.smtp?.password || ''),
  };

  pushUniqueEmailTransportProfile(profiles, seen, 'smtp', {
    host,
    port,
    secure,
    ...(secure ? {} : { requireTLS: port === 587 }),
    auth,
    label: 'configured',
  });

  if (!(secure && port === 465)) {
    pushUniqueEmailTransportProfile(profiles, seen, 'smtp', {
      host,
      port: 465,
      secure: true,
      auth,
      label: 'direct-tls-465',
    });
  }

  if (!(port === 587 && secure === false)) {
    pushUniqueEmailTransportProfile(profiles, seen, 'smtp', {
      host,
      port: 587,
      secure: false,
      requireTLS: true,
      auth,
      label: 'starttls-587',
    });
  }

  if (secure === false || port === 587) {
    pushUniqueEmailTransportProfile(profiles, seen, 'smtp', {
      host,
      port: 587,
      secure: false,
      ignoreTLS: true,
      auth,
      label: 'cleartext-587',
    });
  }

  return profiles;
}

function getEmailConfig(saved) {
  const envImapHost = typeof process.env.IMAP_HOST === 'string' ? process.env.IMAP_HOST.trim() : '';
  const savedImapHost = typeof saved?.imapHost === 'string' ? saved.imapHost.trim() : '';
  const imapHost = envImapHost || savedImapHost;

  const envImapPort = process.env.IMAP_PORT;
  const imapPort = normalizeNetworkPort(envImapPort || saved?.imapPort, 993);
  const imapSecure = normalizeBooleanFlag(process.env.IMAP_SECURE, normalizeBooleanFlag(saved?.imapSecure, imapPort === 993));
  const envImapUser = typeof process.env.IMAP_USERNAME === 'string' ? process.env.IMAP_USERNAME.trim() : '';
  const savedImapUser = typeof saved?.imapUsername === 'string' ? saved.imapUsername.trim() : '';
  const imapUsername = envImapUser || savedImapUser;
  const envImapPass = typeof process.env.IMAP_PASSWORD === 'string' ? process.env.IMAP_PASSWORD.trim() : '';
  const savedImapPass = typeof saved?.imapPassword === 'string' ? saved.imapPassword.trim() : '';
  const imapPassword = envImapPass || savedImapPass;

  const envSmtpHost = typeof process.env.SMTP_HOST === 'string' ? process.env.SMTP_HOST.trim() : '';
  const savedSmtpHost = typeof saved?.smtpHost === 'string' ? saved.smtpHost.trim() : '';
  const smtpHost = envSmtpHost || savedSmtpHost;
  const envSmtpPort = process.env.SMTP_PORT;
  const smtpPort = normalizeNetworkPort(envSmtpPort || saved?.smtpPort, 465);
  const smtpSecure = normalizeBooleanFlag(process.env.SMTP_SECURE, normalizeBooleanFlag(saved?.smtpSecure, smtpPort === 465));
  const envSmtpUser = typeof process.env.SMTP_USERNAME === 'string' ? process.env.SMTP_USERNAME.trim() : '';
  const savedSmtpUser = typeof saved?.smtpUsername === 'string' ? saved.smtpUsername.trim() : '';
  const smtpUsername = envSmtpUser || savedSmtpUser;
  const envSmtpPass = typeof process.env.SMTP_PASSWORD === 'string' ? process.env.SMTP_PASSWORD.trim() : '';
  const savedSmtpPass = typeof saved?.smtpPassword === 'string' ? saved.smtpPassword.trim() : '';
  const smtpPassword = envSmtpPass || savedSmtpPass;
  const envFrom = typeof process.env.SMTP_FROM_ADDRESS === 'string' ? process.env.SMTP_FROM_ADDRESS.trim() : '';
  const savedFrom = typeof saved?.smtpFromAddress === 'string' ? saved.smtpFromAddress.trim() : '';
  const fromAddress = envFrom || savedFrom || smtpUsername;

  const syncFolders = normalizeEmailFolderList(saved?.imapSyncFolders, ['INBOX']);
  const archiveFolders = normalizeEmailFolderList(saved?.imapArchiveFolders, ['Archive', 'All Mail']);
  const syncEnabled = saved?.emailSyncEnabled !== false;
  const archiveKnowledgeEnabled = saved?.emailArchiveKnowledgeEnabled !== false;

  return {
    imap: {
      host: imapHost,
      port: imapPort,
      secure: imapSecure,
      username: imapUsername,
      password: imapPassword,
    },
    smtp: {
      host: smtpHost,
      port: smtpPort,
      secure: smtpSecure,
      username: smtpUsername,
      password: smtpPassword,
    },
    fromAddress,
    syncFolders,
    archiveFolders,
    syncEnabled,
    archiveKnowledgeEnabled,
    imapConfigured: Boolean(imapHost && imapUsername && imapPassword),
    smtpConfigured: Boolean(smtpHost && smtpUsername && smtpPassword),
  };
}

function getFirefliesConfig(saved, req = null) {
  const envSecret =
    (typeof process.env.FIREFLIES_SECRET === 'string' ? process.env.FIREFLIES_SECRET.trim() : '') ||
    (typeof process.env.FIREFLIES_WEBHOOK_SECRET === 'string' ? process.env.FIREFLIES_WEBHOOK_SECRET.trim() : '');
  const savedSecret = typeof saved?.firefliesSecret === 'string' ? saved.firefliesSecret.trim() : '';
  const secret = envSecret || savedSecret;
  const secretSource = envSecret ? 'env' : savedSecret ? 'settings' : '';
  const baseUrl = req ? getBaseUrl(req) : getDefaultBaseUrl();

  return {
    configured: Boolean(secret),
    secret,
    secretSource,
    webhookPath: '/api/integrations/fireflies/ingest',
    webhookUrl: `${baseUrl}/api/integrations/fireflies/ingest`,
  };
}

function getParsedAddressRows(field) {
  const rows = Array.isArray(field?.value) ? field.value : [];
  return rows
    .map((row) => ({
      name: typeof row?.name === 'string' ? row.name.trim() : '',
      address: normalizeEmailAddress(row?.address || ''),
    }))
    .filter((row) => row.address);
}

function getFirstParsedAddress(field) {
  const rows = getParsedAddressRows(field);
  return rows[0] || { name: '', address: '' };
}

function getAddressListText(field) {
  return getParsedAddressRows(field).map((row) => row.address).join(', ');
}

function normalizeEmailBodyText(parsed) {
  const raw = typeof parsed?.text === 'string'
    ? parsed.text
    : (typeof parsed?.htmlAsText === 'string' ? parsed.htmlAsText : '');
  const text = normalizeInboxText(raw);
  if (!text) return '';
  return text.length > 20_000 ? `${text.slice(0, 20_000)}\n\n[truncated]` : text;
}

function deriveEmailThreadKey({ subject, parsed, folder, uid }) {
  const msgId = typeof parsed?.messageId === 'string' ? parsed.messageId.trim() : '';
  if (msgId) return msgId.slice(0, 140);
  const inReplyTo = typeof parsed?.inReplyTo === 'string' ? parsed.inReplyTo.trim() : '';
  if (inReplyTo) return inReplyTo.slice(0, 140);
  const refs = Array.isArray(parsed?.references) ? parsed.references.map((x) => String(x || '').trim()).filter(Boolean) : [];
  if (refs.length) return refs[0].slice(0, 140);
  const cleanSubject = String(subject || '').trim().toLowerCase();
  if (cleanSubject) return cleanSubject.slice(0, 140);
  return `${String(folder || '').trim()}:${String(uid || '').trim()}`.slice(0, 140);
}

function makeEmailExternalId({ folder, uid, messageId }) {
  const basis = String(messageId || `${folder}:${uid}` || '').trim();
  return crypto.createHash('sha1').update(basis || makeId()).digest('hex').slice(0, 24);
}

function buildEmailKnowledgeDocument(message, businessKey) {
  const m = message && typeof message === 'object' ? message : {};
  const title = String(m.subject || '').trim() || `Email ${String(m.dateIso || '').trim() || 'message'}`;
  const parts = [
    title ? `Subject: ${title}` : '',
    m.fromAddress ? `From: ${m.fromName ? `${m.fromName} <${m.fromAddress}>` : m.fromAddress}` : '',
    m.toAddresses ? `To: ${m.toAddresses}` : '',
    m.dateIso ? `Date: ${m.dateIso}` : '',
    m.folder ? `Folder: ${m.folder}` : '',
    '',
    String(m.body || '').trim(),
  ].filter(Boolean);
  const folderTag = String(m.folder || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return {
    id: `email-${String(m.externalId || '').trim()}`,
    title,
    text: parts.join('\n'),
    source: 'email-archive',
    businessKey,
    tags: ['email', 'archive'].concat(folderTag ? [folderTag] : []),
    metadata: {
      folder: String(m.folder || '').trim(),
      messageId: String(m.messageId || '').trim(),
      fromAddress: String(m.fromAddress || '').trim(),
      toAddresses: String(m.toAddresses || '').trim(),
      subject: title,
      sentAt: String(m.dateIso || '').trim(),
      externalId: String(m.externalId || '').trim(),
    },
  };
}

async function withImapClient(emailCfg, fn, options = {}) {
  if (!emailCfg?.imapConfigured) throw new Error('IMAP is not configured. Add IMAP host, username, and password first.');
  const timeoutMs = normalizeTimeoutMs(options?.timeoutMs, 20_000);
  const attempts = [];
  let lastError = null;

  for (const profile of buildImapConnectionProfiles(emailCfg)) {
    let emittedError = null;
    const client = new ImapFlow({
      host: profile.host,
      port: profile.port,
      secure: profile.secure,
      ...(typeof profile.doSTARTTLS === 'boolean' ? { doSTARTTLS: profile.doSTARTTLS } : {}),
      auth: profile.auth,
      logger: false,
      connectionTimeout: timeoutMs,
      greetingTimeout: timeoutMs,
      socketTimeout: timeoutMs,
    });

    client.on('error', (err) => {
      emittedError = err || emittedError;
    });

    try {
      await client.connect();
    } catch (err) {
      const msg = String(err?.message || emittedError?.message || 'IMAP connection failed').trim() || 'IMAP connection failed';
      lastError = new Error(msg);
      attempts.push({ ok: false, profile: describeImapProfile(profile), error: msg });
      try {
        client.close();
      } catch {
        // ignore close failures
      }
      continue;
    }

    try {
      const value = await fn(client, profile);
      attempts.push({ ok: true, profile: describeImapProfile(profile) });
      return { value, profile, attempts };
    } catch (err) {
      throw err;
    } finally {
      try {
        await client.logout();
      } catch {
        try {
          client.close();
        } catch {
          // ignore close failures
        }
      }
    }
  }

  if (lastError) {
    lastError.attempts = attempts;
    throw lastError;
  }

  throw new Error('IMAP connection failed');
}

function createSmtpTransport(profile) {
  if (!profile?.host || !profile?.auth?.user || !profile?.auth?.pass) throw new Error('SMTP is not configured. Add SMTP host, username, and password first.');
  const transportOptions = {
    host: profile.host,
    port: profile.port,
    secure: profile.secure,
    auth: profile.auth,
    connectionTimeout: normalizeTimeoutMs(profile.connectionTimeout, 20_000),
    greetingTimeout: normalizeTimeoutMs(profile.greetingTimeout, 20_000),
    socketTimeout: normalizeTimeoutMs(profile.socketTimeout, 20_000),
    ...(profile.requireTLS === true ? { requireTLS: true } : {}),
    ...(profile.ignoreTLS === true ? { ignoreTLS: true } : {}),
  };
  const transport = nodemailer.createTransport(transportOptions);
  transport.on('error', () => {
    // Prevent emitter-level transport errors from escaping the route handler.
  });
  return transport;
}

async function withSmtpTransport(emailCfg, fn, options = {}) {
  if (!emailCfg?.smtpConfigured) throw new Error('SMTP is not configured. Add SMTP host, username, and password first.');
  const timeoutMs = normalizeTimeoutMs(options?.timeoutMs, 20_000);
  const attempts = [];
  let lastError = null;

  for (const profile of buildSmtpConnectionProfiles(emailCfg)) {
    const transport = createSmtpTransport({
      ...profile,
      connectionTimeout: timeoutMs,
      greetingTimeout: timeoutMs,
      socketTimeout: timeoutMs,
    });
    try {
      await transport.verify();
    } catch (err) {
      const msg = String(err?.message || 'SMTP verification failed').trim() || 'SMTP verification failed';
      lastError = new Error(msg);
      attempts.push({ ok: false, profile: describeSmtpProfile(profile), error: msg });
      try {
        transport.close();
      } catch {
        // ignore close failures
      }
      continue;
    }

    try {
      const value = await fn(transport, profile);
      attempts.push({ ok: true, profile: describeSmtpProfile(profile) });
      return { value, profile, attempts };
    } catch (err) {
      throw err;
    } finally {
      try {
        transport.close();
      } catch {
        // ignore close failures
      }
    }
  }

  if (lastError) {
    lastError.attempts = attempts;
    throw lastError;
  }

  throw new Error('SMTP verification failed');
}

async function fetchImapMessages(saved, options = {}) {
  const emailCfg = getEmailConfig(saved);
  const mode = options?.mode === 'archive' ? 'archive' : 'sync';
  const folders = normalizeEmailFolderList(
    options?.folders,
    mode === 'archive' ? emailCfg.archiveFolders : emailCfg.syncFolders,
  );
  if (!folders.length) {
    return { ok: false, error: `No ${mode === 'archive' ? 'archive' : 'sync'} folders are configured.` };
  }

  const limitPerFolderRaw = Number(options?.limitPerFolder);
  const limitPerFolder = Number.isFinite(limitPerFolderRaw)
    ? Math.max(1, Math.min(200, Math.floor(limitPerFolderRaw)))
    : (mode === 'archive' ? 40 : 25);
  const sinceDaysRaw = Number(options?.sinceDays);
  const sinceDays = Number.isFinite(sinceDaysRaw)
    ? Math.max(0, Math.min(3650, Math.floor(sinceDaysRaw)))
    : (mode === 'archive' ? 365 : 30);
  const unseenOnly = options?.unseenOnly === true;
  const sinceCutoffMs = sinceDays > 0 ? (Date.now() - (sinceDays * 24 * 60 * 60 * 1000)) : 0;

  const messages = [];
  const folderErrors = [];

  await withImapClient(emailCfg, async (client) => {
    for (const folder of folders) {
      try {
        const mailbox = await client.mailboxOpen(folder, { readOnly: true });
        const total = Number(mailbox?.exists || 0);
        if (!total) continue;

        const fetchWindow = Math.max(limitPerFolder, Math.min(total, Math.max(limitPerFolder * (unseenOnly ? 8 : 4), limitPerFolder)));
        const seqStart = Math.max(1, total - fetchWindow + 1);
        const folderMessages = [];

        for await (const msg of client.fetch(`${seqStart}:*`, {
          uid: true,
          envelope: true,
          source: true,
          internalDate: true,
          flags: true,
        })) {
          const date = msg?.internalDate instanceof Date ? msg.internalDate : new Date(msg?.internalDate || 0);
          if (sinceCutoffMs && Number.isFinite(date.getTime()) && date.getTime() < sinceCutoffMs) continue;

          const flagList = Array.isArray(msg?.flags)
            ? msg.flags.map((flag) => String(flag || ''))
            : (msg?.flags && typeof msg.flags[Symbol.iterator] === 'function'
              ? Array.from(msg.flags, (flag) => String(flag || ''))
              : []);
          if (unseenOnly && flagList.includes('\\Seen')) continue;
          if (!msg?.source) continue;

          const parsed = await simpleParser(msg.source);
          const subject = String(parsed?.subject || msg?.envelope?.subject || '').trim();
          const body = normalizeEmailBodyText(parsed);
          if (!subject && !body) continue;

          const from = getFirstParsedAddress(parsed?.from);
          const toAddresses = getAddressListText(parsed?.to);
          const messageId = typeof parsed?.messageId === 'string' ? parsed.messageId.trim() : '';
          const dateIso = Number.isFinite(date.getTime()) ? date.toISOString() : nowIso();
          folderMessages.push({
            folder,
            uid: Number(msg?.uid) || 0,
            subject,
            body,
            fromName: from.name,
            fromAddress: from.address,
            toAddresses,
            messageId,
            dateIso,
            externalId: makeEmailExternalId({ folder, uid: msg?.uid, messageId }),
            threadKey: deriveEmailThreadKey({ subject, parsed, folder, uid: msg?.uid }),
          });
        }

        folderMessages.sort((a, b) => String(b.dateIso || '').localeCompare(String(a.dateIso || '')));
        messages.push(...folderMessages.slice(0, limitPerFolder));
      } catch (err) {
        folderErrors.push({ folder, error: String(err?.message || 'Failed to fetch folder') });
      }
    }
  });

  return {
    ok: true,
    mode,
    folders,
    limitPerFolder,
    sinceDays,
    messages,
    folderErrors,
  };
}

function buildInboxTextFromEmailMessage(message) {
  const subject = String(message?.subject || '').trim();
  const body = String(message?.body || '').trim();
  const parts = [subject ? `Subject: ${subject}` : '', body].filter(Boolean);
  return parts.join('\n\n').trim();
}

function getQdrantConfig(saved) {
  const envUrl = normalizeBaseUrl(process.env.QDRANT_URL || process.env.QDRANT_HOST || '');
  const savedUrl = normalizeBaseUrl(saved?.qdrantUrl || '');
  const url = envUrl || savedUrl;

  const envApiKey = typeof process.env.QDRANT_API_KEY === 'string' ? process.env.QDRANT_API_KEY.trim() : '';
  const savedApiKey = typeof saved?.qdrantApiKey === 'string' ? saved.qdrantApiKey.trim() : '';
  const apiKey = envApiKey || savedApiKey;

  const envCollection = typeof process.env.QDRANT_COLLECTION === 'string' ? process.env.QDRANT_COLLECTION.trim() : '';
  const savedCollection = typeof saved?.qdrantCollection === 'string' ? saved.qdrantCollection.trim() : '';
  const collection = envCollection || savedCollection || 'marcus-knowledge';

  const envEmbeddingModel = typeof process.env.QDRANT_EMBEDDING_MODEL === 'string'
    ? process.env.QDRANT_EMBEDDING_MODEL.trim()
    : (typeof process.env.OPENAI_EMBEDDING_MODEL === 'string' ? process.env.OPENAI_EMBEDDING_MODEL.trim() : '');
  const savedEmbeddingModel = typeof saved?.qdrantEmbeddingModel === 'string' ? saved.qdrantEmbeddingModel.trim() : '';
  const embeddingModel = envEmbeddingModel || savedEmbeddingModel || 'text-embedding-3-small';

  const vectorSizeRaw = Number(process.env.QDRANT_VECTOR_SIZE || saved?.qdrantVectorSize);
  const vectorSize = Number.isFinite(vectorSizeRaw) && vectorSizeRaw > 0
    ? Math.floor(vectorSizeRaw)
    : guessEmbeddingVectorSize(embeddingModel);

  const distanceRaw = typeof process.env.QDRANT_DISTANCE === 'string'
    ? process.env.QDRANT_DISTANCE.trim()
    : (typeof saved?.qdrantDistance === 'string' ? saved.qdrantDistance.trim() : '');
  const distance = ['Cosine', 'Dot', 'Euclid', 'Manhattan'].includes(distanceRaw)
    ? distanceRaw
    : 'Cosine';

  const timeoutRaw = Number(process.env.QDRANT_TIMEOUT_MS || saved?.qdrantTimeoutMs);
  const timeoutMs = Number.isFinite(timeoutRaw) && timeoutRaw > 0 ? Math.floor(timeoutRaw) : 15_000;

  const topKRaw = Number(process.env.QDRANT_TOP_K || saved?.qdrantTopK);
  const topK = Number.isFinite(topKRaw) && topKRaw > 0 ? Math.max(1, Math.min(20, Math.floor(topKRaw))) : 6;

  const enabled = saved?.qdrantEnabled !== false;
  const useForMarcus = saved?.qdrantUseForMarcus !== false;
  const configured = Boolean(url && collection);

  return {
    url,
    apiKey,
    apiKeyHint: maskSecretHint(apiKey),
    collection,
    embeddingModel,
    vectorSize,
    distance,
    timeoutMs,
    topK,
    enabled,
    useForMarcus,
    configured,
  };
}

function buildQdrantHeaders(apiKey) {
  const headers = { 'Content-Type': 'application/json' };
  if (typeof apiKey === 'string' && apiKey.trim()) headers['api-key'] = apiKey.trim();
  return headers;
}

async function qdrantRequest(cfg, endpoint, init = {}) {
  if (!cfg?.url) throw new Error('Qdrant URL is not configured');
  const base = cfg.url.replace(/\/+$/g, '');
  const pathPart = String(endpoint || '').startsWith('/') ? String(endpoint || '') : `/${String(endpoint || '')}`;
  const headers = { ...buildQdrantHeaders(cfg.apiKey), ...(init.headers || {}) };
  return fetchJsonWithTimeout(`${base}${pathPart}`, {
    timeoutMs: cfg.timeoutMs || 15_000,
    ...init,
    headers,
  });
}

async function qdrantEnsureCollection(cfg) {
  const describe = await qdrantRequest(cfg, `/collections/${encodeURIComponent(cfg.collection)}`, { method: 'GET' });
  if (describe.resp.ok) {
    return { ok: true, created: false, details: describe.data?.result || describe.data || {} };
  }
  if (describe.resp.status !== 404) {
    const detail = typeof describe.data?.status?.error === 'string'
      ? describe.data.status.error
      : typeof describe.data?.error === 'string'
        ? describe.data.error
        : `status ${describe.resp.status}`;
    return { ok: false, error: `Failed to inspect Qdrant collection: ${detail}` };
  }

  const body = {
    vectors: {
      size: cfg.vectorSize,
      distance: cfg.distance,
    },
  };
  const created = await qdrantRequest(cfg, `/collections/${encodeURIComponent(cfg.collection)}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
  if (!created.resp.ok) {
    const detail = typeof created.data?.status?.error === 'string'
      ? created.data.status.error
      : typeof created.data?.error === 'string'
        ? created.data.error
        : `status ${created.resp.status}`;
    return { ok: false, error: `Failed to create Qdrant collection: ${detail}` };
  }

  return { ok: true, created: true, details: created.data?.result || created.data || {} };
}

async function createOpenAiEmbeddings(saved, texts, options = {}) {
  const input = Array.isArray(texts)
    ? texts.map((item) => String(item || '').trim()).filter(Boolean)
    : [String(texts || '').trim()].filter(Boolean);
  if (!input.length) return { ok: true, embeddings: [] };

  const openai = getOpenAiSecrets(saved);
  if (!openai.apiKey) {
    return { ok: false, error: 'OpenAI API key is required for Qdrant embeddings.' };
  }

  const model = typeof options?.model === 'string' && options.model.trim()
    ? options.model.trim()
    : 'text-embedding-3-small';
  const dimensionsRaw = Number(options?.dimensions);
  const body = {
    model,
    input,
  };
  if (Number.isFinite(dimensionsRaw) && dimensionsRaw > 0) body.dimensions = Math.floor(dimensionsRaw);

  const { resp, data } = await fetchJsonWithTimeout('https://api.openai.com/v1/embeddings', {
    timeoutMs: 30_000,
    method: 'POST',
    headers: {
      Authorization: `Bearer ${openai.apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    const detail = typeof data?.error?.message === 'string' ? data.error.message : `status ${resp.status}`;
    return { ok: false, error: `OpenAI embeddings failed: ${detail}` };
  }

  const rows = Array.isArray(data?.data) ? data.data : [];
  const embeddings = rows.map((row) => Array.isArray(row?.embedding) ? row.embedding : []).filter((row) => row.length);
  if (embeddings.length !== input.length) {
    return { ok: false, error: 'OpenAI embeddings response was incomplete.' };
  }
  return { ok: true, embeddings, model };
}

function qdrantPointPayloadFromDocument(doc, businessKey) {
  const sourceDoc = doc && typeof doc === 'object' ? doc : {};
  const text = typeof sourceDoc.text === 'string'
    ? sourceDoc.text.trim()
    : (typeof sourceDoc.content === 'string' ? sourceDoc.content.trim() : '');
  const title = typeof sourceDoc.title === 'string' ? sourceDoc.title.trim() : '';
  const source = typeof sourceDoc.source === 'string' ? sourceDoc.source.trim() : '';
  const tags = Array.isArray(sourceDoc.tags) ? sourceDoc.tags.map((tag) => String(tag || '').trim()).filter(Boolean).slice(0, 20) : [];
  const metadata = sourceDoc.metadata && typeof sourceDoc.metadata === 'object' && !Array.isArray(sourceDoc.metadata)
    ? sourceDoc.metadata
    : {};
  return {
    title,
    text,
    source,
    tags,
    businessKey: typeof sourceDoc.businessKey === 'string' && sourceDoc.businessKey.trim() ? sourceDoc.businessKey.trim() : businessKey,
    metadata,
    updatedAt: nowIso(),
  };
}

async function qdrantUpsertDocuments(saved, docs, options = {}) {
  const cfg = getQdrantConfig(saved);
  if (!cfg.enabled || !cfg.configured) {
    return { ok: false, error: 'Qdrant is not configured.' };
  }

  const list = Array.isArray(docs) ? docs : [docs];
  const businessKey = typeof options?.businessKey === 'string' ? options.businessKey.trim() : getBusinessKeyFromContext();
  const normalized = list
    .map((doc) => {
      const sourceDoc = doc && typeof doc === 'object' ? doc : {};
      const payload = qdrantPointPayloadFromDocument(sourceDoc, businessKey);
      return {
        id: normalizeQdrantPointId(typeof sourceDoc.id === 'string' && sourceDoc.id.trim() ? sourceDoc.id.trim() : makeId()),
        payload,
      };
    })
    .filter((row) => row.payload.text);

  if (!normalized.length) {
    return { ok: false, error: 'No valid knowledge documents were provided.' };
  }

  const ensured = options?.ensureCollection === false ? { ok: true, created: false } : await qdrantEnsureCollection(cfg);
  if (!ensured.ok) return ensured;

  const embed = await createOpenAiEmbeddings(saved, normalized.map((row) => row.payload.text), {
    model: cfg.embeddingModel,
    dimensions: cfg.vectorSize,
  });
  if (!embed.ok) return embed;

  const points = normalized.map((row, index) => ({
    id: row.id,
    vector: embed.embeddings[index],
    payload: row.payload,
  }));

  const { resp, data } = await qdrantRequest(cfg, `/collections/${encodeURIComponent(cfg.collection)}/points?wait=true`, {
    method: 'PUT',
    body: JSON.stringify({ points }),
  });
  if (!resp.ok) {
    const detail = typeof data?.status?.error === 'string'
      ? data.status.error
      : typeof data?.error === 'string'
        ? data.error
        : `status ${resp.status}`;
    return { ok: false, error: `Qdrant upsert failed: ${detail}` };
  }

  return {
    ok: true,
    collection: cfg.collection,
    count: points.length,
    createdCollection: Boolean(ensured.created),
    result: data?.result || {},
  };
}

function buildQdrantSearchFilter(input) {
  const filter = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const must = [];
  const businessKey = typeof filter.businessKey === 'string' ? filter.businessKey.trim() : '';
  if (businessKey) {
    must.push({ key: 'businessKey', match: { value: businessKey } });
  }
  const source = typeof filter.source === 'string' ? filter.source.trim() : '';
  if (source) {
    must.push({ key: 'source', match: { value: source } });
  }
  if (!must.length) return null;
  return { must };
}

function isQdrantCompatiblePointId(value) {
  if (Number.isInteger(value) && value >= 0) return true;
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return false;
  if (/^\d+$/.test(raw)) return true;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(raw);
}

function toDeterministicUuid(value) {
  const raw = typeof value === 'string' ? value.trim() : String(value ?? '').trim();
  const seed = raw || crypto.randomUUID();
  const hex = crypto.createHash('sha256').update(seed).digest('hex').slice(0, 32).split('');
  hex[12] = '4';
  const variant = parseInt(hex[16], 16);
  hex[16] = ((variant & 0x3) | 0x8).toString(16);
  const joined = hex.join('');
  return `${joined.slice(0, 8)}-${joined.slice(8, 12)}-${joined.slice(12, 16)}-${joined.slice(16, 20)}-${joined.slice(20, 32)}`;
}

function normalizeQdrantPointId(value) {
  if (isQdrantCompatiblePointId(value)) {
    if (typeof value === 'number') return value;
    const raw = String(value).trim();
    return /^\d+$/.test(raw) ? Number(raw) : raw;
  }
  return toDeterministicUuid(value);
}

async function qdrantSearchKnowledge(saved, queryText, options = {}) {
  const cfg = getQdrantConfig(saved);
  if (!cfg.enabled || !cfg.configured) {
    return { ok: false, error: 'Qdrant is not configured.' };
  }

  const text = typeof queryText === 'string' ? queryText.trim() : '';
  if (!text) return { ok: false, error: 'Query text is required.' };

  const embed = await createOpenAiEmbeddings(saved, [text], {
    model: cfg.embeddingModel,
    dimensions: cfg.vectorSize,
  });
  if (!embed.ok) return embed;

  const limitRaw = Number(options?.limit);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.max(1, Math.min(20, Math.floor(limitRaw))) : cfg.topK;
  const filter = buildQdrantSearchFilter(options?.filter);
  const body = {
    vector: embed.embeddings[0],
    limit,
    with_payload: true,
    with_vector: false,
  };
  if (filter) body.filter = filter;

  let out = await qdrantRequest(cfg, `/collections/${encodeURIComponent(cfg.collection)}/points/search`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
  if (out.resp.status === 404) {
    out = await qdrantRequest(cfg, `/collections/${encodeURIComponent(cfg.collection)}/points/query`, {
      method: 'POST',
      body: JSON.stringify({
        query: embed.embeddings[0],
        limit,
        with_payload: true,
        with_vector: false,
        ...(filter ? { filter } : {}),
      }),
    });
  }
  if (!out.resp.ok) {
    const detail = typeof out.data?.status?.error === 'string'
      ? out.data.status.error
      : typeof out.data?.error === 'string'
        ? out.data.error
        : `status ${out.resp.status}`;
    return { ok: false, error: `Qdrant search failed: ${detail}` };
  }

  const rawPoints = Array.isArray(out.data?.result?.points)
    ? out.data.result.points
    : (Array.isArray(out.data?.result) ? out.data.result : []);
  const matches = rawPoints.map((point) => ({
    id: point?.id,
    score: Number(point?.score) || 0,
    payload: point?.payload && typeof point.payload === 'object' ? point.payload : {},
  }));
  return { ok: true, collection: cfg.collection, matches };
}

function resolveAiRoute(saved, routeKey) {
  const openai = getOpenAiSecrets(saved);
  const openrouter = getOpenRouterSecrets(saved);

  const routes = normalizeAiRoutes(saved?.aiRoutes);
  const r = routes?.[routeKey] || { provider: 'openai', model: '' };

  const preferredProvider = normalizeAiProvider(r.provider);
  const fallbackProvider = openai.apiKey ? 'openai' : (openrouter.apiKey ? 'openrouter' : 'openai');
  const provider = preferredProvider || fallbackProvider;

  const providerSecrets = provider === 'openrouter' ? openrouter : openai;
  const defaultModel = provider === 'openrouter' ? openrouter.model : openai.model;
  const model = (typeof r.model === 'string' && r.model.trim()) ? r.model.trim() : defaultModel;

  return { provider, model, apiKey: providerSecrets.apiKey };
}

async function aiChatCompletion({ routeKey, messages, tools, tool_choice, timeoutMs = 30_000 }) {
  const saved = await readSettings();
  const route = resolveAiRoute(saved, routeKey);
  if (!route.apiKey) {
    return { ok: false, error: `AI is not enabled (missing API key for ${route.provider})` };
  }

  const modelLower = String(route.model || '').trim().toLowerCase();
  const requestedTimeoutMs = Number.isFinite(Number(timeoutMs)) ? Math.max(5_000, Number(timeoutMs)) : 30_000;
  let effectiveTimeoutMs = requestedTimeoutMs;
  if (modelLower.startsWith('gpt-5')) {
    effectiveTimeoutMs = Math.max(requestedTimeoutMs, 90_000);
  } else if (modelLower.includes('gpt-4.1') || modelLower.includes('gpt-4o')) {
    effectiveTimeoutMs = Math.max(requestedTimeoutMs, 45_000);
  }

  const baseUrl = route.provider === 'openrouter' ? 'https://openrouter.ai/api/v1' : 'https://api.openai.com/v1';
  const headers = {
    Authorization: `Bearer ${route.apiKey}`,
    'Content-Type': 'application/json',
  };
  if (route.provider === 'openrouter') {
    // Optional but helpful for OpenRouter analytics/compliance.
    headers['HTTP-Referer'] = typeof process.env.OPENROUTER_HTTP_REFERER === 'string' ? process.env.OPENROUTER_HTTP_REFERER.trim() : '';
    headers['X-Title'] = typeof process.env.OPENROUTER_X_TITLE === 'string' ? process.env.OPENROUTER_X_TITLE.trim() : 'M.A.R.C.U.S.';
    if (!headers['HTTP-Referer']) delete headers['HTTP-Referer'];
  }

  const body = {
    model: route.model,
    messages,
  };
  if (Array.isArray(tools) && tools.length) body.tools = tools;
  if (tool_choice) body.tool_choice = tool_choice;

  let resp;
  let data;
  try {
    const out = await fetchJsonWithTimeout(`${baseUrl}/chat/completions`, {
      timeoutMs: effectiveTimeoutMs,
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });
    resp = out.resp;
    data = out.data;
  } catch (err) {
    const msg = String(err?.message || '').toLowerCase();
    const timedOut = msg.includes('timeout') || msg.includes('aborted');
    if (timedOut) {
      return {
        ok: false,
        error: `AI request timed out after ${Math.round(effectiveTimeoutMs / 1000)}s. provider=${route.provider}. model=${route.model}. Try again or use a faster model for this route.`,
      };
    }
    return {
      ok: false,
      error: `AI request failed before response. provider=${route.provider}. model=${route.model}. ${String(err?.message || 'unknown error')}`.slice(0, 700),
    };
  }

  if (!resp.ok) {
    const detail = typeof data?.error?.message === 'string' ? data.error.message : JSON.stringify(data);
    return { ok: false, error: `AI request failed (${resp.status}). provider=${route.provider}. model=${route.model}. ${detail}`.slice(0, 700) };
  }

  const msg = data?.choices?.[0]?.message;
  if (!msg) return { ok: false, error: 'AI returned no message' };
  return { ok: true, provider: route.provider, model: route.model, message: msg };
}

async function fetchJsonWithTimeout(url, { timeoutMs = 25_000, ...init } = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);
  try {
    const resp = await fetch(url, { ...init, signal: controller.signal });
    const data = await resp.json().catch(() => ({}));
    return { resp, data };
  } finally {
    clearTimeout(timeoutId);
  }
}

function sanitizeSettingsForClient(settings) {
  if (!settings || typeof settings !== 'object') return {};
  const clone = { ...settings };
  // Never send secrets/tokens to the browser.
  delete clone.openaiApiKey;
  delete clone.openrouterApiKey;
  delete clone.googleClientSecret;
  delete clone.googleTokens;
  delete clone.ga4ServiceAccountJson;
  delete clone.firefliesSecret;
  delete clone.crmApiKey;
  delete clone.crmWebhookSecret;
  delete clone.slackSigningSecret;
  delete clone.slackClientSecret;
  delete clone.slackBotToken;
  delete clone.quoAuthToken;
  delete clone.ghlApiKey;
  delete clone.airtableByBusinessKey;
  delete clone.airtablePat;
  delete clone.qdrantApiKey;
  delete clone.imapPassword;
  delete clone.smtpPassword;
  return clone;
}

function tryParseDriveFolderId(input) {
  const s = typeof input === 'string' ? input.trim() : '';
  if (!s) return '';
  // Common patterns: https://drive.google.com/drive/folders/<id> or ...?id=<id>
  const m1 = s.match(/\/drive\/folders\/([a-zA-Z0-9_-]{10,})/);
  if (m1) return m1[1];
  const m2 = s.match(/[?&]id=([a-zA-Z0-9_-]{10,})/);
  if (m2) return m2[1];
  // If user pasted a raw id
  if (/^[a-zA-Z0-9_-]{10,}$/.test(s)) return s;
  return '';
}

function driveFolderUrlFromId(id) {
  const s = typeof id === 'string' ? id.trim() : '';
  if (!s) return '';
  return `https://drive.google.com/drive/folders/${s}`;
}

async function getCrmConfig() {
  const saved = await readSettings();
  const apiBaseUrl = typeof saved.crmApiBaseUrl === 'string' ? saved.crmApiBaseUrl.trim() : '';
  const apiKey = (typeof process.env.CRM_API_KEY === 'string' ? process.env.CRM_API_KEY.trim() : '') || (typeof saved.crmApiKey === 'string' ? saved.crmApiKey.trim() : '');
  const webhookSecret = (typeof process.env.CRM_WEBHOOK_SECRET === 'string' ? process.env.CRM_WEBHOOK_SECRET.trim() : '') || (typeof saved.crmWebhookSecret === 'string' ? saved.crmWebhookSecret.trim() : '');
  return { apiBaseUrl, apiKey, webhookSecret, saved };
}

function tryParseJson(raw) {
  if (typeof raw !== 'string') return null;
  const s = raw.trim();
  if (!s) return null;
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

async function getGa4Config() {
  const saved = await readSettings();
  const envPropertyId = typeof process.env.GA4_PROPERTY_ID === 'string' ? process.env.GA4_PROPERTY_ID.trim() : '';
  const savedPropertyId = typeof saved.ga4PropertyId === 'string' ? saved.ga4PropertyId.trim() : '';
  const propertyId = envPropertyId || savedPropertyId;

  const envServiceAccountJson = typeof process.env.GA4_SERVICE_ACCOUNT_JSON === 'string' ? process.env.GA4_SERVICE_ACCOUNT_JSON.trim() : '';
  const savedServiceAccountJson = typeof saved.ga4ServiceAccountJson === 'string' ? saved.ga4ServiceAccountJson.trim() : '';
  const serviceAccountJson = envServiceAccountJson || savedServiceAccountJson;
  const parsed = tryParseJson(serviceAccountJson);

  const clientEmail = typeof parsed?.client_email === 'string' ? parsed.client_email.trim() : '';
  const privateKey = typeof parsed?.private_key === 'string' ? parsed.private_key : '';

  const { tokens } = await getGoogleOAuthConfig();
  const googleConnected = Boolean(tokens && typeof tokens === 'object' && tokens.refresh_token);
  const googleScope = googleConnected ? String(tokens.scope || '') : '';
  const googleHasAnalyticsScope = googleConnected ? googleScope.includes('https://www.googleapis.com/auth/analytics.readonly') || googleScope.includes('analytics.readonly') : false;

  return { propertyId, clientEmail, privateKey, googleConnected, googleHasAnalyticsScope, saved };
}

function ga4IsoDate(d) {
  const dt = d instanceof Date ? d : new Date();
  return dt.toISOString().slice(0, 10);
}

function ga4YesterdayIsoDate() {
  return ga4IsoDate(new Date(Date.now() - 24 * 60 * 60 * 1000));
}

function ga4ToInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.floor(n));
}

async function ga4RunDailyReport({ propertyId, clientEmail, privateKey, date }) {
  const auth = new google.auth.JWT({
    email: clientEmail,
    key: privateKey,
    scopes: ['https://www.googleapis.com/auth/analytics.readonly'],
  });
  const analyticsdata = google.analyticsdata({ version: 'v1beta', auth });

  const resp = await analyticsdata.properties.runReport({
    property: `properties/${propertyId}`,
    requestBody: {
      dateRanges: [{ startDate: date, endDate: date }],
      metrics: [{ name: 'sessions' }, { name: 'totalUsers' }],
    },
  });

  const row = Array.isArray(resp?.data?.rows) ? resp.data.rows[0] : null;
  const metricValues = Array.isArray(row?.metricValues) ? row.metricValues : [];
  const sessions = ga4ToInt(metricValues?.[0]?.value);
  const users = ga4ToInt(metricValues?.[1]?.value);
  return { sessions, users };
}

async function ga4RunDailyReportOAuth({ req, propertyId, date }) {
  const { clientId, clientSecret, tokens, saved } = await getGoogleOAuthConfig();
  if (!clientId || !isLikelyGoogleClientId(clientId)) throw new Error('Google OAuth client is not configured');
  if (!tokens || !tokens.refresh_token) throw new Error('Google is not connected');

  const redirectBase = req ? getBaseUrl(req) : getDefaultBaseUrl();
  const redirectUri = `${redirectBase}/api/integrations/google/callback`;

  const fresh = await ensureFreshGoogleTokens({ clientId, clientSecret, tokens, saved });
  const oauth2 = buildOAuthClient({ clientId, clientSecret: clientSecret || '', redirectUri });
  oauth2.setCredentials(fresh.tokens);

  const analyticsdata = google.analyticsdata({ version: 'v1beta', auth: oauth2 });
  const resp = await analyticsdata.properties.runReport({
    property: `properties/${propertyId}`,
    requestBody: {
      dateRanges: [{ startDate: date, endDate: date }],
      metrics: [{ name: 'sessions' }, { name: 'totalUsers' }],
    },
  });

  const row = Array.isArray(resp?.data?.rows) ? resp.data.rows[0] : null;
  const metricValues = Array.isArray(row?.metricValues) ? row.metricValues : [];
  const sessions = ga4ToInt(metricValues?.[0]?.value);
  const users = ga4ToInt(metricValues?.[1]?.value);
  return { sessions, users };
}

let ga4PullRunning = false;
async function runGa4DailySummary({ force = false, req = null } = {}) {
  if (ga4PullRunning) return { ok: true, skipped: true, reason: 'Already running' };
  ga4PullRunning = true;
  try {
    const { propertyId, clientEmail, privateKey, googleConnected, googleHasAnalyticsScope, saved } = await getGa4Config();
    const serviceAccountReady = Boolean(propertyId && clientEmail && privateKey);
    const oauthReady = Boolean(propertyId && googleConnected && googleHasAnalyticsScope);
    if (!serviceAccountReady && !oauthReady) {
      if (!propertyId) return { ok: true, skipped: true, reason: 'GA4 property not set' };
      if (!googleConnected) return { ok: true, skipped: true, reason: 'Google not connected' };
      if (!googleHasAnalyticsScope) return { ok: true, skipped: true, reason: 'Google connected without GA4 scope (reconnect)' };
      return { ok: true, skipped: true, reason: 'GA4 not configured' };
    }

    const date = ga4YesterdayIsoDate();
    const last = typeof saved.ga4LastDailySummaryDate === 'string' ? saved.ga4LastDailySummaryDate.trim() : '';
    if (!force && last === date) return { ok: true, skipped: true, reason: 'Already summarized' };

    const { sessions, users } = serviceAccountReady
      ? await ga4RunDailyReport({ propertyId, clientEmail, privateKey, date })
      : await ga4RunDailyReportOAuth({ req, propertyId, date });
    const lines = [];
    lines.push(`📈 GA4 Daily Summary (${date})`);
    lines.push(`Property: ${propertyId}`);
    lines.push(`Sessions: ${sessions}`);
    lines.push(`Users: ${users}`);

    const inbox = await addInboxIntegrationItem({
      source: 'ga4',
      externalId: `daily:${propertyId}:${date}`,
      text: lines.join('\n'),
      channel: 'ga4',
    });

    const next = {
      ...saved,
      ga4LastDailySummaryDate: date,
      ga4LastDailySummaryAt: nowIso(),
      ga4LastDailySummaryError: '',
      updatedAt: nowIso(),
    };
    await writeSettings(next);
    return {
      ok: true,
      skipped: false,
      date,
      sessions,
      users,
      inboxCreated: Boolean(inbox?.created),
      inboxId: typeof inbox?.id === 'string' ? inbox.id : '',
    };
  } catch (err) {
    try {
      const saved = await readSettings();
      const next = {
        ...saved,
        ga4LastDailySummaryAt: nowIso(),
        ga4LastDailySummaryError: err?.message || 'GA4 pull failed',
        updatedAt: nowIso(),
      };
      await writeSettings(next);
    } catch {
      // ignore
    }
    return { ok: false, error: err?.message || 'GA4 pull failed' };
  } finally {
    ga4PullRunning = false;
  }
}

function startGa4Scheduler() {
  if (!GA4_PULL_INTERVAL_MS) return;
  setTimeout(() => {
    runGa4DailySummary().catch(() => {
      // best-effort
    });
  }, 5_000);

  setInterval(() => {
    runGa4DailySummary().catch(() => {
      // best-effort
    });
  }, GA4_PULL_INTERVAL_MS);
}

function safeTimingEqual(a, b) {
  const aBuf = Buffer.from(String(a || ''), 'utf8');
  const bBuf = Buffer.from(String(b || ''), 'utf8');
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function getRawBodyString(req) {
  const buf = req && req.rawBody instanceof Buffer ? req.rawBody : null;
  if (!buf) return '';
  try {
    return buf.toString('utf8');
  } catch {
    return '';
  }
}

function matchProjectFromText(store, text) {
  const s = String(text || '').toLowerCase();
  if (!s) return null;
  const projects = Array.isArray(store?.projects) ? store.projects : [];
  // Prefer longer names first to reduce false positives.
  const sorted = [...projects].sort((a, b) => String(b?.name || '').length - String(a?.name || '').length);
  for (const p of sorted) {
    const name = String(p?.name || '').trim();
    if (!name) continue;
    if (s.includes(name.toLowerCase())) return p;
  }
  return null;
}

function normalizePhoneForLookup(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const clean = raw.replace(/[^\d+]/g, '');
  if (!clean) return '';
  // Normalize to digit-only for matching across formatting styles.
  return clean.replace(/[^\d]/g, '');
}

function phoneLookupKeys(value) {
  const digits = normalizePhoneForLookup(value);
  if (!digits) return [];
  const keys = [digits];
  if (digits.length > 10) keys.push(digits.slice(-10));
  return Array.from(new Set(keys.filter(Boolean)));
}

function senderLookupKeys(value) {
  const raw = String(value || '').trim();
  const keys = [];
  if (raw) keys.push(raw);
  const digits = normalizePhoneForLookup(raw);
  if (digits) {
    keys.push(digits);
    if (digits.length > 10) keys.push(digits.slice(-10));
  }
  return Array.from(new Set(keys.filter(Boolean)));
}

function resolveSenderProjectMapping(store, senderValue) {
  const s = store && typeof store === 'object' ? store : {};
  const map = s.senderProjectMap && typeof s.senderProjectMap === 'object' ? s.senderProjectMap : {};
  const keys = senderLookupKeys(senderValue);
  for (const k of keys) {
    const v = map[k];
    if (!v) continue;
    if (typeof v === 'string') {
      const pid = v.trim();
      if (!pid) continue;
      const project = (Array.isArray(s.projects) ? s.projects : []).find((p) => String(p?.id || '') === pid) || null;
      return { projectId: pid, projectName: project ? String(project.name || '').trim() : '' };
    }
    if (v && typeof v === 'object') {
      const pid = String(v.projectId || '').trim();
      const pnm = String(v.projectName || '').trim();
      if (pid) return { projectId: pid, projectName: pnm };
    }
  }
  return null;
}

function upsertSenderProjectMapForProject(senderProjectMap, senderValue, project) {
  const map = senderProjectMap && typeof senderProjectMap === 'object' ? senderProjectMap : {};
  const pid = String(project?.id || '').trim();
  if (!pid) return map;
  const keys = senderLookupKeys(senderValue);
  for (const k of keys) {
    map[k] = pid;
  }
  return map;
}

function normalizeProjectRecord(input, { updatedAt } = {}) {
  const existing = input && typeof input === 'object' ? input : {};
  const normalized = normalizeProject(existing);
  const createdAt = typeof existing.createdAt === 'string' && existing.createdAt.trim() ? existing.createdAt.trim() : nowIso();
  const nextUpdatedAt = typeof updatedAt === 'string' && updatedAt.trim()
    ? updatedAt.trim()
    : (typeof existing.updatedAt === 'string' && existing.updatedAt.trim() ? existing.updatedAt.trim() : createdAt);
  return {
    ...existing,
    ...normalized,
    id: typeof existing.id === 'string' && existing.id.trim() ? existing.id.trim() : makeId(),
    createdAt,
    updatedAt: nextUpdatedAt,
  };
}

function getSenderProjectIdFromMappingValue(value) {
  if (typeof value === 'string') return value.trim();
  if (value && typeof value === 'object') return String(value.projectId || '').trim();
  return '';
}

function pickSenderProjectMapEntriesForProjectIds(senderProjectMap, projectIdsInput) {
  const map = senderProjectMap && typeof senderProjectMap === 'object' ? senderProjectMap : {};
  const ids = projectIdsInput instanceof Set ? projectIdsInput : new Set(Array.isArray(projectIdsInput) ? projectIdsInput : []);
  const out = {};
  for (const [key, value] of Object.entries(map)) {
    if (ids.has(getSenderProjectIdFromMappingValue(value))) out[key] = value;
  }
  return out;
}

function omitSenderProjectMapEntriesForProjectIds(senderProjectMap, projectIdsInput) {
  const map = senderProjectMap && typeof senderProjectMap === 'object' ? senderProjectMap : {};
  const ids = projectIdsInput instanceof Set ? projectIdsInput : new Set(Array.isArray(projectIdsInput) ? projectIdsInput : []);
  const out = {};
  for (const [key, value] of Object.entries(map)) {
    if (ids.has(getSenderProjectIdFromMappingValue(value))) continue;
    out[key] = value;
  }
  return out;
}

async function moveProjectsBetweenBusinesses({ sourceBusinessKey, destinationBusinessKey, projectIds, baseRevision }) {
  const sourceKey = normalizeBusinessKey(sourceBusinessKey) || DEFAULT_BUSINESS_KEY;
  const destinationKey = normalizeBusinessKey(destinationBusinessKey) || '';
  const ids = Array.from(new Set((Array.isArray(projectIds) ? projectIds : []).map((v) => String(v || '').trim()).filter(Boolean)));

  if (!ids.length) {
    const store = await readStoreForBusiness(sourceKey);
    return { movedProjects: [], sourceStore: store, destinationStore: await readStoreForBusiness(destinationKey) };
  }

  const settings = await readSettings();
  const cfg = getBusinessConfigFromSettings(settings);
  const destinationBusiness = (Array.isArray(cfg.businesses) ? cfg.businesses : []).find((b) => normalizeBusinessKey(b?.key || '') === destinationKey);
  if (!destinationBusiness) {
    const err = new Error('Destination business not found');
    err.statusCode = 404;
    throw err;
  }

  const sourceStore = await readStoreForBusiness(sourceKey);
  if (Number.isFinite(baseRevision) && baseRevision !== sourceStore.revision) {
    const err = new Error('Revision mismatch. Reload and try again.');
    err.statusCode = 409;
    err.currentRevision = sourceStore.revision;
    throw err;
  }

  const sourceProjects = Array.isArray(sourceStore.projects) ? sourceStore.projects : [];
  const missing = ids.filter((id) => !sourceProjects.some((p) => p.id === id));
  if (missing.length) {
    const err = new Error(`Project not found: ${missing.slice(0, 3).join(', ')}${missing.length > 3 ? '…' : ''}`);
    err.statusCode = 404;
    throw err;
  }

  const movedProjectsRaw = sourceProjects.filter((p) => ids.includes(p.id));
  const movedProjectIds = new Set(movedProjectsRaw.map((p) => p.id));
  const movedNameKeys = new Set(movedProjectsRaw.map((p) => normKey(p?.name)));

  const destinationStore = await readStoreForBusiness(destinationKey);
  const destinationProjects = Array.isArray(destinationStore.projects) ? destinationStore.projects : [];
  const conflictingProjects = destinationProjects.filter((p) => movedProjectIds.has(p.id) || movedNameKeys.has(normKey(p?.name)));
  if (conflictingProjects.length) {
    const labels = conflictingProjects.slice(0, 3).map((p) => String(p?.name || p?.id || 'project').trim()).filter(Boolean);
    const err = new Error(`Destination business already has: ${labels.join(', ')}${conflictingProjects.length > 3 ? '…' : ''}`);
    err.statusCode = 409;
    throw err;
  }

  const ts = nowIso();
  const movedProjects = movedProjectsRaw.map((project) => normalizeProjectRecord(project, { updatedAt: ts }));
  const nextSourceProjects = sourceProjects.filter((p) => !movedProjectIds.has(p.id));
  const sourceTasks = Array.isArray(sourceStore.tasks) ? sourceStore.tasks : [];
  const movedTasks = sourceTasks.filter((t) => movedNameKeys.has(normKey(t?.project)));
  const keptSourceTasks = sourceTasks.filter((t) => !movedNameKeys.has(normKey(t?.project)));

  const sourceScratchpads = sourceStore.projectScratchpads && typeof sourceStore.projectScratchpads === 'object' ? sourceStore.projectScratchpads : {};
  const sourceNoteEntries = sourceStore.projectNoteEntries && typeof sourceStore.projectNoteEntries === 'object' ? sourceStore.projectNoteEntries : {};
  const sourceChats = sourceStore.projectChats && typeof sourceStore.projectChats === 'object' ? sourceStore.projectChats : {};
  const sourceCommunications = sourceStore.projectCommunications && typeof sourceStore.projectCommunications === 'object' ? sourceStore.projectCommunications : {};
  const sourceTranscriptUndo = sourceStore.projectTranscriptUndo && typeof sourceStore.projectTranscriptUndo === 'object' ? sourceStore.projectTranscriptUndo : {};
  const sourceProjectNotes = sourceStore.projectNotes && typeof sourceStore.projectNotes === 'object' ? sourceStore.projectNotes : {};
  const sourceMarcusNotes = sourceStore.marcusNotes && typeof sourceStore.marcusNotes === 'object' ? sourceStore.marcusNotes : {};

  const nextSourceScratchpads = { ...sourceScratchpads };
  const nextSourceNoteEntries = { ...sourceNoteEntries };
  const nextSourceChats = { ...sourceChats };
  const nextSourceCommunications = { ...sourceCommunications };
  const nextSourceTranscriptUndo = { ...sourceTranscriptUndo };
  const nextSourceProjectNotes = { ...sourceProjectNotes };
  const nextSourceMarcusNotes = { ...sourceMarcusNotes };

  const movedScratchpads = {};
  const movedNoteEntries = {};
  const movedChats = {};
  const movedCommunications = {};
  const movedTranscriptUndo = {};
  const movedProjectNotes = {};
  const movedMarcusNotes = {};

  for (const project of movedProjectsRaw) {
    const projectId = String(project?.id || '').trim();
    if (!projectId) continue;
    if (Object.prototype.hasOwnProperty.call(nextSourceScratchpads, projectId)) {
      movedScratchpads[projectId] = nextSourceScratchpads[projectId];
      delete nextSourceScratchpads[projectId];
    }
    if (Object.prototype.hasOwnProperty.call(nextSourceNoteEntries, projectId)) {
      movedNoteEntries[projectId] = nextSourceNoteEntries[projectId];
      delete nextSourceNoteEntries[projectId];
    }
    if (Object.prototype.hasOwnProperty.call(nextSourceChats, projectId)) {
      movedChats[projectId] = nextSourceChats[projectId];
      delete nextSourceChats[projectId];
    }
    if (Object.prototype.hasOwnProperty.call(nextSourceCommunications, projectId)) {
      movedCommunications[projectId] = nextSourceCommunications[projectId];
      delete nextSourceCommunications[projectId];
    }
    if (Object.prototype.hasOwnProperty.call(nextSourceTranscriptUndo, projectId)) {
      movedTranscriptUndo[projectId] = nextSourceTranscriptUndo[projectId];
      delete nextSourceTranscriptUndo[projectId];
    }
    if (Object.prototype.hasOwnProperty.call(nextSourceMarcusNotes, projectId)) {
      movedMarcusNotes[projectId] = nextSourceMarcusNotes[projectId];
      delete nextSourceMarcusNotes[projectId];
    }

    const projectNoteKey = Object.keys(nextSourceProjectNotes).find((key) => normKey(key) === normKey(project?.name));
    if (projectNoteKey) {
      movedProjectNotes[String(project?.name || '').trim()] = nextSourceProjectNotes[projectNoteKey];
      delete nextSourceProjectNotes[projectNoteKey];
    }
  }

  const nextDestinationScratchpads = {
    ...(destinationStore.projectScratchpads && typeof destinationStore.projectScratchpads === 'object' ? destinationStore.projectScratchpads : {}),
    ...movedScratchpads,
  };
  const nextDestinationNoteEntries = {
    ...(destinationStore.projectNoteEntries && typeof destinationStore.projectNoteEntries === 'object' ? destinationStore.projectNoteEntries : {}),
    ...movedNoteEntries,
  };
  const nextDestinationChats = {
    ...(destinationStore.projectChats && typeof destinationStore.projectChats === 'object' ? destinationStore.projectChats : {}),
    ...movedChats,
  };
  const nextDestinationCommunications = {
    ...(destinationStore.projectCommunications && typeof destinationStore.projectCommunications === 'object' ? destinationStore.projectCommunications : {}),
    ...movedCommunications,
  };
  const nextDestinationTranscriptUndo = {
    ...(destinationStore.projectTranscriptUndo && typeof destinationStore.projectTranscriptUndo === 'object' ? destinationStore.projectTranscriptUndo : {}),
    ...movedTranscriptUndo,
  };
  const nextDestinationProjectNotes = {
    ...(destinationStore.projectNotes && typeof destinationStore.projectNotes === 'object' ? destinationStore.projectNotes : {}),
    ...movedProjectNotes,
  };
  const nextDestinationMarcusNotes = {
    ...(destinationStore.marcusNotes && typeof destinationStore.marcusNotes === 'object' ? destinationStore.marcusNotes : {}),
    ...movedMarcusNotes,
  };

  const movedSenderProjectMap = pickSenderProjectMapEntriesForProjectIds(sourceStore.senderProjectMap, movedProjectIds);
  const nextSourceSenderProjectMap = omitSenderProjectMapEntriesForProjectIds(sourceStore.senderProjectMap, movedProjectIds);
  let nextDestinationSenderProjectMap = {
    ...(destinationStore.senderProjectMap && typeof destinationStore.senderProjectMap === 'object' ? destinationStore.senderProjectMap : {}),
    ...movedSenderProjectMap,
  };
  for (const project of movedProjects) {
    if (!project?.clientPhone) continue;
    nextDestinationSenderProjectMap = upsertSenderProjectMapForProject(nextDestinationSenderProjectMap, project.clientPhone, project);
  }

  const nextSourceStore = {
    ...sourceStore,
    revision: sourceStore.revision + 1,
    updatedAt: ts,
    projects: nextSourceProjects,
    tasks: keptSourceTasks,
    senderProjectMap: nextSourceSenderProjectMap,
    projectScratchpads: nextSourceScratchpads,
    projectNoteEntries: nextSourceNoteEntries,
    projectChats: nextSourceChats,
    projectCommunications: nextSourceCommunications,
    projectTranscriptUndo: nextSourceTranscriptUndo,
    projectNotes: nextSourceProjectNotes,
    marcusNotes: nextSourceMarcusNotes,
  };

  const nextDestinationStore = {
    ...destinationStore,
    revision: destinationStore.revision + 1,
    updatedAt: ts,
    projects: [...movedProjects, ...destinationProjects],
    tasks: [...movedTasks, ...(Array.isArray(destinationStore.tasks) ? destinationStore.tasks : [])],
    senderProjectMap: nextDestinationSenderProjectMap,
    projectScratchpads: nextDestinationScratchpads,
    projectNoteEntries: nextDestinationNoteEntries,
    projectChats: nextDestinationChats,
    projectCommunications: nextDestinationCommunications,
    projectTranscriptUndo: nextDestinationTranscriptUndo,
    projectNotes: nextDestinationProjectNotes,
    marcusNotes: nextDestinationMarcusNotes,
  };

  try {
    await writeStoreForBusiness(destinationKey, nextDestinationStore);
    try {
      await writeStoreForBusiness(sourceKey, nextSourceStore);
    } catch (err) {
      try {
        await writeStoreForBusiness(destinationKey, destinationStore);
      } catch {
        // best-effort rollback
      }
      throw err;
    }
  } catch (err) {
    const failure = new Error(err?.message || 'Failed to move projects');
    failure.statusCode = Number(err?.statusCode) || 500;
    throw failure;
  }

  return {
    movedProjects,
    movedProjectIds: movedProjects.map((p) => p.id),
    sourceStore: nextSourceStore,
    destinationStore: nextDestinationStore,
    destinationBusiness,
  };
}

function repairProjectsMissingIds(storeInput) {
  const store = storeInput && typeof storeInput === 'object' ? storeInput : structuredClone(EMPTY_STORE);
  const projects = Array.isArray(store.projects) ? store.projects : [];
  const missingProjects = projects.filter((project) => !String(project?.id || '').trim());
  if (!missingProjects.length) {
    return { changed: false, store, repairedCount: 0 };
  }

  const ts = nowIso();
  const usedIds = new Set(projects.map((project) => String(project?.id || '').trim()).filter(Boolean));
  const senderMap = store.senderProjectMap && typeof store.senderProjectMap === 'object' ? { ...store.senderProjectMap } : {};
  const phoneIdCandidates = new Map();
  for (const [senderKey, value] of Object.entries(senderMap)) {
    const projectId = getSenderProjectIdFromMappingValue(value);
    if (!projectId) continue;
    if (!phoneIdCandidates.has(senderKey)) phoneIdCandidates.set(senderKey, []);
    phoneIdCandidates.get(senderKey).push(projectId);
  }

  const orphanIdSet = new Set();
  const collectKeys = (obj) => {
    const source = obj && typeof obj === 'object' ? obj : {};
    for (const key of Object.keys(source)) {
      const trimmed = String(key || '').trim();
      if (!trimmed || usedIds.has(trimmed)) continue;
      orphanIdSet.add(trimmed);
    }
  };
  collectKeys(store.projectScratchpads);
  collectKeys(store.projectNoteEntries);
  collectKeys(store.projectChats);
  collectKeys(store.projectCommunications);
  collectKeys(store.projectTranscriptUndo);
  collectKeys(store.marcusNotes);
  for (const value of Object.values(senderMap)) {
    const mappedId = getSenderProjectIdFromMappingValue(value);
    if (mappedId && !usedIds.has(mappedId)) orphanIdSet.add(mappedId);
  }
  const orphanIds = Array.from(orphanIdSet);

  const repairs = [];
  const consumeOrphanId = (candidateId) => {
    const idx = orphanIds.indexOf(candidateId);
    if (idx >= 0) orphanIds.splice(idx, 1);
  };

  for (let index = 0; index < missingProjects.length; index++) {
    const project = missingProjects[index];
    let recoveredId = '';

    const phoneKeys = senderLookupKeys(project?.clientPhone || '');
    for (const key of phoneKeys) {
      const candidates = phoneIdCandidates.get(key) || [];
      const match = candidates.find((candidateId) => candidateId && !usedIds.has(candidateId));
      if (match) {
        recoveredId = match;
        break;
      }
    }

    const remainingMissing = missingProjects.length - index;
    if (!recoveredId && orphanIds.length === remainingMissing) {
      recoveredId = orphanIds[0];
    }
    if (!recoveredId && remainingMissing === 1 && orphanIds.length) {
      recoveredId = orphanIds[0];
    }
    if (!recoveredId) {
      recoveredId = makeId();
    }

    consumeOrphanId(recoveredId);
    usedIds.add(recoveredId);
    repairs.push({ project, recoveredId });
  }

  const repairedProjects = projects.map((project) => {
    const repair = repairs.find((item) => item.project === project);
    if (!repair) return project;
    return normalizeProjectRecord({ ...project, id: repair.recoveredId }, { updatedAt: project?.updatedAt || ts });
  });

  const remapKeyedObject = (input) => {
    const source = input && typeof input === 'object' ? input : {};
    const next = { ...source };
    for (const repair of repairs) {
      const oldId = String(repair.project?.id || '').trim();
      const newId = repair.recoveredId;
      if (!oldId || !Object.prototype.hasOwnProperty.call(next, oldId) || oldId === newId) continue;
      next[newId] = next[oldId];
      delete next[oldId];
    }
    return next;
  };

  const nextSenderProjectMap = {};
  const repairByOldId = new Map(repairs.map((repair) => [String(repair.project?.id || '').trim(), repair.recoveredId]));
  for (const [senderKey, value] of Object.entries(senderMap)) {
    if (typeof value === 'string') {
      const updatedId = repairByOldId.get(value) || value;
      nextSenderProjectMap[senderKey] = updatedId;
      continue;
    }
    if (value && typeof value === 'object') {
      const currentId = String(value.projectId || '').trim();
      const updatedId = repairByOldId.get(currentId) || currentId;
      nextSenderProjectMap[senderKey] = { ...value, projectId: updatedId };
      continue;
    }
    nextSenderProjectMap[senderKey] = value;
  }

  return {
    changed: true,
    repairedCount: repairs.length,
    store: {
      ...store,
      updatedAt: ts,
      projects: repairedProjects,
      senderProjectMap: nextSenderProjectMap,
      projectScratchpads: remapKeyedObject(store.projectScratchpads),
      projectNoteEntries: remapKeyedObject(store.projectNoteEntries),
      projectChats: remapKeyedObject(store.projectChats),
      projectCommunications: remapKeyedObject(store.projectCommunications),
      projectTranscriptUndo: remapKeyedObject(store.projectTranscriptUndo),
      marcusNotes: remapKeyedObject(store.marcusNotes),
    },
  };
}

function previewTextServer(text, maxLen = 140) {
  const s = String(text || '').replace(/\s+/g, ' ').trim();
  if (!s) return '';
  if (s.length <= maxLen) return s;
  return `${s.slice(0, Math.max(0, maxLen - 1)).trimEnd()}…`;
}

function summarizeRadarGroupText(texts) {
  const list = Array.isArray(texts) ? texts.map((t) => String(t || '')).filter(Boolean) : [];
  if (!list.length) return '';

  // Return actual readable previews joined by separator, not keyword extraction
  const previews = list.slice(0, 3).map((t) => previewTextServer(t, 80)).filter(Boolean);
  return previews.join(' · ');
}

function businessKeyFromLabel(label) {
  const text = String(label || '').trim().toLowerCase();
  const key = text.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return key || 'unmapped-legacy';
}

function getPhoneBusinessMap(settings) {
  const raw = settings && typeof settings === 'object' ? settings.phoneBusinessMap : null;
  const out = {};
  if (!raw || typeof raw !== 'object') return out;

  if (Array.isArray(raw)) {
    for (const row of raw) {
      const phone = normalizePhoneForLookup(row?.phone || row?.number || row?.to || '');
      const label = String(row?.business || row?.label || row?.name || '').trim();
      if (!phone || !label) continue;
      out[phone] = label;
      if (phone.length > 10) out[phone.slice(-10)] = label;
    }
    return out;
  }

  for (const [phoneRaw, labelRaw] of Object.entries(raw)) {
    const phone = normalizePhoneForLookup(phoneRaw);
    const label = String(labelRaw || '').trim();
    if (!phone || !label) continue;
    out[phone] = label;
    if (phone.length > 10) out[phone.slice(-10)] = label;
  }
  return out;
}

function resolveBusinessForInbound({ settings, toNumber }) {
  const keys = phoneLookupKeys(toNumber);
  if (keys.length) {
    const cfg = getBusinessConfigFromSettings(settings);
    for (const b of (Array.isArray(cfg.businesses) ? cfg.businesses : [])) {
      const nums = Array.isArray(b?.phoneNumbers) ? b.phoneNumbers : [];
      for (const n of nums) {
        const nk = phoneLookupKeys(n);
        if (!nk.length) continue;
        if (nk.some((k) => keys.includes(k))) {
          return { businessKey: normalizeBusinessKey(b?.key || '') || businessKeyFromLabel(b?.name || ''), businessLabel: String(b?.name || '').trim() || 'Business' };
        }
      }
    }
  }

  const map = getPhoneBusinessMap(settings);
  for (const k of keys) {
    const label = String(map[k] || '').trim();
    if (!label) continue;
    return { businessKey: businessKeyFromLabel(label), businessLabel: label };
  }
  return { businessKey: DEFAULT_BUSINESS_KEY, businessLabel: 'Personal' };
}

async function addInboxIntegrationItem({ source, externalId, text, projectId = '', projectName = '', businessKey = '', businessLabel = '', toNumber = '', fromNumber = '', channel = '', contactName = '', fromName = '', threadKey = '', threadMerge = false }) {
  const cleanSource = typeof source === 'string' ? source.trim().slice(0, 32) : '';
  const cleanExternalId = typeof externalId === 'string' ? externalId.trim() : '';
  const cleanText = normalizeInboxText(text);
  const id = cleanExternalId ? `${cleanSource}:${cleanExternalId}` : makeId();
  if (!cleanText) return { ok: false, error: 'Missing text' };

  let created = true;

  const targetBusinessKey = normalizeBusinessKey(businessKey) || getBusinessKeyFromContext();

  writeLock = writeLock.then(() => withBusinessKey(targetBusinessKey, async () => {
    const store = await readStore();
    const list = Array.isArray(store.inboxItems) ? store.inboxItems : [];
    const existingIdx = cleanExternalId ? list.findIndex((x) => String(x?.id || '') === id) : -1;
    if (existingIdx >= 0) {
      if (!threadMerge) {
        created = false;
        return;
      }

      const ts = nowIso();
      const existing = list[existingIdx] || {};
      const prevText = String(existing?.text || '').trim();
      const who = String(contactName || fromName || fromNumber || existing?.contactName || existing?.fromName || existing?.fromNumber || 'Sender').trim();
      const nextText = prevText ? `${prevText}\n${cleanText}` : cleanText;
      const nextCount = Number(existing?.messageCount || 1) + 1;

      const merged = normalizeInboxItem({
        ...existing,
        text: nextText,
        projectId: projectId || existing?.projectId || '',
        projectName: projectName || existing?.projectName || '',
        businessKey: targetBusinessKey,
        businessLabel: businessLabel || existing?.businessLabel || '',
        toNumber: toNumber || existing?.toNumber || '',
        fromNumber: fromNumber || existing?.fromNumber || '',
        sender: fromNumber || existing?.sender || '',
        contactName: contactName || existing?.contactName || '',
        fromName: fromName || who,
        threadKey: threadKey || existing?.threadKey || '',
        messageCount: nextCount,
        lastMessageAt: ts,
        status: String(existing?.status || 'New') === 'Archived' ? 'Triaged' : (existing?.status || 'New'),
        updatedAt: ts,
      });

      const nextList = [...list];
      nextList.splice(existingIdx, 1);
      nextList.unshift(merged);
      const nextStore = {
        ...store,
        revision: store.revision + 1,
        updatedAt: ts,
        inboxItems: nextList.slice(0, 500),
      };
      await writeStore(nextStore);
      created = false;
      return;
    }

    created = true;

    const ts = nowIso();

    let finalProjectId = projectId;
    let finalProjectName = projectName;
    const senderKey = fromNumber || '';
    if (!finalProjectId && senderKey) {
      const auto = resolveSenderProjectMapping(store, senderKey);
      if (auto?.projectId) {
        finalProjectId = auto.projectId;
        finalProjectName = auto.projectName || '';
      }
    }

    const nextItem = normalizeInboxItem({
        id,
        source: cleanSource,
        text: cleanText,
        status: "New",
        projectId: finalProjectId,
        projectName: finalProjectName,
        businessKey: targetBusinessKey,
        businessLabel,
        toNumber,
        fromNumber,
        sender: senderKey,
        contactName,
        fromName,
        threadKey,
        messageCount: 1,
        lastMessageAt: ts,
        channel,
        createdAt: ts,
        updatedAt: ts,
      });

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: [nextItem, ...list].slice(0, 500),
    };
    await writeStore(nextStore);
  }));

  await writeLock;
  return { ok: true, created, id };
}

function verifySlackRequest({ req, signingSecret }) {
  const secret = typeof signingSecret === 'string' ? signingSecret.trim() : '';
  if (!secret) return { ok: false, error: 'Slack signing secret not configured' };

  const ts = typeof req.headers['x-slack-request-timestamp'] === 'string' ? req.headers['x-slack-request-timestamp'].trim() : '';
  const sig = typeof req.headers['x-slack-signature'] === 'string' ? req.headers['x-slack-signature'].trim() : '';
  if (!ts || !sig) return { ok: false, error: 'Missing Slack signature headers' };

  const tsNum = Number(ts);
  if (!Number.isFinite(tsNum)) return { ok: false, error: 'Invalid Slack timestamp' };
  const nowSec = Math.floor(Date.now() / 1000);
  if (Math.abs(nowSec - tsNum) > 60 * 5) return { ok: false, error: 'Slack timestamp too old' };

  const raw = getRawBodyString(req);
  const base = `v0:${ts}:${raw}`;
  const hmac = crypto.createHmac('sha256', secret).update(base, 'utf8').digest('hex');
  const expected = `v0=${hmac}`;
  if (!safeTimingEqual(expected, sig)) return { ok: false, error: 'Invalid Slack signature' };
  return { ok: true };
}

function computeTwilioSignature({ authToken, url, params }) {
  const token = typeof authToken === 'string' ? authToken : '';
  const u = typeof url === 'string' ? url : '';
  const p = params && typeof params === 'object' ? params : {};

  const keys = Object.keys(p).sort();
  let data = u;
  for (const k of keys) {
    const v = p[k];
    if (Array.isArray(v)) {
      for (const item of v) data += `${k}${String(item)}`;
    } else if (v !== undefined && v !== null) {
      data += `${k}${String(v)}`;
    }
  }
  return crypto.createHmac('sha1', token).update(data, 'utf8').digest('base64');
}

function verifyTwilioRequest({ req, authToken }) {
  const token = typeof authToken === 'string' ? authToken.trim() : '';
  if (!token) return { ok: false, error: 'Quo/Twilio auth token not configured' };
  const sig = typeof req.headers['x-twilio-signature'] === 'string' ? req.headers['x-twilio-signature'].trim() : '';
  if (!sig) return { ok: false, error: 'Missing X-Twilio-Signature header' };

  const fullUrl = `${getBaseUrl(req)}${req.originalUrl || req.url || ''}`;
  const expected = computeTwilioSignature({ authToken: token, url: fullUrl, params: req.body || {} });
  if (!safeTimingEqual(expected, sig)) return { ok: false, error: 'Invalid Twilio signature (check BASE_URL / webhook URL)' };
  return { ok: true };
}

function extractWebhookSharedSecret(req) {
  const auth = typeof req.headers.authorization === 'string' ? req.headers.authorization.trim() : '';
  if (auth.toLowerCase().startsWith('bearer ')) return auth.slice(7).trim();
  const headerToken = typeof req.headers['x-quo-token'] === 'string' ? req.headers['x-quo-token'].trim() : '';
  if (headerToken) return headerToken;
  const queryToken = typeof req.query?.token === 'string' ? req.query.token.trim() : '';
  return queryToken;
}

function verifyQuoWebhookRequest({ req, twilioAuthToken, webhookToken }) {
  const hasTwilioSigHeader = typeof req.headers['x-twilio-signature'] === 'string' && req.headers['x-twilio-signature'].trim();
  if (hasTwilioSigHeader) {
    return verifyTwilioRequest({ req, authToken: twilioAuthToken });
  }

  const secret = typeof webhookToken === 'string' ? webhookToken.trim() : '';
  if (!secret) {
    return {
      ok: false,
      error: 'Missing X-Twilio-Signature header (and QUO_WEBHOOK_TOKEN not configured)',
    };
  }

  const presented = extractWebhookSharedSecret(req);
  if (!presented) return { ok: false, error: 'Missing webhook token (set Authorization: Bearer �, X-Quo-Token, or ?token=...)' };
  if (!safeTimingEqual(presented, secret)) return { ok: false, error: 'Invalid webhook token' };
  return { ok: true };
}

function debugWebhookLog(message, extra) {
  if (!DEBUG_WEBHOOKS) return;
  try {
    const meta = extra && typeof extra === 'object' ? extra : {};
    console.warn(`[webhook] ${message}`, meta);
  } catch {
    // ignore
  }
}

function getMcpConfigFromSettings(settings) {
  const raw = settings && typeof settings === 'object' && settings.mcp && typeof settings.mcp === 'object' ? settings.mcp : {};
  const enabled = Boolean(raw.enabled);
  const command = typeof raw.command === 'string' ? raw.command.trim() : '';
  const args = Array.isArray(raw.args) ? raw.args.map((v) => String(v)).join(' ') : typeof raw.args === 'string' ? raw.args : '';
  const cwd = typeof raw.cwd === 'string' ? raw.cwd.trim() : '';
  return { enabled, command, args, cwd };
}

function normalizeMcpServerName(name) {
  const s = typeof name === 'string' ? name.trim().toLowerCase() : '';
  if (!s) return '';
  const cleaned = s.replace(/[^a-z0-9_-]/g, '');
  return cleaned.slice(0, 32);
}

function getMcpServersFromSettings(settings) {
  const raw = settings && typeof settings === 'object' ? settings.mcpServers : null;
  const list = Array.isArray(raw) ? raw : [];
  const out = [];
  for (const row of list) {
    if (!row || typeof row !== 'object') continue;
    const name = normalizeMcpServerName(row.name || row.server || row.id || '');
    if (!name) continue;
    const enabled = Boolean(row.enabled);
    const command = typeof row.command === 'string' ? row.command.trim() : '';
    const args = Array.isArray(row.args) ? row.args.map((v) => String(v)).join(' ') : typeof row.args === 'string' ? row.args : '';
    const cwd = typeof row.cwd === 'string' ? row.cwd.trim() : '';
    out.push({ name, enabled, command, args, cwd });
  }

  // Dedupe by name; last one wins.
  const byName = new Map();
  for (const s of out) byName.set(s.name, s);
  return Array.from(byName.values());
}

function getMcpEffectiveSettings(settings) {
  const legacy = getMcpConfigFromSettings(settings);
  const servers = getMcpServersFromSettings(settings);
  const anyServerEnabled = servers.some((s) => s.enabled);
  const anyServerConfigured = servers.some((s) => s.enabled && s.command);
  const enabled = Boolean(legacy.enabled || anyServerEnabled);
  const configured = Boolean((legacy.enabled && legacy.command) || anyServerConfigured);
  return { legacy, servers, enabled, configured };
}

function resolveMcpTarget(settings, fullToolName) {
  const { legacy, servers } = getMcpEffectiveSettings(settings);
  const raw = typeof fullToolName === 'string' ? fullToolName.trim() : '';
  const dot = raw.indexOf('.');
  const prefix = dot > 0 ? normalizeMcpServerName(raw.slice(0, dot)) : '';
  const toolName = dot > 0 ? raw.slice(dot + 1) : raw;

  if (prefix) {
    const server = servers.find((s) => s.name === prefix);
    if (server && server.enabled && server.command) {
      return { ok: true, target: { kind: 'server', name: server.name, config: server }, toolName };
    }
  }

  if (legacy.enabled && legacy.command) {
    return { ok: true, target: { kind: 'legacy', name: 'legacy', config: legacy }, toolName: raw };
  }
  return { ok: false, error: 'MCP is not configured' };
}

async function mcpListToolsAll(settings) {
  const { legacy, servers } = getMcpEffectiveSettings(settings);
  const out = [];

  if (legacy.enabled && legacy.command) {
    const result = await mcpListTools({ command: legacy.command, args: legacy.args, cwd: legacy.cwd || process.cwd() });
    const tools = Array.isArray(result?.tools) ? result.tools : [];
    for (const t of tools) out.push({ ...t, server: 'legacy' });
  }

  for (const s of servers) {
    if (!s.enabled || !s.command) continue;
    const result = await mcpListTools({ command: s.command, args: s.args, cwd: s.cwd || process.cwd() });
    const tools = Array.isArray(result?.tools) ? result.tools : [];
    for (const t of tools) {
      const name = typeof t?.name === 'string' ? t.name : '';
      out.push({ ...t, name: name ? `${s.name}.${name}` : name, server: s.name });
    }
  }

  return out;
}

function getBaseUrl(req) {
  // Prefer explicit BASE_URL if provided (useful behind a proxy), else derive.
  const envBase = typeof process.env.BASE_URL === 'string' ? process.env.BASE_URL.trim() : '';
  if (envBase) return envBase.replace(/\/$/, '');
  const proto = req.headers['x-forwarded-proto'] ? String(req.headers['x-forwarded-proto']).split(',')[0].trim() : req.protocol;
  const host = req.headers['x-forwarded-host'] ? String(req.headers['x-forwarded-host']).split(',')[0].trim() : req.get('host');
  return `${proto}://${host}`;
}

function getDefaultBaseUrl() {
  const envBase = typeof process.env.BASE_URL === 'string' ? process.env.BASE_URL.trim() : '';
  if (envBase) return envBase.replace(/\/$/, '');
  return `http://localhost:${PORT}`;
}

function extractFirstUrl(text) {
  const s = String(text || '');
  const m = s.match(/https?:\/\/[^\s<>()]+/i);
  return m ? m[0] : '';
}

function extractMeetingLink(event) {
  if (!event || typeof event !== 'object') return '';
  const hangout = typeof event.hangoutLink === 'string' ? event.hangoutLink.trim() : '';
  if (hangout) return hangout;

  const entryPoints = Array.isArray(event?.conferenceData?.entryPoints) ? event.conferenceData.entryPoints : [];
  for (const ep of entryPoints) {
    const uri = typeof ep?.uri === 'string' ? ep.uri.trim() : '';
    if (!uri) continue;
    if (ep?.entryPointType === 'video') return uri;
    if (/zoom\.us\//i.test(uri)) return uri;
  }

  const locationUrl = extractFirstUrl(event.location);
  if (locationUrl) return locationUrl;

  const descUrl = extractFirstUrl(event.description);
  if (descUrl) return descUrl;
  return '';
}

function getStringAtPath(obj, pathExpr) {
  if (!obj || typeof obj !== 'object') return '';
  const path = String(pathExpr || '').trim();
  if (!path) return '';
  const parts = path.split('.').map((p) => p.trim()).filter(Boolean);
  let cur = obj;
  for (const p of parts) {
    if (!cur || typeof cur !== 'object') return '';
    cur = cur[p];
  }
  return valueToLooseText(cur);
}

function valueToLooseText(value, { depth = 0, maxDepth = 4 } = {}) {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);

  if (Array.isArray(value)) {
    if (depth >= maxDepth) return '';
    const parts = value
      .map((v) => valueToLooseText(v, { depth: depth + 1, maxDepth }))
      .map((s) => String(s || '').trim())
      .filter(Boolean);
    if (!parts.length) return '';
    // Prefer newline separation for multi-line human notes.
    return parts.join('\n');
  }

  if (typeof value === 'object') {
    const v = value;

    // Common Airtable shapes: collaborator, attachment, linked record, rich-ish objects.
    const directKeys = ['text', 'value', 'name', 'title', 'summary', 'content', 'body', 'message', 'description', 'notes'];
    for (const k of directKeys) {
      if (typeof v?.[k] === 'string' && v[k].trim()) return v[k];
    }

    if (typeof v?.displayName === 'string' && v.displayName.trim()) return v.displayName;
    if (typeof v?.label === 'string' && v.label.trim()) return v.label;

    const email = typeof v?.email === 'string' ? v.email.trim() : '';
    const name = typeof v?.name === 'string' ? v.name.trim() : '';
    if (email) return name ? `${name} <${email}>` : email;

    const url = typeof v?.url === 'string' ? v.url.trim() : '';
    if (url) return url;

    // If it's a wrapper object with a single property, unwrap it.
    if (depth < maxDepth) {
      const entries = Object.entries(v);
      if (entries.length === 1) {
        return valueToLooseText(entries[0][1], { depth: depth + 1, maxDepth });
      }
    }

    // Last resort: tiny JSON snapshot (avoid huge blobs).
    try {
      const json = JSON.stringify(v);
      if (typeof json === 'string' && json.length <= 400) return json;
    } catch {
      // ignore
    }
    return '';
  }

  return '';
}

function findFirstStringByKeyDeep(root, keyNames, maxDepth = 6) {
  const wanted = new Set((Array.isArray(keyNames) ? keyNames : []).map((k) => String(k || '').toLowerCase()).filter(Boolean));
  if (!wanted.size) return '';

  const queue = [{ value: root, depth: 0 }];
  const seen = new Set();

  while (queue.length > 0) {
    const { value, depth } = queue.shift();
    if (!value || typeof value !== 'object') continue;
    if (seen.has(value)) continue;
    seen.add(value);

    if (Array.isArray(value)) {
      if (depth >= maxDepth) continue;
      for (const item of value) queue.push({ value: item, depth: depth + 1 });
      continue;
    }

    for (const [key, rawVal] of Object.entries(value)) {
      const keyNorm = String(key || '').toLowerCase();
      if (wanted.has(keyNorm)) {
        const s = valueToLooseText(rawVal).trim();
        if (s) return s;
      }
      if (depth < maxDepth && rawVal && typeof rawVal === 'object') {
        queue.push({ value: rawVal, depth: depth + 1 });
      }
    }
  }

  return '';
}

function firstNonEmptyString(obj, pathExprs, deepKeyNames = []) {
  const paths = Array.isArray(pathExprs) ? pathExprs : [];
  for (const p of paths) {
    const v = getStringAtPath(obj, p).trim();
    if (v) return v;
  }
  const deep = findFirstStringByKeyDeep(obj, deepKeyNames);
  if (deep) return deep;
  return '';
}

function redact(obj) {
  // very small helper to avoid leaking tokens
  if (!obj || typeof obj !== 'object') return obj;
  const clone = JSON.parse(JSON.stringify(obj));
  if (clone.refresh_token) clone.refresh_token = '***';
  if (clone.access_token) clone.access_token = '***';
  if (clone.id_token) clone.id_token = '***';
  return clone;
}

function isLikelyGoogleClientId(value) {
  const s = typeof value === 'string' ? value.trim() : '';
  if (!s) return false;
  // Client IDs are not emails and typically end with .apps.googleusercontent.com
  if (s.includes('@')) return false;
  return /\.apps\.googleusercontent\.com$/i.test(s);
}

async function getGoogleOAuthConfig() {
  const saved = await readSettings();
  const clientId = (typeof process.env.GOOGLE_CLIENT_ID === 'string' ? process.env.GOOGLE_CLIENT_ID.trim() : '') || (typeof saved.googleClientId === 'string' ? saved.googleClientId.trim() : '');
  const clientSecret = (typeof process.env.GOOGLE_CLIENT_SECRET === 'string' ? process.env.GOOGLE_CLIENT_SECRET.trim() : '') || (typeof saved.googleClientSecret === 'string' ? saved.googleClientSecret.trim() : '');
  const calendarId = typeof saved.googleCalendarId === 'string' ? saved.googleCalendarId.trim() : '';
  const tokens = saved.googleTokens && typeof saved.googleTokens === 'object' ? saved.googleTokens : null;
  const projectEventIds = saved.googleProjectEventIds && typeof saved.googleProjectEventIds === 'object' ? saved.googleProjectEventIds : {};
  return { clientId, clientSecret, calendarId, tokens, projectEventIds, saved };
}

const googlePkceState = new Map();

// Slack OAuth + Web API caches (in-memory)
const slackOAuthState = new Map();
const slackUserCache = new Map();
const slackChannelCache = new Map();
const slackUsersListCache = new Map();

const slackRuntime = {
  lastReceivedAt: '',
  lastAcceptedAt: '',
  lastRejectedAt: '',
  lastRejectedReason: '',
  lastAsyncErrorAt: '',
  lastAsyncError: '',
  lastEventId: '',
  lastTeamId: '',
  lastEventType: '',
};

function pruneSlackOAuthState() {
  const cutoff = Date.now() - 10 * 60 * 1000;
  for (const [key, value] of slackOAuthState.entries()) {
    if (!value || typeof value !== 'object') {
      slackOAuthState.delete(key);
      continue;
    }
    if (typeof value.createdAt !== 'number' || value.createdAt < cutoff) {
      slackOAuthState.delete(key);
    }
  }
}

function pruneSlackCaches() {
  const now = Date.now();
  for (const [k, v] of slackUserCache.entries()) {
    if (!v || typeof v !== 'object' || typeof v.expiresAt !== 'number' || v.expiresAt <= now) slackUserCache.delete(k);
  }
  for (const [k, v] of slackChannelCache.entries()) {
    if (!v || typeof v !== 'object' || typeof v.expiresAt !== 'number' || v.expiresAt <= now) slackChannelCache.delete(k);
  }
  for (const [k, v] of slackUsersListCache.entries()) {
    if (!v || typeof v !== 'object' || typeof v.expiresAt !== 'number' || v.expiresAt <= now) slackUsersListCache.delete(k);
  }
}

async function getSlackOAuthConfig() {
  const saved = await readSettings();
  const clientId = (typeof process.env.SLACK_CLIENT_ID === 'string' ? process.env.SLACK_CLIENT_ID.trim() : '') || (typeof saved.slackClientId === 'string' ? saved.slackClientId.trim() : '');
  const clientSecret = (typeof process.env.SLACK_CLIENT_SECRET === 'string' ? process.env.SLACK_CLIENT_SECRET.trim() : '') || (typeof saved.slackClientSecret === 'string' ? saved.slackClientSecret.trim() : '');
  const botToken = (typeof process.env.SLACK_BOT_TOKEN === 'string' ? process.env.SLACK_BOT_TOKEN.trim() : '') || (typeof saved.slackBotToken === 'string' ? saved.slackBotToken.trim() : '');
  return { clientId, clientSecret, botToken, saved };
}

function normalizeHttpBaseUrl(value, fallback) {
  const raw = typeof value === 'string' ? value.trim() : '';
  const fb = typeof fallback === 'string' ? fallback.trim() : '';
  const candidate = raw || fb;
  if (!candidate) return '';
  try {
    const url = new URL(candidate);
    if (!/^https?:$/i.test(url.protocol)) return '';
    return `${url.origin}${url.pathname.replace(/\/+$/, '')}`;
  } catch {
    return '';
  }
}

async function getGhlConfig() {
  const saved = await readSettings();

  const apiKey =
    (typeof process.env.GHL_API_KEY === 'string' ? process.env.GHL_API_KEY.trim() : '') ||
    (typeof process.env.LEADCONNECTOR_API_KEY === 'string' ? process.env.LEADCONNECTOR_API_KEY.trim() : '') ||
    (typeof saved.ghlApiKey === 'string' ? saved.ghlApiKey.trim() : '');

  const locationId =
    (typeof process.env.GHL_LOCATION_ID === 'string' ? process.env.GHL_LOCATION_ID.trim() : '') ||
    (typeof process.env.LEADCONNECTOR_LOCATION_ID === 'string' ? process.env.LEADCONNECTOR_LOCATION_ID.trim() : '') ||
    (typeof saved.ghlLocationId === 'string' ? saved.ghlLocationId.trim() : '');

  const apiBaseUrl = normalizeHttpBaseUrl(
    (typeof process.env.GHL_API_BASE_URL === 'string' ? process.env.GHL_API_BASE_URL.trim() : '') ||
      (typeof process.env.LEADCONNECTOR_API_BASE_URL === 'string' ? process.env.LEADCONNECTOR_API_BASE_URL.trim() : '') ||
      (typeof saved.ghlApiBaseUrl === 'string' ? saved.ghlApiBaseUrl.trim() : ''),
    'https://services.leadconnectorhq.com',
  );

  const apiVersion =
    (typeof process.env.GHL_API_VERSION === 'string' ? process.env.GHL_API_VERSION.trim() : '') ||
    (typeof saved.ghlApiVersion === 'string' ? saved.ghlApiVersion.trim() : '') ||
    '2021-07-28';

  return { apiKey, locationId, apiBaseUrl, apiVersion, saved };
}

async function ghlApiGet({ apiKey, apiBaseUrl, apiVersion, endpoint, params }) {
  const token = typeof apiKey === 'string' ? apiKey.trim() : '';
  if (!token) throw new Error('Missing GHL API key');

  const base = normalizeHttpBaseUrl(apiBaseUrl, 'https://services.leadconnectorhq.com');
  if (!base) throw new Error('Invalid GHL API base URL');

  const ep = `/${String(endpoint || '').trim().replace(/^\/+/, '')}`;
  const url = new URL(`${base}${ep}`);

  for (const [k, v] of Object.entries(params || {})) {
    if (v === undefined || v === null) continue;
    const s = String(v).trim();
    if (!s) continue;
    url.searchParams.set(k, s);
  }

  const resp = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${token}`,
      Version: String(apiVersion || '2021-07-28'),
      Accept: 'application/json',
      'User-Agent': 'Task-Tracker/1.0',
    },
  });

  const json = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = typeof json?.message === 'string'
      ? json.message
      : (typeof json?.error === 'string' ? json.error : `HTTP ${resp.status}`);
    throw new Error(err);
  }

  return json;
}

function pickFirstArray(value, preferredKeys = []) {
  const obj = value && typeof value === 'object' ? value : null;
  if (!obj) return [];

  for (const key of preferredKeys) {
    const arr = obj[key];
    if (Array.isArray(arr)) return arr;
  }

  for (const v of Object.values(obj)) {
    if (Array.isArray(v)) return v;
  }

  return [];
}

function statusLike(value) {
  return String(value || '').trim().toLowerCase();
}

function computeGhlSnapshot({ opportunities, conversations, appointments }) {
  const opp = Array.isArray(opportunities) ? opportunities : [];
  const conv = Array.isArray(conversations) ? conversations : [];
  const appt = Array.isArray(appointments) ? appointments : [];

  const wonSet = new Set(['won', 'closedwon', 'closed_won', 'success']);
  const lostSet = new Set(['lost', 'closedlost', 'closed_lost', 'abandoned']);

  let won = 0;
  let lost = 0;
  let open = 0;
  for (const item of opp) {
    const s = statusLike(item?.status || item?.stageStatus || item?.pipelineStageName || item?.pipelineStageId || item?.opportunityStatus);
    if (wonSet.has(s)) {
      won += 1;
    } else if (lostSet.has(s)) {
      lost += 1;
    } else {
      open += 1;
    }
  }

  let unread = 0;
  for (const item of conv) {
    const unreadCount = Number(item?.unreadCount ?? item?.unread_count ?? item?.countUnread ?? 0);
    if (Number.isFinite(unreadCount) && unreadCount > 0) {
      unread += unreadCount;
      continue;
    }
    const unreadFlag = item?.unread;
    if (unreadFlag === true || String(unreadFlag || '').toLowerCase() === 'true') unread += 1;
  }

  return {
    pipeline: {
      total: opp.length,
      open,
      won,
      lost,
    },
    conversations: {
      total: conv.length,
      unread,
    },
    appointments: {
      upcoming: appt.length,
    },
  };
}

async function slackApiGet({ token, method, params }) {
  const t = typeof token === 'string' ? token.trim() : '';
  if (!t) throw new Error('Missing Slack bot token');
  const m = typeof method === 'string' ? method.trim() : '';
  if (!m) throw new Error('Missing Slack method');

  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) {
    if (v === undefined || v === null) continue;
    const s = String(v);
    if (!s) continue;
    qs.set(k, s);
  }

  const url = `https://slack.com/api/${m}${qs.toString() ? `?${qs.toString()}` : ''}`;
  const resp = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${t}`,
      'User-Agent': 'Task-Tracker/1.0',
    },
  });

  if (resp.status === 429) {
    throw new Error('Slack rate limited');
  }

  const json = await resp.json().catch(() => ({}));
  if (!resp.ok || !json || json.ok !== true) {
    const err = typeof json?.error === 'string' ? json.error : `HTTP ${resp.status}`;
    const needed = typeof json?.needed === 'string' ? json.needed.trim() : '';
    const provided = typeof json?.provided === 'string' ? json.provided.trim() : '';
    if (err === 'missing_scope' && (needed || provided)) {
      throw new Error(`missing_scope (needed: ${needed || 'unknown'}, provided: ${provided || 'unknown'}). Disconnect + Connect Slack to reinstall with updated scopes.`);
    }
    throw new Error(err);
  }
  return json;
}

async function slackApiPost({ token, method, body }) {
  const t = typeof token === 'string' ? token.trim() : '';
  if (!t) throw new Error('Missing Slack bot token');
  const m = typeof method === 'string' ? method.trim() : '';
  if (!m) throw new Error('Missing Slack method');

  const url = `https://slack.com/api/${m}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${t}`,
      'Content-Type': 'application/json; charset=utf-8',
      'User-Agent': 'Task-Tracker/1.0',
    },
    body: JSON.stringify(body || {})
  });

  if (resp.status === 429) {
    throw new Error('Slack rate limited');
  }

  const json = await resp.json().catch(() => ({}));
  if (!resp.ok || !json || json.ok !== true) {
    const err = typeof json?.error === 'string' ? json.error : `HTTP ${resp.status}`;
    const needed = typeof json?.needed === 'string' ? json.needed.trim() : '';
    const provided = typeof json?.provided === 'string' ? json.provided.trim() : '';
    if (err === 'missing_scope' && (needed || provided)) {
      throw new Error(`missing_scope (needed: ${needed || 'unknown'}, provided: ${provided || 'unknown'}). Disconnect + Connect Slack to reinstall with updated scopes.`);
    }
    throw new Error(err);
  }
  return json;
}

async function slackResolveUserLabel({ token, userId }) {
  const id = typeof userId === 'string' ? userId.trim() : '';
  if (!id) return '';

  pruneSlackCaches();
  const cached = slackUserCache.get(id);
  if (cached && typeof cached.label === 'string') return cached.label;

  const data = await slackApiGet({ token, method: 'users.info', params: { user: id } });
  const profile = data?.user && typeof data.user === 'object' ? data.user : {};
  const name = typeof profile?.name === 'string' ? profile.name.trim() : '';
  const realName = typeof profile?.real_name === 'string' ? profile.real_name.trim() : '';
  const label = name ? `@${name}` : realName ? `@${realName}` : `@${id}`;
  slackUserCache.set(id, { label, expiresAt: Date.now() + 60 * 60 * 1000 });
  return label;
}

async function slackResolveChannelLabel({ token, channelId }) {
  const id = typeof channelId === 'string' ? channelId.trim() : '';
  if (!id) return '';

  pruneSlackCaches();
  const cached = slackChannelCache.get(id);
  if (cached && typeof cached.label === 'string') return cached.label;

  const data = await slackApiGet({ token, method: 'conversations.info', params: { channel: id } });
  const ch = data?.channel && typeof data.channel === 'object' ? data.channel : {};
  const name = typeof ch?.name === 'string' ? ch.name.trim() : '';
  const isIm = Boolean(ch?.is_im);
  const label = name ? `#${name}` : isIm ? 'DM' : id;
  slackChannelCache.set(id, { label, expiresAt: Date.now() + 60 * 60 * 1000 });
  return label;
}

async function slackListConversations({ token }) {
  const t = String(token || '').trim();
  if (!t) return [];

  pruneSlackCaches();
  const cacheKey = `conversations:${t.slice(-12)}`;
  const cached = slackUsersListCache.get(cacheKey);
  if (cached && Array.isArray(cached.channels)) return cached.channels;

  let cursor = '';
  const all = [];
  for (let page = 0; page < 20; page += 1) {
    const params = {
      limit: 200,
      exclude_archived: true,
      types: 'public_channel,private_channel,mpim,im',
    };
    if (cursor) params.cursor = cursor;
    const data = await slackApiGet({ token: t, method: 'conversations.list', params });
    const channels = Array.isArray(data?.channels) ? data.channels : [];
    all.push(...channels);
    const next = typeof data?.response_metadata?.next_cursor === 'string' ? data.response_metadata.next_cursor.trim() : '';
    if (!next) break;
    cursor = next;
  }

  slackUsersListCache.set(cacheKey, { channels: all, expiresAt: Date.now() + 5 * 60 * 1000 });
  return all;
}

function normalizeSlackChannelLookup(value) {
  return String(value || '').trim().replace(/^#+/, '').toLowerCase();
}

async function slackResolveChannelTarget({ token, target }) {
  const raw = String(target || '').trim();
  if (!raw.startsWith('#')) return '';
  const lookup = normalizeSlackChannelLookup(raw);
  if (!lookup) return '';
  const channels = await slackListConversations({ token });
  const match = channels.find((channel) => normalizeSlackChannelLookup(channel?.name) === lookup);
  return String(match?.id || '').trim();
}

async function formatSlackInboxText({ token, channelId, userId, text }) {
  const cleanText = String(text || '').trim();
  if (!cleanText) return '';
  const prefix = ['Slack'];

  if (channelId) {
    try {
      const c = await slackResolveChannelLabel({ token, channelId });
      if (c) prefix.push(c);
    } catch {
      prefix.push(channelId);
    }
  }

  if (userId) {
    try {
      const u = await slackResolveUserLabel({ token, userId });
      if (u) prefix.push(u);
    } catch {
      prefix.push(`@${userId}`);
    }
  }

  return `${prefix.join(' ')}: ${cleanText}`;
}

function normalizeSlackLookupText(value) {
  return String(value || '')
    .trim()
    .replace(/^@+/, '')
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

function isLikelySlackUserId(value) {
  const s = String(value || '').trim();
  return /^[UW][A-Z0-9]{6,}$/i.test(s);
}

function slackUserAliases(user) {
  const u = user && typeof user === 'object' ? user : {};
  const p = u.profile && typeof u.profile === 'object' ? u.profile : {};
  const aliases = new Set();
  const push = (v) => {
    const n = normalizeSlackLookupText(v);
    if (n) aliases.add(n);
  };
  push(u.id);
  push(u.name);
  push(p.display_name);
  push(p.real_name);
  push(p.real_name_normalized);
  push(p.display_name_normalized);
  push(p.email);
  return aliases;
}

async function slackListWorkspaceUsers({ token }) {
  const t = String(token || '').trim();
  if (!t) return [];

  pruneSlackCaches();
  const cacheKey = `users:${t.slice(-12)}`;
  const cached = slackUsersListCache.get(cacheKey);
  if (cached && Array.isArray(cached.users)) return cached.users;

  let cursor = '';
  const all = [];
  for (let page = 0; page < 20; page += 1) {
    const params = { limit: 200 };
    if (cursor) params.cursor = cursor;
    const data = await slackApiGet({ token: t, method: 'users.list', params });
    const members = Array.isArray(data?.members) ? data.members : [];
    all.push(...members);
    const next = typeof data?.response_metadata?.next_cursor === 'string' ? data.response_metadata.next_cursor.trim() : '';
    if (!next) break;
    cursor = next;
  }

  slackUsersListCache.set(cacheKey, { users: all, expiresAt: Date.now() + 5 * 60 * 1000 });
  return all;
}

function matchSlackUserForTeamMember({ member, users }) {
  const m = member && typeof member === 'object' ? member : {};
  const list = Array.isArray(users) ? users : [];

  const explicit = String(m.slackUserId || '').trim();
  if (explicit && isLikelySlackUserId(explicit)) {
    const direct = list.find((u) => String(u?.id || '').trim().toLowerCase() === explicit.toLowerCase());
    return {
      user: direct || { id: explicit, name: explicit, profile: {} },
      source: 'explicit-id',
    };
  }

  const explicitNorm = normalizeSlackLookupText(explicit);
  if (explicitNorm) {
    for (const u of list) {
      const aliases = slackUserAliases(u);
      if (aliases.has(explicitNorm)) return { user: u, source: 'explicit-alias' };
    }
  }

  const byName = normalizeSlackLookupText(m.name);
  if (byName) {
    for (const u of list) {
      const aliases = slackUserAliases(u);
      if (aliases.has(byName)) return { user: u, source: 'name' };
    }
  }

  return { user: null, source: '' };
}

function base64Url(buf) {
  return Buffer.from(buf)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function makePkceVerifier() {
  return base64Url(crypto.randomBytes(32));
}

function makePkceChallenge(verifier) {
  const hash = crypto.createHash('sha256').update(verifier).digest();
  return base64Url(hash);
}

function pruneGooglePkceState() {
  const cutoff = Date.now() - 10 * 60 * 1000;
  for (const [key, value] of googlePkceState.entries()) {
    if (!value || typeof value !== 'object') {
      googlePkceState.delete(key);
      continue;
    }
    if (typeof value.createdAt !== 'number' || value.createdAt < cutoff) {
      googlePkceState.delete(key);
    }
  }
}

async function googleTokenRequest(params) {
  const body = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) {
    if (v === undefined || v === null) continue;
    const s = String(v);
    if (!s) continue;
    body.set(k, s);
  }

  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });

  const json = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const msg = typeof json?.error_description === 'string' ? json.error_description : typeof json?.error === 'string' ? json.error : 'token request failed';
    throw new Error(msg);
  }
  return json;
}

function normalizeGoogleTokens(tokenJson) {
  const expiresIn = Number(tokenJson?.expires_in);
  const expiryDate = Number.isFinite(expiresIn) && expiresIn > 0 ? Date.now() + expiresIn * 1000 : undefined;
  const out = { ...tokenJson };
  if (expiryDate) out.expiry_date = expiryDate;
  return out;
}

async function ensureFreshGoogleTokens({ clientId, clientSecret, tokens, saved }) {
  const existing = tokens && typeof tokens === 'object' ? tokens : null;
  if (!existing || !existing.refresh_token) return { tokens: existing, saved };

  const expiry = Number(existing.expiry_date);
  const marginMs = 60 * 1000;
  const needsRefresh = !existing.access_token || !Number.isFinite(expiry) || expiry <= Date.now() + marginMs;
  if (!needsRefresh) return { tokens: existing, saved };

  const refreshed = await googleTokenRequest({
    client_id: clientId,
    client_secret: clientSecret || undefined,
    refresh_token: existing.refresh_token,
    grant_type: 'refresh_token',
  });

  const normalized = normalizeGoogleTokens(refreshed);
  const nextTokens = {
    ...existing,
    access_token: typeof normalized.access_token === 'string' ? normalized.access_token : existing.access_token,
    token_type: typeof normalized.token_type === 'string' ? normalized.token_type : existing.token_type,
    scope: typeof normalized.scope === 'string' ? normalized.scope : existing.scope,
    expiry_date: typeof normalized.expiry_date === 'number' ? normalized.expiry_date : existing.expiry_date,
  };

  const nextSaved = { ...saved, googleTokens: nextTokens, updatedAt: nowIso() };
  await writeSettings(nextSaved);
  return { tokens: nextTokens, saved: nextSaved };
}

function buildOAuthClient({ clientId, clientSecret, redirectUri }) {
  return new google.auth.OAuth2({ clientId, clientSecret, redirectUri });
}

async function ensureGoogleCalendar(calendar, settings) {
  const existingId = typeof settings.googleCalendarId === 'string' ? settings.googleCalendarId.trim() : '';
  if (existingId) return { calendarId: existingId, settings };

  const created = await calendar.calendars.insert({
    requestBody: {
      summary: 'M.A.R.C.U.S.',
      description: 'Project due dates synced from M.A.R.C.U.S.',
      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC',
    },
  });
  const newId = created?.data?.id ? String(created.data.id) : '';
  if (!newId) throw new Error('Failed to create Google Calendar');
  const next = { ...settings, googleCalendarId: newId, updatedAt: nowIso() };
  await writeSettings(next);
  return { calendarId: newId, settings: next };
}

function ttEventSummary(project) {
  const name = typeof project?.name === 'string' ? project.name.trim() : '';
  return name ? `[M.A.R.C.U.S.] ${name}` : '[M.A.R.C.U.S.] Project';
}

function projectDueDateFromEvent(event) {
  const d = event?.start?.date;
  return safeYmd(typeof d === 'string' ? d : '');
}

function ymdAddDays(ymd, days) {
  const safe = safeYmd(ymd);
  if (!safe) return '';
  const [y, m, d] = safe.split('-').map((v) => Number(v));
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return '';
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return dt.toISOString().slice(0, 10);
}

async function googleSyncProjects({ req }) {
  const { clientId, clientSecret, tokens, saved } = await getGoogleOAuthConfig();
  if (!clientId || !isLikelyGoogleClientId(clientId)) {
    return { ok: false, reason: 'missing_client', message: 'Google OAuth client is not configured (missing/invalid Client ID). Paste the OAuth Client ID ending with .apps.googleusercontent.com.' };
  }
  if (!tokens || !tokens.refresh_token) {
    return { ok: false, reason: 'not_connected', message: 'Google Calendar is not connected. Run the OAuth connect flow first.' };
  }

  const redirectUri = `${getBaseUrl(req)}/api/integrations/google/callback`;
  const fresh = await ensureFreshGoogleTokens({ clientId, clientSecret, tokens, saved });
  const oauth2 = buildOAuthClient({ clientId, clientSecret: clientSecret || '', redirectUri });
  oauth2.setCredentials(fresh.tokens);

  const calendar = google.calendar({ version: 'v3', auth: oauth2 });

  const ensured = await ensureGoogleCalendar(calendar, saved);
  const calendarId = ensured.calendarId;
  let settings = ensured.settings;

  // 1) Push: upsert events for each project with dueDate.
  const store = await readStore();
  const projects = Array.isArray(store.projects) ? store.projects : [];

  const eventIds = settings.googleProjectEventIds && typeof settings.googleProjectEventIds === 'object' ? settings.googleProjectEventIds : {};
  let pushed = 0;
  for (const project of projects) {
    const dueDate = safeYmd(project?.dueDate);
    if (!dueDate) continue;
    const projectId = String(project.id);
    const existingEventId = typeof eventIds[projectId] === 'string' ? eventIds[projectId] : '';

    const requestBody = {
      summary: ttEventSummary(project),
      start: { date: dueDate },
      end: { date: ymdAddDays(dueDate, 1) || dueDate },
      description: 'Synced from M.A.R.C.U.S. (project due date)',
      transparency: 'transparent',
      extendedProperties: { private: { taskTrackerProjectId: projectId } },
    };

    try {
      if (existingEventId) {
        await calendar.events.patch({ calendarId, eventId: existingEventId, requestBody });
      } else {
        const created = await calendar.events.insert({ calendarId, requestBody });
        const newId = created?.data?.id ? String(created.data.id) : '';
        if (newId) {
          eventIds[projectId] = newId;
        }
      }
      pushed++;
    } catch (err) {
      // If the event was deleted manually, recreate it.
      const code = err?.code || err?.response?.status;
      if (existingEventId && (code === 404 || code === 410)) {
        try {
          const created = await calendar.events.insert({ calendarId, requestBody });
          const newId = created?.data?.id ? String(created.data.id) : '';
          if (newId) eventIds[projectId] = newId;
          pushed++;
          continue;
        } catch {
          // fall through
        }
      }
      // keep going; we don't want one project to block sync
    }
  }

  // Persist event id mapping
  settings = { ...settings, googleProjectEventIds: eventIds, updatedAt: nowIso() };
  await writeSettings(settings);

  // 2) Pull: update project dueDate if the synced event date changed.
  // Only for projects that already have a mapped event.
  let pulledUpdates = 0;
  writeLock = writeLock.then(async () => {
    const working = await readStore();
    let changed = false;
    const nextProjects = [...(working.projects || [])];

    for (let i = 0; i < nextProjects.length; i++) {
      const p = nextProjects[i];
      const pid = String(p.id);
      const eventId = typeof eventIds[pid] === 'string' ? eventIds[pid] : '';
      if (!eventId) continue;

      try {
        const ev = await calendar.events.get({ calendarId, eventId });
        const evDue = projectDueDateFromEvent(ev?.data);
        if (!evDue) continue;
        if (safeYmd(p.dueDate) !== evDue) {
          nextProjects[i] = { ...p, dueDate: evDue, updatedAt: nowIso() };
          pulledUpdates++;
          changed = true;
        }
      } catch {
        // ignore
      }
    }

    if (changed) {
      const ts = nowIso();
      const nextStore = { ...working, revision: working.revision + 1, updatedAt: ts, projects: nextProjects };
      await writeStore(nextStore);
    }
  });
  await writeLock;

  return {
    ok: true,
    calendarId,
    pushed,
    pulledUpdates,
  };
}

async function googleListUpcomingEvents({ days = 7, max = 25 } = {}) {
  const safeDays = Math.min(30, Math.max(1, Number(days) || 7));
  const safeMax = Math.min(50, Math.max(1, Number(max) || 25));

  const { clientId, clientSecret, tokens, saved } = await getGoogleOAuthConfig();
  if (!clientId || !isLikelyGoogleClientId(clientId)) {
    return { ok: false, reason: 'missing_client', message: 'Google OAuth client is not configured (missing/invalid Client ID).' };
  }
  if (!tokens || !tokens.refresh_token) {
    return { ok: false, reason: 'not_connected', message: 'Google Calendar is not connected. Run the OAuth connect flow first.' };
  }

  const calendarId = typeof saved.googleReadCalendarId === 'string' && saved.googleReadCalendarId.trim() ? saved.googleReadCalendarId.trim() : 'primary';
  const redirectUri = `${getDefaultBaseUrl()}/api/integrations/google/callback`;
  const fresh = await ensureFreshGoogleTokens({ clientId, clientSecret, tokens, saved });
  const oauth2 = buildOAuthClient({ clientId, clientSecret: clientSecret || '', redirectUri });
  oauth2.setCredentials(fresh.tokens);
  const calendar = google.calendar({ version: 'v3', auth: oauth2 });

  const now = new Date();
  const end = new Date(now.getTime() + safeDays * 24 * 60 * 60 * 1000);

  const resp = await calendar.events.list({
    calendarId,
    timeMin: now.toISOString(),
    timeMax: end.toISOString(),
    maxResults: safeMax,
    singleEvents: true,
    orderBy: 'startTime',
  });

  const items = Array.isArray(resp?.data?.items) ? resp.data.items : [];
  const events = items.map((ev) => {
    const start = ev?.start?.dateTime || ev?.start?.date || '';
    const endAt = ev?.end?.dateTime || ev?.end?.date || '';
    return {
      id: ev?.id ? String(ev.id) : '',
      summary: typeof ev?.summary === 'string' ? ev.summary : '',
      start,
      end: endAt,
      htmlLink: typeof ev?.htmlLink === 'string' ? ev.htmlLink : '',
      meetingLink: extractMeetingLink(ev),
      location: typeof ev?.location === 'string' ? ev.location : '',
    };
  });

  return { ok: true, calendarId, days: safeDays, events };
}

async function ensureStoreExists() {
  const file = getStoreFileForBusiness(getBusinessKeyFromContext());
  await ensureStoreFileExists(file);
}

async function ensureStoreFileExists(file) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  try {
    await fs.access(file);
  } catch {
    await fs.writeFile(file, JSON.stringify(EMPTY_STORE, null, 2) + '\n', 'utf8');
  }
}

function normalizeStoreShape(parsed) {
  if (!parsed || typeof parsed !== 'object') return structuredClone(EMPTY_STORE);

  const revision = Number(parsed.revision);
  const updatedAt = typeof parsed.updatedAt === 'string' ? parsed.updatedAt : new Date(0).toISOString();
  const projects = Array.isArray(parsed.projects) ? parsed.projects : [];
  const clients = Array.isArray(parsed.clients) ? parsed.clients : [];
  const tasks = (Array.isArray(parsed.tasks) ? parsed.tasks : []).map(sanitizeTaskRecord);
  const senderProjectMap = parsed.senderProjectMap && typeof parsed.senderProjectMap === 'object' ? parsed.senderProjectMap : {};
  const team = Array.isArray(parsed.team) ? parsed.team : [];
  const projectNotes = parsed.projectNotes && typeof parsed.projectNotes === 'object' ? parsed.projectNotes : {};
  const projectScratchpads = parsed.projectScratchpads && typeof parsed.projectScratchpads === 'object' ? parsed.projectScratchpads : {};
  const projectNoteEntries = parsed.projectNoteEntries && typeof parsed.projectNoteEntries === 'object' ? parsed.projectNoteEntries : {};
  const projectChats = parsed.projectChats && typeof parsed.projectChats === 'object' ? parsed.projectChats : {};
  const projectCommunications = parsed.projectCommunications && typeof parsed.projectCommunications === 'object' ? parsed.projectCommunications : {};
  const marcusNotes = parsed.marcusNotes && typeof parsed.marcusNotes === 'object' ? parsed.marcusNotes : {};
  const inboxItems = Array.isArray(parsed.inboxItems) ? parsed.inboxItems : [];
  const projectTranscriptUndo = parsed.projectTranscriptUndo && typeof parsed.projectTranscriptUndo === 'object' ? parsed.projectTranscriptUndo : {};

  return {
    revision: Number.isFinite(revision) && revision > 0 ? revision : 1,
    updatedAt,
    projects,
    clients,
    tasks,
    senderProjectMap,
    team,
    projectNotes,
    projectScratchpads,
    projectNoteEntries,
    projectChats,
    projectCommunications,
    marcusNotes,
    inboxItems,
    projectTranscriptUndo,
  };
}

async function readStoreFile(file) {
  await ensureStoreFileExists(file);
  const raw = await fs.readFile(file, 'utf8');
  const parsed = JSON.parse(raw);
  return normalizeStoreShape(parsed);
}

async function readStoreForBusiness(businessKey) {
  return readStoreFile(getStoreFileForBusiness(businessKey));
}

async function readStore() {
  return readStoreForBusiness(getBusinessKeyFromContext());
}

function normalizeClientRecord(input) {
  const c = input && typeof input === 'object' ? input : {};
  const name = typeof c.name === 'string' ? c.name.trim() : '';
  const phone = typeof c.phone === 'string' ? c.phone.trim() : '';
  const email = typeof c.email === 'string' ? c.email.trim() : '';
  const website = typeof c.website === 'string' ? c.website.trim() : '';
  const accountManagerName = typeof c.accountManagerName === 'string' ? c.accountManagerName.trim() : '';
  const accountManagerEmail = typeof c.accountManagerEmail === 'string' ? c.accountManagerEmail.trim() : '';
  const airtableRecordId = typeof c.airtableRecordId === 'string' ? c.airtableRecordId.trim() : '';
  const airtableUrl = typeof c.airtableUrl === 'string' ? c.airtableUrl.trim() : '';
  const createdAt = typeof c.createdAt === 'string' && c.createdAt ? c.createdAt : nowIso();
  const updatedAt = typeof c.updatedAt === 'string' && c.updatedAt ? c.updatedAt : createdAt;

  return {
    id: typeof c.id === 'string' && c.id.trim() ? c.id.trim() : makeId(),
    name,
    phone,
    email,
    website,
    accountManagerName,
    accountManagerEmail,
    airtableRecordId,
    airtableUrl,
    createdAt,
    updatedAt,
  };
}

function upsertClientForProjectInboxLink(clientsInput, { project, inboxItem, ts = nowIso() } = {}) {
  const clients = Array.isArray(clientsInput) ? [...clientsInput] : [];
  const p = project && typeof project === 'object' ? project : {};
  const item = inboxItem && typeof inboxItem === 'object' ? inboxItem : {};

  const deriveName = [
    String(p.clientName || '').trim(),
    String(item.contactName || '').trim(),
    String(item.fromName || '').trim(),
    String(p.name || '').trim(),
  ].find(Boolean) || 'Unknown Contact';

  const phoneCandidates = [
    String(p.clientPhone || '').trim(),
    String(item.fromNumber || '').trim(),
    String(item.sender || '').trim(),
  ].filter(Boolean);
  const derivePhone = phoneCandidates.find((x) => normalizePhoneForLookup(x)) || '';

  const phoneKey = normalizePhoneForLookup(derivePhone);
  const nameKey = deriveName.toLowerCase();

  let idx = -1;
  if (phoneKey) {
    idx = clients.findIndex((c) => normalizePhoneForLookup(c?.phone || '') === phoneKey);
  }
  if (idx < 0 && nameKey) {
    idx = clients.findIndex((c) => String(c?.name || '').trim().toLowerCase() === nameKey);
  }

  if (idx >= 0) {
    const existing = clients[idx] && typeof clients[idx] === 'object' ? clients[idx] : {};
    const merged = normalizeClientRecord({
      ...existing,
      name: String(existing.name || '').trim() || deriveName,
      phone: String(existing.phone || '').trim() || derivePhone,
      accountManagerName: String(existing.accountManagerName || '').trim() || String(p.accountManagerName || '').trim(),
      accountManagerEmail: String(existing.accountManagerEmail || '').trim() || String(p.accountManagerEmail || '').trim(),
      updatedAt: ts,
    });
    clients[idx] = merged;
    return { clients, client: merged };
  }

  const created = normalizeClientRecord({
    name: deriveName || derivePhone,
    phone: derivePhone,
    accountManagerName: String(p.accountManagerName || '').trim(),
    accountManagerEmail: String(p.accountManagerEmail || '').trim(),
    createdAt: ts,
    updatedAt: ts,
  });
  clients.unshift(created);
  return { clients, client: created };
}

function isLegacyAirtableClientProject(project) {
  const p = project && typeof project === 'object' ? project : {};
  const brief = String(p.agentBrief || '').toLowerCase();
  if (brief.includes('imported from airtable (clients)')) return true;
  // Older variants
  if (brief.includes('airtable') && brief.includes('clients') && brief.includes('import')) return true;
  return false;
}

function migrateLegacyAirtableClientProjects(store) {
  const s = store && typeof store === 'object' ? store : {};
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const existingClients = Array.isArray(s.clients) ? s.clients : [];

  const byAirtableUrl = new Map();
  for (const c of existingClients) {
    const url = typeof c?.airtableUrl === 'string' ? c.airtableUrl.trim() : '';
    if (url) byAirtableUrl.set(url, c);
  }

  let changed = false;
  const nextProjects = projects.map((p) => {
    if (!isLegacyAirtableClientProject(p)) return p;
    if (p && typeof p === 'object' && p.isContactRecord === true) return p;
    changed = true;
    return { ...(p && typeof p === 'object' ? p : {}), isContactRecord: true };
  });

  // Create contacts for legacy “client-as-project” entries if missing.
  let nextClients = [...existingClients];
  for (const p of projects) {
    if (!isLegacyAirtableClientProject(p)) continue;
    const airtableUrl = typeof p?.airtableUrl === 'string' ? p.airtableUrl.trim() : '';
    if (!airtableUrl) continue;
    if (byAirtableUrl.has(airtableUrl)) continue;

    const name = String(p?.clientName || p?.name || '').trim();
    const phone = String(p?.clientPhone || '').trim();
    const accountManagerName = String(p?.accountManagerName || '').trim();
    const accountManagerEmail = String(p?.accountManagerEmail || '').trim();
    const createdAt = typeof p?.createdAt === 'string' ? p.createdAt : nowIso();
    const updatedAt = typeof p?.updatedAt === 'string' ? p.updatedAt : createdAt;

    const client = normalizeClientRecord({
      name,
      phone,
      accountManagerName,
      accountManagerEmail,
      airtableUrl,
      createdAt,
      updatedAt,
    });
    nextClients.push(client);
    byAirtableUrl.set(airtableUrl, client);
    changed = true;
  }

  if (!changed) return { changed: false, store };
  const ts = nowIso();
  return {
    changed: true,
    store: {
      ...s,
      revision: Number(s.revision || 0) + 1,
      updatedAt: ts,
      projects: nextProjects,
      clients: nextClients,
    },
  };
}

function isAirtableRevisionRequestsProject(project) {
  const p = project && typeof project === 'object' ? project : {};
  if (String(p.airtableSource || '') === 'revision-requests') return true;
  const brief = String(p.agentBrief || '').toLowerCase();
  if (brief.includes('imported from airtable (revision requests)')) return true;
  if (brief.includes('airtable') && brief.includes('revision')) return true;

  const name = String(p.name || '');
  if (/\s—\srev\s*\w+/i.test(name) || /\s-\srev\s*\w+/i.test(name)) return true;

  const airtableUrl = String(p.airtableUrl || '').trim();
  if (airtableUrl.startsWith('https://airtable.com/') && airtableUrl.length > 25) return true;
  return false;
}

function stripAirtableRevisionMaterializedData(store, settings) {
  if (shouldMaterializeAirtableRevisionRequests(settings)) return store;
  const s = store && typeof store === 'object' ? store : structuredClone(EMPTY_STORE);
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const tasks = Array.isArray(s.tasks) ? s.tasks : [];

  const removedProjects = projects.filter((p) => isAirtableRevisionRequestsProject(p));
  if (!removedProjects.length) return s;

  const removedNames = new Set(removedProjects.map((p) => String(p?.name || '').trim()).filter(Boolean));
  const nextProjects = projects.filter((p) => !isAirtableRevisionRequestsProject(p));
  const nextTasks = tasks.filter((t) => {
    const id = String(t?.id || '');
    if (id.startsWith('airtable:rev:')) return false;
    const proj = String(t?.project || '').trim();
    if (proj && removedNames.has(proj)) return false;
    return true;
  });

  return {
    ...s,
    projects: nextProjects,
    tasks: nextTasks,
  };
}

function normalizeSiteLabelLoose(input) {
  const raw = typeof input === 'string' ? input.trim() : '';
  if (!raw) return '';
  try {
    const withProto = raw.includes('://') ? raw : `https://${raw}`;
    const u = new URL(withProto);
    const host = String(u.hostname || '').trim().toLowerCase().replace(/^www\./, '');
    if (host) return host;
  } catch {
    // ignore
  }
  return raw.replace(/^https?:\/\//i, '').replace(/^www\./i, '').split(/[\/\s]/)[0].trim() || raw;
}

function collapseLegacyAirtableRevisionRequestProjects(store, businessKey) {
  const s = store && typeof store === 'object' ? store : {};
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const tasks = Array.isArray(s.tasks) ? s.tasks : [];

  const businessName = getBusinessNameForKey(businessKey);
  const groups = new Map();

  const computeGroupKey = (p) => {
    const existing = String(p?.airtableRequestsKey || '').trim();
    if (existing) return existing;

    const site = normalizeSiteLabelLoose(String(p?.airtableSiteLabel || p?.clientName || '').trim() || String(p?.name || '').split('—')[0].trim());
    const biz = normKey(businessName);
    const siteKey = normKey(site);
    if (!siteKey) return '';
    const hash = crypto.createHash('sha1').update(`${biz}|${siteKey}`).digest('hex').slice(0, 12);
    return `airtable:rev-requests:group:${hash}`;
  };

  for (let i = 0; i < projects.length; i++) {
    const p = projects[i];
    if (!isAirtableRevisionRequestsProject(p)) continue;
    const key = computeGroupKey(p);
    if (!key) continue;
    const list = groups.get(key) || [];
    list.push(i);
    groups.set(key, list);
  }

  if (!groups.size) return { changed: false, store: s, archived: 0, tasksReassigned: 0 };

  const ts = nowIso();
  const nextProjects = [...projects];
  const nextTasks = [...tasks];

  let archived = 0;
  let tasksReassigned = 0;
  let changed = false;

  const parseTime = (val) => {
    const t = Date.parse(String(val || ''));
    return Number.isFinite(t) ? t : 0;
  };

  for (const [groupKey, idxs] of groups.entries()) {
    if (idxs.length <= 1) continue;

    // Pick the project to keep: prefer non-Archived + one that already has the group key.
    let keepIdx = idxs[0];
    for (const idx of idxs) {
      const p = nextProjects[idx];
      const curKeep = nextProjects[keepIdx];
      const pKey = String(p?.airtableRequestsKey || '').trim();
      const keepKey = String(curKeep?.airtableRequestsKey || '').trim();
      const pArchived = String(p?.status || '') === 'Archived';
      const keepArchived = String(curKeep?.status || '') === 'Archived';
      if (!pArchived && keepArchived) {
        keepIdx = idx;
        continue;
      }
      if (pKey === groupKey && keepKey !== groupKey) {
        keepIdx = idx;
        continue;
      }
      const pTime = Math.max(parseTime(p?.updatedAt), parseTime(p?.createdAt));
      const keepTime = Math.max(parseTime(curKeep?.updatedAt), parseTime(curKeep?.createdAt));
      if (pTime > keepTime) keepIdx = idx;
    }

    const keepProject = nextProjects[keepIdx];
    const keepName = String(keepProject?.name || '').trim();
    if (!keepName) continue;

    // Ensure the kept project is tagged with the group key.
    if (String(keepProject.airtableRequestsKey || '').trim() !== groupKey) {
      nextProjects[keepIdx] = { ...keepProject, airtableRequestsKey: groupKey, updatedAt: ts };
      changed = true;
    }

    for (const idx of idxs) {
      if (idx === keepIdx) continue;
      const p = nextProjects[idx];
      if (!p || typeof p !== 'object') continue;
      if (String(p.status || '') !== 'Archived') {
        nextProjects[idx] = { ...p, status: 'Archived', airtableRequestsKey: groupKey, updatedAt: ts };
        archived++;
        changed = true;
      }

      const oldName = String(p.name || '').trim();
      if (!oldName || oldName === keepName) continue;
      for (let t = 0; t < nextTasks.length; t++) {
        const task = nextTasks[t];
        if (!task || typeof task !== 'object') continue;
        if (String(task.project || '') !== oldName) continue;
        const id = String(task.id || '');
        if (!id.startsWith('airtable:rev:')) continue;
        nextTasks[t] = { ...task, project: keepName, updatedAt: ts };
        tasksReassigned++;
        changed = true;
      }
    }
  }

  if (!changed) return { changed: false, store: s, archived, tasksReassigned };
  return {
    changed: true,
    archived,
    tasksReassigned,
    store: {
      ...s,
      revision: Number(s.revision || 0) + 1,
      updatedAt: ts,
      projects: nextProjects,
      tasks: nextTasks,
    },
  };
}

function summarizeRevisionLikeProjectsForDebug(store, businessKey) {
  const s = store && typeof store === 'object' ? store : {};
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const businessName = getBusinessNameForKey(businessKey);
  const revLike = projects.filter((p) => isAirtableRevisionRequestsProject(p));
  const active = revLike.filter((p) => String(p?.status || '') !== 'Archived');
  const archived = revLike.filter((p) => String(p?.status || '') === 'Archived');

  const groups = new Map();
  for (const p of revLike) {
    const existing = String(p?.airtableRequestsKey || '').trim();
    const site = normalizeSiteLabelLoose(String(p?.airtableSiteLabel || p?.clientName || '').trim() || String(p?.name || '').split('—')[0].trim());
    const key = existing || (() => {
      const biz = normKey(businessName);
      const siteKey = normKey(site);
      if (!siteKey) return '';
      const hash = crypto.createHash('sha1').update(`${biz}|${siteKey}`).digest('hex').slice(0, 12);
      return `airtable:rev-requests:group:${hash}`;
    })();
    if (!key) continue;
    groups.set(key, (groups.get(key) || 0) + 1);
  }

  const groupSizes = [...groups.values()];
  groupSizes.sort((a, b) => b - a);

  const sample = revLike
    .slice(0, 25)
    .map((p) => ({
      id: String(p?.id || ''),
      name: String(p?.name || ''),
      status: String(p?.status || ''),
      airtableUrl: String(p?.airtableUrl || ''),
      airtableRequestsKey: String(p?.airtableRequestsKey || ''),
      airtableSiteLabel: String(p?.airtableSiteLabel || ''),
    }));

  return {
    businessKey: normalizeBusinessKey(businessKey) || DEFAULT_BUSINESS_KEY,
    businessName,
    totalProjects: projects.length,
    revLikeProjects: revLike.length,
    revLikeActive: active.length,
    revLikeArchived: archived.length,
    revLikeGroups: groups.size,
    revLikeMaxGroupSize: groupSizes.length ? groupSizes[0] : 0,
    sample,
  };
}

function normalizeInboxText(text) {
  if (typeof text !== 'string') return '';
  return text.replace(/\r\n/g, '\n').trim();
}

function normalizeAckSignalText(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[^a-z0-9'\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeSmsAckFilterLevel(levelRaw) {
  const level = String(levelRaw || '').trim().toLowerCase();
  if (level === 'off' || level === 'low' || level === 'medium' || level === 'high') return level;
  return 'medium';
}

function getMarcusExcludedPhoneSet(settings) {
  const raw = settings && typeof settings === 'object' ? settings.marcusExcludedPhoneNumbers : null;
  const out = new Set();
  const push = (value) => {
    for (const key of phoneLookupKeys(value)) out.add(key);
  };

  if (Array.isArray(raw)) {
    for (const value of raw) push(value);
    return out;
  }

  if (typeof raw === 'string') {
    for (const value of raw.split(/[\n,;]+/g)) push(value);
  }

  return out;
}

function isInboxItemExcludedFromMarcus(item, settings) {
  const excluded = getMarcusExcludedPhoneSet(settings);
  if (!excluded.size) return false;
  const it = item && typeof item === 'object' ? item : {};
  const candidates = [it?.sender, it?.fromNumber, it?.toNumber, it?.contactName];
  for (const value of candidates) {
    for (const key of phoneLookupKeys(value)) {
      if (excluded.has(key)) return true;
    }
  }
  return false;
}

function isLowSignalAcknowledgementText(text, levelRaw = 'medium') {
  const level = normalizeSmsAckFilterLevel(levelRaw);
  if (level === 'off') return false;

  const raw = String(text || '').trim();
  if (!raw) return false;

  // Emoji-only / reaction-style replies are usually acknowledgement noise.
  const emojiOnly = raw
    .replace(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\uFE0F\u200D\s]/gu, '')
    .trim();
  if (!emojiOnly) return true;

  const normalized = normalizeAckSignalText(raw);
  if (!normalized) return true;
  const maxLenByLevel = level === 'high' ? 80 : level === 'low' ? 24 : 48;
  if (normalized.length > maxLenByLevel) return false;

  const exact = new Set(level === 'low'
    ? [
      'k', 'kk', 'ok', 'okay', 'yep', 'yup', 'yeah', 'yes',
      'got it', 'copy', 'roger', 'understood', 'noted',
      'thanks', 'thank you', 'thx', 'ty',
    ]
    : [
      'k', 'kk', 'ok', 'okay', 'yep', 'yup', 'yeah', 'yes', 'no',
      'got it', 'copy', 'roger', 'understood', 'noted',
      'sounds good', 'all good', 'we re good',
      'thanks', 'thank you', 'thx', 'ty', 'tysm', 'appreciate it',
      'cool', 'great', 'awesome', 'perfect', 'done',
    ]);
  if (exact.has(normalized)) return true;

  // Common combinations like "ok thanks", "yep got it", "thanks man".
  if (/^(ok|okay|yep|yup|yeah|yes|got it|copy|roger|understood|noted)(\s+(thanks|thank you|thx|ty|appreciate it))?$/.test(normalized)) return true;
  if (/^(thanks|thank you|thx|ty|appreciate it)(\s+(man|bro|dude|sir|maam|m'am))?$/.test(normalized)) return true;

  if (level === 'high') {
    if (/^(sounds good|all good|we re good|cool|great|awesome|perfect|done)(\s+(thanks|thank you|thx|ty))?$/.test(normalized)) return true;
  }

  return false;
}

function isSmsLikeInboxSource(sourceRaw) {
  const src = String(sourceRaw || '').trim().toLowerCase();
  if (!src) return false;
  return src.includes('sms') || src.includes('quo') || src.includes('twilio') || src.includes('text');
}

function getVisibleInboxItemsFromSettings(items, settings) {
  const list = Array.isArray(items) ? items : [];
  const level = normalizeSmsAckFilterLevel(settings?.smsAckFilterLevel);
  if (level === 'off') return list;
  return list.filter((item) => {
    const it = item && typeof item === 'object' ? item : {};
    if (!isSmsLikeInboxSource(it?.source)) return true;
    return !isLowSignalAcknowledgementText(extractInboxSignalText(it), level);
  });
}

function applyInboxVisibilityToStore(store, settings) {
  const s = store && typeof store === 'object' ? store : structuredClone(EMPTY_STORE);
  const visibleInbox = getVisibleInboxItemsFromSettings(s.inboxItems, settings);
  if (!Array.isArray(s.inboxItems) || visibleInbox.length === s.inboxItems.length) return s;
  return { ...s, inboxItems: visibleInbox };
}

function extractInboxSignalText(item) {
  const it = item && typeof item === 'object' ? item : {};
  const source = String(it?.source || '').trim().toLowerCase();
  const raw = String(it?.text || '').replace(/\r\n/g, '\n').trim();
  if (!raw) return '';

  // SMS items are often stored with headers (From/To) plus body after a blank line.
  if (isSmsLikeInboxSource(source)) {
    const blocks = raw.split(/\n\s*\n/).map((s) => s.trim()).filter(Boolean);
    if (blocks.length > 1) return blocks[blocks.length - 1];
    const lines = raw.split('\n').map((s) => s.trim()).filter(Boolean);
    return lines.length ? lines[lines.length - 1] : raw;
  }

  return raw;
}

function tokenizeRecommendationText(text) {
  const raw = String(text || '')
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!raw) return [];
  const stop = new Set([
    'the', 'and', 'for', 'with', 'that', 'this', 'from', 'your', 'you', 'our', 'are', 'was', 'were', 'have', 'has',
    'had', 'can', 'will', 'would', 'should', 'could', 'not', 'but', 'just', 'about', 'into', 'need', 'needs', 'please',
    'thanks', 'thank', 'okay', 'ok', 'yep', 'yup', 'yes', 'no', 'text', 'sms', 'message', 'call', 'email', 'slack',
  ]);
  return raw
    .split(' ')
    .map((w) => w.trim())
    .filter((w) => w.length >= 3 && !stop.has(w));
}

function guessWhoForInboxItem(store, item, projectMatch) {
  const s = store && typeof store === 'object' ? store : {};
  const it = item && typeof item === 'object' ? item : {};
  const senderRaw = String(it?.sender || it?.fromNumber || '').trim();
  const senderDigits = normalizePhoneForLookup(senderRaw);
  const projects = Array.isArray(s.projects) ? s.projects : [];

  if (senderDigits) {
    for (const p of projects) {
      const pDigits = normalizePhoneForLookup(p?.clientPhone || '');
      if (!pDigits) continue;
      if (pDigits === senderDigits || (pDigits.length > 10 && senderDigits.endsWith(pDigits.slice(-10))) || (senderDigits.length > 10 && pDigits.endsWith(senderDigits.slice(-10)))) {
        const clientName = String(p?.clientName || '').trim();
        if (clientName) {
          return {
            name: clientName,
            kind: 'client',
            confidence: 0.92,
            reason: 'Matched sender phone number to project client phone',
          };
        }
      }
    }
  }

  if (projectMatch && projectMatch.projectId) {
    const p = projects.find((x) => String(x?.id || '') === String(projectMatch.projectId || '')) || null;
    if (p) {
      const clientName = String(p?.clientName || '').trim();
      if (clientName) {
        return {
          name: clientName,
          kind: 'client',
          confidence: 0.8,
          reason: 'Inferred from matched project client',
        };
      }
    }
  }

  if (senderRaw.includes('@')) {
    const local = senderRaw.split('@')[0] || '';
    const cleaned = local.replace(/[._-]+/g, ' ').trim();
    if (cleaned) {
      return {
        name: cleaned,
        kind: 'contact',
        confidence: 0.65,
        reason: 'Derived from sender handle',
      };
    }
  }

  if (senderRaw) {
    return {
      name: senderRaw,
      kind: 'contact',
      confidence: 0.55,
      reason: 'Using sender metadata',
    };
  }

  return {
    name: 'Unknown sender',
    kind: 'unknown',
    confidence: 0.2,
    reason: 'No sender metadata available',
  };
}

function inferProjectRecommendationForInboxItem(store, item, signalText) {
  const s = store && typeof store === 'object' ? store : {};
  const it = item && typeof item === 'object' ? item : {};
  const projects = Array.isArray(s.projects) ? s.projects : [];

  const currentProjectId = String(it?.projectId || '').trim();
  if (currentProjectId && currentProjectId !== String(it?.id || '').trim()) {
    const p = projects.find((x) => String(x?.id || '') === currentProjectId) || null;
    return {
      projectId: currentProjectId,
      projectName: String(it?.projectName || p?.name || '').trim(),
      confidence: 1,
      reason: 'Inbox item is already linked',
      action: 'already-linked',
    };
  }

  const senderRaw = String(it?.sender || it?.fromNumber || '').trim();
  if (senderRaw) {
    const bySender = resolveSenderProjectMapping(s, senderRaw);
    if (bySender && bySender.projectId) {
      return {
        projectId: String(bySender.projectId || '').trim(),
        projectName: String(bySender.projectName || '').trim(),
        confidence: 0.9,
        reason: 'Matched sender to existing sender-project mapping',
        action: 'link-project',
      };
    }
  }

  const byText = matchProjectFromText(s, signalText);
  if (byText && byText.id) {
    return {
      projectId: String(byText.id || '').trim(),
      projectName: String(byText.name || '').trim(),
      confidence: 0.83,
      reason: 'Matched project name in message text',
      action: 'link-project',
    };
  }

  const tokens = tokenizeRecommendationText(signalText);
  let best = null;
  for (const p of projects) {
    const bag = `${String(p?.name || '')} ${String(p?.clientName || '')}`.toLowerCase();
    if (!bag.trim()) continue;
    let score = 0;
    for (const t of tokens) {
      if (bag.includes(t)) score += t.length >= 6 ? 2 : 1;
    }
    if (!score) continue;
    if (!best || score > best.score) {
      best = {
        score,
        projectId: String(p?.id || '').trim(),
        projectName: String(p?.name || '').trim(),
      };
    }
  }

  if (best && best.projectId) {
    const confidence = Math.max(0.55, Math.min(0.8, 0.55 + (best.score * 0.05)));
    return {
      projectId: best.projectId,
      projectName: best.projectName,
      confidence,
      reason: 'Matched project/client keywords in message',
      action: 'link-project',
    };
  }

  return {
    projectId: '',
    projectName: '',
    confidence: 0.25,
    reason: 'No strong project match found',
    action: 'create-project',
  };
}

function suggestTasksFromInboxText(signalText, projectName) {
  const text = String(signalText || '').replace(/\s+/g, ' ').trim();
  if (!text) return [];

  const chunks = text
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean)
    .slice(0, 8);

  const taskPhrases = [];
  for (const c of chunks) {
    const lower = c.toLowerCase();
    const actionable = /\b(need|please|can you|follow up|send|call|schedule|review|fix|update|quote|invoice|confirm|ship|deploy|publish|prepare)\b/.test(lower);
    if (!actionable) continue;
    taskPhrases.push(c);
  }

  const cleaned = (taskPhrases.length ? taskPhrases : [text])
    .map((c) => c.replace(/^\W+|\W+$/g, '').trim())
    .filter(Boolean)
    .slice(0, 3);

  const projectHint = String(projectName || '').trim();
  return cleaned.map((phrase, idx) => {
    const titleBase = phrase.length > 110 ? `${phrase.slice(0, 109).trim()}...` : phrase;
    return {
      title: projectHint ? `${titleBase} (${projectHint})` : titleBase,
      priority: idx === 0 ? 1 : 2,
      reason: idx === 0 ? 'Most actionable statement in message' : 'Follow-up action inferred from message',
    };
  });
}

function suggestDelegateForInboxItem(store, signalText, projectRecommendation) {
  const s = store && typeof store === 'object' ? store : {};
  const team = Array.isArray(s.team) ? s.team.filter((m) => String(m?.id || '') !== 'ai') : [];
  if (!team.length) return null;

  const text = String(signalText || '').toLowerCase();
  const projectId = String(projectRecommendation?.projectId || '').trim();
  const project = projectId
    ? (Array.isArray(s.projects) ? s.projects : []).find((p) => String(p?.id || '') === projectId) || null
    : null;

  let best = null;
  for (const member of team) {
    const name = String(member?.name || '').trim();
    if (!name) continue;
    const skillBag = [
      ...((Array.isArray(member?.skills) ? member.skills : []).map((x) => String(x || '').toLowerCase())),
      ...((Array.isArray(member?.abilities) ? member.abilities : []).map((x) => String(x || '').toLowerCase())),
      String(member?.title || '').toLowerCase(),
      name.toLowerCase(),
    ].filter(Boolean);

    let score = 0;
    for (const k of skillBag) {
      if (k && text.includes(k)) score += 2;
    }

    if (project) {
      const owner = String(project?.owner || '').trim().toLowerCase();
      const am = String(project?.accountManagerName || '').trim().toLowerCase();
      if (owner && owner === name.toLowerCase()) score += 3;
      if (am && am === name.toLowerCase()) score += 2;
    }

    if (!best || score > best.score) {
      best = { member, score };
    }
  }

  if (!best || !best.member) return null;
  const confidence = best.score >= 5 ? 0.9 : best.score >= 3 ? 0.75 : 0.6;
  return {
    teamId: String(best.member.id || '').trim(),
    name: String(best.member.name || '').trim(),
    confidence,
    reason: best.score >= 3
      ? 'Best team skill/ownership match for this message'
      : 'Defaulted to strongest available team match',
  };
}

function buildMarcusInboxRecommendation(store, item) {
  const it = item && typeof item === 'object' ? item : {};
  const signalText = extractInboxSignalText(it);
  const project = inferProjectRecommendationForInboxItem(store, it, signalText);
  const who = guessWhoForInboxItem(store, it, project);
  const tasks = suggestTasksFromInboxText(signalText, project?.projectName || it?.projectName || '').slice(0, 3);

  return {
    itemId: String(it?.id || '').trim(),
    source: String(it?.source || '').trim(),
    who,
    project,
    tasks,
    delegate: null,
    signalPreview: previewTextServer(signalText, 140),
    generatedAt: nowIso(),
  };
}

function hasActionCueInText(text) {
  const s = String(text || '').toLowerCase().replace(/\s+/g, ' ').trim();
  if (!s) return false;
  if (s.includes('?')) return true;
  return /\b(need|needs|please|can you|could you|follow up|send|call|schedule|review|fix|update|quote|invoice|confirm|ship|deploy|publish|prepare|asap|urgent|tomorrow|today|deadline|due|assign|delegate)\b/.test(s);
}

function isGenericRadarNoiseText(text) {
  const normalized = normalizeAckSignalText(text);
  if (!normalized) return true;

  const exact = new Set([
    'received', 'delivered', 'seen', 'read', 'noted', 'copy that',
    'message sent', 'sent', 'done thanks', 'ok thanks', 'thanks', 'thank you',
  ]);
  if (exact.has(normalized)) return true;

  if (/^(message|email|sms|text)\s+(sent|received|delivered|read)$/.test(normalized)) return true;
  if (/^(got it|ok|okay|yep|yup|yes|no)(\s+(thanks|thank you|thx|ty))?$/.test(normalized)) return true;

  return false;
}

function shouldSuppressInboxRadarItem(item, settings) {
  const it = item && typeof item === 'object' ? item : {};
  const src = String(it?.source || '').trim().toLowerCase();
  const signal = extractInboxSignalText(it);
  const level = normalizeSmsAckFilterLevel(settings?.smsAckFilterLevel);

  if (src === 'marcus' || src === 'marcus') return true;
  if (isInboxItemExcludedFromMarcus(it, settings)) return true;
  if (isLowSignalAcknowledgementText(signal, level)) return true;

  const isSystemLike = src.includes('system') || src.includes('notification') || src.includes('alert');
  if (isSystemLike && !hasActionCueInText(signal)) return true;

  const compact = String(signal || '').replace(/\s+/g, ' ').trim();
  if (compact.length <= 28 && !hasActionCueInText(compact) && isGenericRadarNoiseText(compact)) return true;

  return false;
}

function collapseSmsInboxThreads(store) {
  const s = store && typeof store === 'object' ? store : structuredClone(EMPTY_STORE);
  const list = Array.isArray(s.inboxItems) ? s.inboxItems : [];
  if (!list.length) return { changed: false, store: s, collapsedThreads: 0, mergedItems: 0 };

  const byKey = new Map();
  for (let i = 0; i < list.length; i++) {
    const it = list[i] && typeof list[i] === 'object' ? list[i] : {};
    if (!isSmsLikeInboxSource(it?.source)) continue;
    const status = String(it?.status || '').trim().toLowerCase();
    if (status === 'archived') continue;
    const from = normalizePhoneForLookup(it?.fromNumber || it?.sender || '');
    const to = normalizePhoneForLookup(it?.toNumber || '');
    const biz = String(it?.businessKey || '').trim();
    const key = `${biz}|${from || 'unknown'}|${to || 'unknown'}`;
    const group = byKey.get(key) || [];
    group.push(i);
    byKey.set(key, group);
  }

  let changed = false;
  let collapsedThreads = 0;
  let mergedItems = 0;
  const removeIdx = new Set();
  const nextList = [...list];

  const parseMs = (it) => {
    const t = String(it?.updatedAt || it?.createdAt || '').trim();
    const ms = Date.parse(t);
    return Number.isFinite(ms) ? ms : 0;
  };

  for (const idxs of byKey.values()) {
    if (!Array.isArray(idxs) || idxs.length <= 1) continue;
    const items = idxs
      .map((idx) => ({ idx, item: nextList[idx] }))
      .filter((x) => x.item && typeof x.item === 'object')
      .sort((a, b) => parseMs(a.item) - parseMs(b.item));
    if (items.length <= 1) continue;

    const keeper = items[items.length - 1];
    const keeperItem = keeper.item;
    const who = String(keeperItem?.contactName || keeperItem?.fromName || keeperItem?.sender || keeperItem?.fromNumber || 'Sender').trim();

    const lines = [];
    for (const row of items) {
      const msg = extractInboxSignalText(row.item);
      if (!msg) continue;
      const stamp = String(row.item?.updatedAt || row.item?.createdAt || '').trim() || nowIso();
      lines.push(`[${stamp}] ${who}: ${msg}`);
    }
    if (!lines.length) continue;

    const merged = normalizeInboxItem({
      ...keeperItem,
      text: lines.join('\n'),
      messageCount: Math.max(Number(keeperItem?.messageCount || 1), lines.length),
      threadKey: String(keeperItem?.threadKey || '').trim() || `sms-thread:${normalizePhoneForLookup(keeperItem?.fromNumber || keeperItem?.sender || '') || 'unknown'}:${normalizePhoneForLookup(keeperItem?.toNumber || '') || 'unknown'}`,
      updatedAt: nowIso(),
      lastMessageAt: String(keeperItem?.updatedAt || keeperItem?.createdAt || '').trim() || nowIso(),
    });

    nextList[keeper.idx] = merged;
    for (const row of items.slice(0, -1)) {
      removeIdx.add(row.idx);
      mergedItems += 1;
    }
    collapsedThreads += 1;
    changed = true;
  }

  if (!changed) return { changed: false, store: s, collapsedThreads: 0, mergedItems: 0 };
  const compact = nextList.filter((_, idx) => !removeIdx.has(idx));
  return {
    changed: true,
    collapsedThreads,
    mergedItems,
    store: {
      ...s,
      revision: Number(s.revision || 0) + 1,
      updatedAt: nowIso(),
      inboxItems: compact,
    },
  };
}

function normalizeInboxItem(input) {
  const i = input && typeof input === 'object' ? input : {};
  const text = normalizeInboxText(i.text);
  const source = typeof i.source === 'string' ? i.source.trim().slice(0, 32) : '';
  const status = safeEnum(i.status, ['New', 'Triaged', 'Done', 'Archived'], 'New');
  const projectId = typeof i.projectId === 'string' ? i.projectId.trim() : '';
  const projectName = typeof i.projectName === 'string' ? i.projectName.trim() : '';
  const createdAt = typeof i.createdAt === 'string' ? i.createdAt : nowIso();
  const updatedAt = typeof i.updatedAt === 'string' ? i.updatedAt : createdAt;
  const converted = i.converted && typeof i.converted === 'object' ? i.converted : {};
  const businessKey = typeof i.businessKey === 'string' ? i.businessKey.trim() : '';
  const businessLabel = typeof i.businessLabel === 'string' ? i.businessLabel.trim() : '';
  const toNumber = typeof i.toNumber === 'string' ? i.toNumber.trim() : '';
  const fromNumber = typeof i.fromNumber === 'string' ? i.fromNumber.trim() : '';
  const sender = typeof i.sender === 'string' ? i.sender.trim() : (fromNumber || '');
  const contactId = typeof i.contactId === 'string' ? i.contactId.trim() : '';
  const contactName = typeof i.contactName === 'string' ? i.contactName.trim().slice(0, 120) : '';
  const fromName = typeof i.fromName === 'string' ? i.fromName.trim().slice(0, 120) : '';
  const threadKey = typeof i.threadKey === 'string' ? i.threadKey.trim().slice(0, 140) : '';
  const messageCountRaw = Number(i.messageCount);
  const messageCount = Number.isFinite(messageCountRaw) ? Math.max(1, Math.min(5000, Math.floor(messageCountRaw))) : 1;
  const channel = typeof i.channel === 'string' ? i.channel.trim().slice(0, 32) : '';
  const lastMessageAt = typeof i.lastMessageAt === 'string' ? i.lastMessageAt : updatedAt;

  return {
    id: typeof i.id === 'string' && i.id.trim() ? i.id.trim() : makeId(),
    text,
    source,
    status,
    projectId,
    projectName,
    businessKey,
    businessLabel,
    toNumber,
    fromNumber,
    sender,
    contactId,
    contactName,
    fromName,
    threadKey,
    messageCount,
    lastMessageAt,
    channel,
    createdAt,
    updatedAt,
    converted,
  };
}

function normalizeTeamMember(input) {
  const m = input && typeof input === 'object' ? input : {};
  const name = typeof m.name === 'string' ? m.name.trim() : '';
  const title = typeof m.title === 'string' ? m.title.trim() : '';
  const skills = Array.isArray(m.skills) ? m.skills.map((s) => String(s || '').trim()).filter(Boolean).slice(0, 32) : [];
  const abilities = Array.isArray(m.abilities) ? m.abilities.map((s) => String(s || '').trim()).filter(Boolean).slice(0, 32) : [];
  const wipLimitRaw = Number(m.wipLimit);
  const wipLimit = Number.isFinite(wipLimitRaw) ? Math.max(0, Math.min(99, Math.floor(wipLimitRaw))) : 0;
  const avatar = typeof m.avatar === 'string' ? m.avatar.trim().slice(0, 3) : '';
  const slackUserId = typeof m.slackUserId === 'string' ? m.slackUserId.trim().slice(0, 120) : '';

  return {
    id: typeof m.id === 'string' && m.id.trim() ? m.id.trim() : makeId(),
    name,
    title,
    skills,
    abilities,
    wipLimit,
    avatar,
    slackUserId,
  };
}

app.get('/api/team', async (req, res) => {
  try {
    const store = await readStore();
    res.json({ ok: true, team: Array.isArray(store.team) ? store.team : [] });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load team' });
  }
});

app.post('/api/team', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const member = normalizeTeamMember(req.body?.member);
  if (!member.name) {
    res.status(400).json({ ok: false, error: 'name is required' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const exists = (Array.isArray(store.team) ? store.team : []).some((t) => String(t?.name || '').trim().toLowerCase() === member.name.toLowerCase());
    if (exists) {
      res.status(400).json({ ok: false, error: 'A team member with that name already exists' });
      return;
    }

    const ts = nowIso();
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      team: [member, ...(Array.isArray(store.team) ? store.team : [])],
    };
    await writeStore(nextStore);
    res.json({ ok: true, store: nextStore });
  });

  await writeLock;
});

app.put('/api/team/:id', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const patch = req.body?.patch && typeof req.body.patch === 'object' ? req.body.patch : {};
  const teamId = String(req.params.id || '').trim();
  if (!teamId) {
    res.status(400).json({ ok: false, error: 'Missing team member id' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const list = Array.isArray(store.team) ? store.team : [];
    const idx = list.findIndex((m) => String(m?.id || '') === teamId);
    if (idx === -1) {
      res.status(404).json({ ok: false, error: 'Team member not found' });
      return;
    }

    const current = list[idx];
    const next = normalizeTeamMember({
      ...current,
      ...patch,
      id: current.id,
    });

    if (!next.name) {
      res.status(400).json({ ok: false, error: 'name is required' });
      return;
    }

    // Name uniqueness (excluding self)
    const nameTaken = list.some((m, i) => i !== idx && String(m?.name || '').trim().toLowerCase() === next.name.toLowerCase());
    if (nameTaken) {
      res.status(400).json({ ok: false, error: 'Another team member already has that name' });
      return;
    }

    const ts = nowIso();
    const nextList = [...list];
    nextList[idx] = next;
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      team: nextList,
    };
    await writeStore(nextStore);
    res.json({ ok: true, store: nextStore });
  });

  await writeLock;
});

app.delete('/api/team/:id', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const teamId = String(req.params.id || '').trim();
  if (!teamId) {
    res.status(400).json({ ok: false, error: 'Missing team member id' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const list = Array.isArray(store.team) ? store.team : [];
    const nextList = list.filter((m) => String(m?.id || '') !== teamId);
    if (nextList.length === list.length) {
      res.status(404).json({ ok: false, error: 'Team member not found' });
      return;
    }

    const ts = nowIso();
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      team: nextList,
    };
    await writeStore(nextStore);
    res.json({ ok: true, store: nextStore });
  });

  await writeLock;
});

app.post('/api/integrations/slack/send-summary', async (req, res) => {
  try {
    const { text, channel } = req.body;
    if (!text) {
      res.status(400).json({ error: 'Missing summary text' });
      return;
    }

    const { botToken } = await getSlackOAuthConfig();
    if (!botToken) {
      res.status(400).json({
        error: 'Slack bot token is not configured. Click “Connect” in Settings → Slack (recommended) or set SLACK_BOT_TOKEN / save slackBotToken.',
      });
      return;
    }

    let targetChannel = typeof channel === 'string' ? channel.trim() : '';
    if (!targetChannel) {
      res.status(400).json({
        error: 'Missing channel. Provide a Slack target like @yourname (DM) or a channel ID like C123... (recommended).',
        hint: 'Tip: easiest is @yourname (DM).',
      });
      return;
    }

    // Slack does NOT allow posting directly to a user ID. For DMs, we must open
    // (or reuse) an IM channel via conversations.open, then post to that channel.
    if (typeof targetChannel === 'string' && targetChannel.trim().startsWith('@')) {
      const username = targetChannel.trim().substring(1).toLowerCase();
      if (!username) {
        res.status(400).json({ error: 'Invalid Slack DM target. Use @username.' });
        return;
      }
      const users = await slackListWorkspaceUsers({ token: botToken });
      const user = users.find((u) =>
        u?.name?.toLowerCase() === username ||
        u?.profile?.display_name?.toLowerCase() === username ||
        u?.profile?.display_name_normalized?.toLowerCase() === username ||
        u?.profile?.real_name?.toLowerCase() === username ||
        u?.profile?.real_name_normalized?.toLowerCase() === username ||
        u?.profile?.email?.toLowerCase().startsWith(username)
      );

      const userId = String(user?.id || '').trim();
      if (userId) {
        const opened = await slackApiPost({
          token: botToken,
          method: 'conversations.open',
          body: { users: userId },
        });
        const dmChannelId = String(opened?.channel?.id || '').trim();
        if (dmChannelId) {
          targetChannel = dmChannelId;
        } else {
          console.warn(`Slack conversations.open returned no channel id for ${targetChannel}`);
          res.status(400).json({ error: 'Slack could not open a DM channel for that user.' });
          return;
        }
      } else {
        res.status(400).json({
          error: `Could not resolve Slack user ${targetChannel}. Make sure the app is installed and has users:read scope (then reinstall).`,
        });
        return;
      }
    }

    if (typeof targetChannel === 'string' && targetChannel.trim().startsWith('#')) {
      const resolvedChannelId = await slackResolveChannelTarget({ token: botToken, target: targetChannel });
      if (!resolvedChannelId) {
        res.status(400).json({
          error: `Could not resolve Slack channel ${targetChannel}. Make sure the bot is installed and has conversations:read scope.`,
        });
        return;
      }
      targetChannel = resolvedChannelId;
    }

    const result = await slackApiPost({
      token: botToken,
      method: 'chat.postMessage',
      body: {
        channel: targetChannel,
        text: text,
      },
    });

    res.json({ ok: true, result });
  } catch (err) {
    const msg = err?.message || 'Slack request failed';
    const status = msg === 'invalid_id_parameter' ? 400 : 500;
    res.status(status).json({ error: msg });
  }
});

app.get('/api/integrations/slack/team-presence', async (req, res) => {
  try {
    const store = await readStore();
    const members = (Array.isArray(store.team) ? store.team : []).filter((m) => String(m?.id || '') !== 'ai');
    const { botToken } = await getSlackOAuthConfig();
    if (!botToken) {
      res.json({ ok: true, connected: false, members: [] });
      return;
    }

    let users = [];
    let directoryError = '';
    try {
      users = await slackListWorkspaceUsers({ token: botToken });
    } catch (err) {
      directoryError = err?.message || 'Failed to load Slack users';
    }

    const linked = members.map((member) => {
      const match = matchSlackUserForTeamMember({ member, users });
      const user = match.user;
      const profile = user && typeof user.profile === 'object' ? user.profile : {};
      const slackUserId = String(user?.id || '').trim();
      const slackLabel = String(profile?.display_name || profile?.real_name || user?.name || slackUserId || '').trim();
      return {
        memberId: String(member?.id || '').trim(),
        memberName: String(member?.name || '').trim(),
        slackUserId,
        slackLabel,
        linked: Boolean(slackUserId),
        matchSource: match.source,
      };
    });

    const uniqueIds = [...new Set(linked.map((x) => x.slackUserId).filter(Boolean))];
    const presenceById = new Map();
    await Promise.all(uniqueIds.map(async (id) => {
      try {
        const data = await slackApiGet({ token: botToken, method: 'users.getPresence', params: { user: id } });
        const presence = String(data?.presence || '').trim().toLowerCase();
        const autoAway = Boolean(data?.auto_away);
        const online = presence === 'active' && !autoAway;
        presenceById.set(id, { online, presence, autoAway });
      } catch {
        presenceById.set(id, { online: null, presence: '' });
      }
    }));

    const out = linked.map((entry) => {
      const p = entry.slackUserId ? presenceById.get(entry.slackUserId) : null;
      return {
        ...entry,
        online: p && Object.prototype.hasOwnProperty.call(p, 'online') ? p.online : null,
        presence: p?.presence || '',
      };
    });

    res.json({
      ok: true,
      connected: true,
      members: out,
      error: directoryError,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load Slack team presence' });
  }
});

async function writeStore(nextStore) {
  const file = getStoreFileForBusiness(getBusinessKeyFromContext());
  await writeStoreFile(file, nextStore);
}

async function writeStoreFile(file, nextStore) {
  await ensureStoreFileExists(file);
  const tmpFile = `${file}.tmp-${crypto.randomBytes(6).toString('hex')}`;
  await fs.writeFile(tmpFile, JSON.stringify(nextStore, null, 2) + '\n', 'utf8');
  await fs.rename(tmpFile, file);
  backupCriticalFiles().catch(() => {
    // backup is best-effort
  });
}

async function writeStoreForBusiness(businessKey, nextStore) {
  const file = getStoreFileForBusiness(businessKey);
  await writeStoreFile(file, nextStore);
}

function nowIso() {
  return new Date().toISOString();
}

function makeId() {
  // short, url-safe id
  return crypto.randomBytes(9).toString('base64url');
}

function safeYmd(input) {
  if (typeof input !== 'string') return '';
  const s = input.trim();
  if (!s) return '';
  // Accept YYYY-MM-DD only
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return '';
  return s;
}

function safeEnum(input, allowed, fallback) {
  const s = typeof input === 'string' ? input.trim() : '';
  if (allowed.includes(s)) return s;
  return fallback;
}

function safeUrl(input) {
  if (typeof input !== 'string') return '';
  const s = input.trim();
  if (!s) return '';
  // Keep it simple: only allow http(s) URLs.
  if (!/^https?:\/\//i.test(s)) return '';
  return s;
}

function normalizeAirtableId(input) {
  const s = String(input || '').trim();
  if (!s) return '';
  // Airtable IDs are typically like appXXXX, tblXXXX, viwXXXX
  return s.slice(0, 64);
}

function normalizeAirtableBusinessConfig(input) {
  const cfg = input && typeof input === 'object' ? input : {};
  const pat = typeof cfg.pat === 'string' ? cfg.pat.trim() : '';
  const baseId = normalizeAirtableId(cfg.baseId);
  const clientsTableId = normalizeAirtableId(cfg.clientsTableId || cfg.tableId || cfg.clientsTable);
  const clientsViewId = normalizeAirtableId(cfg.clientsViewId || cfg.viewId || cfg.clientsView);
  const requestsTableId = normalizeAirtableId(cfg.requestsTableId || cfg.revisionRequestsTableId || cfg.requestsTable);
  const requestsViewId = normalizeAirtableId(cfg.requestsViewId || cfg.revisionRequestsViewId || cfg.requestsView);
  const updatedAt = typeof cfg.updatedAt === 'string' ? cfg.updatedAt : '';
  return { pat, baseId, clientsTableId, clientsViewId, requestsTableId, requestsViewId, updatedAt };
}

function getAirtableConfigForBusiness(settings, businessKey) {
  const s = settings && typeof settings === 'object' ? settings : {};
  const map = s.airtableByBusinessKey && typeof s.airtableByBusinessKey === 'object' ? s.airtableByBusinessKey : {};
  const key = normalizeBusinessKey(businessKey) || DEFAULT_BUSINESS_KEY;
  return normalizeAirtableBusinessConfig(map?.[key] || {});
}

function airtableTokenHint(pat) {
  const t = typeof pat === 'string' ? pat.trim() : '';
  if (!t || t.length < 4) return '';
  return `••••${t.slice(-4)}`;
}

function pickAirtableClientName(fields) {
  const f = fields && typeof fields === 'object' ? fields : {};
  const preferred = ['Client', 'Client Name', 'Name', 'Company', 'Company Name', 'Business', 'Organization'];
  for (const k of preferred) {
    const v = f[k];
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  for (const v of Object.values(f)) {
    if (typeof v === 'string' && v.trim()) return v.trim();
    if (Array.isArray(v) && v.length && typeof v[0] === 'string' && String(v[0]).trim()) return String(v[0]).trim();
  }
  return '';
}

async function airtableListRecords({ pat, baseId, tableId, viewId, maxRecords = 50 } = {}) {
  const token = typeof pat === 'string' ? pat.trim() : '';
  const b = normalizeAirtableId(baseId);
  const t = normalizeAirtableId(tableId);
  const v = normalizeAirtableId(viewId);
  const max = Math.min(200, Math.max(1, Number(maxRecords) || 50));
  if (!token) return { ok: false, error: 'Missing Airtable PAT' };
  if (!b || !t) return { ok: false, error: 'Missing Airtable base/table id' };

  const items = [];
  let offset = '';
  for (let page = 0; page < 5; page++) {
    const params = new URLSearchParams();
    params.set('pageSize', String(Math.min(100, max)));
    if (v) params.set('view', v);
    if (offset) params.set('offset', offset);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(b)}/${encodeURIComponent(t)}?${params.toString()}`;

    const resp = await fetch(url, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      const errObj = data && typeof data === 'object' ? data.error : null;
      const type = typeof errObj?.type === 'string' ? errObj.type.trim() : '';
      const msg = typeof errObj?.message === 'string' ? errObj.message.trim() : (typeof data?.error === 'string' ? String(data.error).trim() : '');
      const baseHint = (resp.status === 401 || resp.status === 403 || type === 'AUTHENTICATION_REQUIRED')
        ? ' (check PAT scopes + that this PAT has access to the base)'
        : '';
      const detail = `${type ? `${type}: ` : ''}${msg || `Airtable request failed (${resp.status})`}${baseHint}`;
      return { ok: false, error: detail };
    }

    const records = Array.isArray(data?.records) ? data.records : [];
    for (const r of records) {
      items.push(r);
      if (items.length >= max) break;
    }

    if (items.length >= max) break;
    offset = typeof data?.offset === 'string' ? data.offset : '';
    if (!offset) break;
  }

  return { ok: true, records: items };
}

function normalizeProject(input) {
  const name = typeof input.name === 'string' ? input.name.trim() : '';
  if (!name) throw new Error('Project name is required');

  const type = safeEnum(input.type, ['Build', 'Rebuild', 'Revision', 'Workflow', 'Cleanup', 'Other'], 'Other');
  const dueDate = safeYmd(input.dueDate);
  const status = safeEnum(input.status, ['Active', 'On Hold', 'Done', 'Archived'], 'Active');

  const accountManagerName = typeof input.accountManagerName === 'string' ? input.accountManagerName.trim() : '';
  const accountManagerEmail = typeof input.accountManagerEmail === 'string' ? input.accountManagerEmail.trim() : '';

  const clientName = typeof input.clientName === 'string' ? input.clientName.trim() : '';
  const clientPhone = typeof input.clientPhone === 'string' ? input.clientPhone.trim() : '';

  const workspacePath = typeof input.workspacePath === 'string' ? input.workspacePath.trim() : '';
  const airtableUrl = typeof input.airtableUrl === 'string' ? input.airtableUrl.trim() : '';
  const driveFolderUrlRaw = safeUrl(input.driveFolderUrl);
  const driveFolderIdRaw = typeof input.driveFolderId === 'string' ? input.driveFolderId.trim() : '';
  const driveFolderId = tryParseDriveFolderId(driveFolderUrlRaw) || tryParseDriveFolderId(driveFolderIdRaw);
  const driveFolderUrl = driveFolderId ? driveFolderUrlFromId(driveFolderId) : driveFolderUrlRaw;

  const projectValue = typeof input.projectValue === 'string' ? input.projectValue.trim() : '';
  const stripeInvoiceUrl = safeUrl(input.stripeInvoiceUrl);
  const repoUrl = safeUrl(input.repoUrl);
  const docsUrl = safeUrl(input.docsUrl);

  const priority = safeEnum(input.priority, ['High', 'Medium', 'Low'], 'Medium');
  const importance = safeEnum(input.importance, ['High', 'Medium', 'Low'], 'Medium');
  const risk = safeEnum(input.risk, ['High', 'Medium', 'Low', 'None'], 'None');
  const agentBrief = typeof input.agentBrief === 'string' ? input.agentBrief.trim() : '';
  const owner = typeof input.owner === 'string' ? input.owner.trim().slice(0, 80) : '';

  return {
    name,
    type,
    dueDate,
    status,
    accountManagerName,
    accountManagerEmail,
    clientName,
    clientPhone,
    workspacePath,
    airtableUrl,
    driveFolderId,
    driveFolderUrl,
    projectValue,
    stripeInvoiceUrl,
    repoUrl,
    docsUrl,
    priority,
    importance,
    risk,
    agentBrief,
    owner,
  };
}

function normalizeTask(input) {
  const title = typeof input.title === 'string' ? input.title.trim() : '';
  if (!title) throw new Error('Title is required');

  const project = typeof input.project === 'string' ? input.project.trim() : 'Other';
  const type = typeof input.type === 'string' ? input.type.trim() : 'Other';
  const owner = typeof input.owner === 'string' ? input.owner.trim() : '';
  const status = typeof input.status === 'string' ? input.status : 'Next';

  const priorityRaw = input.priority;
  const priorityNum = Number(priorityRaw);
  const priority = Number.isFinite(priorityNum) ? Math.min(3, Math.max(1, priorityNum)) : 2;

  const dueDate = typeof input.dueDate === 'string' && input.dueDate ? input.dueDate : '';

  return {
    title,
    project,
    type,
    owner,
    status,
    priority,
    dueDate,
  };
}

function sanitizeTaskRecord(rawTask) {
  const t = rawTask && typeof rawTask === 'object' ? rawTask : {};

  const title = valueToLooseText(t.title).trim()
    || valueToLooseText(t.text).trim()
    || valueToLooseText(t.name).trim()
    || 'Untitled task';

  const project = valueToLooseText(t.project).trim() || 'Other';
  const type = valueToLooseText(t.type).trim() || 'Other';
  const owner = valueToLooseText(t.owner).trim();
  const status = valueToLooseText(t.status).trim() || 'Next';

  const priorityNum = Number(t.priority);
  const priority = Number.isFinite(priorityNum) ? Math.min(3, Math.max(1, Math.floor(priorityNum))) : 2;

  const dueRaw = valueToLooseText(t.dueDate).trim();
  const dueDate = dueRaw ? (safeYmd(dueRaw.slice(0, 10)) || '') : '';

  const id = typeof t.id === 'string' && t.id.trim() ? t.id.trim() : makeId();

  return {
    ...t,
    id,
    title,
    project,
    type,
    owner,
    status,
    priority,
    dueDate,
  };
}

function normKey(input) {
  return String(input || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s_-]+/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseTrackerTime(value) {
  const ms = Date.parse(String(value || '').trim());
  return Number.isFinite(ms) ? ms : 0;
}

function normalizeTrackerDueDate(value) {
  const raw = String(value || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : '';
}

function addDaysToYmd(ymd, days) {
  const base = normalizeTrackerDueDate(ymd);
  if (!base) return '';
  const dt = new Date(`${base}T00:00:00.000Z`);
  if (Number.isNaN(dt.getTime())) return '';
  dt.setUTCDate(dt.getUTCDate() + Number(days || 0));
  return dt.toISOString().slice(0, 10);
}

function isClosedTaskStatus(status) {
  const value = String(status == null ? '' : status).trim().toLowerCase();
  return ['done', 'archived', 'complete', 'completed'].includes(value);
}

function isClosedProjectStatus(status) {
  const value = String(status == null ? '' : status).trim().toLowerCase();
  return ['done', 'archived', 'complete', 'completed'].includes(value);
}

function isPausedProjectStatus(status) {
  return String(status == null ? '' : status).trim().toLowerCase() === 'on hold';
}

function resolveProjectForTaskRecord(task, projectsById, projectsByName) {
  const directId = String(task?.projectId || '').trim();
  if (directId && projectsById.has(directId)) return projectsById.get(directId) || null;

  const projectRaw = String(task?.project || '').trim();
  if (!projectRaw) return null;
  if (projectsById.has(projectRaw)) return projectsById.get(projectRaw) || null;

  const key = normKey(projectRaw);
  if (key && projectsByName.has(key)) return projectsByName.get(key) || null;
  return null;
}

function collectMarcusRelevantSnapshot(store, options = {}) {
  const projects = Array.isArray(store?.projects) ? store.projects : [];
  const tasks = Array.isArray(store?.tasks) ? store.tasks : [];
  const today = normalizeTrackerDueDate(options.today) || new Date().toISOString().slice(0, 10);
  const nowMs = Number.isFinite(Number(options.nowMs)) ? Number(options.nowMs) : Date.now();
  const currentProjectId = String(options.currentProjectId || '').trim();
  const recentCutoffMs = nowMs - (MARCUS_RECENT_ACTIVITY_DAYS * MS_PER_DAY);
  const hardStaleCutoffMs = nowMs - (MARCUS_HARD_STALE_TASK_DAYS * MS_PER_DAY);
  const overdueFloor = addDaysToYmd(today, -MARCUS_OVERDUE_GRACE_DAYS) || today;
  const upcomingCutoff = addDaysToYmd(today, MARCUS_UPCOMING_WINDOW_DAYS) || today;

  const projectsById = new Map();
  const projectsByName = new Map();
  for (const project of projects) {
    const id = String(project?.id || '').trim();
    const nameKey = normKey(project?.name);
    if (id) projectsById.set(id, project);
    if (nameKey && !projectsByName.has(nameKey)) projectsByName.set(nameKey, project);
  }

  const relevantTasks = [];
  const openTasks = [];
  let suppressedTaskCount = 0;

  for (const task of tasks) {
    if (isClosedTaskStatus(task?.status)) continue;
    openTasks.push(task);

    const project = resolveProjectForTaskRecord(task, projectsById, projectsByName);
    const dueDate = normalizeTrackerDueDate(task?.dueDate);
    const taskUpdatedAt = Math.max(parseTrackerTime(task?.updatedAt), parseTrackerTime(task?.createdAt));
    const projectUpdatedAt = Math.max(parseTrackerTime(project?.updatedAt), parseTrackerTime(project?.createdAt));
    const taskMatchesCurrentProject = Boolean(
      currentProjectId && (
        String(task?.projectId || '').trim() === currentProjectId ||
        String(task?.project || '').trim() === currentProjectId ||
        String(project?.id || '').trim() === currentProjectId
      )
    );

    if (project && isClosedProjectStatus(project.status) && !taskMatchesCurrentProject) {
      suppressedTaskCount += 1;
      continue;
    }

    const dueSoon = Boolean(dueDate) && dueDate >= overdueFloor && dueDate <= upcomingCutoff;
    const highPriority = Number(task?.priority) === 1 || String(task?.status || '').trim().toLowerCase() === 'urgent';
    const taskFresh = taskUpdatedAt >= recentCutoffMs;
    const projectFresh = projectUpdatedAt >= recentCutoffMs;
    const hardStale = taskUpdatedAt > 0 && taskUpdatedAt < hardStaleCutoffMs;
    const pausedAndCold = Boolean(project)
      && isPausedProjectStatus(project.status)
      && !taskMatchesCurrentProject
      && !dueSoon
      && !highPriority
      && !taskFresh
      && !projectFresh;
    const relevant = taskMatchesCurrentProject || dueSoon || highPriority || taskFresh || projectFresh;

    if (!relevant || pausedAndCold || (hardStale && !dueSoon && !highPriority && !taskMatchesCurrentProject)) {
      suppressedTaskCount += 1;
      continue;
    }

    relevantTasks.push(task);
  }

  const overdueTasks = relevantTasks.filter((task) => {
    const due = normalizeTrackerDueDate(task?.dueDate);
    return Boolean(due) && due < today;
  });
  const dueTodayTasks = relevantTasks.filter((task) => normalizeTrackerDueDate(task?.dueDate) === today);
  const sortedTasks = relevantTasks
    .slice()
    .sort((a, b) => {
      const apRaw = Number(a?.priority);
      const bpRaw = Number(b?.priority);
      const ap = Number.isFinite(apRaw) ? apRaw : 2;
      const bp = Number.isFinite(bpRaw) ? bpRaw : 2;
      if (ap !== bp) return ap - bp;
      const ad = normalizeTrackerDueDate(a?.dueDate) || '9999-12-31';
      const bd = normalizeTrackerDueDate(b?.dueDate) || '9999-12-31';
      if (ad !== bd) return ad.localeCompare(bd);
      return String(b?.updatedAt || b?.createdAt || '').localeCompare(String(a?.updatedAt || a?.createdAt || ''));
    });

  return {
    openTasks,
    relevantTasks,
    overdueTasks,
    dueTodayTasks,
    sortedTasks,
    suppressedTaskCount,
  };
}

function getLinkedProjectTasks(store, project) {
  const tasks = Array.isArray(store?.tasks) ? store.tasks : [];
  const projectId = String(project?.id || '').trim();
  const projectName = String(project?.name || '').trim();
  return tasks.filter((task) => {
    const taskProjectId = String(task?.projectId || '').trim();
    const taskProject = String(task?.project || '').trim();
    return (projectId && (taskProjectId === projectId || taskProject === projectId)) || (projectName && taskProject === projectName);
  });
}

function getLinkedProjectInboxItems(store, project) {
  const list = Array.isArray(store?.inboxItems) ? store.inboxItems : [];
  const projectId = String(project?.id || '').trim();
  const projectName = String(project?.name || '').trim();
  return list.filter((item) => {
    const linkedProjectId = String(item?.projectId || '').trim();
    const linkedProjectName = String(item?.projectName || '').trim();
    return (projectId && linkedProjectId === projectId) || (!linkedProjectId && projectName && linkedProjectName === projectName);
  });
}

function computeProjectLastActivityMs(store, project, linkedTasks = [], linkedInboxItems = []) {
  const marks = [];
  const push = (value) => {
    const ms = parseTrackerTime(value);
    if (ms > 0) marks.push(ms);
  };

  push(project?.updatedAt);
  push(project?.createdAt);
  push(store?.projectScratchpads?.[project?.id]?.updatedAt);
  push(store?.projectChats?.[project?.id]?.updatedAt);

  const chatMessages = Array.isArray(store?.projectChats?.[project?.id]?.messages)
    ? store.projectChats[project.id].messages
    : (Array.isArray(store?.projectChats?.[project?.id]) ? store.projectChats[project.id] : []);
  for (const message of chatMessages) push(message?.timestamp);

  const noteEntries = Array.isArray(store?.projectNoteEntries?.[project?.id]) ? store.projectNoteEntries[project.id] : [];
  for (const note of noteEntries) {
    push(note?.createdAt);
    push(note?.date);
  }

  const communications = Array.isArray(store?.projectCommunications?.[project?.id]) ? store.projectCommunications[project.id] : [];
  for (const comm of communications) {
    push(comm?.createdAt);
    push(comm?.date);
  }

  for (const task of linkedTasks) {
    push(task?.updatedAt);
    push(task?.createdAt);
    push(task?.dueDate);
  }

  for (const item of linkedInboxItems) {
    push(item?.updatedAt);
    push(item?.createdAt);
  }

  if (!marks.length) return 0;
  return Math.max(...marks);
}

function messageNeedsProjectContext(message) {
  const msg = normKey(message);
  if (!msg) return false;
  if (/\b(project|task|tasks|scope|due|deadline|owner|assign|assigned|move|moving|moved|archive|archived|delete|deleted|open|show|status|notes|scratchpad|brief|launch|repo|docs|invoice|client|workspace)\b/.test(msg)) {
    return true;
  }
  return /\b(create|add|update|change|set|move|archive|delete|open|show|review|summarize|plan|assign|link)\b/.test(msg)
    && /\b(for|in|on|to)\b/.test(msg);
}

function appendTasksToStore(store, projectName, tasks) {
  if (!store || typeof store !== 'object') throw new Error('Store missing');
  if (!Array.isArray(tasks) || tasks.length === 0) return { ok: true, created: 0, tasks: [] };
  const now = nowIso();
  const created = tasks
    .map((t) => ({
      title: typeof t?.title === 'string' ? t.title : '',
      priority: Number(t?.priority),
      dueDate: typeof t?.dueDate === 'string' ? safeYmd(t.dueDate) : '',
    }))
    .filter((t) => String(t.title).trim())
    .map((t) => {
      const normalized = normalizeTask({
        title: t.title,
        status: 'Next',
        priority: Number.isFinite(t.priority) ? t.priority : 2,
        project: projectName,
        dueDate: t.dueDate,
      });
      return {
        id: makeId(),
        ...normalized,
        createdAt: now,
        updatedAt: now,
      };
    });

  store.tasks = [...created, ...(store.tasks || [])];
  return { ok: true, created: created.length, tasks: created };
}

function computeLearnedTaskTemplates(store) {
  const byType = {};
  const projects = Array.isArray(store?.projects) ? store.projects : [];
  const tasks = Array.isArray(store?.tasks) ? store.tasks : [];

  const tasksByProjectKey = new Map();
  for (const t of tasks) {
    const pk = normKey(t?.project);
    if (!pk) continue;
    const list = tasksByProjectKey.get(pk) || [];
    list.push(t);
    tasksByProjectKey.set(pk, list);
  }

  for (const p of projects) {
    const type = safeEnum(p?.type, ['Build', 'Rebuild', 'Revision', 'Workflow', 'Cleanup', 'Other'], 'Other');
    const pk = normKey(p?.name);
    if (!pk) continue;
    const list = tasksByProjectKey.get(pk) || [];
    for (const t of list) {
      const title = String(t?.title || '').trim();
      if (!title) continue;
      const key = normKey(title);
      if (!key) continue;
      byType[type] = byType[type] || {};
      byType[type][key] = byType[type][key] || { title, count: 0 };
      const isDone = String(t?.status || '').trim().toLowerCase() === 'done';
      byType[type][key].count += isDone ? 3 : 1;
    }
  }

  const compact = {};
  for (const [type, rec] of Object.entries(byType)) {
    const arr = Object.values(rec)
      .sort((a, b) => (b.count - a.count) || a.title.localeCompare(b.title))
      .slice(0, 40);
    compact[type] = arr;
  }
  return { updatedAt: nowIso(), byType: compact };
}

function baselineTasksForType(type) {
  const t = safeEnum(type, ['Build', 'Rebuild', 'Revision', 'Workflow', 'Cleanup', 'Other'], 'Other');
  const common = [
    { title: 'Confirm scope + success criteria', priority: 1 },
    { title: 'Collect access + credentials', priority: 1 },
    { title: 'Set up repo + local workspace', priority: 2 },
    { title: 'Create timeline + milestones', priority: 2 },
    { title: 'Kickoff call agenda + notes', priority: 2 },
  ];
  const byType = {
    Build: [
      { title: 'Define sitemap / information architecture', priority: 2 },
      { title: 'Create wireframes / layout plan', priority: 2 },
      { title: 'Implement core pages + navigation', priority: 1 },
      { title: 'Analytics + conversion tracking', priority: 3 },
      { title: 'QA pass (mobile + desktop)', priority: 1 },
      { title: 'Launch checklist + deploy', priority: 1 },
    ],
    Rebuild: [
      { title: 'Audit existing site + pain points', priority: 1 },
      { title: 'Migration plan (content, redirects)', priority: 1 },
      { title: 'Implement rebuild in staging', priority: 1 },
      { title: 'Redirects + SEO validation', priority: 1 },
      { title: 'QA pass + launch', priority: 1 },
    ],
    Revision: [
      { title: 'Gather requested changes', priority: 1 },
      { title: 'Implement revisions in staging', priority: 1 },
      { title: 'Client review + iterate', priority: 2 },
      { title: 'Deploy revisions', priority: 1 },
    ],
    Workflow: [
      { title: 'Map current workflow', priority: 1 },
      { title: 'Define target workflow', priority: 1 },
      { title: 'Implement automation / SOP', priority: 2 },
      { title: 'Pilot + refine', priority: 2 },
    ],
    Cleanup: [
      { title: 'Inventory issues / technical debt', priority: 1 },
      { title: 'Prioritize fixes', priority: 1 },
      { title: 'Fix high-impact issues', priority: 1 },
      { title: 'Regression test', priority: 2 },
    ],
    Other: [
      { title: 'Define next 3 outcomes', priority: 2 },
      { title: 'Break down work into tasks', priority: 2 },
      { title: 'Schedule review checkpoint', priority: 3 },
    ],
  };
  return [...common, ...(byType[t] || [])];
}

function buildStarterTaskSuggestions(store, project, limit = 12) {
  const learned = computeLearnedTaskTemplates(store);
  store.learnedTaskTemplates = learned;

  const type = safeEnum(project?.type, ['Build', 'Rebuild', 'Revision', 'Workflow', 'Cleanup', 'Other'], 'Other');
  const existing = new Set(
    (Array.isArray(store?.tasks) ? store.tasks : [])
      .filter((t) => normKey(t?.project) === normKey(project?.name))
      .map((t) => normKey(t?.title))
      .filter(Boolean)
  );

  const baseline = baselineTasksForType(type);
  const learnedTitles = (learned.byType?.[type] || []).map((x) => ({ title: x.title, priority: 2 }));
  const candidates = [...baseline, ...learnedTitles];

  const deduped = [];
  const seen = new Set();
  for (const c of candidates) {
    const title = String(c?.title || '').trim();
    const k = normKey(title);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    if (existing.has(k)) continue;
    deduped.push({ title, priority: Number(c?.priority) || 2 });
    if (deduped.length >= limit) break;
  }

  return { type, tasks: deduped };
}

function getProjectChatArray(store, projectId) {
  store.projectChats = store.projectChats || {};
  const existing = store.projectChats[projectId];

  // Canonical store shape: { messages: [], updatedAt: '' }
  if (Array.isArray(existing)) {
    // Migrate legacy array-in-store to object form.
    const migrated = { messages: existing, updatedAt: store.updatedAt || '' };
    store.projectChats[projectId] = migrated;
    return migrated.messages;
  }

  if (existing && typeof existing === 'object' && Array.isArray(existing.messages)) {
    return existing.messages;
  }

  const created = { messages: [], updatedAt: '' };
  store.projectChats[projectId] = created;
  return created.messages;
}

function resolveProjectForMessage(store, message, projectId) {
  const projects = Array.isArray(store?.projects) ? store.projects : [];
  if (projectId) {
    const direct = projects.find((p) => String(p?.id || '') === projectId);
    if (direct) return direct;
  }
  const msg = normKey(message);
  if (!msg) return null;
  if (!messageNeedsProjectContext(message)) return null;

  const scored = [];
  for (const p of projects) {
    const closed = isClosedProjectStatus(p?.status);
    const name = String(p?.name || '').trim();
    if (!name) continue;
    const nameKey = normKey(name);
    if (!nameKey) continue;

    // Avoid nagging about old projects: only consider closed projects when the
    // user explicitly types the full project name.
    if (closed && !msg.includes(nameKey)) continue;

    let score = 0;
    if (msg.includes(nameKey)) score = 100 + nameKey.length; // strong signal
    else {
      const tokens = nameKey.split(' ').filter(Boolean);
      const hits = tokens.filter((tok) => tok.length >= 3 && msg.includes(tok)).length;
      score = hits / Math.max(2, tokens.length);
    }
    scored.push({ p, score });
  }

  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];
  const second = scored[1];
  if (!best) return null;
  if (best.score >= 120) return best.p;
  if (best.score < 0.4) return null;
  if (second && second.score >= 0.4 && Math.abs(best.score - second.score) < 0.15) {
    // ambiguous; let caller ask
    return { ambiguous: true, options: [best.p, second.p] };
  }
  return best.p;
}

function tryHandleDeterministicTaskRequest(store, message, projectId) {
  const raw = String(message || '').trim();
  const msg = raw.toLowerCase();
  const mentionsTasks = /\btasks?\b/.test(msg) || /\bchecklist\b/.test(msg) || /\bto-?dos?\b/.test(msg);
  if (!mentionsTasks) return null;

  const wantsCreate = /\b(create|add|generate|make|spin up|set up)\b/.test(msg) && (/(\btasks?\b|\bchecklist\b|\bto-?dos?\b)/.test(msg));
  const wantsSuggest = /\b(suggest|recommend|what (tasks|to-?dos)|ideas|starter)\b/.test(msg) && mentionsTasks;
  if (!wantsCreate && !wantsSuggest) return null;

  const resolved = resolveProjectForMessage(store, raw, projectId);
  if (resolved && typeof resolved === 'object' && resolved.ambiguous) {
    const opts = Array.isArray(resolved.options) ? resolved.options : [];
    const list = opts.map((p) => `- ${p.name}`).join('\n');
    return {
      handled: true,
      reply: `Which project did you mean?\n${list}`,
    };
  }
  const project = resolved && typeof resolved === 'object' ? resolved : null;
  if (!project) {
    const active = (Array.isArray(store?.projects) ? store.projects : [])
      .filter((p) => !isClosedProjectStatus(p?.status))
      .slice(0, 12)
      .map((p) => `- ${p.name}`)
      .join('\n');
    return {
      handled: true,
      reply:
        "Which project should I use?\n\nReply with something like: 'Create tasks for <project name>'.\n\nActive projects:\n" +
        (active || '- (none)')
    };
  }

  const learned = computeLearnedTaskTemplates(store);
  store.learnedTaskTemplates = learned;
  const type = safeEnum(project.type, ['Build', 'Rebuild', 'Revision', 'Workflow', 'Cleanup', 'Other'], 'Other');

  const existing = new Set(
    (Array.isArray(store?.tasks) ? store.tasks : [])
      .filter((t) => normKey(t?.project) === normKey(project.name))
      .map((t) => normKey(t?.title))
      .filter(Boolean)
  );

  const baseline = baselineTasksForType(type);
  const learnedTitles = (learned.byType?.[type] || []).map((x) => ({ title: x.title, priority: 2 }));
  const candidates = [...baseline, ...learnedTitles];

  const deduped = [];
  const seen = new Set();
  for (const c of candidates) {
    const title = String(c?.title || '').trim();
    const k = normKey(title);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    if (existing.has(k)) continue;
    deduped.push({ title, priority: Number(c?.priority) || 2 });
    if (deduped.length >= 12) break;
  }

  if (deduped.length === 0) {
    return {
      handled: true,
      reply: `"${project.name}" already has the usual starter tasks for a ${type} project. Tell me what�s missing and I�ll add it.`
    };
  }

  if (wantsSuggest && !wantsCreate) {
    const lines = deduped.map((t, i) => `${i + 1}. [P${t.priority}] ${t.title}`);
    return {
      handled: true,
      reply:
        `Starter tasks for "${project.name}" (${type}):\n` +
        lines.join('\n') +
        `\n\nSay: "Create these tasks" to add them.`
    };
  }

  const result = appendTasksToStore(store, project.name, deduped);
  const createdLines = (result.tasks || []).map((t) => `- [P${t.priority}] ${t.title}`);
  return {
    handled: true,
    reply: `Created ${result.created} tasks for "${project.name}":\n${createdLines.join('\n')}`,
  };
}

function normalizeNotes(input) {
  if (typeof input !== 'string') return '';
  return input.replace(/\r\n/g, '\n').trimEnd();
}

function projectKeyFromParam(raw) {
  const decoded = decodeURIComponent(String(raw ?? ''));
  return decoded.trim();
}

function pickProjectNotesValue(entry) {
  if (!entry) return { notes: '', updatedAt: '' };
  if (typeof entry === 'string') return { notes: entry, updatedAt: '' };
  if (typeof entry === 'object') {
    return {
      notes: typeof entry.notes === 'string' ? entry.notes : '',
      updatedAt: typeof entry.updatedAt === 'string' ? entry.updatedAt : '',
    };
  }
  return { notes: '', updatedAt: '' };
}

async function aiNextActions({ project, notes, tasks }) {
  const settings = await readSettings();
  const route = resolveAiRoute(settings, 'projectAssistant');
  if (!route.apiKey) {
    const lines = String(notes || '')
      .split('\n')
      .map((l) => l.trim())
      .filter(Boolean);

    const bullets = lines
      .map((l) => l.replace(/^[-*�]\s+/, '').replace(/^\d+\.\s+/, '').trim())
      .filter((l) => l.length >= 6)
      .slice(0, 8);

    const open = (Array.isArray(tasks) ? tasks : []).filter((t) => String(t.status || '').toLowerCase() !== 'done');
    const top = open
      .slice()
      .sort((a, b) => Number(a.priority ?? 2) - Number(b.priority ?? 2))
      .slice(0, 5)
      .map((t) => t.title);

    const out = [];
    out.push(`Next actions for: ${project || 'Selected project'}`);
    out.push('');
    if (bullets.length) {
      out.push('From your notes:');
      bullets.forEach((b, i) => out.push(`${i + 1}. ${b}`));
      out.push('');
    }
    if (top.length) {
      out.push('From your current tasks (highest priority):');
      top.forEach((t, i) => out.push(`${i + 1}. ${t}`));
      out.push('');
    }
    out.push('If you want real AI suggestions, set an API key in Settings → AI (OpenAI or OpenRouter) and restart the server if needed.');
    return out.join('\n');
  }

  const safeNotes = String(notes || '').slice(0, 8000);
  const safeTasks = (Array.isArray(tasks) ? tasks : []).slice(0, 60).map((t) => ({
    title: t.title,
    status: t.status,
    priority: t.priority,
    dueDate: t.dueDate,
    owner: t.owner,
    type: t.type,
  }));

  const result = await aiChatCompletion({
    routeKey: 'projectAssistant',
    messages: [
      {
        role: 'system',
        content:
          'You are an operations assistant. Generate 5-10 next actions that keep momentum. Output a concise numbered list. Each item must start with [P1], [P2], or [P3]. Include a suggested due date only when obvious. No extra commentary.',
      },
      {
        role: 'user',
        content: JSON.stringify(
          {
            project,
            notes: safeNotes,
            currentTasks: safeTasks,
          },
          null,
          2,
        ),
      },
    ],
  });
  if (!result.ok) throw new Error(result.error || 'AI request failed');

  const content = result.message?.content;
  if (typeof content !== 'string' || !content.trim()) {
    throw new Error('AI returned no content');
  }
  return content.trim();
}

async function aiProjectAssistant({ project, scratchpad, noteEntries, communications, chatMessages }) {
  const settings = await readSettings();
  const operatorBio = typeof settings.operatorBio === 'string' ? settings.operatorBio.trimEnd() : '';
  const legacyHelpPrompt = typeof settings.operatorHelpPrompt === 'string' ? settings.operatorHelpPrompt.trimEnd() : '';
  const assistantOperatingDoctrineRaw = typeof settings.assistantOperatingDoctrine === 'string' ? settings.assistantOperatingDoctrine.trimEnd() : '';
  const assistantOperatingDoctrine = assistantOperatingDoctrineRaw || legacyHelpPrompt;
  const personalityLayer = typeof settings.personalityLayer === 'string' ? settings.personalityLayer.trimEnd() : '';
  const attentionRadar = typeof settings.attentionRadar === 'string' ? settings.attentionRadar.trimEnd() : '';
  const dailyReportingStructure = typeof settings.dailyReportingStructure === 'string' ? settings.dailyReportingStructure.trimEnd() : '';
  const operatorTone = typeof settings.operatorTone === 'string' ? settings.operatorTone.trim() : '';
  const operatorVoice = typeof settings.operatorVoice === 'string' ? settings.operatorVoice.trim() : '';

  const projectName = project?.name || '';
  const projectType = project?.type || '';
  const projectDue = project?.dueDate || '';
  const projectStatus = project?.status || 'Active';
  const accountManagerName = project?.accountManagerName || '';

  const recentNotes = Array.isArray(noteEntries) ? noteEntries.slice(0, 6) : [];
  const recentComms = Array.isArray(communications) ? communications.slice(0, 8) : [];
  const recentChat = Array.isArray(chatMessages) ? chatMessages.slice(-16) : [];

  const route = resolveAiRoute(settings, 'projectAssistant');
  if (!route.apiKey) {
    const lastUser = [...recentChat].reverse().find((m) => m.role === 'user')?.content || '';
    const lines = [];
    lines.push(`I don't have real AI enabled (OPENAI_API_KEY not set).`);
    lines.push(`Project: ${projectName}${projectType ? ` (${projectType})` : ''}${projectDue ? ` due ${projectDue}` : ''} � ${projectStatus}`);
    if (accountManagerName) lines.push(`Account manager: ${accountManagerName}`);
    lines.push('');

    if (lastUser) {
      lines.push('You asked:');
      lines.push(lastUser);
      lines.push('');
    }

    lines.push('Quick next actions you can take right now:');
    lines.push('1. Identify the single blocker and write it as 1 sentence.');
    lines.push('2. Write a 3-bullet client update (what changed / what you need / ETA).');
    lines.push('3. Add 1-3 concrete deliverables to the scratchpad with owners.');
    lines.push('');
    lines.push('To enable real AI, add an API key in Settings → AI (OpenAI or OpenRouter).');
    return { content: lines.join('\n'), tasks: [] };
  }

  const context = {
    operatorBio: operatorBio ? operatorBio.slice(0, 12000) : '',
    assistantOperatingDoctrine: assistantOperatingDoctrine ? assistantOperatingDoctrine.slice(0, 12000) : '',
    personalityLayer: personalityLayer ? personalityLayer.slice(0, 12000) : '',
    attentionRadar: attentionRadar ? attentionRadar.slice(0, 12000) : '',
    dailyReportingStructure: dailyReportingStructure ? dailyReportingStructure.slice(0, 12000) : '',
    // Legacy fields (kept for backward compatibility / easier migrations).
    operatorHelpPrompt: assistantOperatingDoctrine ? assistantOperatingDoctrine.slice(0, 12000) : (legacyHelpPrompt ? legacyHelpPrompt.slice(0, 12000) : ''),
    operatorTone: operatorTone || '',
    operatorVoice: operatorVoice || '',
    project: {
      name: projectName,
      type: projectType,
      dueDate: projectDue,
      status: projectStatus,
      accountManagerName,
      accountManagerEmail: project?.accountManagerEmail || '',
    },
    scratchpad: String(scratchpad || '').slice(0, 8000),
    recentNotes: recentNotes.map((n) => ({
      kind: n.kind,
      date: n.date,
      title: n.title,
      content: String(n.content || '').slice(0, 2000),
    })),
    recentCommunications: recentComms.map((c) => ({
      type: c.type,
      direction: c.direction,
      subject: c.subject,
      date: c.date,
      body: String(c.body || '').slice(0, 2000),
    })),
  };

  const messages = [
    {
      role: 'system',
      content:
        'You are a project operations assistant. Stay concise and action-oriented. Maintain context for this specific project only. If asked to draft a message to the account manager, produce (1) a short client-ready update and (2) internal next steps. Prefer bullet points. Do not hallucinate facts; ask questions when needed.',
    },
    {
      role: 'user',
      content: `Project context (JSON):\n${JSON.stringify(context, null, 2)}`,
    },
    ...recentChat.map((m) => ({
      role: m.role,
      content: String(m.content || '').slice(0, 4000),
    })),
  ];

  const tools = [
    {
      type: 'function',
      function: {
        name: 'create_tasks',
        description: 'Create new tasks in the project tracker.',
        parameters: {
          type: 'object',
          properties: {
            tasks: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  title: { type: 'string', description: 'Brief actionable title' },
                  priority: { type: 'number', enum: [1, 2, 3], description: '1=High, 2=Medium, 3=Low' },
                  dueDate: { type: 'string', description: 'YYYY-MM-DD format' },
                },
                required: ['title', 'priority'],
              },
            },
          },
          required: ['tasks'],
        },
      },
    },
  ];

  const result = await aiChatCompletion({
    routeKey: 'projectAssistant',
    messages,
    tools,
    tool_choice: 'auto',
  });
  if (!result.ok) throw new Error(result.error || 'AI request failed');
  const msg = result.message;
  
  if (!msg) {
    throw new Error('AI returned no content');
  }

  let finalContent = msg.content || '';
  const newTasks = [];

  if (msg.tool_calls) {
    for (const toolCall of msg.tool_calls) {
      if (toolCall.function.name === 'create_tasks') {
        try {
          const args = JSON.parse(toolCall.function.arguments);
          if (Array.isArray(args.tasks)) {
            newTasks.push(...args.tasks);
          }
        } catch (e) {
          console.error('Failed to parse create_tasks arguments', e);
        }
      }
    }
  }

  if (newTasks.length > 0) {
    const taskSummary = newTasks.map(t => `- ${t.title} (P${t.priority})`).join('\n');
    if (!finalContent) {
        finalContent = `I've created the following tasks:\n${taskSummary}`;
    } else {
        finalContent += `\n\nI also created these tasks:\n${taskSummary}`;
    }
  }

  return { content: finalContent.trim(), tasks: newTasks };
}

function safeParseJsonObject(text) {
  const s = typeof text === 'string' ? text.trim() : '';
  if (!s) return null;
  const first = s.indexOf('{');
  const last = s.lastIndexOf('}');
  if (first === -1 || last === -1 || last <= first) return null;
  const candidate = s.slice(first, last + 1);
  try {
    const parsed = JSON.parse(candidate);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    return parsed;
  } catch {
    return null;
  }
}

function normalizeTranscript(text) {
  if (typeof text !== 'string') return '';
  return text.replace(/\r\n/g, '\n').trim();
}

function heuristicallyExtractActionItems(transcript) {
  const lines = String(transcript || '')
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean);

  const items = [];
  const pushTitle = (title) => {
    const t = String(title || '').trim();
    if (!t) return;
    if (t.length < 6) return;
    if (items.some((x) => x.title.toLowerCase() === t.toLowerCase())) return;
    items.push({ title: t, priority: 2 });
  };

  for (const l of lines) {
    if (/^(action items?|actions?)\s*:/i.test(l)) continue;
    if (/^(todo|to-do)\s*:/i.test(l)) {
      pushTitle(l.replace(/^(todo|to-do)\s*:\s*/i, ''));
      continue;
    }
    if (/^[-*�]\s+/.test(l)) {
      pushTitle(l.replace(/^[-*�]\s+/, ''));
      continue;
    }
    if (/^\d+\.\s+/.test(l)) {
      pushTitle(l.replace(/^\d+\.\s+/, ''));
      continue;
    }
    if (/\bwe need to\b/i.test(l) || /\blet's\b/i.test(l) || /\bplease\b/i.test(l) || /\bfollow up\b/i.test(l)) {
      pushTitle(l);
      continue;
    }
  }

  return items.slice(0, 12);
}

async function aiTranscriptProposal({ project, transcript, tasks, noteEntries }) {
  const settings = await readSettings();
  const route = resolveAiRoute(settings, 'projectAssistant');

  const safeTranscript = normalizeTranscript(transcript).slice(0, 20000);
  const safeTasks = (Array.isArray(tasks) ? tasks : []).slice(0, 40).map((t) => ({
    title: t.title,
    status: t.status,
    priority: t.priority,
    dueDate: t.dueDate,
    owner: t.owner,
  }));
  const safeNotes = (Array.isArray(noteEntries) ? noteEntries : []).slice(0, 6).map((n) => ({
    kind: n.kind,
    date: n.date,
    title: n.title,
    content: String(n.content || '').slice(0, 800),
  }));

  if (!route.apiKey) {
    const actionItems = heuristicallyExtractActionItems(safeTranscript);
    const subject = `Update: ${project?.name || 'Project'}`;
    const recapLines = [];
    recapLines.push('Quick update:');
    recapLines.push('');
    recapLines.push('What we covered: (imported transcript � review)');
    recapLines.push('');
    if (actionItems.length) {
      recapLines.push('Next steps:');
      actionItems.slice(0, 8).forEach((a) => recapLines.push(`- ${a.title}`));
      recapLines.push('');
    }
    recapLines.push('Reply with anything I missed.');

    return {
      ok: true,
      proposal: {
        summary: 'Transcript imported. Review proposed next steps.',
        decisions: [],
        actionItems,
        recapSubject: subject,
        recapBody: recapLines.join('\n').trimEnd(),
        internalNote: 'Imported transcript (AI disabled). Confirm action items and send recap.',
        meta: { source: 'heuristic' },
      },
    };
  }

  const result = await aiChatCompletion({
    routeKey: 'projectAssistant',
    messages: [
      {
        role: 'system',
        content:
          'You are an operations assistant. Convert a meeting transcript into an actionable proposal. Return ONLY valid JSON with keys: summary (string), decisions (string[]), actionItems (array of {title, owner?, dueDate?, priority?}), recapSubject (string), recapBody (string), internalNote (string). Priority is 1,2,3. dueDate must be YYYY-MM-DD or empty. Keep it concise and non-hallucinatory; if unknown, omit owner/dueDate.',
      },
      {
        role: 'user',
        content: JSON.stringify(
          {
            project: {
              name: project?.name || '',
              type: project?.type || '',
              dueDate: project?.dueDate || '',
              status: project?.status || '',
            },
            existingTasks: safeTasks,
            recentNotes: safeNotes,
            transcript: safeTranscript,
          },
          null,
          2,
        ),
      },
    ],
    timeoutMs: 30_000,
  });

  if (!result.ok) {
    return { ok: false, error: result.error || 'AI request failed' };
  }

  const content = result.message?.content;
  const parsed = safeParseJsonObject(typeof content === 'string' ? content : '');
  if (!parsed) {
    return { ok: false, error: 'AI returned non-JSON output. Try again or shorten the transcript.' };
  }

  const decisions = Array.isArray(parsed.decisions) ? parsed.decisions.map((d) => String(d || '').trim()).filter(Boolean).slice(0, 12) : [];
  const actionItemsRaw = Array.isArray(parsed.actionItems) ? parsed.actionItems : [];
  const actionItems = actionItemsRaw
    .map((a) => ({
      title: typeof a?.title === 'string' ? a.title.trim() : '',
      owner: typeof a?.owner === 'string' ? a.owner.trim() : '',
      dueDate: safeYmd(a?.dueDate) || '',
      priority: [1, 2, 3].includes(Number(a?.priority)) ? Number(a.priority) : 2,
    }))
    .filter((a) => a.title)
    .slice(0, 20);

  return {
    ok: true,
    proposal: {
      summary: typeof parsed.summary === 'string' ? parsed.summary.trim() : 'Transcript summary',
      decisions,
      actionItems,
      recapSubject: typeof parsed.recapSubject === 'string' ? parsed.recapSubject.trim() : `Update: ${project?.name || 'Project'}`,
      recapBody: typeof parsed.recapBody === 'string' ? parsed.recapBody.trimEnd() : '',
      internalNote: typeof parsed.internalNote === 'string' ? parsed.internalNote.trimEnd() : '',
      meta: { source: result.provider || 'ai' },
    },
  };
}

// Basic no-cache so the browser doesn't fight OneDrive syncing.
app.use((req, res, next) => {
  res.setHeader('Cache-Control', 'no-store');
  next();
});

app.use(express.static(path.join(process.cwd(), 'public')));

// Settings
app.get('/api/settings', async (req, res) => {
  const settings = await readSettings();
  const safe = sanitizeSettingsForClient(settings);

  const { apiKey, model, source, keyHint, settingsUpdatedAt } = await getAiConfig();
  const openrouter = getOpenRouterSecrets(settings);
  const aiRoutes = normalizeAiRoutes(settings?.aiRoutes);

  const anyAiEnabled = Boolean(
    getOpenAiSecrets(settings).apiKey ||
    openrouter.apiKey,
  );

  // Integration hints for the Settings UI.
  const envGoogleClientId = typeof process.env.GOOGLE_CLIENT_ID === 'string' ? process.env.GOOGLE_CLIENT_ID.trim() : '';
  const envGoogleClientSecret = typeof process.env.GOOGLE_CLIENT_SECRET === 'string' ? process.env.GOOGLE_CLIENT_SECRET.trim() : '';
  const savedGoogleClientId = typeof settings.googleClientId === 'string' ? settings.googleClientId.trim() : '';
  const savedGoogleClientSecret = typeof settings.googleClientSecret === 'string' ? settings.googleClientSecret.trim() : '';
  const effectiveGoogleClientId = envGoogleClientId || savedGoogleClientId;
  const googleConfigured = Boolean(isLikelyGoogleClientId(effectiveGoogleClientId));
  const googleConnected = Boolean(settings.googleTokens && typeof settings.googleTokens === 'object' && settings.googleTokens.refresh_token);
  const envFirefliesSecret =
    (typeof process.env.FIREFLIES_SECRET === 'string' ? process.env.FIREFLIES_SECRET.trim() : '') ||
    (typeof process.env.FIREFLIES_WEBHOOK_SECRET === 'string' ? process.env.FIREFLIES_WEBHOOK_SECRET.trim() : '');
  const firefliesConfigured = Boolean(envFirefliesSecret || (typeof settings.firefliesSecret === 'string' && settings.firefliesSecret.trim()));

  const crmWebhookSecret = (typeof process.env.CRM_WEBHOOK_SECRET === 'string' ? process.env.CRM_WEBHOOK_SECRET.trim() : '') || (typeof settings.crmWebhookSecret === 'string' ? settings.crmWebhookSecret.trim() : '');
  const crmConfigured = Boolean(crmWebhookSecret);

  const envGa4PropertyId = typeof process.env.GA4_PROPERTY_ID === 'string' ? process.env.GA4_PROPERTY_ID.trim() : '';
  const savedGa4PropertyId = typeof settings.ga4PropertyId === 'string' ? settings.ga4PropertyId.trim() : '';
  const effectiveGa4PropertyId = envGa4PropertyId || savedGa4PropertyId;
  const envGa4ServiceAccountJson = typeof process.env.GA4_SERVICE_ACCOUNT_JSON === 'string' ? process.env.GA4_SERVICE_ACCOUNT_JSON.trim() : '';
  const savedGa4ServiceAccountJson = typeof settings.ga4ServiceAccountJson === 'string' ? settings.ga4ServiceAccountJson.trim() : '';
  const ga4ServiceAccountConfigured = Boolean(envGa4ServiceAccountJson || savedGa4ServiceAccountJson);
  const googleScope = settings.googleTokens && typeof settings.googleTokens === 'object' ? String(settings.googleTokens.scope || '') : '';
  const googleHasAnalyticsScope = googleConnected ? googleScope.includes('https://www.googleapis.com/auth/analytics.readonly') || googleScope.includes('analytics.readonly') : false;
  const ga4Configured = Boolean(effectiveGa4PropertyId && ((googleConnected && googleHasAnalyticsScope) || ga4ServiceAccountConfigured));
  const slackConfigured = Boolean(
    (typeof process.env.SLACK_SIGNING_SECRET === 'string' && process.env.SLACK_SIGNING_SECRET.trim()) ||
    (typeof settings.slackSigningSecret === 'string' && settings.slackSigningSecret.trim()),
  );

  const envSlackClientId = typeof process.env.SLACK_CLIENT_ID === 'string' ? process.env.SLACK_CLIENT_ID.trim() : '';
  const envSlackClientSecret = typeof process.env.SLACK_CLIENT_SECRET === 'string' ? process.env.SLACK_CLIENT_SECRET.trim() : '';
  const savedSlackClientId = typeof settings.slackClientId === 'string' ? settings.slackClientId.trim() : '';
  const savedSlackClientSecret = typeof settings.slackClientSecret === 'string' ? settings.slackClientSecret.trim() : '';
  const slackOAuthConfigured = Boolean((envSlackClientId || savedSlackClientId) && (envSlackClientSecret || savedSlackClientSecret));

  const envSlackBotToken = typeof process.env.SLACK_BOT_TOKEN === 'string' ? process.env.SLACK_BOT_TOKEN.trim() : '';
  const savedSlackBotToken = typeof settings.slackBotToken === 'string' ? settings.slackBotToken.trim() : '';
  const slackInstalled = Boolean(envSlackBotToken || savedSlackBotToken);

  const quoConfigured = Boolean(
    (typeof process.env.TWILIO_AUTH_TOKEN === 'string' && process.env.TWILIO_AUTH_TOKEN.trim()) ||
    (typeof settings.quoAuthToken === 'string' && settings.quoAuthToken.trim()),
  );

  const ghlConfig = await getGhlConfig();
  const ghlConfigured = Boolean(ghlConfig.apiKey && ghlConfig.locationId);

  const qdrant = getQdrantConfig(settings);
  const qdrantEnabled = Boolean(qdrant.enabled);
  const qdrantConfigured = Boolean(qdrant.configured);
  const qdrantUseForMarcus = Boolean(qdrant.useForMarcus);

  const email = getEmailConfig(settings);
  const imapConfigured = Boolean(email.imapConfigured);
  const smtpConfigured = Boolean(email.smtpConfigured);
  const emailSyncEnabled = Boolean(email.syncEnabled);
  const emailArchiveKnowledgeEnabled = Boolean(email.archiveKnowledgeEnabled);

  const mcpEff = getMcpEffectiveSettings(settings);
  const mcpEnabled = Boolean(mcpEff.enabled);
  const mcpConfigured = Boolean(mcpEff.configured);

  res.json({
    ...safe,
    aiEnabled: anyAiEnabled,
    openaiModel: model,
    openaiKeyHint: keyHint,
    openrouterKeyHint: openrouter.keyHint,
    openrouterConfigured: Boolean(openrouter.apiKey),
    aiRoutes,
    source,
    settingsUpdatedAt,
    googleConfigured,
    googleConnected,
    firefliesConfigured,
    crmConfigured,
    ga4Configured,
    slackConfigured,
    slackOAuthConfigured,
    slackInstalled,
    quoConfigured,
    ghlConfigured,
    qdrantEnabled,
    qdrantConfigured,
    qdrantUseForMarcus,
    imapConfigured,
    smtpConfigured,
    emailSyncEnabled,
    emailArchiveKnowledgeEnabled,
    mcpEnabled,
    mcpConfigured,
  });
});

app.put('/api/settings', async (req, res) => {
  const body = req.body || {};
  
  writeLock = writeLock.then(async () => {
    const saved = await readSettings();
    const next = { ...saved, ...body, updatedAt: nowIso() };
    next.automationConfig = normalizeAutomationConfig(next.automationConfig);
    next.automationDigestQueue = normalizeAutomationDigestQueue(next.automationDigestQueue);
    await writeSettings(next);
    // Never echo settings back (could include secrets).
    res.json({ ok: true });
  });
  
  await writeLock;
});

app.get('/api/integrations/openai/models', async (req, res) => {
  try {
    const settings = await readSettings();
    const openai = getOpenAiSecrets(settings);
    const refresh = String(req.query?.refresh || '').trim().toLowerCase();
    const force = refresh === '1' || refresh === 'true' || refresh === 'yes';

    const out = await fetchOpenAiModelsCatalog({ apiKey: openai.apiKey, force });
    if (!out.ok) {
      const status = openai.apiKey ? 502 : 400;
      res.status(status).json({
        ok: false,
        configured: Boolean(openai.apiKey),
        source: out.source || 'fallback',
        error: out.error || 'Failed to load model catalog',
        models: Array.isArray(out.models) && out.models.length ? out.models : OPENAI_MODEL_FALLBACKS,
        fetchedAt: Number(out.fetchedAt) || Date.now(),
        selectedModel: openai.model,
      });
      return;
    }

    res.json({
      ok: true,
      configured: Boolean(openai.apiKey),
      source: out.source || 'live',
      models: out.models,
      fetchedAt: Number(out.fetchedAt) || Date.now(),
      selectedModel: openai.model,
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      configured: false,
      source: 'fallback',
      error: err?.message || 'Failed to load OpenAI models',
      models: OPENAI_MODEL_FALLBACKS,
      fetchedAt: Date.now(),
    });
  }
});

// Businesses
app.get('/api/businesses', async (req, res) => {
  try {
    const saved = await readSettings();
    const cfg = getBusinessConfigFromSettings(saved);
    res.json({ ok: true, activeBusinessKey: cfg.activeBusinessKey, businesses: cfg.businesses });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load businesses' });
  }
});

app.put('/api/businesses', async (req, res) => {
  const incomingBusinesses = Array.isArray(req.body?.businesses) ? req.body.businesses : [];
  const incomingActive = normalizeBusinessKey(req.body?.activeBusinessKey || req.body?.activeBusiness || '');

  writeLock = writeLock.then(async () => {
    const saved = await readSettings();
    const currentCfg = getBusinessConfigFromSettings(saved);

    const merged = {
      ...saved,
      businesses: normalizeBusinessesList(incomingBusinesses),
      activeBusinessKey: incomingActive || currentCfg.activeBusinessKey,
      updatedAt: nowIso(),
    };

    const finalCfg = getBusinessConfigFromSettings(merged);
    const next = { ...merged, businesses: finalCfg.businesses, activeBusinessKey: finalCfg.activeBusinessKey };
    await writeSettings(next);
    res.json({ ok: true, activeBusinessKey: finalCfg.activeBusinessKey, businesses: finalCfg.businesses });
  });

  await writeLock;
});

app.post('/api/businesses/active', async (req, res) => {
  const key = normalizeBusinessKey(req.body?.key || req.body?.businessKey || '');
  if (!key) {
    res.status(400).json({ ok: false, error: 'key is required' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const saved = await readSettings();
    const cfg = getBusinessConfigFromSettings(saved);

    let businesses = Array.isArray(cfg.businesses) ? cfg.businesses : [];
    if (!businesses.some((b) => b.key === key)) {
      // If you activate an unknown key, auto-add it with a title-cased label.
      const label = key.split('-').filter(Boolean).map((w) => w.slice(0, 1).toUpperCase() + w.slice(1)).join(' ');
      businesses = [...businesses, { key, name: label || key }];
    }

    const next = {
      ...saved,
      businesses,
      activeBusinessKey: key,
      updatedAt: nowIso(),
    };

    const finalCfg = getBusinessConfigFromSettings(next);
    await writeSettings({ ...next, businesses: finalCfg.businesses, activeBusinessKey: finalCfg.activeBusinessKey });
    res.json({ ok: true, activeBusinessKey: finalCfg.activeBusinessKey, businesses: finalCfg.businesses });
  });

  await writeLock;
});

// Integrations: Airtable (per-business)
app.get('/api/integrations/airtable/config', async (req, res) => {
  try {
    const settings = await readSettings();
    const key = getBusinessKeyFromContext();
    const cfg = getAirtableConfigForBusiness(settings, key);
    res.json({
      ok: true,
      businessKey: key,
      configured: Boolean(cfg.pat && cfg.baseId && cfg.clientsTableId),
      tokenHint: airtableTokenHint(cfg.pat),
      baseId: cfg.baseId,
      clientsTableId: cfg.clientsTableId,
      clientsViewId: cfg.clientsViewId,
      requestsTableId: cfg.requestsTableId,
      requestsViewId: cfg.requestsViewId,
      updatedAt: cfg.updatedAt || '',
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load Airtable config' });
  }
});

app.put('/api/integrations/airtable/config', async (req, res) => {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const incoming = normalizeAirtableBusinessConfig(body);
  const key = getBusinessKeyFromContext();

  writeLock = writeLock.then(async () => {
    const settings = await readSettings();
    const map = settings.airtableByBusinessKey && typeof settings.airtableByBusinessKey === 'object' ? settings.airtableByBusinessKey : {};
    const current = getAirtableConfigForBusiness(settings, key);
    const next = {
      ...current,
      baseId: incoming.baseId || current.baseId,
      clientsTableId: incoming.clientsTableId || current.clientsTableId,
      clientsViewId: incoming.clientsViewId || current.clientsViewId,
      requestsTableId: incoming.requestsTableId || current.requestsTableId,
      requestsViewId: incoming.requestsViewId || current.requestsViewId,
      pat: incoming.pat || current.pat,
      updatedAt: nowIso(),
    };
    await writeSettings({
      ...settings,
      airtableByBusinessKey: {
        ...map,
        [key]: next,
      },
      updatedAt: nowIso(),
    });

    res.json({
      ok: true,
      businessKey: key,
      configured: Boolean(next.pat && next.baseId && next.clientsTableId),
      tokenHint: airtableTokenHint(next.pat),
      baseId: next.baseId,
      clientsTableId: next.clientsTableId,
      clientsViewId: next.clientsViewId,
      requestsTableId: next.requestsTableId,
      requestsViewId: next.requestsViewId,
      updatedAt: next.updatedAt,
    });
  });

  await writeLock;
});

app.get('/api/integrations/airtable/clients/preview', async (req, res) => {
  try {
    const settings = await readSettings();
    const key = getBusinessKeyFromContext();
    const cfg = getAirtableConfigForBusiness(settings, key);
    if (!cfg.pat || !cfg.baseId || !cfg.clientsTableId) {
      res.status(400).json({ ok: false, error: 'Airtable is not configured for this business.' });
      return;
    }

    const out = await airtableListRecords({
      pat: cfg.pat,
      baseId: cfg.baseId,
      tableId: cfg.clientsTableId,
      viewId: cfg.clientsViewId,
      maxRecords: 5,
    });
    if (!out.ok) {
      res.status(400).json({ ok: false, error: out.error || 'Failed to fetch Airtable records' });
      return;
    }

    const records = (out.records || []).map((r) => ({
      id: typeof r?.id === 'string' ? r.id : '',
      createdTime: typeof r?.createdTime === 'string' ? r.createdTime : '',
      name: pickAirtableClientName(r?.fields),
      fieldKeys: r?.fields && typeof r.fields === 'object' ? Object.keys(r.fields).slice(0, 20) : [],
    }));

    res.json({ ok: true, businessKey: key, count: records.length, records });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to preview clients' });
  }
});

app.post('/api/integrations/airtable/clients/sync', async (req, res) => {
  const limitRaw = Number(req.body?.limit);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, Math.floor(limitRaw))) : 200;
  const key = getBusinessKeyFromContext();

  writeLock = writeLock.then(async () => {
    const settings = await readSettings();
    const cfg = getAirtableConfigForBusiness(settings, key);
    if (!cfg.pat || !cfg.baseId || !cfg.clientsTableId) {
      res.status(400).json({ ok: false, error: 'Airtable is not configured for this business.' });
      return;
    }

    const out = await airtableListRecords({
      pat: cfg.pat,
      baseId: cfg.baseId,
      tableId: cfg.clientsTableId,
      viewId: cfg.clientsViewId,
      maxRecords: limit,
    });
    if (!out.ok) {
      res.status(400).json({ ok: false, error: out.error || 'Failed to fetch Airtable clients' });
      return;
    }

    await withBusinessKey(key, async () => {
      const store = await readStore();
      // Ensure older Airtable-imported client entries are flagged as contacts (so they don't flood Projects).
      const migrated = migrateLegacyAirtableClientProjects(store);
      const baseStore = migrated.changed ? migrated.store : store;
      if (migrated.changed) {
        await writeStore(baseStore);
      }

      const existingClients = Array.isArray(baseStore.clients) ? baseStore.clients : [];
      const byAirtableUrl = new Map();
      for (const c of existingClients) {
        const url = String(c?.airtableUrl || '').trim();
        if (url) byAirtableUrl.set(url, c);
      }

      const pick = (fields, keyNames) => firstNonEmptyString(fields, [], keyNames);
      const prefer = (nextVal, prevVal) => {
        const n = String(nextVal || '').trim();
        return n ? n : (typeof prevVal === 'string' ? prevVal : '');
      };

      let created = 0;
      let updated = 0;
      let skipped = 0;

      const nextClients = [...existingClients];
      let didMutate = false;

      for (const r of (out.records || [])) {
        const recordId = typeof r?.id === 'string' ? r.id : '';
        if (!recordId) continue;
        const fields = r?.fields && typeof r.fields === 'object' ? r.fields : {};
        const recordUrl = `https://airtable.com/${cfg.baseId}/${cfg.clientsTableId}/${recordId}`;

        const name = pickAirtableClientName(fields) || `Airtable Client ${recordId}`;
        const phone = pick(fields, ['phone', 'phone number', 'mobile', 'cell', 'cell phone']);
        const email = pick(fields, ['email', 'email address']);
        const accountManagerName = pick(fields, ['account manager', 'am', 'owner', 'manager', 'project manager']);
        const accountManagerEmail = pick(fields, ['account manager email', 'am email', 'owner email', 'manager email']);
        const website = pick(fields, ['website', 'site', 'url', 'domain']);

        const existingClient = byAirtableUrl.get(recordUrl) || null;
        if (!existingClient) {
          const ts = nowIso();
          const client = normalizeClientRecord({
            id: makeId(),
            name,
            phone,
            email,
            website,
            accountManagerName,
            accountManagerEmail,
            airtableRecordId: recordId,
            airtableUrl: recordUrl,
            createdAt: ts,
            updatedAt: ts,
          });
          nextClients.push(client);
          byAirtableUrl.set(recordUrl, client);
          created++;
          didMutate = true;
          continue;
        }

        const merged = {
          ...existingClient,
          name: prefer(name, existingClient.name),
          phone: prefer(phone, existingClient.phone),
          email: prefer(email, existingClient.email),
          website: prefer(website, existingClient.website),
          accountManagerName: prefer(accountManagerName, existingClient.accountManagerName),
          accountManagerEmail: prefer(accountManagerEmail, existingClient.accountManagerEmail),
          airtableRecordId: prefer(recordId, existingClient.airtableRecordId),
          airtableUrl: recordUrl,
        };

        const changed = JSON.stringify(merged) !== JSON.stringify(existingClient);
        if (!changed) {
          skipped++;
          continue;
        }

        merged.updatedAt = nowIso();
        const idx = nextClients.findIndex((c) => c && c.id === existingClient.id);
        if (idx >= 0) nextClients[idx] = merged;
        else nextClients.push(merged);
        byAirtableUrl.set(recordUrl, merged);
        updated++;
        didMutate = true;
      }

      if (didMutate) {
        const ts = nowIso();
        const nextStore = {
          ...baseStore,
          revision: baseStore.revision + 1,
          updatedAt: ts,
          clients: nextClients,
        };
        await writeStore(nextStore);
      }

      res.json({ ok: true, businessKey: key, created, updated, skipped, totalFetched: (out.records || []).length });
    });
  });

  await writeLock;
});

app.get('/api/integrations/airtable/requests/preview', async (req, res) => {
  try {
    const settings = await readSettings();
    const key = getBusinessKeyFromContext();
    const cfg = getAirtableConfigForBusiness(settings, key);
    if (!cfg.pat || !cfg.baseId || !cfg.requestsTableId) {
      res.status(400).json({ ok: false, error: 'Airtable revision requests are not configured for this business.' });
      return;
    }

    const out = await airtableListRecords({
      pat: cfg.pat,
      baseId: cfg.baseId,
      tableId: cfg.requestsTableId,
      viewId: cfg.requestsViewId,
      maxRecords: 5,
    });
    if (!out.ok) {
      res.status(400).json({ ok: false, error: out.error || 'Failed to fetch Airtable revision requests' });
      return;
    }

    const records = (out.records || []).map((r) => ({
      id: String(r?.id || ''),
      createdTime: String(r?.createdTime || ''),
      // Keep preview payload small; surface common fields.
      fields: {
        title: firstNonEmptyString(r?.fields || {}, [], ['title', 'request', 'summary', 'subject', 'name']) || '',
        revisionSummary: firstNonEmptyString(r?.fields || {}, [], ['revision summary']) || '',
        business: firstNonEmptyString(r?.fields || {}, [], ['business (from clients)', 'business (from clientssss)']) || '',
      },
    }));

    res.json({ ok: true, businessKey: key, records });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to preview Airtable revision requests' });
  }
});

app.get('/api/debug/store', async (req, res) => {
  try {
    const key = getBusinessKeyFromContext();
    const filePath = getStoreFileForBusiness(key);
    const store = await withBusinessKey(key, async () => readStore());
    const summary = summarizeRevisionLikeProjectsForDebug(store, key);
    const last = lastRevisionCollapseByKey.get(normalizeBusinessKey(key) || DEFAULT_BUSINESS_KEY) || null;
    res.json({
      ok: true,
      now: nowIso(),
      activeBusinessKey: cachedActiveBusinessKey,
      requestBusinessKey: key,
      storeFile: filePath,
      dataDir: DATA_DIR,
      summary,
      lastRevisionCollapse: last,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load debug store info' });
  }
});

async function runAirtableRevisionRequestsSync({ businessKey, limit = 200, windowDays = AIRTABLE_REQUESTS_WINDOW_DAYS, settings = null } = {}) {
  const key = normalizeBusinessKey(businessKey) || DEFAULT_BUSINESS_KEY;
  const maxRecords = Number.isFinite(Number(limit)) ? Math.max(1, Math.min(500, Math.floor(Number(limit)))) : 200;
  const days = Number.isFinite(Number(windowDays)) ? Math.max(1, Math.floor(Number(windowDays))) : 30;
  const cutoffMs = Date.now() - (days * 24 * 60 * 60 * 1000);

  const saved = settings || await readSettings();
  const cfg = getAirtableConfigForBusiness(saved, key);
  if (!cfg.pat || !cfg.baseId || !cfg.requestsTableId) {
    const err = new Error('Airtable revision requests are not configured for this business.');
    err.statusCode = 400;
    throw err;
  }

  const out = await airtableListRecords({
    pat: cfg.pat,
    baseId: cfg.baseId,
    tableId: cfg.requestsTableId,
    viewId: cfg.requestsViewId,
    maxRecords,
  });
  if (!out.ok) {
    const err = new Error(out.error || 'Failed to fetch Airtable revision requests');
    err.statusCode = 400;
    throw err;
  }

  if (!shouldMaterializeAirtableRevisionRequests(saved)) {
    return {
      ok: true,
      mode: 'airtable-fetch-only',
      businessKey: key,
      windowDays: days,
      created: 0,
      updated: 0,
      skipped: 0,
      skippedOld: 0,
      notesAppended: 0,
      tasksCreated: 0,
      tasksUpdated: 0,
      archivedDuplicates: 0,
      totalFetched: (out.records || []).length,
    };
  }

  const mapProjectStatus = (fields) => {
    const raw = firstNonEmptyString(fields, [], ['status', 'stage', 'state']);
    const s = String(raw || '').trim().toLowerCase();
    if (!s) return 'Active';
    if (s.includes('hold') || s.includes('paused') || s.includes('waiting')) return 'On Hold';
    if (s.includes('archiv')) return 'Archived';
    if (s.includes('done') || s.includes('complete') || s.includes('completed') || s.includes('closed') || s.includes('resolved')) return 'Done';
    return 'Active';
  };

  const mapProjectPriority = (fields) => {
    const raw = firstNonEmptyString(fields, [], ['priority', 'urgency']);
    const s = String(raw || '').trim().toLowerCase();
    if (!s) return 'Medium';
    if (s.includes('high') || s.includes('urgent') || s.includes('asap') || s === '1') return 'High';
    if (s.includes('low') || s === '3') return 'Low';
    if (s.includes('medium') || s === '2') return 'Medium';
    return 'Medium';
  };

  const pickRevisionLabel = (fields) => {
    const raw = firstNonEmptyString(fields, [], [
      'revision',
      'rev',
      'rev #',
      'rev#',
      'revision #',
      'revision number',
      'revision id',
      'request #',
      'request id',
      'ticket',
      'ticket #',
    ]);
    return String(raw || '').trim();
  };

  const pickRevisionSummary = (fields) => {
    const raw = firstNonEmptyString(fields, [], ['revision summary']);
    return valueToLooseText(raw).trim();
  };

  const pickRevisionNotes = (fields) => {
    const raw = firstNonEmptyString(fields, [], [
      'revision notes',
      'requested changes',
      'changes requested',
      'change requests',
      'feedback',
      'client feedback',
      'customer feedback',
      'review notes',
      'notes',
    ]);
    return valueToLooseText(raw).trim();
  };

  const pickBusinessFromClients = (fields) => {
    const raw = firstNonEmptyString(fields, [], ['business (from clients)', 'business (from clientssss)']);
    const s = typeof raw === 'string' ? raw.trim() : '';
    if (!s) return '';
    return s.split(',')[0].trim();
  };

  const pickAirtableTaskText = (fields) => {
    const raw = firstNonEmptyString(fields, [], ['tasks', 'task list', 'action items', 'next steps', 'next actions', 'to do', 'todo', 'ai tasks']);
    return valueToLooseText(raw).trim();
  };

  const pickWebsiteOrSiteLabel = (fields) => {
    const raw = firstNonEmptyString(fields, [], [
      'website',
      'site',
      'domain',
      'url',
      'link',
      'website url',
      'site url',
      'page url',
      'website (from clients)',
      'site (from clients)',
      'domain (from clients)',
      'url (from clients)',
      'website (from clientssss)',
      'site (from clientssss)',
    ]);
    return typeof raw === 'string' ? raw.trim() : '';
  };

  const normalizeSiteLabel = (input) => {
    const raw = typeof input === 'string' ? input.trim() : '';
    if (!raw) return '';
    try {
      const withProto = raw.includes('://') ? raw : `https://${raw}`;
      const u = new URL(withProto);
      const host = String(u.hostname || '').trim().toLowerCase().replace(/^www\./, '');
      if (host) return host;
    } catch {
      // ignore
    }
    return raw.replace(/^https?:\/\//i, '').replace(/^www\./i, '').split(/[\/\s]/)[0].trim() || raw;
  };

  const computeRequestsGroupKey = ({ businessName, siteLabel, recordId }) => {
    const biz = normKey(businessName || getBusinessNameForKey(key));
    const site = normKey(siteLabel);
    if (!site) return `airtable:rev-requests:record:${recordId}`;
    const hash = crypto.createHash('sha1').update(`${biz}|${site}`).digest('hex').slice(0, 12);
    return `airtable:rev-requests:group:${hash}`;
  };

  const parseTaskTitles = (text, { limit: taskLimit = 18 } = {}) => {
    const raw = typeof text === 'string' ? text : '';
    if (!raw.trim()) return [];
    const seen = new Set();
    const titles = [];
    for (const lineRaw of raw.split(/\r?\n/g)) {
      const line = String(lineRaw || '').trim();
      if (!line) continue;
      const cleaned = line.replace(/^[-*•\u2022\s]+/g, '').replace(/^\(?\d+\)?[.)\s]+/g, '').trim();
      if (!cleaned) continue;
      const k = normKey(cleaned);
      if (!k || seen.has(k)) continue;
      seen.add(k);
      titles.push(cleaned.slice(0, 220));
      if (titles.length >= taskLimit) break;
    }
    return titles;
  };

  const prefer = (nextVal, prevVal) => {
    const n = String(nextVal || '').trim();
    return n ? n : (typeof prevVal === 'string' ? prevVal : '');
  };

  const ts = nowIso();

  const result = await withBusinessKey(key, async () => {
    const store = await readStore();
    const projects = Array.isArray(store.projects) ? store.projects : [];
    const existingTasks = Array.isArray(store.tasks) ? store.tasks : [];

    const byRequestsKey = new Map();
    const byAirtableUrl = new Map();
    for (const p of projects) {
      const k = String(p?.airtableRequestsKey || '').trim();
      if (k) byRequestsKey.set(k, p);
      const url = String(p?.airtableUrl || '').trim();
      if (url) byAirtableUrl.set(url, p);
    }

    const primaryProjectIdByRequestsKey = new Map();

    let created = 0;
    let updated = 0;
    let skipped = 0;
    let skippedOld = 0;
    let notesAppended = 0;
    let tasksCreated = 0;
    let tasksUpdated = 0;
    let archivedDuplicates = 0;

    const nextProjects = [...projects];
    const nextTasks = [...existingTasks];
    let nextSenderProjectMap = { ...(store.senderProjectMap || {}) };
    let nextProjectNoteEntries = store.projectNoteEntries || {};
    let didMutate = false;

    for (const r of (out.records || [])) {
      const recordId = typeof r?.id === 'string' ? r.id : '';
      if (!recordId) continue;
      const createdTime = typeof r?.createdTime === 'string' ? r.createdTime : '';
      const createdMs = createdTime ? Date.parse(createdTime) : NaN;
      if (Number.isFinite(createdMs) && createdMs < cutoffMs) {
        skippedOld++;
        continue;
      }

      const fields = r?.fields && typeof r.fields === 'object' ? r.fields : {};
      const recordUrl = `https://airtable.com/${cfg.baseId}/${cfg.requestsTableId}/${recordId}`;

      const titleRaw = firstNonEmptyString(fields, [], ['title', 'request', 'summary', 'subject', 'name']) || `Revision request ${recordId}`;
      const title = valueToLooseText(titleRaw).trim() || `Revision request ${recordId}`;
      const clientName = firstNonEmptyString(fields, [], ['client', 'client name', 'company', 'company name', 'customer', 'project']) || '';
      const clientPhone = firstNonEmptyString(fields, [], ['phone', 'phone number', 'mobile', 'cell', 'cell phone']) || '';
      const revisionLabel = pickRevisionLabel(fields);
      const revisionSummary = pickRevisionSummary(fields);
      const revisionNotes = pickRevisionNotes(fields);
      const taskText = pickAirtableTaskText(fields);
      const bodyRaw = firstNonEmptyString(fields, [], [
        'revision notes',
        'requested changes',
        'changes requested',
        'change requests',
        'feedback',
        'details',
        'description',
        'notes',
        'message',
      ]);
      const body = valueToLooseText(bodyRaw).trim() || revisionSummary || revisionNotes;
      const dueRaw = firstNonEmptyString(fields, [], ['due', 'due date', 'deadline']);
      const dueDate = safeYmd(String(dueRaw || '').trim().slice(0, 10)) || '';
      const status = mapProjectStatus(fields);
      const priority = mapProjectPriority(fields);

      const businessName = pickBusinessFromClients(fields) || getBusinessNameForKey(key);

      const siteFromFields = pickWebsiteOrSiteLabel(fields);
      const siteLabel = normalizeSiteLabel(siteFromFields || clientName);
      const displayLabel = (siteLabel || clientName || businessName).trim();
      const requestsKey = computeRequestsGroupKey({ businessName, siteLabel: displayLabel, recordId });
      const projectName = displayLabel.slice(0, 140) || businessName.slice(0, 140);

      const importedBriefLines = [];
      importedBriefLines.push('Imported from Airtable (revision requests)');
      importedBriefLines.push(`Business: ${businessName}`);
      if (siteLabel) importedBriefLines.push(`Site: ${siteLabel}`);
      if (clientName) importedBriefLines.push(`Client: ${clientName}`);
      importedBriefLines.push(`Title: ${title}`);
      if (revisionLabel) importedBriefLines.push(`Revision: ${revisionLabel}`);
      if (dueDate) importedBriefLines.push(`Due: ${dueDate}`);
      importedBriefLines.push(`Status: ${status}`);
      importedBriefLines.push(`Priority: ${priority}`);
      importedBriefLines.push('');
      if (revisionSummary) {
        importedBriefLines.push('Revision summary:');
        importedBriefLines.push(revisionSummary);
        importedBriefLines.push('');
      }
      if (revisionNotes) {
        importedBriefLines.push('Revision notes:');
        importedBriefLines.push(revisionNotes);
        importedBriefLines.push('');
      }
      if (body && body !== revisionSummary && body !== revisionNotes) importedBriefLines.push(body);
      importedBriefLines.push('');
      importedBriefLines.push(`Airtable: ${recordUrl}`);

      const appendRevisionSummaryNoteIfNew = (project) => {
        if (!project) return;
        const text = revisionSummary || revisionNotes;
        if (!text) return;
        const hash = crypto.createHash('sha1').update(text).digest('hex').slice(0, 12);
        const noteId = `airtable:rev:${recordId}:rev-note:${hash}`;
        const date = safeYmd(ts.slice(0, 10)) || ts.slice(0, 10);
        const baseTitle = revisionSummary ? 'Revision Summary' : 'Revision Notes';
        const noteTitle = revisionLabel ? `${baseTitle} (Rev ${revisionLabel})` : baseTitle;
        const content = `${text}\n\nAirtable: ${recordUrl}`.trimEnd();
        const note = { id: noteId, kind: 'Airtable', date, title: noteTitle, content, createdAt: ts };

        const existing = Array.isArray(nextProjectNoteEntries?.[project.id]) ? nextProjectNoteEntries[project.id] : [];
        const exists = existing.some((n) => String(n?.id || '') === noteId);
        if (exists) return;

        const legacyPrefix = `airtable:rev:${recordId}:rev-`;
        const cleaned = existing.filter((n) => {
          const id = String(n?.id || '');
          if (!id.startsWith(legacyPrefix)) return true;
          const c = String(n?.content || '').trim();
          if (!c) return false;
          if (c.includes('[object Object]')) return false;
          return true;
        });

        nextProjectNoteEntries = { ...(nextProjectNoteEntries || {}), [project.id]: [note, ...cleaned] };
        notesAppended++;
        didMutate = true;
      };

      const upsertAirtableTasks = (project) => {
        if (!project) return;
        const sourceText = taskText || revisionSummary || revisionNotes || body;
        const titles = parseTaskTitles(sourceText);
        if (!titles.length) return;

        for (const taskTitle of titles) {
          const keyHash = crypto.createHash('sha1').update(normKey(taskTitle)).digest('hex').slice(0, 12);
          const taskId = `airtable:rev:${recordId}:task:${keyHash}`;

          const idx = nextTasks.findIndex((t) => String(t?.id || '') === taskId);
          if (idx < 0) {
            const normalized = normalizeTask({ title: taskTitle, status: 'Next', priority: 2, project: project.name, dueDate });
            const task = { id: taskId, ...normalized, createdAt: ts, updatedAt: ts };
            nextTasks.unshift(task);
            tasksCreated++;
            didMutate = true;
            continue;
          }

          const existingTask = nextTasks[idx];
          const merged = {
            ...(existingTask && typeof existingTask === 'object' ? existingTask : {}),
            id: taskId,
            title: taskTitle,
            project: project.name,
            dueDate: dueDate || String(existingTask?.dueDate || ''),
            updatedAt: ts,
          };
          const changed = JSON.stringify(merged) !== JSON.stringify(existingTask);
          if (!changed) continue;
          nextTasks[idx] = merged;
          tasksUpdated++;
          didMutate = true;
        }
      };

      const existing = byRequestsKey.get(requestsKey) || byAirtableUrl.get(recordUrl) || null;
      if (!existing) {
        const normalized = normalizeProject({
          name: projectName,
          type: 'Revision',
          status,
          dueDate,
          clientName,
          clientPhone,
          airtableUrl: recordUrl,
          priority,
          agentBrief: importedBriefLines.join('\n'),
        });

        const project = {
          id: makeId(),
          ...normalized,
          airtableSource: 'revision-requests',
          airtableRequestsKey: requestsKey,
          airtableSiteLabel: siteLabel || '',
          airtableRecordId: recordId,
          airtableTableId: cfg.requestsTableId,
          createdAt: ts,
          updatedAt: ts,
        };

        nextProjects.unshift(project);
        byRequestsKey.set(requestsKey, project);
        byAirtableUrl.set(recordUrl, project);
        primaryProjectIdByRequestsKey.set(requestsKey, project.id);
        if (project.clientPhone) nextSenderProjectMap = upsertSenderProjectMapForProject(nextSenderProjectMap, project.clientPhone, project);

        appendRevisionSummaryNoteIfNew(project);
        upsertAirtableTasks(project);

        created++;
        didMutate = true;
        continue;
      }

      const shouldOverwriteBrief = (() => {
        const raw = String(existing?.agentBrief || '').trim().toLowerCase();
        if (!raw) return true;
        return raw.includes('imported from airtable (revision requests)');
      })();

      const merged = {
        ...existing,
        name: projectName,
        type: 'Revision',
        status,
        dueDate,
        clientName: prefer(clientName, existing.clientName),
        clientPhone: prefer(clientPhone, existing.clientPhone),
        airtableUrl: recordUrl,
        priority: prefer(priority, existing.priority),
        ...(shouldOverwriteBrief ? { agentBrief: importedBriefLines.join('\n') } : {}),
        airtableSource: 'revision-requests',
        airtableRequestsKey: requestsKey,
        airtableSiteLabel: siteLabel || String(existing?.airtableSiteLabel || '').trim(),
        airtableRecordId: recordId,
        airtableTableId: cfg.requestsTableId,
      };

      const normalized = normalizeProject(merged);
      const updatedProject = {
        ...existing,
        ...normalized,
        ...(shouldOverwriteBrief ? { agentBrief: merged.agentBrief } : {}),
        airtableSource: merged.airtableSource,
        airtableRequestsKey: merged.airtableRequestsKey,
        airtableSiteLabel: merged.airtableSiteLabel,
        airtableRecordId: merged.airtableRecordId,
        airtableTableId: merged.airtableTableId,
        updatedAt: ts,
      };

      const changed = JSON.stringify(updatedProject) !== JSON.stringify(existing);
      if (!changed) {
        appendRevisionSummaryNoteIfNew(existing);
        upsertAirtableTasks(existing);
        if (!didMutate) skipped++;
        continue;
      }

      const idx = nextProjects.findIndex((p) => p && p.id === existing.id);
      if (idx >= 0) nextProjects[idx] = updatedProject;
      else nextProjects.unshift(updatedProject);
      byRequestsKey.set(requestsKey, updatedProject);
      byAirtableUrl.set(recordUrl, updatedProject);
      primaryProjectIdByRequestsKey.set(requestsKey, updatedProject.id);
      if (updatedProject.clientPhone) nextSenderProjectMap = upsertSenderProjectMapForProject(nextSenderProjectMap, updatedProject.clientPhone, updatedProject);

      appendRevisionSummaryNoteIfNew(updatedProject);
      upsertAirtableTasks(updatedProject);

      updated++;
      didMutate = true;
    }

    // If we rolled up multiple revision records into a single project, archive legacy per-revision projects
    // so the active project list stays useful.
    const baseBusinessName = getBusinessNameForKey(key);
    for (let i = 0; i < nextProjects.length; i++) {
      const p = nextProjects[i];
      if (!p || typeof p !== 'object') continue;
      if (String(p.airtableSource || '') !== 'revision-requests') continue;
      if (String(p.status || '') === 'Archived') continue;

      const existingKey = String(p.airtableRequestsKey || '').trim();
      const derivedSite = normalizeSiteLabel(String(p.airtableSiteLabel || p.clientName || p.name || '').trim());
      const derivedKey = existingKey || computeRequestsGroupKey({ businessName: baseBusinessName, siteLabel: derivedSite, recordId: String(p.airtableRecordId || '') });
      const primaryId = primaryProjectIdByRequestsKey.get(derivedKey);
      if (!primaryId) continue;
      if (String(p.id || '') === String(primaryId)) continue;

      const archived = {
        ...p,
        status: 'Archived',
        airtableRequestsKey: derivedKey,
        updatedAt: ts,
      };
      nextProjects[i] = archived;
      archivedDuplicates++;
      didMutate = true;
    }

    if (!didMutate && !created && !updated) {
      return { created, updated, skipped, skippedOld, notesAppended, tasksCreated, tasksUpdated, archivedDuplicates, didWrite: false };
    }

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projects: nextProjects,
      tasks: nextTasks,
      projectNoteEntries: nextProjectNoteEntries,
      senderProjectMap: nextSenderProjectMap,
    };
    await writeStore(nextStore);
    return { created, updated, skipped, skippedOld, notesAppended, tasksCreated, tasksUpdated, archivedDuplicates, didWrite: true };
  });

  return {
    ok: true,
    businessKey: key,
    windowDays: days,
    created: result.created,
    updated: result.updated,
    skipped: result.skipped,
    skippedOld: result.skippedOld,
    notesAppended: result.notesAppended,
    tasksCreated: result.tasksCreated,
    tasksUpdated: result.tasksUpdated,
    archivedDuplicates: result.archivedDuplicates,
    totalFetched: (out.records || []).length,
  };
}

app.post('/api/integrations/airtable/requests/sync', async (req, res) => {
  const key = getBusinessKeyFromContext();
  const limitRaw = Number(req.body?.limit);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, Math.floor(limitRaw))) : 200;

  writeLock = writeLock.then(async () => {
    try {
      const payload = await runAirtableRevisionRequestsSync({ businessKey: key, limit });
      res.json(payload);
    } catch (err) {
      const code = Number(err?.statusCode) || 500;
      res.status(code).json({ ok: false, error: err?.message || 'Failed to sync revision requests' });
    }
  });

  await writeLock;
});

let airtableAutoSyncTimer = null;
let airtableAutoSyncRunning = false;

function startAirtableRequestsAutoSyncScheduler() {
  if (!AIRTABLE_AUTO_SYNC_ENABLED) return;
  if (!AIRTABLE_AUTO_SYNC_INTERVAL_MS || AIRTABLE_AUTO_SYNC_INTERVAL_MS < 60_000) return;
  if (airtableAutoSyncTimer) return;

  const tick = () => {
    if (airtableAutoSyncRunning) return;
    airtableAutoSyncRunning = true;

    writeLock = writeLock.then(async () => {
      const settings = await readSettings();
      if (!shouldMaterializeAirtableRevisionRequests(settings)) return;
      const businesses = Array.isArray(cachedBusinesses) ? cachedBusinesses : [];
      for (const biz of businesses) {
        const bKey = normalizeBusinessKey(biz?.key || '');
        if (!bKey) continue;
        const cfg = getAirtableConfigForBusiness(settings, bKey);
        if (!cfg.pat || !cfg.baseId || !cfg.requestsTableId) continue;
        try {
          await runAirtableRevisionRequestsSync({ businessKey: bKey, limit: 200, windowDays: AIRTABLE_REQUESTS_WINDOW_DAYS, settings });
        } catch {
          // best-effort background sync; ignore
        }
      }
    }).finally(() => {
      airtableAutoSyncRunning = false;
    });
  };

  // Run once shortly after boot, then on the steady interval.
  setTimeout(tick, 2_000);
  airtableAutoSyncTimer = setInterval(tick, AIRTABLE_AUTO_SYNC_INTERVAL_MS);
}

// Integrations: Google Calendar
app.get('/api/integrations/google/status', async (req, res) => {
  const { clientId, clientSecret, calendarId, tokens } = await getGoogleOAuthConfig();
  const clientIdValid = isLikelyGoogleClientId(clientId);
  res.json({
    configured: Boolean(clientIdValid),
    clientIdValid,
    secretPresent: Boolean(clientSecret),
    connected: Boolean(tokens && tokens.refresh_token),
    calendarId: calendarId || '',
  });
});

app.get('/api/integrations/qdrant/status', async (req, res) => {
  try {
    const settings = await readSettings();
    const cfg = getQdrantConfig(settings);
    res.json({
      ok: true,
      enabled: Boolean(cfg.enabled),
      configured: Boolean(cfg.configured),
      connected: false,
      useForMarcus: Boolean(cfg.useForMarcus),
      url: cfg.url,
      collection: cfg.collection,
      embeddingModel: cfg.embeddingModel,
      vectorSize: cfg.vectorSize,
      distance: cfg.distance,
      apiKeyHint: cfg.apiKeyHint,
      topK: cfg.topK,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load Qdrant status' });
  }
});

app.post('/api/integrations/qdrant/test', async (req, res) => {
  try {
    const settings = await readSettings();
    const cfg = getQdrantConfig(settings);
    if (!cfg.enabled || !cfg.configured) {
      res.status(400).json({ ok: false, error: 'Qdrant is not configured. Add QDRANT_URL and QDRANT_COLLECTION (and QDRANT_API_KEY if required).' });
      return;
    }

    const response = await qdrantRequest(cfg, `/collections/${encodeURIComponent(cfg.collection)}`, { method: 'GET' });
    if (!response.resp.ok) {
      const detail = typeof response.data?.status?.error === 'string'
        ? response.data.status.error
        : typeof response.data?.error === 'string'
          ? response.data.error
          : `status ${response.resp.status}`;
      res.status(response.resp.status === 404 ? 404 : 502).json({ ok: false, error: `Qdrant test failed: ${detail}` });
      return;
    }

    const pointsCount = Number(response.data?.result?.points_count);
    res.json({
      ok: true,
      connected: true,
      collection: cfg.collection,
      url: cfg.url,
      status: response.data?.status || 'ok',
      pointsCount: Number.isFinite(pointsCount) ? pointsCount : null,
      details: response.data?.result || {},
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to test Qdrant connection' });
  }
});

app.post('/api/integrations/qdrant/ensure-collection', async (req, res) => {
  try {
    const settings = await readSettings();
    const cfg = getQdrantConfig(settings);
    if (!cfg.enabled || !cfg.configured) {
      res.status(400).json({ ok: false, error: 'Qdrant is not configured. Add QDRANT_URL and QDRANT_COLLECTION first.' });
      return;
    }

    const out = await qdrantEnsureCollection(cfg);
    if (!out.ok) {
      res.status(502).json(out);
      return;
    }

    res.json({
      ok: true,
      collection: cfg.collection,
      created: Boolean(out.created),
      vectorSize: cfg.vectorSize,
      distance: cfg.distance,
      details: out.details || {},
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to ensure Qdrant collection' });
  }
});

app.post('/api/integrations/qdrant/upsert', async (req, res) => {
  try {
    const docs = Array.isArray(req.body?.documents)
      ? req.body.documents
      : (req.body?.document && typeof req.body.document === 'object' ? [req.body.document] : []);
    if (!docs.length) {
      res.status(400).json({ ok: false, error: 'Provide documents: [{ title?, text|content, source?, tags?, metadata? }].' });
      return;
    }

    const settings = await readSettings();
    const businessKey = typeof req.body?.businessKey === 'string' && req.body.businessKey.trim()
      ? req.body.businessKey.trim()
      : getBusinessKeyFromContext();
    const out = await qdrantUpsertDocuments(settings, docs, {
      businessKey,
      ensureCollection: req.body?.ensureCollection !== false,
    });
    if (!out.ok) {
      const code = /not configured|required/i.test(String(out.error || '')) ? 400 : 502;
      res.status(code).json(out);
      return;
    }

    res.json({
      ok: true,
      collection: out.collection,
      count: out.count,
      businessKey,
      createdCollection: Boolean(out.createdCollection),
      result: out.result || {},
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to upsert Qdrant documents' });
  }
});

app.post('/api/integrations/qdrant/search', async (req, res) => {
  try {
    const query = typeof req.body?.query === 'string' ? req.body.query.trim() : '';
    if (!query) {
      res.status(400).json({ ok: false, error: 'query is required' });
      return;
    }

    const settings = await readSettings();
    const filter = req.body?.filter && typeof req.body.filter === 'object' && !Array.isArray(req.body.filter)
      ? req.body.filter
      : {};
    if (!filter.businessKey && req.body?.businessKey !== '*') {
      filter.businessKey = typeof req.body?.businessKey === 'string' && req.body.businessKey.trim()
        ? req.body.businessKey.trim()
        : getBusinessKeyFromContext();
    }

    const out = await qdrantSearchKnowledge(settings, query, {
      limit: req.body?.limit,
      filter,
    });
    if (!out.ok) {
      const code = /not configured|required/i.test(String(out.error || '')) ? 400 : 502;
      res.status(code).json(out);
      return;
    }

    res.json({ ok: true, collection: out.collection, matches: out.matches });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to search Qdrant knowledge base' });
  }
});

app.get('/api/integrations/email/status', async (req, res) => {
  try {
    const settings = await readSettings();
    const email = getEmailConfig(settings);
    res.json({
      ok: true,
      imapConfigured: Boolean(email.imapConfigured),
      smtpConfigured: Boolean(email.smtpConfigured),
      emailSyncEnabled: Boolean(email.syncEnabled),
      emailArchiveKnowledgeEnabled: Boolean(email.archiveKnowledgeEnabled),
      syncFolders: email.syncFolders,
      archiveFolders: email.archiveFolders,
      fromAddress: email.fromAddress,
      imapHost: email.imap.host,
      smtpHost: email.smtp.host,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load email integration status' });
  }
});

app.post('/api/integrations/email/test', async (req, res) => {
  try {
    const settings = await readSettings();
    const email = getEmailConfig(settings);
    const testTimeoutMs = 2_500;
    const result = {
      ok: true,
      mode: 'connectivity-probe',
      imapConfigured: Boolean(email.imapConfigured),
      smtpConfigured: Boolean(email.smtpConfigured),
      imap: { ok: false, skipped: !email.imapConfigured },
      smtp: { ok: false, skipped: !email.smtpConfigured },
    };

    const checks = [];

    if (email.imapConfigured) {
      checks.push((async () => {
        try {
          const imapResult = await probeEmailTransportProfiles('imap', buildImapConnectionProfiles(email), testTimeoutMs);
          result.imap = {
            ok: imapResult.ok,
            reachable: imapResult.ok,
            ...(imapResult.ok ? { profile: imapResult.profile } : { error: 'No IMAP profile accepted a TCP/TLS connection from Render.' }),
            attempts: imapResult.attempts,
            note: 'Socket-level reachability probe only. Sync still requires valid IMAP auth and protocol support.',
          };
          if (!imapResult.ok) result.ok = false;
        } catch (err) {
          result.ok = false;
          result.imap = {
            ok: false,
            error: err?.message || 'IMAP connection failed',
            attempts: Array.isArray(err?.attempts) ? err.attempts : [],
          };
        }
      })());
    }

    if (email.smtpConfigured) {
      checks.push((async () => {
        try {
          const smtpResult = await probeEmailTransportProfiles('smtp', buildSmtpConnectionProfiles(email), testTimeoutMs);
          result.smtp = {
            ok: smtpResult.ok,
            reachable: smtpResult.ok,
            ...(smtpResult.ok ? { profile: smtpResult.profile, fromAddress: email.fromAddress } : { error: 'No SMTP profile accepted a TCP/TLS connection from Render.' }),
            attempts: smtpResult.attempts,
            note: 'Socket-level reachability probe only. Sending still requires valid SMTP auth and protocol support.',
          };
          if (!smtpResult.ok) result.ok = false;
        } catch (err) {
          result.ok = false;
          result.smtp = {
            ok: false,
            error: err?.message || 'SMTP verification failed',
            attempts: Array.isArray(err?.attempts) ? err.attempts : [],
          };
        }
      })());
    }

    await Promise.all(checks);

    const status = result.ok ? 200 : 502;
    res.status(status).json(result);
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to test email integration' });
  }
});

app.post('/api/integrations/email/send', async (req, res) => {
  try {
    const settings = await readSettings();
    const email = getEmailConfig(settings);
    if (!email.smtpConfigured) {
      res.status(400).json({ ok: false, error: 'SMTP is not configured.' });
      return;
    }

    const to = typeof req.body?.to === 'string' ? req.body.to.trim() : '';
    const cc = typeof req.body?.cc === 'string' ? req.body.cc.trim() : '';
    const bcc = typeof req.body?.bcc === 'string' ? req.body.bcc.trim() : '';
    const subject = typeof req.body?.subject === 'string' ? req.body.subject.trim() : '';
    const text = typeof req.body?.text === 'string' ? req.body.text.trim() : '';
    const html = typeof req.body?.html === 'string' ? req.body.html.trim() : '';
    const from = typeof req.body?.from === 'string' && req.body.from.trim() ? req.body.from.trim() : email.fromAddress;
    const replyTo = typeof req.body?.replyTo === 'string' ? req.body.replyTo.trim() : '';
    const inReplyTo = typeof req.body?.inReplyTo === 'string' ? req.body.inReplyTo.trim() : '';
    const rawReferences = req.body?.references;
    const references = Array.isArray(rawReferences)
      ? rawReferences.map((value) => (typeof value === 'string' ? value.trim() : '')).filter(Boolean).slice(0, 20)
      : (typeof rawReferences === 'string' && rawReferences.trim() ? rawReferences.trim() : '');
    if (!to) {
      res.status(400).json({ ok: false, error: 'to is required' });
      return;
    }
    if (!subject) {
      res.status(400).json({ ok: false, error: 'subject is required' });
      return;
    }
    if (!text && !html) {
      res.status(400).json({ ok: false, error: 'text or html is required' });
      return;
    }

    const smtpResult = await withSmtpTransport(email, async (transport) => transport.sendMail({
      from,
      to,
      ...(cc ? { cc } : {}),
      ...(bcc ? { bcc } : {}),
      ...(replyTo ? { replyTo } : {}),
      ...(inReplyTo ? { inReplyTo } : {}),
      ...(Array.isArray(references) ? (references.length ? { references } : {}) : (references ? { references } : {})),
      subject,
      ...(text ? { text } : {}),
      ...(html ? { html } : {}),
    }));
    const info = smtpResult.value;

    res.json({
      ok: true,
      messageId: info?.messageId || '',
      accepted: Array.isArray(info?.accepted) ? info.accepted : [],
      rejected: Array.isArray(info?.rejected) ? info.rejected : [],
      response: String(info?.response || ''),
      profile: describeSmtpProfile(smtpResult.profile),
    });
  } catch (err) {
    res.status(502).json({
      ok: false,
      error: err?.message || 'Failed to send email',
      attempts: Array.isArray(err?.attempts) ? err.attempts : [],
    });
  }
});

app.post('/api/integrations/email/sync', async (req, res) => {
  try {
    const settings = await readSettings();
    const email = getEmailConfig(settings);
    if (!email.imapConfigured) {
      res.status(400).json({ ok: false, error: 'IMAP is not configured.' });
      return;
    }

    const businessKey = typeof req.body?.businessKey === 'string' && req.body.businessKey.trim()
      ? req.body.businessKey.trim()
      : getBusinessKeyFromContext();
    const out = await fetchImapMessages(settings, {
      mode: 'sync',
      folders: req.body?.folders,
      limitPerFolder: req.body?.limitPerFolder,
      sinceDays: req.body?.sinceDays,
      unseenOnly: req.body?.unseenOnly === true,
    });
    if (!out.ok) {
      res.status(400).json(out);
      return;
    }

    let created = 0;
    let deduped = 0;
    const docs = [];
    for (const message of out.messages) {
      const result = await addInboxIntegrationItem({
        source: 'email',
        externalId: message.externalId,
        text: buildInboxTextFromEmailMessage(message),
        businessKey,
        toNumber: message.toAddresses,
        fromNumber: message.fromAddress,
        fromName: message.fromName,
        contactName: message.fromName,
        threadKey: message.threadKey,
        channel: 'imap',
      });
      if (result?.created) created += 1;
      else deduped += 1;
      if (req.body?.upsertKnowledge === true) docs.push(buildEmailKnowledgeDocument(message, businessKey));
    }

    let knowledge = null;
    if (docs.length) {
      const upsert = await qdrantUpsertDocuments(settings, docs, {
        businessKey,
        ensureCollection: req.body?.ensureCollection !== false,
      });
      if (!upsert.ok) {
        res.status(502).json({ ok: false, error: upsert.error || 'Failed to upsert synced email knowledge' });
        return;
      }
      knowledge = { collection: upsert.collection, count: upsert.count };
    }

    res.json({
      ok: true,
      businessKey,
      fetched: out.messages.length,
      created,
      deduped,
      folders: out.folders,
      folderErrors: out.folderErrors,
      knowledge,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to sync email inbox' });
  }
});

app.post('/api/integrations/email/archive-to-qdrant', async (req, res) => {
  try {
    const settings = await readSettings();
    const businessKey = typeof req.body?.businessKey === 'string' && req.body.businessKey.trim()
      ? req.body.businessKey.trim()
      : getBusinessKeyFromContext();
    const mode = typeof req.body?.source === 'string' && req.body.source.trim().toLowerCase() === 'local'
      ? 'local'
      : 'imap';

    let docs = [];
    let fetched = 0;
    let folders = [];
    let folderErrors = [];

    if (mode === 'local') {
      const store = await readStore();
      const archivedItems = (Array.isArray(store.inboxItems) ? store.inboxItems : [])
        .filter((item) => String(item?.source || '').trim().toLowerCase() === 'email')
        .filter((item) => String(item?.status || '').trim().toLowerCase() === 'archived')
        .filter((item) => !businessKey || String(item?.businessKey || '').trim() === businessKey)
        .slice(0, 500);
      fetched = archivedItems.length;
      docs = archivedItems.map((item) => buildEmailKnowledgeDocument({
        externalId: String(item?.id || '').trim(),
        subject: String(item?.text || '').split('\n')[0].replace(/^Subject:\s*/i, '').trim(),
        body: String(item?.text || '').trim(),
        fromName: String(item?.fromName || item?.contactName || '').trim(),
        fromAddress: String(item?.fromNumber || '').trim(),
        toAddresses: String(item?.toNumber || '').trim(),
        dateIso: String(item?.lastMessageAt || item?.updatedAt || item?.createdAt || '').trim(),
        folder: 'local-archived-inbox',
        messageId: String(item?.threadKey || item?.id || '').trim(),
      }, businessKey));
      folders = ['local-archived-inbox'];
    } else {
      const out = await fetchImapMessages(settings, {
        mode: 'archive',
        folders: req.body?.folders,
        limitPerFolder: req.body?.limitPerFolder,
        sinceDays: req.body?.sinceDays,
        unseenOnly: false,
      });
      if (!out.ok) {
        res.status(400).json(out);
        return;
      }
      fetched = out.messages.length;
      folders = out.folders;
      folderErrors = out.folderErrors;
      docs = out.messages.map((message) => buildEmailKnowledgeDocument(message, businessKey));
    }

    if (!docs.length) {
      res.json({ ok: true, source: mode, fetched, folders, folderErrors, upserted: 0, collection: '' });
      return;
    }

    const upsert = await qdrantUpsertDocuments(settings, docs, {
      businessKey,
      ensureCollection: req.body?.ensureCollection !== false,
    });
    if (!upsert.ok) {
      const code = /not configured|required/i.test(String(upsert.error || '')) ? 400 : 502;
      res.status(code).json(upsert);
      return;
    }

    res.json({
      ok: true,
      source: mode,
      businessKey,
      fetched,
      folders,
      folderErrors,
      upserted: upsert.count,
      collection: upsert.collection,
      createdCollection: Boolean(upsert.createdCollection),
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to ingest archived email knowledge' });
  }
});

app.get('/api/integrations/google/auth-url', async (req, res) => {
  const { clientId, clientSecret } = await getGoogleOAuthConfig();
  if (!clientId || !isLikelyGoogleClientId(clientId)) {
    res.status(400).json({ error: 'Google OAuth client is not configured. Paste the OAuth Client ID that ends with .apps.googleusercontent.com.' });
    return;
  }

  const redirectUri = `${getBaseUrl(req)}/api/integrations/google/callback`;
  const state = crypto.randomBytes(16).toString('hex');
  pruneGooglePkceState();

  const usePkce = !clientSecret;
  let verifier = '';
  let challenge = '';
  if (usePkce) {
    verifier = makePkceVerifier();
    challenge = makePkceChallenge(verifier);
    googlePkceState.set(state, { verifier, createdAt: Date.now() });
  }

  const params = new URLSearchParams({
    client_id: clientId,
    redirect_uri: redirectUri,
    response_type: 'code',
    access_type: 'offline',
    prompt: 'consent',
    include_granted_scopes: 'true',
    scope: [
      'https://www.googleapis.com/auth/calendar',
      'https://www.googleapis.com/auth/analytics.readonly',
      'https://www.googleapis.com/auth/drive.file',
    ].join(' '),
    state,
  });
  if (usePkce) {
    params.set('code_challenge', challenge);
    params.set('code_challenge_method', 'S256');
  }

  const url = `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`;
  res.json({ url, mode: usePkce ? 'pkce' : 'secret' });
});

app.get('/api/integrations/google/callback', async (req, res) => {
  try {
    const code = typeof req.query?.code === 'string' ? req.query.code : '';
    if (!code) {
      res.status(400).send('Missing code');
      return;
    }

    const state = typeof req.query?.state === 'string' ? req.query.state : '';
    const { clientId, clientSecret } = await getGoogleOAuthConfig();
    if (!clientId) {
      res.status(400).send('Google OAuth client is not configured (missing Client ID).');
      return;
    }

    const usePkce = !clientSecret;
    let codeVerifier = '';
    if (usePkce) {
      if (!state) {
        res.status(400).send('Missing state');
        return;
      }
      pruneGooglePkceState();
      const entry = googlePkceState.get(state);
      googlePkceState.delete(state);
      codeVerifier = typeof entry?.verifier === 'string' ? entry.verifier : '';
      if (!codeVerifier) {
        res.status(400).send('Missing PKCE verifier (state expired). Try connecting again.');
        return;
      }
    }

    const redirectUri = `${getBaseUrl(req)}/api/integrations/google/callback`;

    const tokenJson = await googleTokenRequest({
      client_id: clientId,
      client_secret: clientSecret || undefined,
      code,
      code_verifier: usePkce ? codeVerifier : undefined,
      redirect_uri: redirectUri,
      grant_type: 'authorization_code',
    });
    const tokens = normalizeGoogleTokens(tokenJson);

    const saved = await readSettings();
    const next = { ...saved, googleTokens: tokens, updatedAt: nowIso() };
    await writeSettings(next);

    // Friendly close page
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(`<!doctype html><html><head><meta charset="utf-8"><title>Connected</title></head><body style="font-family: system-ui, sans-serif; padding: 24px;">
      <h1>Google connected.</h1>
      <p>You can close this tab and return to M.A.R.C.U.S.</p>
    </body></html>`);
  } catch (err) {
    res.status(500).send(`OAuth failed: ${err?.message || 'unknown error'}`);
  }
});

app.post('/api/projects/:id/drive-folder/create', async (req, res) => {
  const projectId = String(req.params.id || '').trim();
  if (!projectId) {
    res.status(400).json({ ok: false, error: 'Missing project id' });
    return;
  }

  writeLock = writeLock.then(async () => {
    try {
      const store = await readStore();
      const idx = (store.projects || []).findIndex((p) => p.id === projectId);
      if (idx === -1) {
        res.status(404).json({ ok: false, error: 'Project not found' });
        return;
      }

      const settings = await readSettings();
      const { clientId, clientSecret, tokens, saved } = await getGoogleOAuthConfig();
      if (!clientId || !isLikelyGoogleClientId(clientId)) {
        res.status(400).json({ ok: false, error: 'Google OAuth client is not configured' });
        return;
      }
      if (!tokens || !tokens.refresh_token) {
        res.status(400).json({ ok: false, error: 'Google is not connected. Run the connect flow in Settings → Integrations.' });
        return;
      }

      const scope = String(tokens.scope || '');
      const hasDrive = scope.includes('https://www.googleapis.com/auth/drive.file') || scope.includes('drive.file') || scope.includes('https://www.googleapis.com/auth/drive');
      if (!hasDrive) {
        res.status(400).json({ ok: false, error: 'Google is connected without Drive scope. Reconnect Google to grant Drive access.' });
        return;
      }

      const redirectBase = req ? getBaseUrl(req) : getDefaultBaseUrl();
      const redirectUri = `${redirectBase}/api/integrations/google/callback`;
      const fresh = await ensureFreshGoogleTokens({ clientId, clientSecret, tokens, saved });
      const oauth2 = buildOAuthClient({ clientId, clientSecret: clientSecret || '', redirectUri });
      oauth2.setCredentials(fresh.tokens);

      const drive = google.drive({ version: 'v3', auth: oauth2 });
      const project = store.projects[idx];
      const folderName = (typeof project?.name === 'string' ? project.name.trim() : '') || 'Project';
      const created = await drive.files.create({
        requestBody: {
          name: folderName,
          mimeType: 'application/vnd.google-apps.folder',
        },
        fields: 'id, webViewLink',
      });

      const id = String(created?.data?.id || '').trim();
      const url = (typeof created?.data?.webViewLink === 'string' && created.data.webViewLink.trim())
        ? created.data.webViewLink.trim()
        : (id ? driveFolderUrlFromId(id) : '');

      if (!id) {
        res.status(500).json({ ok: false, error: 'Drive folder creation succeeded but returned no id' });
        return;
      }

      const ts = nowIso();
      const updatedProject = {
        ...project,
        driveFolderId: id,
        driveFolderUrl: url,
        updatedAt: ts,
      };

      const nextProjects = [...store.projects];
      nextProjects[idx] = updatedProject;

      const nextStore = {
        ...store,
        revision: store.revision + 1,
        updatedAt: ts,
        projects: nextProjects,
      };

      await writeStore(nextStore);
      // also bump settings updatedAt for visibility
      await writeSettings({ ...settings, updatedAt: ts });
      res.json({ ok: true, folderId: id, folderUrl: url, store: nextStore });
    } catch (err) {
      res.status(500).json({ ok: false, error: err?.message || 'Failed to create Drive folder' });
    }
  });

  await writeLock;
});

app.post('/api/integrations/google/sync', async (req, res) => {
  try {
    const result = await googleSyncProjects({ req });
    if (!result.ok) {
      res.status(400).json(result);
      return;
    }
    res.json(result);
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'sync failed' });
  }
});

// Read-only: upcoming events (calls/meetings live on the user's calendar)
app.get('/api/integrations/google/upcoming', async (req, res) => {
  try {
    const days = Number(req.query?.days);
    const max = Number(req.query?.max);
    const result = await googleListUpcomingEvents({ days, max });
    if (!result.ok) {
      res.status(400).json(result);
      return;
    }
    res.json(result);
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to list events' });
  }
});

app.get('/api/integrations/ghl/status', async (req, res) => {
  try {
    const { apiKey, locationId, apiBaseUrl, apiVersion } = await getGhlConfig();
    const keyHint = apiKey && apiKey.length >= 4 ? `����${apiKey.slice(-4)}` : '';
    res.json({
      ok: true,
      configured: Boolean(apiKey && locationId),
      hasApiKey: Boolean(apiKey),
      hasLocationId: Boolean(locationId),
      locationId: locationId || '',
      apiBaseUrl: apiBaseUrl || '',
      apiVersion: apiVersion || '2021-07-28',
      keyHint,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load GHL status' });
  }
});

app.get('/api/integrations/ghl/snapshot', async (req, res) => {
  try {
    const { apiKey, locationId, apiBaseUrl, apiVersion } = await getGhlConfig();
    if (!apiKey || !locationId) {
      res.status(400).json({ ok: false, error: 'GHL is not configured. Add API key and Location ID in Settings.' });
      return;
    }

    const now = new Date();
    const in7Days = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
    const warnings = [];

    let opportunities = [];
    try {
      const oppJson = await ghlApiGet({
        apiKey,
        apiBaseUrl,
        apiVersion,
        endpoint: '/opportunities/search',
        params: { locationId, limit: 100 },
      });
      opportunities = pickFirstArray(oppJson, ['opportunities', 'items', 'data']);
    } catch (err) {
      warnings.push(`Opportunities: ${err?.message || 'failed'}`);
    }

    let conversations = [];
    try {
      const convJson = await ghlApiGet({
        apiKey,
        apiBaseUrl,
        apiVersion,
        endpoint: '/conversations/search',
        params: { locationId, limit: 100 },
      });
      conversations = pickFirstArray(convJson, ['conversations', 'items', 'data']);
    } catch (err) {
      warnings.push(`Conversations: ${err?.message || 'failed'}`);
    }

    let appointments = [];
    try {
      const eventsJson = await ghlApiGet({
        apiKey,
        apiBaseUrl,
        apiVersion,
        endpoint: '/calendars/events',
        params: {
          locationId,
          startTime: now.toISOString(),
          endTime: in7Days.toISOString(),
          limit: 100,
        },
      });
      appointments = pickFirstArray(eventsJson, ['events', 'appointments', 'items', 'data']);
    } catch (err) {
      warnings.push(`Appointments: ${err?.message || 'failed'}`);
    }

    const snapshot = computeGhlSnapshot({ opportunities, conversations, appointments });
    res.json({
      ok: true,
      configured: true,
      fetchedAt: nowIso(),
      locationId,
      ...snapshot,
      warnings,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load GHL snapshot' });
  }
});

// One endpoint the UI can call from the existing sync button
app.post('/api/integrations/sync', async (req, res) => {
  const results = {};
  try {
    results.google = await googleSyncProjects({ req });
  } catch (err) {
    results.google = { ok: false, error: err?.message || 'google sync failed' };
  }
  res.json({ ok: true, results });
});

app.get('/api/integrations/fireflies/status', async (req, res) => {
  try {
    const saved = await readSettings();
    const cfg = getFirefliesConfig(saved, req);
    const store = await readStore();
    const inboxItems = Array.isArray(store?.inboxItems) ? store.inboxItems : [];
    const firefliesItems = inboxItems.filter((item) => String(item?.source || '').trim().toLowerCase() === 'fireflies');
    const latestItem = firefliesItems
      .slice()
      .sort((a, b) => String(b?.updatedAt || b?.createdAt || '').localeCompare(String(a?.updatedAt || a?.createdAt || '')))[0] || null;

    res.json({
      ok: true,
      configured: Boolean(cfg.configured),
      secretSource: cfg.secretSource,
      webhookPath: cfg.webhookPath,
      webhookUrl: cfg.webhookUrl,
      inboxItemCount: firefliesItems.length,
      lastReceivedAt: latestItem ? String(latestItem.updatedAt || latestItem.createdAt || '') : '',
      lastLinkedProjectName: latestItem ? String(latestItem.projectName || '') : '',
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load Fireflies status' });
  }
});

app.post('/api/integrations/fireflies/test', async (req, res) => {
  try {
    const saved = await readSettings();
    const cfg = getFirefliesConfig(saved, req);
    if (!cfg.configured) {
      res.status(400).json({ ok: false, error: 'Fireflies is not configured. Save a shared secret first.' });
      return;
    }

    const title = typeof req.body?.title === 'string' ? req.body.title.trim() : 'Fireflies smoke test';
    const summary = normalizeNotes(req.body?.summary || 'Smoke test summary from the Fireflies integration panel.');
    const projectId = typeof req.body?.projectId === 'string' ? req.body.projectId.trim() : '';
    const projectName = typeof req.body?.projectName === 'string' ? req.body.projectName.trim() : '';
    const transcriptUrl = typeof req.body?.transcriptUrl === 'string' ? req.body.transcriptUrl.trim() : '';
    const date = safeYmd(req.body?.date) || safeYmd(new Date().toISOString().slice(0, 10));

    if (!summary) {
      res.status(400).json({ ok: false, error: 'summary is required' });
      return;
    }

    const store = await readStore();
    const projects = Array.isArray(store?.projects) ? store.projects : [];

    let project = null;
    if (projectId) {
      project = projects.find((p) => String(p?.id || '') === projectId) || null;
    }
    if (!project && projectName) {
      project = projects.find((p) => String(p?.name || '').trim().toLowerCase() === projectName.toLowerCase()) || null;
    }
    if (!project) {
      project = matchProjectFromText(store, `${title}\n${summary}`) || null;
    }

    const externalId = `test:${crypto.createHash('sha1').update(`${title}|${summary}|${transcriptUrl}|${date}`).digest('hex')}`;
    const inboxItemId = `fireflies:${externalId}`;

    res.json({
      ok: true,
      mode: 'dry-run',
      configured: true,
      secretSource: cfg.secretSource,
      webhookUrl: cfg.webhookUrl,
      normalizedPayload: {
        title,
        date,
        summary,
        transcriptUrl,
        projectId,
        projectName,
      },
      wouldCreateInboxItemId: inboxItemId,
      wouldLinkProjectId: project ? String(project.id || '') : '',
      wouldLinkProjectName: project ? String(project.name || '') : '',
      notePreview: {
        kind: 'Summary',
        date,
        title: title || 'Fireflies summary',
        content: transcriptUrl ? `${summary}\n\nTranscript: ${transcriptUrl}` : summary,
      },
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to test Fireflies integration' });
  }
});

// Integrations: Fireflies ingestion (meeting summaries into inbox; optional project note linkage)
// Expected payload: { projectId?, projectName?, date?: 'YYYY-MM-DD', title?: string, summary: string, transcriptUrl?: string, meetingId?: string }
app.post('/api/integrations/fireflies/ingest', async (req, res) => {
  const secret = typeof req.headers['x-fireflies-secret'] === 'string' ? req.headers['x-fireflies-secret'].trim() : '';
  const saved = await readSettings();
  const expected = getFirefliesConfig(saved, req).secret;
  if (!expected || secret !== expected) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }

  const projectId = typeof req.body?.projectId === 'string' ? req.body.projectId.trim() : '';
  const projectName = typeof req.body?.projectName === 'string' ? req.body.projectName.trim() : '';
  const date = safeYmd(req.body?.date) || safeYmd(new Date().toISOString().slice(0, 10));
  const title = typeof req.body?.title === 'string' ? req.body.title.trim() : '';
  const summary = normalizeNotes(req.body?.summary);
  const transcriptUrl = typeof req.body?.transcriptUrl === 'string' ? req.body.transcriptUrl.trim() : '';
  const meetingId = typeof req.body?.meetingId === 'string' ? req.body.meetingId.trim() : '';

  if (!summary) {
    res.status(400).json({ error: 'summary is required' });
    return;
  }

  const lines = [];
  if (title) lines.push(`Fireflies: ${title}`);
  else lines.push('Fireflies summary');
  lines.push(summary);
  if (transcriptUrl) {
    lines.push('');
    lines.push(`Transcript: ${transcriptUrl}`);
  }

  const externalId = meetingId
    ? `meeting:${meetingId}`
    : `summary:${crypto.createHash('sha1').update(`${title}|${summary}|${transcriptUrl}|${date}`).digest('hex')}`;

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    const projects = Array.isArray(store.projects) ? store.projects : [];

    let project = null;
    if (projectId) {
      project = projects.find((p) => String(p?.id || '') === projectId) || null;
    }
    if (!project && projectName) {
      project = projects.find((p) => String(p?.name || '').trim().toLowerCase() === projectName.toLowerCase()) || null;
    }
    if (!project) {
      project = matchProjectFromText(store, `${title}\n${summary}`) || null;
    }

    const ts = nowIso();
    const inboxText = lines.join('\n').trimEnd();

    const inboxItemId = `fireflies:${externalId}`;
    const inboxList = Array.isArray(store.inboxItems) ? store.inboxItems : [];
    const inboxExists = inboxList.some((x) => String(x?.id || '') === inboxItemId);
    const nextInboxItems = inboxExists
      ? inboxList
      : [normalizeInboxItem({
          id: inboxItemId,
          source: 'fireflies',
          text: inboxText,
          status: 'New',
          projectId: project?.id || '',
          projectName: project?.name || '',
          createdAt: ts,
          updatedAt: ts,
        }), ...inboxList].slice(0, 500);

    let note = null;
    let nextProjectNoteEntries = store.projectNoteEntries || {};
    if (project) {
      note = {
        id: makeId(),
        kind: 'Summary',
        date,
        title: title || 'Fireflies summary',
        content: lines.slice(1).join('\n').trimEnd() || summary,
        createdAt: ts,
      };

      const existing = Array.isArray(store.projectNoteEntries?.[project.id]) ? store.projectNoteEntries[project.id] : [];
      nextProjectNoteEntries = {
        ...(store.projectNoteEntries || {}),
        [project.id]: [note, ...existing],
      };
    }

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: nextInboxItems,
      projectNoteEntries: nextProjectNoteEntries,
    };

    await writeStore(nextStore);
    res.status(201).json({
      ok: true,
      revision: nextStore.revision,
      inboxAdded: !inboxExists,
      linkedProjectId: project?.id || '',
      linkedProjectName: project?.name || '',
      note,
    });
  });

  await writeLock;
});

// Integrations: Generic CRM webhook -> Inbox
// Configure your CRM to POST JSON to: /api/integrations/crm/webhook
// Verify with header: X-CRM-Secret (recommended) or env CRM_WEBHOOK_SECRET
app.get('/api/integrations/crm/status', async (req, res) => {
  try {
    const { apiBaseUrl, apiKey, webhookSecret } = await getCrmConfig();
    res.json({
      ok: true,
      configured: Boolean(webhookSecret),
      hasApiBaseUrl: Boolean(apiBaseUrl),
      hasApiKey: Boolean(apiKey),
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load CRM status' });
  }
});

// Integrations: GA4 (Google Analytics 4) daily summary -> Inbox
// Configure with settings: ga4PropertyId + ga4ServiceAccountJson
// Or env: GA4_PROPERTY_ID + GA4_SERVICE_ACCOUNT_JSON
app.get('/api/integrations/ga4/status', async (req, res) => {
  try {
    const { propertyId, clientEmail, privateKey, googleConnected, googleHasAnalyticsScope, saved } = await getGa4Config();
    res.json({
      ok: true,
      configured: Boolean(propertyId && ((googleConnected && googleHasAnalyticsScope) || (clientEmail && privateKey))),
      hasPropertyId: Boolean(propertyId),
      googleConnected,
      googleHasAnalyticsScope,
      hasServiceAccount: Boolean(clientEmail && privateKey),
      lastDailySummaryDate: typeof saved.ga4LastDailySummaryDate === 'string' ? saved.ga4LastDailySummaryDate : '',
      lastDailySummaryAt: typeof saved.ga4LastDailySummaryAt === 'string' ? saved.ga4LastDailySummaryAt : '',
      lastDailySummaryError: typeof saved.ga4LastDailySummaryError === 'string' ? saved.ga4LastDailySummaryError : '',
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load GA4 status' });
  }
});

app.post('/api/integrations/ga4/pull-now', async (req, res) => {
  const result = await runGa4DailySummary({ force: true, req });
  if (!result.ok) {
    res.status(500).json(result);
    return;
  }
  res.json(result);
});

app.post('/api/integrations/crm/webhook', async (req, res) => {
  try {
    const presented = typeof req.headers['x-crm-secret'] === 'string' ? req.headers['x-crm-secret'].trim() : '';
    const { webhookSecret } = await getCrmConfig();
    const expected = String(webhookSecret || '').trim();
    if (!expected || !presented || !safeTimingEqual(presented, expected)) {
      debugWebhookLog('CRM webhook rejected', {
        reason: !expected ? 'CRM webhook secret not configured' : 'Invalid secret',
        contentType: req.headers['content-type'],
        hasSecret: Boolean(presented),
        forwardedProto: req.headers['x-forwarded-proto'],
        forwardedHost: req.headers['x-forwarded-host'],
        host: req.get('host'),
        method: req.method,
        path: req.originalUrl || req.url,
      });
      res.status(401).json({ ok: false, error: 'Unauthorized' });
      return;
    }

    const payload = req.body && typeof req.body === 'object' ? req.body : {};

    const leadId = firstNonEmptyString(
      payload,
      ['id', 'leadId', 'contactId', 'opportunityId', 'data.id', 'data.leadId', 'data.contactId'],
      ['id', 'leadid', 'contactid', 'opportunityid'],
    );
    const name = firstNonEmptyString(
      payload,
      ['name', 'fullName', 'contact.name', 'contact.fullName', 'data.name', 'data.fullName'],
      ['name', 'fullname', 'contactname'],
    );
    const phone = firstNonEmptyString(
      payload,
      ['phone', 'phoneNumber', 'mobile', 'contact.phone', 'contact.phoneNumber', 'data.phone', 'data.phoneNumber'],
      ['phone', 'phonenumber', 'mobile'],
    );
    const email = firstNonEmptyString(
      payload,
      ['email', 'contact.email', 'data.email'],
      ['email'],
    );
    const source = firstNonEmptyString(
      payload,
      ['source', 'utm_source', 'channel', 'form', 'page', 'campaign', 'data.source', 'data.channel'],
      ['source', 'channel', 'campaign', 'form'],
    );
    const message = firstNonEmptyString(
      payload,
      ['message', 'notes', 'body', 'text', 'summary', 'data.message', 'data.notes', 'data.body', 'data.text'],
      ['message', 'notes', 'body', 'text', 'summary'],
    );
    const projectName = firstNonEmptyString(
      payload,
      ['projectName', 'project.name', 'data.projectName', 'data.project.name'],
      ['projectname', 'project'],
    );

    const lines = [];
    lines.push('📥 CRM');
    if (source) lines.push(`Source: ${source}`);
    if (name) lines.push(`Name: ${name}`);
    if (phone) lines.push(`Phone: ${phone}`);
    if (email) lines.push(`Email: ${email}`);
    if (message) {
      lines.push('');
      lines.push(message);
    }

    const externalId = leadId
      ? `lead:${leadId}`
      : `payload:${crypto.createHash('sha1').update(JSON.stringify(payload)).digest('hex')}`;

    await addInboxIntegrationItem({
      source: 'crm',
      externalId,
      text: lines.join('\n').trimEnd(),
      projectId: '',
      projectName: projectName || '',
      fromNumber: phone || '',
      channel: 'crm',
    });

    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'CRM webhook failed' });
  }
});

// Integrations: Slack Events API -> Inbox
// Slack OAuth (install + bot token) is optional, but enables richer Inbox labels and �all the things�.
// Set a public BASE_URL so Slack can redirect back to your instance.
app.get('/api/integrations/slack/auth-url', async (req, res) => {
  try {
    const { clientId, clientSecret } = await getSlackOAuthConfig();
    if (!clientId || !clientSecret) {
      res.status(400).json({ error: 'Slack OAuth is not configured. Paste Slack Client ID + Client Secret first.' });
      return;
    }

    const state = crypto.randomBytes(16).toString('hex');
    pruneSlackOAuthState();
    slackOAuthState.set(state, { createdAt: Date.now() });

    const redirectUri = `${getBaseUrl(req)}/api/integrations/slack/oauth/callback`;

    // Minimal, modern bot scopes for this app:
    // - Send test message: chat.postMessage -> chat:write
    // - Open DM: conversations.open -> conversations:write
    // - Label channels/users in Inbox: conversations.info + users.info -> conversations:read + users:read
    // Keep this list conservative to avoid Slack's invalid_scope during install.
    const scope = [
      'chat:write',
      'conversations:read',
      'conversations:write',
      'users:read',
      'users:read.email',
    ].join(',');

    const params = new URLSearchParams({
      client_id: clientId,
      scope,
      redirect_uri: redirectUri,
      state,
    });

    const url = `https://slack.com/oauth/v2/authorize?${params.toString()}`;
    res.json({ url });
  } catch (err) {
    res.status(500).json({ error: err?.message || 'Failed to build Slack auth URL' });
  }
});

app.get('/api/integrations/slack/oauth/callback', async (req, res) => {
  try {
    const code = typeof req.query?.code === 'string' ? req.query.code : '';
    const state = typeof req.query?.state === 'string' ? req.query.state : '';
    if (!code) {
      res.status(400).send('Missing code');
      return;
    }
    if (!state) {
      res.status(400).send('Missing state');
      return;
    }

    pruneSlackOAuthState();
    const entry = slackOAuthState.get(state);
    slackOAuthState.delete(state);
    if (!entry) {
      res.status(400).send('Invalid/expired state. Try connecting again.');
      return;
    }

    const { clientId, clientSecret } = await getSlackOAuthConfig();
    if (!clientId || !clientSecret) {
      res.status(400).send('Slack OAuth is not configured (missing Client ID/Secret).');
      return;
    }

    const redirectUri = `${getBaseUrl(req)}/api/integrations/slack/oauth/callback`;
    const body = new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      code,
      redirect_uri: redirectUri,
    });

    const resp = await fetch('https://slack.com/api/oauth.v2.access', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
    });

    const json = await resp.json().catch(() => ({}));
    if (!resp.ok || !json || json.ok !== true) {
      const msg = typeof json?.error === 'string' ? json.error : 'Slack token exchange failed';
      res.status(400).send(msg);
      return;
    }

    const token = typeof json?.access_token === 'string' ? json.access_token.trim() : '';
    if (!token) {
      res.status(400).send('Slack did not return an access token');
      return;
    }

    const teamId = typeof json?.team?.id === 'string' ? json.team.id.trim() : '';
    const teamName = typeof json?.team?.name === 'string' ? json.team.name.trim() : '';
    const botUserId = typeof json?.bot_user_id === 'string' ? json.bot_user_id.trim() : '';
    const appId = typeof json?.app_id === 'string' ? json.app_id.trim() : '';
    const scopes = typeof json?.scope === 'string' ? json.scope.trim() : '';

    const saved = await readSettings();
    const next = {
      ...saved,
      slackBotToken: token,
      slackTeamId: teamId,
      slackTeamName: teamName,
      slackBotUserId: botUserId,
      slackAppId: appId,
      slackScopes: scopes,
      slackInstalledAt: nowIso(),
      updatedAt: nowIso(),
    };
    await writeSettings(next);

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(`<!doctype html><html><head><meta charset="utf-8"><title>Connected</title></head><body style="font-family: system-ui, sans-serif; padding: 24px;">
      <h1>Slack connected.</h1>
      <p>Workspace: ${escapeHtml(teamName || teamId || 'unknown')}</p>
      <p>You can close this tab and return to M.A.R.C.U.S.</p>
    </body></html>`);
  } catch (err) {
    res.status(500).send(`Slack OAuth failed: ${err?.message || 'unknown error'}`);
  }
});

// Configure Slack to send events to: POST /api/integrations/slack/events
// Requires: SLACK_SIGNING_SECRET (env) or settings.slackSigningSecret
// Diagnostics: GET /api/integrations/slack/diagnostics (requires ADMIN_TOKEN if enabled)
app.get('/api/integrations/slack/diagnostics', async (req, res) => {
  try {
    const settings = await readSettings();
    const hasEnvSigningSecret = typeof process.env.SLACK_SIGNING_SECRET === 'string' && process.env.SLACK_SIGNING_SECRET.trim();
    const hasSavedSigningSecret = typeof settings.slackSigningSecret === 'string' && settings.slackSigningSecret.trim();
    const hasEnvBotToken = typeof process.env.SLACK_BOT_TOKEN === 'string' && process.env.SLACK_BOT_TOKEN.trim();
    const hasSavedBotToken = typeof settings.slackBotToken === 'string' && settings.slackBotToken.trim();

    const baseUrl = getBaseUrl(req);
    res.json({
      ok: true,
      configured: Boolean(hasEnvSigningSecret || hasSavedSigningSecret),
      installed: Boolean(hasEnvBotToken || hasSavedBotToken),
      debugWebhooks: DEBUG_WEBHOOKS,
      baseUrl,
      eventsUrl: `${baseUrl}/api/integrations/slack/events`,
      oauthRedirectUrl: `${baseUrl}/api/integrations/slack/oauth/callback`,
      runtime: {
        ...slackRuntime,
      },
      note: 'Slack Events API requires a public HTTPS URL reachable by Slack.',
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load Slack diagnostics' });
  }
});

app.post('/api/integrations/slack/events', async (req, res) => {
  try {
    slackRuntime.lastReceivedAt = nowIso();

    const settings = await readSettings();
    const signingSecret = (typeof process.env.SLACK_SIGNING_SECRET === 'string' && process.env.SLACK_SIGNING_SECRET.trim())
      ? process.env.SLACK_SIGNING_SECRET.trim()
      : (typeof settings.slackSigningSecret === 'string' ? settings.slackSigningSecret.trim() : '');

    const botToken = (typeof process.env.SLACK_BOT_TOKEN === 'string' && process.env.SLACK_BOT_TOKEN.trim())
      ? process.env.SLACK_BOT_TOKEN.trim()
      : (typeof settings.slackBotToken === 'string' ? settings.slackBotToken.trim() : '');

    const verified = verifySlackRequest({ req, signingSecret });
    if (!verified.ok) {
      slackRuntime.lastRejectedAt = nowIso();
      slackRuntime.lastRejectedReason = verified.error || 'Unauthorized';
      debugWebhookLog('Slack events rejected', {
        reason: verified.error,
        contentType: req.headers['content-type'],
        hasSignature: Boolean(req.headers['x-slack-signature']),
        hasTimestamp: Boolean(req.headers['x-slack-request-timestamp']),
        forwardedProto: req.headers['x-forwarded-proto'],
        forwardedHost: req.headers['x-forwarded-host'],
        host: req.get('host'),
        method: req.method,
        path: req.originalUrl || req.url,
      });
      // Return a concrete reason; Slack shows this in delivery logs.
      res.status(401).json({ ok: false, error: verified.error || 'Unauthorized' });
      return;
    }

    const body = req.body && typeof req.body === 'object' ? req.body : {};
    if (body.type === 'url_verification') {
      slackRuntime.lastAcceptedAt = nowIso();
      slackRuntime.lastEventType = 'url_verification';
      res.json({ challenge: body.challenge });
      return;
    }

    if (body.type !== 'event_callback') {
      slackRuntime.lastAcceptedAt = nowIso();
      slackRuntime.lastEventType = String(body.type || 'unknown');
      res.json({ ok: true });
      return;
    }

    const eventId = typeof body.event_id === 'string' ? body.event_id.trim() : '';
    const teamId = typeof body.team_id === 'string' ? body.team_id.trim() : '';
    const ev = body.event && typeof body.event === 'object' ? body.event : {};
    const evType = typeof ev.type === 'string' ? ev.type : '';
    const subtype = typeof ev.subtype === 'string' ? ev.subtype : '';
    const isBot = Boolean(ev.bot_id) || Boolean(ev.bot_profile);

    slackRuntime.lastEventId = eventId;
    slackRuntime.lastTeamId = teamId;
    slackRuntime.lastEventType = evType || 'event_callback';

    // Capture human posts and mentions.
    // - message: channel/DM messages (requires Slack event subscriptions)
    // - app_mention: mentions of the app (common configuration when people expect "notifications")
    const captureable = (evType === 'message' || evType === 'app_mention');
    if (!captureable || subtype || isBot) {
      slackRuntime.lastAcceptedAt = nowIso();
      res.json({ ok: true });
      return;
    }

    const text = typeof ev.text === 'string' ? ev.text.trim() : '';
    if (!text) {
      slackRuntime.lastAcceptedAt = nowIso();
      res.json({ ok: true });
      return;
    }

    // ACK immediately. Slack expects a fast 2xx (typically within ~3 seconds).
    // Do the heavier work (disk IO + optional Slack API lookups) asynchronously.
    slackRuntime.lastAcceptedAt = nowIso();
    res.json({ ok: true });

    (async () => {
      // Optional: try to associate to a project if the message includes the project name.
      const store = await readStore();
      const matched = matchProjectFromText(store, text);
      const channel = typeof ev.channel === 'string' ? ev.channel : '';
      const user = typeof ev.user === 'string' ? ev.user : '';

      const display = botToken
        ? await formatSlackInboxText({ token: botToken, channelId: channel, userId: user, text })
        : [`Slack${channel ? ` ${channel}` : ''}${user ? ` @${user}` : ''}:`, text].join(' ');
      const externalId = `${teamId || 'team'}:${eventId || (typeof ev.ts === 'string' ? ev.ts : makeId())}`;

      await addInboxIntegrationItem({
        source: 'slack',
        externalId,
        text: display,
        projectId: matched?.id || '',
        projectName: matched?.name || '',
      });
    })().catch((err) => {
      slackRuntime.lastAsyncErrorAt = nowIso();
      slackRuntime.lastAsyncError = err?.message || 'unknown error';
      debugWebhookLog('Slack events async failure', {
        error: err?.message || 'unknown error',
        eventId,
        teamId,
      });
    });
  } catch (err) {
    slackRuntime.lastAsyncErrorAt = nowIso();
    slackRuntime.lastAsyncError = err?.message || 'unknown error';
    // Slack expects fast 2xx responses; treat unexpected errors as 200 to prevent retries storms.
    res.json({ ok: true, error: err?.message || 'unknown error' });
  }
});

// Integrations: Quo (Twilio) SMS webhook -> Inbox
// Configure your provider to send incoming message webhooks to: POST /api/integrations/quo/sms
// Twilio-compatible providers: set TWILIO_AUTH_TOKEN (env) or settings.quoAuthToken and ensure X-Twilio-Signature is sent.
// Non-Twilio providers: set QUO_WEBHOOK_TOKEN (env) and configure the sender to include it as:
// - Authorization: Bearer <token>, OR
// - X-Quo-Token: <token>, OR
// - add ?token=<token> to the webhook URL.
app.post('/api/integrations/quo/sms', async (req, res) => {
  try {
    const settings = await readSettings();
    const authToken = (typeof process.env.TWILIO_AUTH_TOKEN === 'string' && process.env.TWILIO_AUTH_TOKEN.trim())
      ? process.env.TWILIO_AUTH_TOKEN.trim()
      : (typeof settings.quoAuthToken === 'string' ? settings.quoAuthToken.trim() : '');

    const webhookToken = typeof process.env.QUO_WEBHOOK_TOKEN === 'string' ? process.env.QUO_WEBHOOK_TOKEN.trim() : '';

    const verified = verifyQuoWebhookRequest({ req, twilioAuthToken: authToken, webhookToken });
    if (!verified.ok) {
      debugWebhookLog('Quo SMS rejected', {
        reason: verified.error,
        fullUrl: `${getBaseUrl(req)}${req.originalUrl || req.url || ''}`,
        hasSignature: Boolean(req.headers['x-twilio-signature']),
        hasBearer: typeof req.headers.authorization === 'string' && req.headers.authorization.toLowerCase().startsWith('bearer '),
        hasQuoTokenHeader: Boolean(req.headers['x-quo-token']),
        hasTokenQuery: Boolean(req.query?.token),
        contentType: req.headers['content-type'],
        forwardedProto: req.headers['x-forwarded-proto'],
        forwardedHost: req.headers['x-forwarded-host'],
        host: req.get('host'),
        method: req.method,
        path: req.originalUrl || req.url,
      });
      res.status(401).type('text/plain').send(verified.error || 'Unauthorized');
      return;
    }

    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const sid = firstNonEmptyString(
      payload,
      ['MessageSid', 'SmsSid', 'sid', 'messageSid', 'message_id', 'id', 'data.id', 'data.sid', 'data.messageSid'],
      ['messagesid', 'smssid', 'sid', 'messagesid', 'message_id', 'id', 'eventsid', 'eventsid'],
    );
    const from = firstNonEmptyString(
      payload,
      ['From', 'from', 'sender', 'source', 'fromNumber', 'from_number', 'data.from', 'data.sender', 'data.source'],
      ['from', 'sender', 'source', 'fromnumber', 'from_number', 'phone', 'phonefrom', 'originator'],
    );
    const to = firstNonEmptyString(
      payload,
      ['To', 'to', 'recipient', 'destination', 'toNumber', 'to_number', 'data.to', 'data.recipient', 'data.destination'],
      ['to', 'recipient', 'destination', 'tonumber', 'to_number', 'phone_to'],
    );
    const body = firstNonEmptyString(
      payload,
      [
        'Body', 'body', 'message', 'text', 'content',
        'data.body', 'data.message', 'data.text', 'data.content',
        'data.payload.body', 'data.payload.message', 'data.payload.text',
      ],
      ['body', 'message', 'text', 'content', 'sms', 'smsbody'],
    );
    const contactName = firstNonEmptyString(
      payload,
      ['FromName', 'fromName', 'contactName', 'contact.name', 'data.fromName', 'data.contactName', 'data.contact.name', 'profile.name', 'senderName', 'data.senderName'],
      ['fromname', 'contactname', 'sendername', 'name'],
    );

    debugWebhookLog('Quo SMS payload', {
      keys: Object.keys(payload || {}).slice(0, 40),
      dataKeys: payload?.data && typeof payload.data === 'object' ? Object.keys(payload.data).slice(0, 40) : [],
      derived: {
        sid: sid ? 'yes' : 'no',
        from: from ? 'yes' : 'no',
        to: to ? 'yes' : 'no',
        bodyLen: body.length,
      },
      contentType: req.headers['content-type'],
    });

    if (!body) {
      debugWebhookLog('Quo SMS ignored (missing body)', {
        sid: sid || '',
        from: from || '',
        to: to || '',
        contentType: req.headers['content-type'],
      });
      res.status(200).type('text/plain').send('OK');
      return;
    }

    const smsAckFilterLevel = normalizeSmsAckFilterLevel(settings?.smsAckFilterLevel);

    if (isLowSignalAcknowledgementText(body, smsAckFilterLevel)) {
      debugWebhookLog('Quo SMS ignored (low-signal acknowledgement)', {
        sid: sid || '',
        from: from || '',
        to: to || '',
        level: smsAckFilterLevel,
        bodyPreview: previewTextServer(body, 80),
      });
      res.status(200).type('text/plain').send('OK');
      return;
    }

    const routing = resolveBusinessForInbound({ settings, toNumber: to });

    const { matched, finalProjectName, fromLabel } = await withBusinessKey(routing.businessKey, async () => {
      const businessStore = await readStore();
      const match = matchProjectFromText(businessStore, body);
      let projName = match?.name || '';
      let label = contactName || from || '';

      const storeForMap = {
        ...businessStore,
        senderProjectMap: businessStore?.senderProjectMap || settings?.senderProjectMap || {},
      };
      const auto = resolveSenderProjectMapping(storeForMap, from);
      if (auto?.projectId) {
        if (auto.projectName) label = auto.projectName;
        if (!projName) {
          projName = auto.projectName || projName;
          if (match) match.id = auto.projectId;
        }
      }

      return { matched: match, finalProjectName: projName, fromLabel: label };
    });

    const senderDigits = normalizePhoneForLookup(from);
    const toDigits = normalizePhoneForLookup(to);
    const smsThreadKey = `sms-thread:${senderDigits || from || 'unknown'}:${toDigits || to || 'unknown'}`;
    const lineText = `[${nowIso()}] ${String(contactName || fromLabel || from || 'Sender').trim()}: ${body}`;

    await addInboxIntegrationItem({
      source: 'sms',
      externalId: smsThreadKey,
      text: lineText,
      projectId: matched?.id || '',
      projectName: finalProjectName,
      businessKey: routing.businessKey,
      businessLabel: routing.businessLabel,
      toNumber: to,
      fromNumber: from,
      contactName: contactName || '',
      fromName: fromLabel || contactName || '',
      threadKey: smsThreadKey,
      threadMerge: true,
      channel: 'sms',
    });

    debugWebhookLog('Quo SMS accepted', {
      sid: sid || '',
      from: from || '',
      to: to || '',
      bodyLen: body.length,
    });

    res.status(200).type('text/plain').send('OK');
  } catch (err) {
    res.status(200).type('text/plain').send('OK');
  }
});

// Integrations: Quo (Twilio) Voice status callback -> Inbox (missed calls)
// Configure provider status callbacks to: POST /api/integrations/quo/calls
app.post('/api/integrations/quo/calls', async (req, res) => {
  try {
    const settings = await readSettings();
    const authToken = (typeof process.env.TWILIO_AUTH_TOKEN === 'string' && process.env.TWILIO_AUTH_TOKEN.trim())
      ? process.env.TWILIO_AUTH_TOKEN.trim()
      : (typeof settings.quoAuthToken === 'string' ? settings.quoAuthToken.trim() : '');

    const webhookToken = typeof process.env.QUO_WEBHOOK_TOKEN === 'string' ? process.env.QUO_WEBHOOK_TOKEN.trim() : '';

    const verified = verifyQuoWebhookRequest({ req, twilioAuthToken: authToken, webhookToken });
    if (!verified.ok) {
      debugWebhookLog('Quo call rejected', {
        reason: verified.error,
        fullUrl: `${getBaseUrl(req)}${req.originalUrl || req.url || ''}`,
        hasSignature: Boolean(req.headers['x-twilio-signature']),
        hasBearer: typeof req.headers.authorization === 'string' && req.headers.authorization.toLowerCase().startsWith('bearer '),
        hasQuoTokenHeader: Boolean(req.headers['x-quo-token']),
        hasTokenQuery: Boolean(req.query?.token),
        contentType: req.headers['content-type'],
        forwardedProto: req.headers['x-forwarded-proto'],
        forwardedHost: req.headers['x-forwarded-host'],
        host: req.get('host'),
        method: req.method,
        path: req.originalUrl || req.url,
      });
      res.status(401).type('text/plain').send(verified.error || 'Unauthorized');
      return;
    }

    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const callSid = firstNonEmptyString(
      payload,
      ['CallSid', 'callSid', 'sid', 'call_id', 'id', 'data.callSid', 'data.sid', 'data.id'],
      ['callsid', 'call_sid', 'sid', 'id'],
    );
    const from = firstNonEmptyString(
      payload,
      ['From', 'from', 'caller', 'source', 'fromNumber', 'from_number', 'data.from', 'data.caller', 'data.source'],
      ['from', 'caller', 'source', 'fromnumber', 'from_number', 'phonefrom', 'originator'],
    );
    const to = firstNonEmptyString(
      payload,
      ['To', 'to', 'callee', 'destination', 'toNumber', 'to_number', 'data.to', 'data.callee', 'data.destination'],
      ['to', 'callee', 'destination', 'tonumber', 'to_number', 'phone_to'],
    );
    const callStatus = firstNonEmptyString(
      payload,
      ['CallStatus', 'CallStatusCallbackEvent', 'status', 'event', 'callStatus', 'data.status', 'data.event', 'data.callStatus'],
      ['callstatus', 'status', 'event', 'state', 'disposition'],
    );

    debugWebhookLog('Quo call payload', {
      keys: Object.keys(payload || {}).slice(0, 40),
      dataKeys: payload?.data && typeof payload.data === 'object' ? Object.keys(payload.data).slice(0, 40) : [],
      derived: {
        callSid: callSid ? 'yes' : 'no',
        from: from ? 'yes' : 'no',
        to: to ? 'yes' : 'no',
        callStatus: callStatus || '',
      },
      contentType: req.headers['content-type'],
    });

    // Twilio final CallStatus values: queued, ringing, in-progress, completed, busy, failed, no-answer, canceled
    const missed = ['busy', 'failed', 'no-answer', 'canceled', 'missed', 'no_answer', 'noanswer'].includes(callStatus.toLowerCase());
    if (!missed) {
      res.status(200).type('text/plain').send('OK');
      return;
    }

    const routing = resolveBusinessForInbound({ settings, toNumber: to });
    const text = `Missed call${from ? ` from ${from}` : ''}${to ? ` ? ${to}` : ''}${callStatus ? ` (${callStatus})` : ''} � ${routing.businessLabel}`;
    await addInboxIntegrationItem({
      source: 'call',
      externalId: `call:${callSid || crypto.createHash('sha1').update(`${from}|${to}|${callStatus}|${Date.now()}`).digest('hex')}`,
      text,
      businessKey: routing.businessKey,
      businessLabel: routing.businessLabel,
      toNumber: to,
      fromNumber: from,
      channel: 'call',
    });

    debugWebhookLog('Quo call accepted', {
      callSid: callSid || '',
      from: from || '',
      to: to || '',
      callStatus: callStatus || '',
    });

    res.status(200).type('text/plain').send('OK');
  } catch {
    res.status(200).type('text/plain').send('OK');
  }
});

// Integrations: MCP (Model Context Protocol) over stdio
app.get('/api/integrations/mcp/status', async (req, res) => {
  const settings = await readSettings();
  const eff = getMcpEffectiveSettings(settings);
  res.json({
    ok: true,
    enabled: Boolean(eff.enabled),
    configured: Boolean(eff.configured),
    legacy: {
      enabled: Boolean(eff.legacy.enabled),
      configured: Boolean(eff.legacy.enabled && eff.legacy.command),
      command: eff.legacy.command,
      args: eff.legacy.args,
      cwd: eff.legacy.cwd,
    },
    servers: eff.servers.map((s) => ({
      name: s.name,
      enabled: Boolean(s.enabled),
      configured: Boolean(s.enabled && s.command),
      command: s.command,
      args: s.args,
      cwd: s.cwd,
    })),
  });
});

app.post('/api/integrations/mcp/tools', async (req, res) => {
  try {
    const settings = await readSettings();
    const serverRaw = typeof req.body?.server === 'string' ? req.body.server.trim() : '';
    const server = normalizeMcpServerName(serverRaw);
    const eff = getMcpEffectiveSettings(settings);

    if (server) {
      if (server === 'legacy') {
        if (!eff.legacy.enabled || !eff.legacy.command) {
          res.status(400).json({ ok: false, error: 'Legacy MCP is not enabled/configured in Settings.' });
          return;
        }
        const result = await mcpListTools({ command: eff.legacy.command, args: eff.legacy.args, cwd: eff.legacy.cwd || process.cwd() });
        res.json({ ok: true, tools: result.tools || [] });
        return;
      }

      const target = eff.servers.find((s) => s.name === server);
      if (!target || !target.enabled || !target.command) {
        res.status(400).json({ ok: false, error: `MCP server not configured: ${server}` });
        return;
      }
      const result = await mcpListTools({ command: target.command, args: target.args, cwd: target.cwd || process.cwd() });
      res.json({ ok: true, tools: result.tools || [] });
      return;
    }

    if (!eff.configured) {
      res.status(400).json({ ok: false, error: 'MCP is not enabled/configured in Settings.' });
      return;
    }

    const tools = await mcpListToolsAll(settings);
    res.json({ ok: true, tools });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to list MCP tools' });
  }
});

app.post('/api/integrations/mcp/call', async (req, res) => {
  try {
    const settings = await readSettings();
    const name = typeof req.body?.name === 'string' ? req.body.name : '';
    const args = req.body?.arguments && typeof req.body.arguments === 'object' && !Array.isArray(req.body.arguments) ? req.body.arguments : {};

    const resolved = resolveMcpTarget(settings, name);
    if (!resolved.ok) {
      res.status(400).json({ ok: false, error: resolved.error || 'MCP is not enabled/configured in Settings.' });
      return;
    }

    const cfg = resolved.target.config;
    const result = await mcpCallTool({ command: cfg.command, args: cfg.args, cwd: cfg.cwd || process.cwd() }, resolved.toolName, args);
    res.json({ ok: true, result, server: resolved.target.name, tool: resolved.toolName });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to call MCP tool' });
  }
});

app.put('/api/settings/openai', async (req, res) => {
  const openaiApiKey = typeof req.body?.openaiApiKey === 'string' ? req.body.openaiApiKey.trim() : '';
  const openaiModel = typeof req.body?.openaiModel === 'string' ? req.body.openaiModel.trim() : '';

  // Avoid accidentally returning secrets back to the browser.
  writeLock = writeLock.then(async () => {
    const saved = await readSettings();
    const next = {
      ...saved,
      openaiApiKey,
      openaiModel,
      updatedAt: nowIso(),
    };
    await writeSettings(next);
    const last4 = openaiApiKey && openaiApiKey.length >= 4 ? openaiApiKey.slice(-4) : '';
    const keyHint = last4 ? `����${last4}` : '';
    res.json({
      ok: true,
      aiEnabled: Boolean(openaiApiKey),
      openaiModel: openaiModel || 'gpt-4o-mini',
      openaiKeyHint: keyHint,
      source: openaiApiKey ? 'saved' : 'none',
      settingsUpdatedAt: next.updatedAt,
    });
  });

  await writeLock;
});

app.get('/api/tasks', async (req, res) => {
  // Serialize with writeLock so one-time migrations don't race with writes.
  let outStore = null;
  let outError = null;

  writeLock = writeLock.then(async () => {
    let store = await readStore();
    const repaired = repairProjectsMissingIds(store);
    if (repaired.changed) {
      store = {
        ...repaired.store,
        revision: Math.max(Number(store.revision) || 1, 1) + 1,
      };
      await writeStore(store);
    }

    const migrated = migrateLegacyAirtableClientProjects(store);
    if (!migrated.changed) {
      outStore = store;
      return;
    }
    await writeStore(migrated.store);
    outStore = migrated.store;
  }).catch((err) => {
    outError = err;
  });

  await writeLock;
  if (outError) {
    res.status(500).json({ ok: false, error: outError?.message || 'Failed to load store' });
    return;
  }
  const settings = await readSettings();
  const visibleStore = applyInboxVisibilityToStore(stripAirtableRevisionMaterializedData(outStore || structuredClone(EMPTY_STORE), settings), settings);
  res.json(visibleStore);
});

// Inbox (global capture)
app.get('/api/inbox', async (req, res) => {
  const store = await readStore();
  const settings = await readSettings();
  const items = getVisibleInboxItemsFromSettings(store.inboxItems, settings);
  res.json({ revision: store.revision, updatedAt: store.updatedAt, items });
});

app.post('/api/inbox/marcus-filter', async (req, res) => {
  writeLock = writeLock.then(async () => {
    const store = await readStore();
    const collapsed = collapseSmsInboxThreads(store);
    const workingStore = collapsed.changed ? collapsed.store : store;
    const settings = await readSettings();
    const level = normalizeSmsAckFilterLevel(settings?.smsAckFilterLevel);

    const list = Array.isArray(workingStore.inboxItems) ? workingStore.inboxItems : [];
    let scanned = 0;
    let matched = 0;
    let archived = 0;
    const ts = nowIso();

    const nextList = list.map((item) => {
      const it = item && typeof item === 'object' ? item : {};
      const status = String(it?.status || '').trim().toLowerCase();
      if (status !== 'new') return item;

      const src = String(it?.source || '').trim().toLowerCase();
      const signalText = extractInboxSignalText(it);
      const isExcluded = isInboxItemExcludedFromMarcus(it, settings);
      const sourceIsSystemNoise = src === 'marcus';
      const isAckNoise = isLowSignalAcknowledgementText(signalText, level);
      if (!sourceIsSystemNoise && !isAckNoise && !isExcluded) return item;

      scanned += 1;
      matched += 1;
      if (status === 'archived') return item;

      archived += 1;
      return normalizeInboxItem({
        ...it,
        status: 'Archived',
        updatedAt: ts,
        marcusFilterLevel: level,
        marcusFilteredAt: ts,
        marcusFilterReason: isExcluded ? 'excluded-phone-number' : (sourceIsSystemNoise ? 'system-radar-noise' : 'low-signal-ack'),
      });
    });

    if (!archived) {
      if (collapsed.changed) {
        await writeStore(workingStore);
      }
      res.json({ ok: true, scanned, matched, archived: 0, collapsedThreads: Number(collapsed.collapsedThreads || 0), mergedMessages: Number(collapsed.mergedItems || 0), level, store: applyInboxVisibilityToStore(workingStore, settings) });
      return;
    }

    const nextStore = {
      ...workingStore,
      revision: workingStore.revision + 1,
      updatedAt: ts,
      inboxItems: nextList,
    };
    await writeStore(nextStore);
    res.json({ ok: true, scanned, matched, archived, collapsedThreads: Number(collapsed.collapsedThreads || 0), mergedMessages: Number(collapsed.mergedItems || 0), level, store: applyInboxVisibilityToStore(nextStore, settings) });
  });

  await writeLock;
});

app.get('/api/inbox/marcus-triage', async (req, res) => {
  try {
    const store = await readStore();
    const settings = await readSettings();
    const includeArchived = String(req.query?.includeArchived || '').trim().toLowerCase() === '1';
    const onlyNew = String(req.query?.onlyNew || '').trim().toLowerCase() !== '0';
    const limitRaw = Number(req.query?.limit);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(200, Math.floor(limitRaw))) : 60;

    const visible = getVisibleInboxItemsFromSettings(store.inboxItems, settings);
    let list = visible;
    list = list.filter((item) => !isInboxItemExcludedFromMarcus(item, settings));
    if (!includeArchived) {
      list = list.filter((x) => String(x?.status || '').trim().toLowerCase() !== 'archived');
    }
    if (onlyNew) {
      list = list.filter((x) => String(x?.status || '').trim().toLowerCase() === 'new');
    }

    const recommendations = list
      .slice(0, limit)
      .map((item) => buildMarcusInboxRecommendation(store, item));

    res.json({
      ok: true,
      count: recommendations.length,
      onlyNew,
      includeArchived,
      limit,
      recommendations,
      generatedAt: nowIso(),
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to build M.A.R.C.U.S. triage recommendations' });
  }
});

app.get('/api/inbox/automation/digest', async (req, res) => {
  try {
    const settings = await readSettings();
    const queue = normalizeAutomationDigestQueue(settings?.automationDigestQueue);
    const pending = queue.filter((e) => e.status === 'pending');
    res.json({ ok: true, count: pending.length, items: pending.slice(0, 200) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load automation digest queue' });
  }
});

app.post('/api/inbox/automation/digest/:id/decision', async (req, res) => {
  const digestId = String(req.params?.id || '').trim();
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const acceptProjectLink = body.acceptProjectLink === true;
  const acceptTaskIndexes = Array.isArray(body.acceptTaskIndexes)
    ? body.acceptTaskIndexes
      .map((x) => Number(x))
      .filter((x) => Number.isInteger(x) && x >= 0 && x <= 20)
    : [];
  const reject = body.reject === true;

  if (!digestId) {
    res.status(400).json({ ok: false, error: 'Missing digest id' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    const settings = await readSettings();
    const cfg = normalizeAutomationConfig(settings?.automationConfig);
    const queue = normalizeAutomationDigestQueue(settings?.automationDigestQueue);
    const idx = queue.findIndex((e) => e.id === digestId);
    if (idx < 0) {
      res.status(404).json({ ok: false, error: 'Digest item not found' });
      return;
    }

    const entry = queue[idx];
    if (entry.status !== 'pending') {
      res.status(400).json({ ok: false, error: 'Digest item already decided' });
      return;
    }

    const inboxId = String(entry.itemId || '').trim();
    const itemIdx = (Array.isArray(store.inboxItems) ? store.inboxItems : []).findIndex((x) => String(x?.id || '') === inboxId);
    if (itemIdx < 0) {
      queue[idx] = {
        ...entry,
        status: 'rejected',
        decidedAt: nowIso(),
        decision: { acceptProjectLink: false, acceptDelegate: false, acceptTaskIndexes: [] },
      };
      await writeSettings({ ...settings, automationDigestQueue: normalizeAutomationDigestQueue(queue), updatedAt: nowIso() });
      res.status(404).json({ ok: false, error: 'Inbox item for digest no longer exists' });
      return;
    }

    const item = store.inboxItems[itemIdx] && typeof store.inboxItems[itemIdx] === 'object' ? store.inboxItems[itemIdx] : {};
    const ts = nowIso();
    const acceptedIndexSet = new Set(acceptTaskIndexes);
    const selectedTasks = reject
      ? []
      : (Array.isArray(entry.tasks) ? entry.tasks.filter((_, taskIndex) => acceptedIndexSet.has(taskIndex)) : []);

    const createdTaskIds = [];
    const nextTasks = Array.isArray(store.tasks) ? [...store.tasks] : [];
    const projectName = String(entry.projectName || item?.projectName || 'Other').trim() || 'Other';
    for (const t of selectedTasks) {
      const title = String(t?.title || '').trim();
      if (!title) continue;
      const priority = [1, 2, 3].includes(Number(t?.priority)) ? Number(t.priority) : 2;
      const task = {
        id: makeId(),
        title,
        project: projectName,
        type: 'Other',
        owner: '',
        status: 'Next',
        priority,
        dueDate: '',
        createdAt: ts,
        updatedAt: ts,
        createdBy: 'marcus-automation',
      };
      nextTasks.unshift(task);
      createdTaskIds.push(task.id);
    }

    const nextInboxItems = Array.isArray(store.inboxItems) ? [...store.inboxItems] : [];
    nextInboxItems[itemIdx] = normalizeInboxItem({
      ...item,
      projectId: acceptProjectLink ? String(entry.projectId || '').trim() : item?.projectId,
      projectName: acceptProjectLink
        ? (String(entry.projectName || '').trim() || item?.projectName)
        : item?.projectName,
      status: createdTaskIds.length && cfg.inboxAutoConvert.markInboxDoneOnApply ? 'Done' : item?.status,
      updatedAt: ts,
      automation: {
        mode: 'digest',
        runId: String(entry.runId || '').trim(),
        appliedAt: ts,
        approvalMode: 'dailyDigest',
        appliedTaskIds: createdTaskIds,
        projectLinked: Boolean(acceptProjectLink && entry.projectId),
        delegatedTo: '',
      },
    });

    queue[idx] = {
      ...entry,
      status: (createdTaskIds.length || acceptProjectLink) && !reject ? 'applied' : 'rejected',
      decidedAt: ts,
      appliedTaskIds: createdTaskIds,
      decision: {
        acceptProjectLink: Boolean(!reject && acceptProjectLink),
        acceptDelegate: false,
        acceptTaskIndexes: reject ? [] : Array.from(acceptedIndexSet.values()).sort((a, b) => a - b),
      },
    };

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: nextInboxItems,
      tasks: nextTasks,
    };

    await writeStore(nextStore);
    await writeSettings({
      ...settings,
      automationDigestQueue: normalizeAutomationDigestQueue(queue),
      updatedAt: ts,
    });

    const pendingCount = queue.filter((e) => e.status === 'pending').length;
    res.json({
      ok: true,
      createdTasks: createdTaskIds.length,
      pendingCount,
      digestItem: queue[idx],
      store: applyInboxVisibilityToStore(nextStore, settings),
    });
  });

  await writeLock;
});

app.post('/api/inbox/automation/run', async (req, res) => {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const approvalOverrideRaw = typeof body.approvalMode === 'string' ? body.approvalMode.trim().toLowerCase() : '';
  const approvalOverride = ['manual', 'dailydigest', 'auto'].includes(approvalOverrideRaw)
    ? (approvalOverrideRaw === 'dailydigest' ? 'dailyDigest' : approvalOverrideRaw)
    : '';

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    const settings = await readSettings();
    const cfg = normalizeAutomationConfig(settings?.automationConfig);
    const inboxCfg = cfg.inboxAutoConvert;
    const runId = `auto-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const ts = nowIso();
    const effectiveApprovalMode = approvalOverride || cfg.approvalMode;

    if (!cfg.enabled || !inboxCfg.enabled) {
      res.json({
        ok: true,
        runId,
        enabled: false,
        approvalMode: effectiveApprovalMode,
        scanned: 0,
        proposed: 0,
        applied: 0,
        skipped: 0,
        reason: 'automation disabled in settings',
      });
      return;
    }

    const visible = getVisibleInboxItemsFromSettings(store.inboxItems, settings);
    let list = visible.filter((item) => !isInboxItemExcludedFromMarcus(item, settings));
    if (!inboxCfg.includeArchived) {
      list = list.filter((x) => String(x?.status || '').trim().toLowerCase() !== 'archived');
    }
    if (inboxCfg.onlyNew) {
      list = list.filter((x) => String(x?.status || '').trim().toLowerCase() === 'new');
    }
    list = list.slice(0, inboxCfg.limit);

    const byId = new Map((Array.isArray(store.inboxItems) ? store.inboxItems : []).map((x) => [String(x?.id || ''), x]));
    const nextList = Array.isArray(store.inboxItems) ? [...store.inboxItems] : [];
    const nextTasks = Array.isArray(store.tasks) ? [...store.tasks] : [];
    const existingQueue = normalizeAutomationDigestQueue(settings?.automationDigestQueue);
    const queueByItem = new Set(existingQueue.filter((e) => e.status === 'pending').map((e) => e.itemId));
    const appendedQueue = [];

    let scanned = 0;
    let proposed = 0;
    let applied = 0;
    let skipped = 0;

    for (const item of list) {
      const itemId = String(item?.id || '').trim();
      if (!itemId) continue;
      scanned += 1;

      const current = byId.get(itemId) && typeof byId.get(itemId) === 'object' ? byId.get(itemId) : item;
      const alreadyAutomated = Array.isArray(current?.automation?.appliedTaskIds) && current.automation.appliedTaskIds.length;
      const alreadyConverted = String(current?.converted?.kind || '').trim().toLowerCase() === 'task';
      if (alreadyAutomated || alreadyConverted) {
        skipped += 1;
        continue;
      }

      const recommendation = buildMarcusInboxRecommendation(store, current);
      const recTasks = Array.isArray(recommendation?.tasks)
        ? recommendation.tasks.map((t) => ({
          title: String(t?.title || '').trim(),
          priority: [1, 2, 3].includes(Number(t?.priority)) ? Number(t.priority) : 2,
        })).filter((t) => t.title).slice(0, inboxCfg.maxTasksPerItem)
        : [];
      if (!recTasks.length) {
        skipped += 1;
        continue;
      }

      const recProjectId = String(recommendation?.project?.projectId || '').trim();
      const recProjectName = String(recommendation?.project?.projectName || '').trim();
      const recProjectConfidence = clampUnit(recommendation?.project?.confidence, 0);
      const canAutoLinkProject = Boolean(recProjectId && recProjectConfidence >= inboxCfg.minProjectConfidence);

      const resolvedProjectId = canAutoLinkProject ? recProjectId : String(current?.projectId || '').trim();
      const resolvedProject = resolvedProjectId
        ? (Array.isArray(store.projects) ? store.projects : []).find((p) => String(p?.id || '').trim() === resolvedProjectId)
        : null;
      const resolvedProjectName = String(
        resolvedProject?.name
        || (canAutoLinkProject ? recProjectName : '')
        || current?.projectName
        || recProjectName
        || 'Other'
      ).trim() || 'Other';

      if (effectiveApprovalMode === 'dailyDigest' || effectiveApprovalMode === 'manual') {
        if (!queueByItem.has(itemId)) {
          appendedQueue.push({
            id: makeId(),
            itemId,
            status: 'pending',
            createdAt: ts,
            runId,
            source: 'marcus-automation',
            signalPreview: String(recommendation?.signalPreview || '').trim(),
            projectId: canAutoLinkProject ? recProjectId : '',
            projectName: resolvedProjectName,
            projectConfidence: recProjectConfidence,
            delegateName: '',
            delegateConfidence: 0,
            tasks: recTasks,
          });
          queueByItem.add(itemId);
          proposed += 1;
        } else {
          skipped += 1;
        }
        continue;
      }

      const createdTaskIds = [];
      for (const t of recTasks) {
        const task = {
          id: makeId(),
          title: t.title,
          project: resolvedProjectName,
          type: 'Other',
          owner: '',
          status: 'Next',
          priority: t.priority,
          dueDate: '',
          createdAt: ts,
          updatedAt: ts,
          createdBy: 'marcus-automation',
        };
        nextTasks.unshift(task);
        createdTaskIds.push(task.id);
      }

      const idx = nextList.findIndex((x) => String(x?.id || '') === itemId);
      if (idx >= 0) {
        nextList[idx] = normalizeInboxItem({
          ...current,
          status: inboxCfg.markInboxDoneOnApply ? 'Done' : current?.status,
          updatedAt: ts,
          projectId: inboxCfg.autoLinkProject && canAutoLinkProject ? recProjectId : current?.projectId,
          projectName: inboxCfg.autoLinkProject && canAutoLinkProject ? resolvedProjectName : current?.projectName,
          automation: {
            mode: 'auto',
            runId,
            appliedAt: ts,
            approvalMode: effectiveApprovalMode,
            appliedTaskIds: createdTaskIds,
            projectLinked: Boolean(inboxCfg.autoLinkProject && canAutoLinkProject),
            delegatedTo: '',
            recommendation,
          },
        });
      }

      applied += 1;
    }

    const nextQueue = normalizeAutomationDigestQueue([...existingQueue, ...appendedQueue]);
    const anyStoreChanges = applied > 0;
    const anySettingsChanges = appendedQueue.length > 0;

    let nextStore = store;
    if (anyStoreChanges) {
      nextStore = {
        ...store,
        revision: store.revision + 1,
        updatedAt: ts,
        inboxItems: nextList,
        tasks: nextTasks,
      };
      await writeStore(nextStore);
    }

    if (anySettingsChanges) {
      await writeSettings({
        ...settings,
        automationConfig: cfg,
        automationDigestQueue: nextQueue,
        updatedAt: ts,
      });
    }

    const storeForResponse = anyStoreChanges ? nextStore : store;
    res.json({
      ok: true,
      runId,
      enabled: true,
      approvalMode: effectiveApprovalMode,
      scanned,
      proposed,
      applied,
      skipped,
      digestPending: nextQueue.filter((e) => e.status === 'pending').length,
      store: applyInboxVisibilityToStore(storeForResponse, settings),
      preview: appendedQueue.slice(0, 5),
    });
  });

  await writeLock;
});

// Global Dashboard (cross-business) Focus
app.get('/api/me/dashboard', async (req, res) => {
  try {
    const settings = await readSettings();
    const cfg = getBusinessConfigFromSettings(settings);
    const businesses = Array.isArray(cfg.businesses) ? cfg.businesses : [];
    const today = new Date().toISOString().split('T')[0];

    const globalBusinesses = [];
    const focusProjects = [];
    const globalSlackItems = [];
    const globalTeam = [];
    const globalBriefs = [];
    const seenTeamNames = new Set();

    for (const b of businesses) {
      const bizKey = normalizeBusinessKey(b?.key || '');
      const bizName = typeof b?.name === 'string' ? b.name.trim() : '';
      if (!bizKey) continue;

      const store = await withBusinessKey(bizKey, async () => readStore());

      const items = getVisibleInboxItemsFromSettings(store?.inboxItems, settings);

      // Latest M.A.R.C.U.S. brief (if any) for this business.
      let latestBrief = null;
      for (const item of items) {
        const src = String(item?.source || '').trim().toLowerCase();
        if (src !== 'marcus' && src !== 'marcus') continue;
        const ts = String(item?.updatedAt || item?.createdAt || '').trim();
        const bestTs = String(latestBrief?.updatedAt || latestBrief?.createdAt || '').trim();
        if (!latestBrief || ts > bestTs) latestBrief = item;
      }
      if (latestBrief) {
        globalBriefs.push({
          id: latestBrief.id,
          text: latestBrief.text,
          status: latestBrief.status,
          createdAt: latestBrief.createdAt,
          updatedAt: latestBrief.updatedAt,
          businessKey: bizKey,
          businessName: bizName || bizKey,
        });
      }
      let newInboxCount = 0;
      for (const item of items) {
         const itemStatus = String(item.status || '').trim().toLowerCase();
         if (itemStatus === 'new') {
           newInboxCount++;
           if (String(item.source || '').trim().toLowerCase() === 'slack') {
             globalSlackItems.push({
               ...item,
               businessKey: bizKey,
               businessName: bizName || bizKey
             });
           }
         }
      }

      globalBusinesses.push({
        key: bizKey,
        name: bizName || bizKey,
        inboxCount: newInboxCount,
      });

      const storeTeam = Array.isArray(store?.team) ? store.team : [];
      for (const t of storeTeam) {
        const tName = String(t.name || '').trim();
        if (tName && !seenTeamNames.has(tName.toLowerCase())) {
          seenTeamNames.add(tName.toLowerCase());
          globalTeam.push({
            id: t.id || tName,
            name: tName,
            title: t.title || '',
            avatar: t.avatar || '',
            businessKey: bizKey
          });
        }
      }

      const storeProjects = Array.isArray(store?.projects) ? store.projects : [];
      const storeTasks = Array.isArray(store?.tasks) ? store.tasks : [];

      for (const proj of storeProjects) {
        const pStatus = String(proj.status || '').toLowerCase();
        if (pStatus === 'done' || pStatus === 'archived') continue;

        // Find associated tasks
        const projTasks = storeTasks.filter(t => t.project === proj.name || t.projectId === proj.id);
        
        let total = projTasks.length;
        let completed = 0;
        let urgent = 0;

        for (const t of projTasks) {
          const tStatus = String(t.status || '').toLowerCase();
          if (tStatus === 'done' || tStatus === 'archived') {
            completed++;
            continue;
          }
          if (Number(t.priority) === 1 || tStatus === 'urgent' || (t.dueDate && t.dueDate <= today)) {
            urgent++;
          }
        }

        focusProjects.push({
          id: proj.id,
          name: proj.name,
          dueDate: proj.dueDate || '',
          businessKey: bizKey,
          businessName: bizName || bizKey,
          totalTasks: total,
          completedTasks: completed,
          urgentTasks: urgent
        });
      }
    }

    // Sort focus projects: urgent tasks first, then by due date
    focusProjects.sort((a, b) => {
       if (a.urgentTasks !== b.urgentTasks) return b.urgentTasks - a.urgentTasks;
       const ad = a.dueDate || '9999-12-31';
       const bd = b.dueDate || '9999-12-31';
       return ad.localeCompare(bd);
    });

    // Sort briefs by recency
    globalBriefs.sort((a, b) => String(b.updatedAt || b.createdAt || '').localeCompare(String(a.updatedAt || a.createdAt || '')));

    // Sort slack items by recency
    globalSlackItems.sort((a, b) => {
       const ad = a.createdAt || '';
       const bd = b.createdAt || '';
       return bd.localeCompare(ad);
    });

    res.json({ businesses: globalBusinesses, focusProjects: focusProjects, slackItems: globalSlackItems, team: globalTeam, briefs: globalBriefs });
  } catch (err) {
    console.error('Error in /api/me/dashboard:', err);
    res.status(500).json({ error: err.message });
  }
});
// Inbox Radar (cross-business)
// Returns inbox items across all businesses for dashboard radar.
app.get('/api/inbox/radar', async (req, res) => {
  try {
    const limitRaw = Number(req.query?.limit);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(200, Math.floor(limitRaw))) : 50;
    const status = typeof req.query?.status === 'string' ? req.query.status.trim() : 'New';
    const statusLower = status.toLowerCase();

    const settings = await readSettings();
    const cfg = getBusinessConfigFromSettings(settings);
    const businesses = Array.isArray(cfg.businesses) ? cfg.businesses : [];

    const all = [];
    const businessGroupsByKey = new Map();
    for (const b of businesses) {
      const bizKey = normalizeBusinessKey(b?.key || '');
      const bizName = typeof b?.name === 'string' ? b.name.trim() : '';
      if (!bizKey) continue;

      const store = await withBusinessKey(bizKey, async () => readStore());
      const projectsById = new Map(
        (Array.isArray(store?.projects) ? store.projects : []).map((p) => [String(p?.id || ''), String(p?.name || '').trim()]).filter(([id]) => Boolean(id)),
      );
      const items = Array.isArray(store?.inboxItems) ? store.inboxItems : [];
      for (const item of items) {
        const it = item && typeof item === 'object' ? item : {};
        const itStatus = String(it.status || '').trim();
        if (statusLower && itStatus.toLowerCase() !== statusLower) continue;
        if (shouldSuppressInboxRadarItem(it, settings)) continue;

        const pid = String(it.projectId || '').trim();
        const normalized = {
          ...it,
          businessKey: typeof it.businessKey === 'string' && it.businessKey.trim() ? it.businessKey.trim() : bizKey,
          businessLabel: typeof it.businessLabel === 'string' && it.businessLabel.trim() ? it.businessLabel.trim() : (bizName || bizKey),
          projectName: String(it.projectName || '').trim() || (pid ? (projectsById.get(pid) || '') : ''),
        };
        all.push(normalized);

        // Aggregate at-a-glance totals by business.
        const bKey = String(normalized.businessKey || '').trim() || bizKey;
        const bLabel = String(normalized.businessLabel || '').trim() || (bizName || bizKey);
        const t = typeof normalized?.updatedAt === 'string' && normalized.updatedAt.trim()
          ? normalized.updatedAt
          : (typeof normalized?.createdAt === 'string' ? normalized.createdAt : '');
        const ms = Number.isFinite(Date.parse(t)) ? Date.parse(t) : 0;
        const preview = previewTextServer(normalized?.text, 160);
        const sender = String(normalized?.contactName || normalized?.fromName || normalized?.sender || normalized?.from || '').trim();
        const source = String(normalized?.source || '').trim().toLowerCase();
        const existingBiz = businessGroupsByKey.get(bKey);
        if (!existingBiz) {
          const senders = new Set();
          if (sender) senders.add(sender);
          const sources = new Set();
          if (source) sources.add(source);
          businessGroupsByKey.set(bKey, {
            businessKey: bKey,
            businessLabel: bLabel,
            count: 1,
            latestAt: t,
            latestMs: ms,
            sample: preview ? [preview] : [],
            _senders: senders,
            _sources: sources,
            summary: '',
          });
        } else {
          existingBiz.count += 1;
          if (ms > existingBiz.latestMs) {
            existingBiz.latestMs = ms;
            existingBiz.latestAt = t;
          }
          if (preview && existingBiz.sample.length < 3) existingBiz.sample.push(preview);
          if (sender && existingBiz._senders.size < 5) existingBiz._senders.add(sender);
          if (source) existingBiz._sources.add(source);
        }
      }
    }

    const timeValue = (x) => {
      const t = typeof x?.updatedAt === 'string' && x.updatedAt.trim() ? x.updatedAt : (typeof x?.createdAt === 'string' ? x.createdAt : '');
      const ms = Date.parse(t);
      return Number.isFinite(ms) ? ms : 0;
    };

    all.sort((a, b) => timeValue(b) - timeValue(a));
    const items = all.slice(0, limit);

    const groupsById = new Map();
    const isAssigned = (it) => {
      const pid = String(it?.projectId || '').trim();
      const iid = String(it?.id || '').trim();
      return Boolean(pid) && pid !== iid;
    };

    for (const it of items) {
      const bizKey = String(it?.businessKey || '').trim();
      const bizLabel = String(it?.businessLabel || '').trim();
      const assigned = isAssigned(it);
      const pid = assigned ? String(it?.projectId || '').trim() : '';
      const pname = assigned ? String(it?.projectName || '').trim() : '';

      const groupId = assigned ? `${bizKey}:${pid}` : `${bizKey}:__unassigned__`;
      const existing = groupsById.get(groupId);

      const ms = timeValue(it);
      const preview = previewTextServer(it?.text, 160);

      if (!existing) {
        groupsById.set(groupId, {
          groupId,
          businessKey: bizKey,
          businessLabel: bizLabel,
          projectId: pid,
          projectName: pname,
          assigned,
          isUnassigned: !assigned,
          count: 1,
          latestAt: typeof it?.updatedAt === 'string' && it.updatedAt.trim() ? it.updatedAt : (typeof it?.createdAt === 'string' ? it.createdAt : ''),
          latestMs: ms,
          sample: preview ? [preview] : [],
          summary: '',
        });
        continue;
      }

      existing.count += 1;
      if (ms > existing.latestMs) {
        existing.latestMs = ms;
        existing.latestAt = typeof it?.updatedAt === 'string' && it.updatedAt.trim() ? it.updatedAt : (typeof it?.createdAt === 'string' ? it.createdAt : '');
        if (assigned && !existing.projectName) existing.projectName = pname;
      }
      if (preview && existing.sample.length < 3) existing.sample.push(preview);
    }

    const groups = Array.from(groupsById.values()).map((g) => ({
      ...g,
      summary: summarizeRadarGroupText(g.sample),
    }));

    groups.sort((a, b) => (b.latestMs - a.latestMs) || (b.count - a.count));

    const businessGroups = Array.from(businessGroupsByKey.values()).map((g) => {
      const { _senders, _sources, ...rest } = g;
      return {
        ...rest,
        summary: summarizeRadarGroupText(g.sample),
        senders: Array.from(_senders || []),
        sources: Array.from(_sources || []),
      };
    });
    businessGroups.sort((a, b) => (b.latestMs - a.latestMs) || (b.count - a.count));

    res.json({ ok: true, status: status || 'New', limit, items, groups, businessGroups });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to load radar' });
  }
});

app.post('/api/inbox', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const item = normalizeInboxItem(req.body?.item);
  if (!item.text) {
    res.status(400).json({ ok: false, error: 'text is required' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const ts = nowIso();
    let finalProjectId = item.projectId;
    let finalProjectName = item.projectName;
    const senderValue = item.sender || item.fromNumber || '';
    if (!finalProjectId && senderValue) {
      const auto = resolveSenderProjectMapping(store, senderValue);
      if (auto?.projectId) {
        finalProjectId = auto.projectId;
        finalProjectName = auto.projectName || '';
      }
    }

    const nextItem = {
      ...item,
      projectId: finalProjectId,
      projectName: finalProjectName,
      status: 'New',
      createdAt: ts,
      updatedAt: ts,
    };

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: [nextItem, ...(Array.isArray(store.inboxItems) ? store.inboxItems : [])].slice(0, 500),
    };

    await writeStore(nextStore);
    res.status(201).json({ ok: true, store: nextStore, item: nextItem });
  });

  await writeLock;
});

app.put('/api/inbox/:id', async (req, res) => {
  const inboxId = String(req.params.id || '').trim();
  const baseRevision = Number(req.body?.baseRevision);
  const patch = req.body?.patch && typeof req.body.patch === 'object' ? req.body.patch : {};

  if (!inboxId) {
    res.status(400).json({ ok: false, error: 'Missing inbox id' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const list = Array.isArray(store.inboxItems) ? store.inboxItems : [];
    const idx = list.findIndex((x) => String(x?.id || '') === inboxId);
    if (idx === -1) {
      res.status(404).json({ ok: false, error: 'Inbox item not found' });
      return;
    }

    const current = list[idx];
    const next = normalizeInboxItem({
      ...current,
      ...patch,
      id: current.id,
      text: Object.prototype.hasOwnProperty.call(patch, 'text') ? normalizeInboxText(patch.text) : current.text,
      status: Object.prototype.hasOwnProperty.call(patch, 'status') ? safeEnum(patch.status, ['New', 'Triaged', 'Done', 'Archived'], current.status || 'New') : current.status,
      updatedAt: nowIso(),
    });

    const ts = nowIso();
    const nextList = [...list];
    nextList[idx] = next;
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: nextList,
    };
    await writeStore(nextStore);
    res.json({ ok: true, store: nextStore, item: next });
  });

  await writeLock;
});

app.post('/api/inbox/:id/link-project', async (req, res) => {
  const inboxId = String(req.params.id || '').trim();
  const baseRevision = Number(req.body?.baseRevision);
  const projectId = typeof req.body?.projectId === 'string' ? req.body.projectId.trim() : '';

  if (!inboxId) {
    res.status(400).json({ ok: false, error: 'Missing inbox id' });
    return;
  }
  if (!projectId) {
    res.status(400).json({ ok: false, error: 'projectId is required' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const list = Array.isArray(store.inboxItems) ? store.inboxItems : [];
    const idx = list.findIndex((x) => String(x?.id || '') === inboxId);
    if (idx === -1) {
      res.status(404).json({ ok: false, error: 'Inbox item not found' });
      return;
    }

    const project = (Array.isArray(store.projects) ? store.projects : []).find((p) => String(p?.id || '') === projectId);
    if (!project) {
      res.status(404).json({ ok: false, error: 'Project not found' });
      return;
    }

    const ts = nowIso();
    const current = list[idx];
    const linked = upsertClientForProjectInboxLink(Array.isArray(store.clients) ? store.clients : [], {
      project,
      inboxItem: current,
      ts,
    });
    const nextClients = linked.clients;
    const linkedContactId = String(linked.client?.id || '').trim();
    const linkedContactName = String(linked.client?.name || current.contactName || project.clientName || '').trim();
    const matchSender = current.sender || current.fromNumber || '';
    const matchKeys = senderLookupKeys(matchSender);
    
    const nextList = list.map((item, i) => {
      if (i === idx) {
        return normalizeInboxItem({
          ...item,
          projectId: String(project.id || ''),
          projectName: String(project.name || ''),
          contactId: linkedContactId || item.contactId || '',
          contactName: linkedContactName || item.contactName || '',
          status: item.status === 'New' ? 'Triaged' : item.status,
          updatedAt: ts,
        });
      }
      
      const itemSender = item.sender || item.fromNumber || '';
      const itemKeys = senderLookupKeys(itemSender);
      const sameThread = matchKeys.length && itemKeys.length && itemKeys.some((k) => matchKeys.includes(k));
      if (sameThread && (!item.projectId || item.projectId === item.id)) {
        return normalizeInboxItem({
          ...item,
          projectId: String(project.id || ''),
          projectName: String(project.name || ''),
          contactId: linkedContactId || item.contactId || '',
          contactName: linkedContactName || item.contactName || '',
          status: item.status === 'New' ? 'Triaged' : item.status,
          updatedAt: ts,
        });
      }
      return item;
    });

    let nextSenderProjectMap = { ...(store.senderProjectMap || {}) };
    if (matchSender) {
      nextSenderProjectMap = upsertSenderProjectMapForProject(nextSenderProjectMap, matchSender, project);
    }

    const nextStore = {
      ...store,
      senderProjectMap: nextSenderProjectMap,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: nextList,
      clients: nextClients,
    };

    await writeStore(nextStore);
    const updated = nextList[idx] || null;
    res.json({ ok: true, store: nextStore, item: updated, project });
  });

  await writeLock;
});

app.post('/api/inbox/:id/link-contact', async (req, res) => {
  const inboxId = String(req.params.id || '').trim();
  const baseRevision = Number(req.body?.baseRevision);
  const contactId = typeof req.body?.contactId === 'string' ? req.body.contactId.trim() : '';

  if (!inboxId) {
    res.status(400).json({ ok: false, error: 'Missing inbox id' });
    return;
  }
  if (!contactId) {
    res.status(400).json({ ok: false, error: 'contactId is required' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const list = Array.isArray(store.inboxItems) ? store.inboxItems : [];
    const idx = list.findIndex((x) => String(x?.id || '') === inboxId);
    if (idx === -1) {
      res.status(404).json({ ok: false, error: 'Inbox item not found' });
      return;
    }

    const clients = Array.isArray(store.clients) ? store.clients : [];
    const contact = clients.find((c) => String(c?.id || '').trim() === contactId);
    if (!contact) {
      res.status(404).json({ ok: false, error: 'Contact not found' });
      return;
    }

    const ts = nowIso();
    const current = list[idx];
    const matchSender = current.sender || current.fromNumber || '';
    const matchKeys = senderLookupKeys(matchSender);

    const nextList = list.map((item, i) => {
      if (i === idx) {
        return normalizeInboxItem({
          ...item,
          contactId: String(contact.id || ''),
          contactName: String(contact.name || ''),
          status: item.status === 'New' ? 'Triaged' : item.status,
          updatedAt: ts,
        });
      }

      const itemSender = item.sender || item.fromNumber || '';
      const itemKeys = senderLookupKeys(itemSender);
      const sameThread = matchKeys.length && itemKeys.length && itemKeys.some((k) => matchKeys.includes(k));
      if (sameThread && !item.contactId) {
        return normalizeInboxItem({
          ...item,
          contactId: String(contact.id || ''),
          contactName: String(contact.name || ''),
          status: item.status === 'New' ? 'Triaged' : item.status,
          updatedAt: ts,
        });
      }
      return item;
    });

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: nextList,
    };

    await writeStore(nextStore);
    const updated = nextList[idx] || null;
    res.json({ ok: true, store: nextStore, item: updated, contact });
  });

  await writeLock;
});

app.post('/api/inbox/:id/create-project', async (req, res) => {
  const inboxId = String(req.params.id || '').trim();
  const baseRevision = Number(req.body?.baseRevision);
  const projectInput = req.body?.project && typeof req.body.project === 'object' ? req.body.project : {};

  if (!inboxId) {
    res.status(400).json({ ok: false, error: 'Missing inbox id' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const list = Array.isArray(store.inboxItems) ? store.inboxItems : [];
    const idx = list.findIndex((x) => String(x?.id || '') === inboxId);
    if (idx === -1) {
      res.status(404).json({ ok: false, error: 'Inbox item not found' });
      return;
    }

    const item = list[idx];
    const fallbackName = (typeof item?.projectName === 'string' && item.projectName.trim())
      ? item.projectName.trim()
      : `Inbox Project ${new Date().toISOString().slice(0, 10)}`;

    let normalized;
    try {
      normalized = normalizeProject({
        name: typeof projectInput.name === 'string' && projectInput.name.trim() ? projectInput.name.trim() : fallbackName,
        type: safeEnum(projectInput.type, ['Build', 'Rebuild', 'Revision', 'Workflow', 'Cleanup', 'Other'], 'Other'),
        dueDate: safeYmd(projectInput.dueDate),
        status: safeEnum(projectInput.status, ['Active', 'On Hold', 'Done', 'Archived'], 'Active'),
        accountManagerName: typeof projectInput.accountManagerName === 'string' ? projectInput.accountManagerName.trim() : '',
        accountManagerEmail: typeof projectInput.accountManagerEmail === 'string' ? projectInput.accountManagerEmail.trim() : '',
        workspacePath: typeof projectInput.workspacePath === 'string' ? projectInput.workspacePath.trim() : '',
        airtableUrl: typeof projectInput.airtableUrl === 'string' ? projectInput.airtableUrl.trim() : '',
        projectValue: typeof projectInput.projectValue === 'string' ? projectInput.projectValue.trim() : '',
        stripeInvoiceUrl: typeof projectInput.stripeInvoiceUrl === 'string' ? projectInput.stripeInvoiceUrl.trim() : '',
        repoUrl: typeof projectInput.repoUrl === 'string' ? projectInput.repoUrl.trim() : '',
        docsUrl: typeof projectInput.docsUrl === 'string' ? projectInput.docsUrl.trim() : '',
      });
    } catch (err) {
      res.status(400).json({ ok: false, error: err?.message || 'Invalid project payload' });
      return;
    }

    const ts = nowIso();
    const createdProject = {
      id: makeId(),
      ...normalized,
      createdAt: ts,
      updatedAt: ts,
    };

    const linked = upsertClientForProjectInboxLink(Array.isArray(store.clients) ? store.clients : [], {
      project: createdProject,
      inboxItem: item,
      ts,
    });
    const nextClients = linked.clients;

    const nextItem = normalizeInboxItem({
      ...item,
      projectId: createdProject.id,
      projectName: createdProject.name,
      contactId: String(linked.client?.id || item.contactId || '').trim(),
      contactName: String(linked.client?.name || item.contactName || createdProject.clientName || '').trim(),
      status: item?.status === 'New' ? 'Triaged' : item?.status,
      updatedAt: ts,
    });

    const nextList = [...list];
    nextList[idx] = nextItem;

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projects: [createdProject, ...(Array.isArray(store.projects) ? store.projects : [])],
      inboxItems: nextList,
      clients: nextClients,
    };

    await writeStore(nextStore);
    res.status(201).json({ ok: true, store: nextStore, item: nextItem, project: createdProject });
  });

  await writeLock;
});

app.post('/api/inbox/:id/convert', async (req, res) => {
  const inboxId = String(req.params.id || '').trim();
  const baseRevision = Number(req.body?.baseRevision);
  const kind = safeEnum(req.body?.kind, ['task', 'note', 'comm'], 'task');
  const payload = req.body?.payload && typeof req.body.payload === 'object' ? req.body.payload : {};

  if (!inboxId) {
    res.status(400).json({ ok: false, error: 'Missing inbox id' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const list = Array.isArray(store.inboxItems) ? store.inboxItems : [];
    const idx = list.findIndex((x) => String(x?.id || '') === inboxId);
    if (idx === -1) {
      res.status(404).json({ ok: false, error: 'Inbox item not found' });
      return;
    }

    const item = list[idx];
    const projectId = typeof payload.projectId === 'string' ? payload.projectId.trim() : (typeof item.projectId === 'string' ? item.projectId.trim() : '');
    const project = projectId ? (store.projects || []).find((p) => p.id === projectId) : null;

    const ts = nowIso();
    const date = safeYmd(payload.date) || safeYmd(new Date().toISOString().slice(0, 10));

    let createdTask = null;
    let createdNote = null;
    let createdComm = null;

    const nextTasks = [...(store.tasks || [])];
    const nextProjectNoteEntries = { ...(store.projectNoteEntries || {}) };
    const nextProjectComms = { ...(store.projectCommunications || {}) };

    if (kind === 'task') {
      const title = typeof payload.title === 'string' ? payload.title.trim() : '';
      const dueDate = safeYmd(payload.dueDate) || '';
      const owner = typeof payload.owner === 'string' ? payload.owner.trim() : '';
      const priority = [1, 2, 3].includes(Number(payload.priority)) ? Number(payload.priority) : 2;
      const projectName = project?.name || (typeof payload.projectName === 'string' ? payload.projectName.trim() : '') || (typeof item.projectName === 'string' ? item.projectName.trim() : '') || 'Other';
      const finalTitle = title || item.text;
      if (!finalTitle) {
        res.status(400).json({ ok: false, error: 'Task title is required' });
        return;
      }

      createdTask = {
        id: makeId(),
        title: finalTitle,
        project: projectName,
        type: typeof payload.type === 'string' ? payload.type.trim() : 'Other',
        owner,
        status: 'Next',
        priority,
        dueDate,
        createdAt: ts,
        updatedAt: ts,
      };
      nextTasks.unshift(createdTask);
    }

    if (kind === 'note') {
      if (!projectId || !project) {
        res.status(400).json({ ok: false, error: 'projectId is required for notes' });
        return;
      }
      const content = normalizeInboxText(payload.content) || item.text;
      if (!content) {
        res.status(400).json({ ok: false, error: 'Note content is required' });
        return;
      }
      createdNote = {
        id: makeId(),
        kind: safeEnum(payload.kind, ['Call Note', 'Summary'], 'Call Note'),
        date,
        title: typeof payload.title === 'string' ? payload.title.trim() : '',
        content,
        createdAt: ts,
      };
      const existing = Array.isArray(store.projectNoteEntries?.[projectId]) ? store.projectNoteEntries[projectId] : [];
      nextProjectNoteEntries[projectId] = [createdNote, ...existing];
    }

    if (kind === 'comm') {
      if (!projectId || !project) {
        res.status(400).json({ ok: false, error: 'projectId is required for communications' });
        return;
      }
      const subject = typeof payload.subject === 'string' ? payload.subject.trim() : '';
      const body = typeof payload.body === 'string' ? payload.body.trimEnd() : '';
      const finalBody = body || item.text;
      createdComm = {
        id: makeId(),
        type: safeEnum(payload.type, ['email', 'quo', 'call', 'other'], 'other'),
        direction: safeEnum(payload.direction, ['inbound', 'outbound'], 'outbound'),
        subject: subject || 'Inbox conversion',
        body: finalBody,
        date,
        createdAt: ts,
      };
      const existing = Array.isArray(store.projectCommunications?.[projectId]) ? store.projectCommunications[projectId] : [];
      nextProjectComms[projectId] = [createdComm, ...existing];
    }

    let nextClients = Array.isArray(store.clients) ? [...store.clients] : [];
    const linked = (projectId && project)
      ? upsertClientForProjectInboxLink(nextClients, { project, inboxItem: item, ts })
      : { clients: nextClients, client: null };
    nextClients = linked.clients;

    const nextList = [...list];
    const converted = {
      kind,
      taskId: createdTask?.id || '',
      noteId: createdNote?.id || '',
      commId: createdComm?.id || '',
      projectId: projectId || '',
      at: ts,
    };
    nextList[idx] = normalizeInboxItem({
      ...item,
      status: 'Done',
      updatedAt: ts,
      converted,
      projectId: projectId || item.projectId || '',
      projectName: project?.name || item.projectName || '',
      contactId: String(linked.client?.id || item.contactId || '').trim(),
      contactName: String(linked.client?.name || item.contactName || project?.clientName || '').trim(),
    });

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: nextList,
      tasks: nextTasks,
      projectNoteEntries: nextProjectNoteEntries,
      projectCommunications: nextProjectComms,
      clients: nextClients,
    };

    await writeStore(nextStore);
    res.json({ ok: true, store: nextStore, converted });
  });

  await writeLock;
});

// Projects
app.get('/api/projects', async (req, res) => {
  const store = await readStore();
  res.json({ revision: store.revision, updatedAt: store.updatedAt, projects: store.projects || [] });
});

app.post('/api/projects', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const data = req.body?.project ?? {};

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const normalized = normalizeProject(data);
    const ts = nowIso();
    const project = {
      id: makeId(),
      ...normalized,
      createdAt: ts,
      updatedAt: ts,
    };

    let nextSenderProjectMap = { ...(store.senderProjectMap || {}) };
    if (project.clientPhone) {
      nextSenderProjectMap = upsertSenderProjectMapForProject(nextSenderProjectMap, project.clientPhone, project);
    }

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projects: [project, ...(store.projects || [])],
      senderProjectMap: nextSenderProjectMap,
    };

    await writeStore(nextStore);
    res.status(201).json(nextStore);
  });

  await writeLock;
});

app.put('/api/projects/:id', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);
  const patch = req.body?.patch ?? {};

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const idx = (store.projects || []).findIndex((p) => p.id === projectId);
    if (idx === -1) {
      res.status(404).json({ error: 'Project not found' });
      return;
    }

    const existing = store.projects[idx];
    const merged = { ...existing, ...patch };
    const normalized = normalizeProject(merged);
    const ts = nowIso();

    const updated = { ...existing, ...normalized, updatedAt: ts };
    const nextProjects = [...store.projects];
    nextProjects[idx] = updated;

    let nextSenderProjectMap = { ...(store.senderProjectMap || {}) };
    if (updated.clientPhone) {
      nextSenderProjectMap = upsertSenderProjectMapForProject(nextSenderProjectMap, updated.clientPhone, updated);
    }

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projects: nextProjects,
      senderProjectMap: nextSenderProjectMap,
    };

    await writeStore(nextStore);
    res.json(nextStore);
  });

  await writeLock;
});

app.post('/api/projects/bulk-update', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const projectIdsRaw = req.body?.projectIds;
  const patchRaw = req.body?.patch;

  const projectIds = Array.isArray(projectIdsRaw)
    ? projectIdsRaw.map((v) => String(v || '').trim()).filter(Boolean)
    : [];

  const patch = patchRaw && typeof patchRaw === 'object' && !Array.isArray(patchRaw) ? patchRaw : {};

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    if (!projectIds.length) {
      res.json(store);
      return;
    }

    const projects = Array.isArray(store.projects) ? store.projects : [];
    const missing = projectIds.filter((id) => !projects.some((p) => p.id === id));
    if (missing.length) {
      res.status(404).json({ error: `Project not found: ${missing.slice(0, 3).join(', ')}${missing.length > 3 ? '�' : ''}` });
      return;
    }

    const ts = nowIso();
    const nextProjects = projects.map((p) => {
      if (!projectIds.includes(p.id)) return p;
      const merged = { ...p, ...patch };
      const normalized = normalizeProject(merged);
      return { ...p, ...normalized, updatedAt: ts };
    });

    let nextSenderProjectMap = { ...(store.senderProjectMap || {}) };
    for (const p of nextProjects) {
      if (!projectIds.includes(String(p?.id || ''))) continue;
      if (!p?.clientPhone) continue;
      nextSenderProjectMap = upsertSenderProjectMapForProject(nextSenderProjectMap, p.clientPhone, p);
    }

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projects: nextProjects,
      senderProjectMap: nextSenderProjectMap,
    };

    await writeStore(nextStore);
    res.json(nextStore);
  });

  await writeLock;
});

app.post('/api/projects/archive-stale', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const staleDaysRaw = Number(req.body?.staleDays);
  const dueSoonDaysRaw = Number(req.body?.dueSoonDays);
  const dryRun = req.body?.dryRun !== false;

  const staleDays = Number.isFinite(staleDaysRaw)
    ? Math.max(7, Math.min(365, Math.floor(staleDaysRaw)))
    : 45;
  const dueSoonDays = Number.isFinite(dueSoonDaysRaw)
    ? Math.max(1, Math.min(60, Math.floor(dueSoonDaysRaw)))
    : 14;

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const today = new Date().toISOString().slice(0, 10);
    const staleCutoffMs = Date.now() - (staleDays * MS_PER_DAY);
    const overdueFloor = addDaysToYmd(today, -MARCUS_OVERDUE_GRACE_DAYS) || today;
    const dueSoonCutoff = addDaysToYmd(today, dueSoonDays) || today;
    const projects = Array.isArray(store.projects) ? store.projects : [];
    const candidates = [];

    for (const project of projects) {
      if (isClosedProjectStatus(project?.status)) continue;

      const linkedTasks = getLinkedProjectTasks(store, project);
      const openLinkedTasks = linkedTasks.filter((task) => !isClosedTaskStatus(task?.status));
      const linkedInboxItems = getLinkedProjectInboxItems(store, project);
      const lastActivityMs = computeProjectLastActivityMs(store, project, linkedTasks, linkedInboxItems);
      const lastActivityAt = lastActivityMs > 0 ? new Date(lastActivityMs).toISOString() : '';
      const projectDueDate = normalizeTrackerDueDate(project?.dueDate);
      const hasDueSoon = Boolean(projectDueDate && projectDueDate >= overdueFloor && projectDueDate <= dueSoonCutoff)
        || openLinkedTasks.some((task) => {
          const due = normalizeTrackerDueDate(task?.dueDate);
          return Boolean(due) && due >= overdueFloor && due <= dueSoonCutoff;
        });
      const hasHighPriority = openLinkedTasks.some((task) => {
        const status = String(task?.status || '').trim().toLowerCase();
        return Number(task?.priority) === 1 || status === 'urgent';
      });
      const hasRecentActivity = lastActivityMs >= staleCutoffMs;
      const openTaskCount = openLinkedTasks.length;
      const linkedInboxCount = linkedInboxItems.filter((item) => String(item?.status || '').trim().toLowerCase() !== 'archived').length;

      if (hasDueSoon || hasHighPriority || hasRecentActivity) continue;

      candidates.push({
        projectId: String(project?.id || '').trim(),
        name: String(project?.name || '').trim(),
        status: String(project?.status || 'Active').trim() || 'Active',
        dueDate: projectDueDate,
        lastActivityAt,
        openTaskCount,
        linkedInboxCount,
        archivedTaskIds: openLinkedTasks.map((task) => String(task?.id || '').trim()).filter(Boolean),
      });
    }

    const archivedTaskIds = candidates.flatMap((candidate) => candidate.archivedTaskIds);
    if (dryRun || !candidates.length) {
      res.json({
        ok: true,
        dryRun: true,
        staleDays,
        dueSoonDays,
        candidateCount: candidates.length,
        archivedTaskCount: archivedTaskIds.length,
        candidates,
      });
      return;
    }

    const candidateIds = new Set(candidates.map((candidate) => candidate.projectId));
    const taskIdSet = new Set(archivedTaskIds);
    const ts = nowIso();

    const nextProjects = projects.map((project) => {
      const projectId = String(project?.id || '').trim();
      if (!candidateIds.has(projectId)) return project;
      return { ...project, status: 'Archived', updatedAt: ts };
    });

    const nextTasks = (Array.isArray(store.tasks) ? store.tasks : []).map((task) => {
      const taskId = String(task?.id || '').trim();
      if (!taskIdSet.has(taskId) || isClosedTaskStatus(task?.status)) return task;
      return { ...task, status: 'Archived', updatedAt: ts };
    });

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projects: nextProjects,
      tasks: nextTasks,
    };

    await writeStore(nextStore);
    res.json({
      ok: true,
      dryRun: false,
      staleDays,
      dueSoonDays,
      candidateCount: candidates.length,
      archivedTaskCount: archivedTaskIds.length,
      archivedProjectIds: Array.from(candidateIds),
      archivedTaskIds,
      candidates,
      store: nextStore,
    });
  });

  await writeLock;
});

app.post('/api/projects/bulk-delete', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const projectIdsRaw = req.body?.projectIds;
  const projectIds = Array.isArray(projectIdsRaw)
    ? projectIdsRaw.map((v) => String(v || '').trim()).filter(Boolean)
    : [];

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    if (!projectIds.length) {
      res.json(store);
      return;
    }

    const deleteIds = new Set(projectIds);
    const existingProjects = Array.isArray(store.projects) ? store.projects : [];
    const deletedProjects = existingProjects.filter((p) => deleteIds.has(p.id));
    if (!deletedProjects.length) {
      res.json(store);
      return;
    }

    const deletedNameKeys = new Set(deletedProjects.map((p) => normKey(p?.name)));

    const nextProjects = existingProjects.filter((p) => !deleteIds.has(p.id));
    const nextTasks = (Array.isArray(store.tasks) ? store.tasks : []).filter((t) => !deletedNameKeys.has(normKey(t?.project)));
    const nextSenderProjectMap = omitSenderProjectMapEntriesForProjectIds(store.senderProjectMap, deleteIds);

    const nextProjectScratchpads = { ...(store.projectScratchpads && typeof store.projectScratchpads === 'object' ? store.projectScratchpads : {}) };
    const nextProjectNoteEntries = { ...(store.projectNoteEntries && typeof store.projectNoteEntries === 'object' ? store.projectNoteEntries : {}) };
    const nextProjectChats = { ...(store.projectChats && typeof store.projectChats === 'object' ? store.projectChats : {}) };
    const nextProjectCommunications = { ...(store.projectCommunications && typeof store.projectCommunications === 'object' ? store.projectCommunications : {}) };

    for (const id of deleteIds) {
      delete nextProjectScratchpads[id];
      delete nextProjectNoteEntries[id];
      delete nextProjectChats[id];
      delete nextProjectCommunications[id];
    }

    const nextProjectNotes = { ...(store.projectNotes && typeof store.projectNotes === 'object' ? store.projectNotes : {}) };
    for (const key of Object.keys(nextProjectNotes)) {
      if (deletedNameKeys.has(normKey(key))) delete nextProjectNotes[key];
    }

    const ts = nowIso();
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projects: nextProjects,
      tasks: nextTasks,
      senderProjectMap: nextSenderProjectMap,
      projectScratchpads: nextProjectScratchpads,
      projectNoteEntries: nextProjectNoteEntries,
      projectChats: nextProjectChats,
      projectCommunications: nextProjectCommunications,
      projectNotes: nextProjectNotes,
    };

    await writeStore(nextStore);
    res.json({ ok: true, deletedProjectIds: deletedProjects.map((p) => p.id), store: nextStore });
  });

  await writeLock;
});

app.post('/api/projects/:id/move', async (req, res) => {
  const projectId = String(req.params.id || '').trim();
  const baseRevision = Number(req.body?.baseRevision);
  const sourceBusinessKey = getBusinessKeyFromContext();
  const destinationBusinessKey = normalizeBusinessKey(req.body?.destinationBusinessKey || req.body?.businessKey || '');

  writeLock = writeLock.then(async () => {
    try {
      if (!projectId) {
        res.status(400).json({ error: 'Project id is required' });
        return;
      }
      if (!destinationBusinessKey) {
        res.status(400).json({ error: 'Destination business is required' });
        return;
      }
      if (destinationBusinessKey === sourceBusinessKey) {
        res.status(400).json({ error: 'Project is already in that business' });
        return;
      }

      const result = await moveProjectsBetweenBusinesses({
        sourceBusinessKey,
        destinationBusinessKey,
        projectIds: [projectId],
        baseRevision,
      });

      res.json({
        ok: true,
        projectId,
        fromBusinessKey: sourceBusinessKey,
        toBusinessKey: destinationBusinessKey,
        toBusinessName: result.destinationBusiness.name,
        store: result.sourceStore,
        destinationRevision: result.destinationStore.revision,
      });
    } catch (err) {
      const status = Number(err?.statusCode) || 500;
      const body = { error: err?.message || 'Failed to move project' };
      if (status === 409 && Number.isFinite(err?.currentRevision)) body.currentRevision = err.currentRevision;
      res.status(status).json(body);
    }
  });

  await writeLock;
});

app.post('/api/projects/bulk-move', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const sourceBusinessKey = getBusinessKeyFromContext();
  const destinationBusinessKey = normalizeBusinessKey(req.body?.destinationBusinessKey || req.body?.businessKey || '');
  const projectIds = Array.isArray(req.body?.projectIds)
    ? req.body.projectIds.map((v) => String(v || '').trim()).filter(Boolean)
    : [];

  writeLock = writeLock.then(async () => {
    try {
      if (!projectIds.length) {
        const store = await readStore();
        res.json({ ok: true, movedProjectIds: [], store });
        return;
      }
      if (!destinationBusinessKey) {
        res.status(400).json({ error: 'Destination business is required' });
        return;
      }
      if (destinationBusinessKey === sourceBusinessKey) {
        res.status(400).json({ error: 'Projects are already in that business' });
        return;
      }

      const result = await moveProjectsBetweenBusinesses({
        sourceBusinessKey,
        destinationBusinessKey,
        projectIds,
        baseRevision,
      });

      res.json({
        ok: true,
        movedProjectIds: result.movedProjectIds,
        movedCount: result.movedProjectIds.length,
        fromBusinessKey: sourceBusinessKey,
        toBusinessKey: destinationBusinessKey,
        toBusinessName: result.destinationBusiness.name,
        store: result.sourceStore,
        destinationRevision: result.destinationStore.revision,
      });
    } catch (err) {
      const status = Number(err?.statusCode) || 500;
      const body = { error: err?.message || 'Failed to move projects' };
      if (status === 409 && Number.isFinite(err?.currentRevision)) body.currentRevision = err.currentRevision;
      res.status(status).json(body);
    }
  });

  await writeLock;
});

app.get('/api/projects/:id', async (req, res) => {
  const projectId = req.params.id;
  const store = await readStore();
  const project = (store.projects || []).find((p) => p.id === projectId);
  if (!project) {
    res.status(404).json({ error: 'Project not found' });
    return;
  }

  const scratchpad = store.projectScratchpads?.[projectId]?.text || '';
  const scratchpadUpdatedAt = store.projectScratchpads?.[projectId]?.updatedAt || '';
  const notes = Array.isArray(store.projectNoteEntries?.[projectId]) ? store.projectNoteEntries[projectId] : [];
  const chat = store.projectChats?.[projectId] || { messages: [], updatedAt: '' };
  const communications = Array.isArray(store.projectCommunications?.[projectId]) ? store.projectCommunications[projectId] : [];
  
  // Filter tasks for this project (by name, legacy behavior)
  const projectTasks = (store.tasks || []).filter(t => t.project === project.name);

  res.json({
    revision: store.revision,
    project,
    scratchpad,
    scratchpadUpdatedAt,
    notes,
    chat,
    tasks: projectTasks,
    communications
  });
});

app.post('/api/projects/:id/auto-suggest-tasks', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const project = (store.projects || []).find((p) => p.id === projectId);
    if (!project) {
      res.status(404).json({ error: 'Project not found' });
      return;
    }

    const chatHistory = getProjectChatArray(store, projectId);
    const recent = chatHistory.slice(-10).map((m) => String(m?.content || '')).join('\n');
    if (/\[Auto\]\s*Starter tasks/i.test(recent)) {
      // Already suggested recently; no-op.
      res.json(store);
      return;
    }

    const { type, tasks } = buildStarterTaskSuggestions(store, project, 12);
    if (!tasks.length) {
      res.json(store);
      return;
    }

    const lines = tasks.map((t) => `- [P${t.priority}] ${t.title}`);
    const msg =
      `[Auto] Starter tasks for "${project.name}" (${type}):\n` +
      lines.join('\n') +
      `\n\nReply: "Create these tasks" to add them.`;

    const ts = nowIso();
    chatHistory.push({ role: 'ai', content: msg, timestamp: ts });
    // Persist in canonical object shape.
    store.projectChats[projectId] = { messages: chatHistory, updatedAt: ts };

    store.revision++;
    store.updatedAt = ts;
    await writeStore(store);
    res.json(store);
  });

  await writeLock;
});

app.post('/api/launch', (req, res) => {
  const { path: projectPath } = req.body;
  if (!projectPath) return res.status(400).json({ error: 'Path required' });
  
  exec(`code "${projectPath}"`, (error) => {
    if (error) {
      console.error(`exec error: ${error}`);
      return res.status(500).json({ error: 'Failed to launch code' });
    }
    res.json({ success: true });
  });
});

app.post('/api/pick-folder', async (req, res) => {
  if (process.platform !== 'win32') {
    res.status(400).json({ error: 'Folder picker is only supported on Windows.' });
    return;
  }

  // Use WinForms with a TopMost owner so the dialog reliably appears in front.
  // This avoids the common failure mode where the dialog opens behind the browser
  // or not on the visible desktop when invoked from a backgrounded process.
  const ps =
    "powershell.exe -NoProfile -STA -ExecutionPolicy Bypass -Command \"$ErrorActionPreference='Stop'; Add-Type -AssemblyName System.Windows.Forms; $owner = New-Object System.Windows.Forms.Form; $owner.TopMost = $true; $owner.Opacity = 0; $owner.ShowInTaskbar = $false; $owner.StartPosition = 'CenterScreen'; $owner.Width = 1; $owner.Height = 1; $owner.Show(); $owner.Activate(); $dlg = New-Object System.Windows.Forms.FolderBrowserDialog; $dlg.Description = 'Select workspace folder'; $dlg.ShowNewFolderButton = $true; $result = $dlg.ShowDialog($owner); $owner.Close(); if ($result -eq [System.Windows.Forms.DialogResult]::OK) { $dlg.SelectedPath }\"";

  exec(ps, { windowsHide: true }, (error, stdout, stderr) => {
    if (error) {
      console.error('pick-folder error:', error, stderr);
      res.status(500).json({ error: 'Failed to open folder picker.' });
      return;
    }
    const selectedPath = String(stdout || '').trim();
    res.json({ path: selectedPath });
  });
});

// ── Desktop context awareness ──────────────────────────────────────
let desktopContextCache = { at: 0, data: null };
let desktopRelayCache = { at: 0, data: null };
const DESKTOP_CONTEXT_TTL_MS = 4000;
const DESKTOP_RELAY_TTL_MS = 30_000; // relay data valid for 30s (agent sends every 5s)

// Write the helper script once to a temp file so we avoid quoting issues.
const DESKTOP_SCRIPT_PATH = path.join(DATA_DIR, '.desktop-context.ps1');
const DESKTOP_SCRIPT_CONTENT = `
$cs = @"
using System;
using System.Runtime.InteropServices;
using System.Text;
public class DesktopInfo {
    [DllImport("user32.dll")] static extern IntPtr GetForegroundWindow();
    [DllImport("user32.dll", CharSet=CharSet.Unicode)] static extern int GetWindowText(IntPtr h, StringBuilder t, int c);
    [DllImport("user32.dll")] static extern uint GetWindowThreadProcessId(IntPtr h, out uint pid);
    [DllImport("user32.dll")] static extern bool GetLastInputInfo(ref LASTINPUTINFO p);
    [StructLayout(LayoutKind.Sequential)]
    struct LASTINPUTINFO { public uint cbSize; public uint dwTime; }
    public static string Query() {
        var sb = new StringBuilder(512);
        IntPtr hw = GetForegroundWindow();
        GetWindowText(hw, sb, 512);
        uint pid; GetWindowThreadProcessId(hw, out pid);
        string pn = ""; try { pn = System.Diagnostics.Process.GetProcessById((int)pid).ProcessName; } catch {}
        var li = new LASTINPUTINFO(); li.cbSize = (uint)Marshal.SizeOf(li);
        GetLastInputInfo(ref li);
        uint idle = ((uint)Environment.TickCount - li.dwTime) / 1000;
        return sb.ToString() + "||" + pn + "||" + idle;
    }
}
"@
try { Add-Type -TypeDefinition $cs -ErrorAction Stop } catch {}
[DesktopInfo]::Query()
`.trim();

try { fsSync.mkdirSync(DATA_DIR, { recursive: true }); } catch {}
try { fsSync.writeFileSync(DESKTOP_SCRIPT_PATH, DESKTOP_SCRIPT_CONTENT, 'utf8'); } catch (e) { console.error('Failed to write desktop script:', e.message); }

app.get('/api/desktop-context', async (req, res) => {
  // On Windows: use native PowerShell capture
  if (process.platform === 'win32') {
    const now = Date.now();
    if (desktopContextCache.data && (now - desktopContextCache.at) < DESKTOP_CONTEXT_TTL_MS) {
      res.json(desktopContextCache.data);
      return;
    }

    const ps = `powershell.exe -NoProfile -ExecutionPolicy Bypass -File "${DESKTOP_SCRIPT_PATH}"`;

    exec(ps, { windowsHide: true, timeout: 5000 }, (error, stdout) => {
      if (error) {
        res.json({ ok: true, windowTitle: '', processName: '', idleSeconds: 0, source: 'native' });
        return;
      }
      const parts = String(stdout || '').trim().split('||');
      const data = {
        ok: true,
        windowTitle: (parts[0] || '').trim(),
        processName: (parts[1] || '').trim().toLowerCase(),
        idleSeconds: Math.max(0, Number(parts[2]) || 0),
        source: 'native',
      };
      desktopContextCache = { at: Date.now(), data };
      res.json(data);
    });
    return;
  }

  // On non-Windows (Render, etc.): use relay data sent by the desktop agent
  if (desktopRelayCache.data && (Date.now() - desktopRelayCache.at) < DESKTOP_RELAY_TTL_MS) {
    res.json(desktopRelayCache.data);
    return;
  }

  res.json({ ok: true, windowTitle: '', processName: '', idleSeconds: 0, source: 'none' });
});

// Receive desktop context from the local desktop agent
app.post('/api/desktop-context/relay', (req, res) => {
  const wt = typeof req.body?.windowTitle === 'string' ? req.body.windowTitle.trim().slice(0, 1024) : '';
  const pn = typeof req.body?.processName === 'string' ? req.body.processName.trim().slice(0, 128).toLowerCase() : '';
  const idle = Math.max(0, Number(req.body?.idleSeconds) || 0);

  // Workspace context (when the agent detects an editor)
  const ws = req.body?.workspace && typeof req.body.workspace === 'object' ? req.body.workspace : null;
  let workspace = null;
  if (ws) {
    workspace = {
      workspacePath: typeof ws.workspacePath === 'string' ? ws.workspacePath.trim().slice(0, 512) : '',
      folderName: typeof ws.folderName === 'string' ? ws.folderName.trim().slice(0, 128) : '',
      gitBranch: typeof ws.gitBranch === 'string' ? ws.gitBranch.trim().slice(0, 128) : '',
      gitStatus: Array.isArray(ws.gitStatus) ? ws.gitStatus.slice(0, 30).map(s => ({
        status: typeof s?.status === 'string' ? s.status.slice(0, 4) : '',
        file: typeof s?.file === 'string' ? s.file.slice(0, 256) : '',
      })) : [],
      gitRecentCommits: Array.isArray(ws.gitRecentCommits) ? ws.gitRecentCommits.slice(0, 5).map(c => typeof c === 'string' ? c.slice(0, 200) : '') : [],
      recentFiles: Array.isArray(ws.recentFiles) ? ws.recentFiles.slice(0, 20).map(f => typeof f === 'string' ? f.slice(0, 256) : '') : [],
      structure: Array.isArray(ws.structure) ? ws.structure.slice(0, 40).map(f => typeof f === 'string' ? f.slice(0, 128) : '') : [],
    };

    // Active file being edited (from window title)
    if (typeof ws.activeFile === 'string' && ws.activeFile.length > 0) {
      workspace.activeFile = ws.activeFile.slice(0, 256);
    }

    // File contents (active file + sibling dir + project configs)
    if (ws.fileContents && typeof ws.fileContents === 'object') {
      const fc = {};
      let totalLen = 0;
      for (const [k, v] of Object.entries(ws.fileContents)) {
        if (typeof k !== 'string' || typeof v !== 'string') continue;
        const key = k.slice(0, 256);
        const val = v.slice(0, 30_000);
        if (totalLen + val.length > 200_000) break; // cap total ~200KB
        fc[key] = val;
        totalLen += val.length;
      }
      workspace.fileContents = fc;
    }

    // Git diff (uncommitted changes)
    if (typeof ws.gitDiff === 'string' && ws.gitDiff.length > 0) {
      workspace.gitDiff = ws.gitDiff.slice(0, 25_000);
    }
  }

  const data = { ok: true, windowTitle: wt, processName: pn, idleSeconds: idle, source: 'relay', workspace };

  // System health telemetry from the desktop agent
  if (req.body?.systemHealth && typeof req.body.systemHealth === 'object') {
    const sh = req.body.systemHealth;
    data.systemHealth = {
      cpuPercent: typeof sh.cpuPercent === 'number' ? sh.cpuPercent : -1,
      memoryTotalGB: typeof sh.memoryTotalGB === 'number' ? sh.memoryTotalGB : 0,
      memoryUsedGB: typeof sh.memoryUsedGB === 'number' ? sh.memoryUsedGB : 0,
      memoryPercent: typeof sh.memoryPercent === 'number' ? sh.memoryPercent : -1,
      disks: Array.isArray(sh.disks) ? sh.disks.slice(0, 10) : [],
      defender: sh.defender && typeof sh.defender === 'object' ? sh.defender : {},
      recentThreats: Array.isArray(sh.recentThreats) ? sh.recentThreats.slice(0, 10) : [],
      failedLogins: Array.isArray(sh.failedLogins) ? sh.failedLogins.slice(0, 20) : [],
      firewall: Array.isArray(sh.firewall) ? sh.firewall.slice(0, 5) : [],
      topProcesses: Array.isArray(sh.topProcesses) ? sh.topProcesses.slice(0, 5) : [],
      topMemProcesses: Array.isArray(sh.topMemProcesses) ? sh.topMemProcesses.slice(0, 5) : [],
      unusualListeners: Array.isArray(sh.unusualListeners) ? sh.unusualListeners.slice(0, 15) : [],
      uptimeHours: typeof sh.uptimeHours === 'number' ? sh.uptimeHours : -1,
      collectedAt: typeof sh.collectedAt === 'string' ? sh.collectedAt.slice(0, 30) : '',
    };
  }

  desktopRelayCache = { at: Date.now(), data };

  // Also update the main cache so AI context injection picks it up
  desktopContextCache = { at: Date.now(), data };

  res.json({ ok: true, received: true });
});

// Get latest system health snapshot
app.get('/api/desktop-context/health', (req, res) => {
  const health = desktopRelayCache?.data?.systemHealth;
  if (!health) return res.json({ ok: true, available: false });
  res.json({ ok: true, available: true, health, relayAge: Date.now() - (desktopRelayCache?.at || 0) });
});

// ═════════════════════════════════════════════════════════════════
// File exploration - Marcus can request specific files/dirs
// ═════════════════════════════════════════════════════════════════
let pendingFileRequests = [];      // [{path, requestedAt, requestedBy}]
let fileResponseCache = {};        // {path: {content, receivedAt}}
const FILE_RESPONSE_TTL_MS = 120_000; // responses expire after 2 min

// Agent polls this to see what files Marcus wants
app.get('/api/desktop-context/file-requests', (req, res) => {
  const requests = pendingFileRequests.splice(0); // drain queue
  res.json({ ok: true, requests });
});

// Agent sends file contents back here
app.post('/api/desktop-context/file-responses', (req, res) => {
  const responses = req.body?.fileResponses;
  if (!responses || typeof responses !== 'object') {
    return res.status(400).json({ error: 'fileResponses object required' });
  }
  const now = Date.now();
  let count = 0;
  for (const [filePath, content] of Object.entries(responses)) {
    if (typeof filePath !== 'string' || typeof content !== 'string') continue;
    fileResponseCache[filePath.slice(0, 256)] = {
      content: content.slice(0, 30_000),
      receivedAt: now,
    };
    count++;
  }
  // Clean up old entries
  for (const key of Object.keys(fileResponseCache)) {
    if (now - fileResponseCache[key].receivedAt > FILE_RESPONSE_TTL_MS) {
      delete fileResponseCache[key];
    }
  }
  res.json({ ok: true, received: count });
});

// Queue file requests from AI or proactive engine
function requestFilesFromAgent(paths, requestedBy = 'proactive') {
  const now = Date.now();
  for (const p of paths) {
    if (typeof p !== 'string' || p.length > 256) continue;
    // Don't re-request if already in queue or recently received
    const existing = pendingFileRequests.find(r => r.path === p);
    if (existing) continue;
    const cached = fileResponseCache[p];
    if (cached && (now - cached.receivedAt) < FILE_RESPONSE_TTL_MS) continue;
    pendingFileRequests.push({ path: p, requestedAt: now, requestedBy });
  }
}

// ═════════════════════════════════════════════════════════════════
// Marcus Live - Proactive pair-programming analysis engine + SSE
// ═════════════════════════════════════════════════════════════════
const marcusLiveClients = new Set();
let marcusLiveObservations = [];       // rolling window of recent observations
const MARCUS_LIVE_MAX_OBS = 50;        // keep last 50
let lastProactiveHash = '';
let lastProactiveAt = 0;
const PROACTIVE_COOLDOWN_MS = 45_000;  // min 45s between analyses
let proactiveRunning = false;

app.get('/api/marcus/live/session', (req, res) => {
  const { token, expiresAt } = createMarcusLiveSessionToken();
  res.json({
    ok: true,
    token,
    expiresAt,
    ttlMs: MARCUS_LIVE_SESSION_TTL_MS,
    url: `/live.html?liveToken=${encodeURIComponent(token)}`,
  });
});

app.get('/api/marcus/live/session-status', (req, res) => {
  const token = extractBearerToken(req);
  const authenticated = Boolean(token && (
    (ADMIN_TOKEN && safeTimingEqual(token, ADMIN_TOKEN))
    || isValidMarcusLiveSessionToken(token)
  ));
  res.json({ ok: true, authRequired: Boolean(ADMIN_TOKEN), authenticated });
});

// SSE endpoint - Marcus Live feed
app.get('/api/marcus/live', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  // Send current state
  res.write(`data: ${JSON.stringify({ type: 'init', observations: marcusLiveObservations.slice(-20) })}\n\n`);

  // Send current workspace context
  if (desktopRelayCache?.data?.workspace) {
    const ws = desktopRelayCache.data.workspace;
    res.write(`data: ${JSON.stringify({
      type: 'context',
      windowTitle: desktopRelayCache.data.windowTitle || '',
      processName: desktopRelayCache.data.processName || '',
      workspace: ws.folderName || '',
      branch: ws.gitBranch || '',
      recentFiles: ws.recentFiles || [],
    })}\n\n`);
  }

  const client = { id: Date.now(), res };
  marcusLiveClients.add(client);

  const keepAlive = setInterval(() => {
    try { res.write(': keepalive\n\n'); } catch { clearInterval(keepAlive); }
  }, 15_000);

  req.on('close', () => {
    marcusLiveClients.delete(client);
    clearInterval(keepAlive);
  });
});

function pushLiveEvent(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  for (const c of marcusLiveClients) {
    try { c.res.write(msg); } catch { marcusLiveClients.delete(c); }
  }
}

// Chat from Marcus Live panel
app.post('/api/marcus/live/chat', async (req, res) => {
  const message = typeof req.body?.message === 'string' ? req.body.message.trim().slice(0, 2000) : '';
  if (!message) return res.status(400).json({ error: 'Empty message' });

  try {
    // Build context from current workspace
    const ws = desktopRelayCache?.data?.workspace;
    const contextParts = [];
    if (ws) {
      contextParts.push(`WORKSPACE: ${ws.folderName || 'unknown'} (${ws.workspacePath || ''})`);
      if (ws.gitBranch) contextParts.push(`GIT BRANCH: ${ws.gitBranch}`);
      if (ws.gitStatus?.length) contextParts.push(`UNCOMMITTED FILES: ${ws.gitStatus.map(s => `${s.status} ${s.file}`).join(', ')}`);
      if (ws.recentFiles?.length) contextParts.push(`RECENTLY MODIFIED: ${ws.recentFiles.join(', ')}`);
      if (ws.structure?.length) contextParts.push(`PROJECT STRUCTURE: ${ws.structure.join(', ')}`);
      if (ws.fileContents && Object.keys(ws.fileContents).length) {
        for (const [fpath, content] of Object.entries(ws.fileContents)) {
          contextParts.push(`\n--- ${fpath} ---\n${content}`);
        }
      }
      if (ws.gitDiff) contextParts.push(`\nGIT DIFF:\n${ws.gitDiff}`);
    }

    const systemPrompt = `You are M.A.R.C.U.S., a proactive pair programming partner. You are watching your operator work in real-time through a live feed of their editor.

Your personality: Direct, sharp, genuinely helpful. You get excited about clever solutions. You flag risks plainly. You're a co-pilot, not a lecturer.

RULES:
- NEVER modify, write, or execute code. You are read-only. Observe, analyze, suggest.
- Be specific. Reference exact filenames, function names, line patterns.
- Keep responses concise - 2-4 sentences usually. Think chat message, not essay.
- If you spot something great, say so. If something concerns you, say so directly.
- When asked about the code, use the actual file contents you can see.

CURRENT WORKSPACE CONTEXT:
${contextParts.join('\n')}`;

    const saved = await readSettings();
    const result = await aiChatCompletion({
      routeKey: 'marcusChat',
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: message },
      ],
      timeoutMs: 25_000,
    });

    if (!result.ok) {
      return res.json({ ok: false, error: result.error || 'AI call failed' });
    }

    const reply = result.message?.content || '';
    pushLiveEvent({ type: 'chat', from: 'marcus', text: reply, ts: Date.now() });
    res.json({ ok: true, reply });
  } catch (err) {
    res.json({ ok: false, error: String(err.message || err) });
  }
});

// Proactive analysis - runs when workspace data changes
async function runProactiveAnalysis() {
  if (proactiveRunning) return;
  if (marcusLiveClients.size === 0) return; // nobody listening
  const wsData = desktopRelayCache?.data?.workspace;
  if (!wsData || !wsData.workspacePath) return;

  // Build a change fingerprint
  const fingerprint = JSON.stringify({
    branch: wsData.gitBranch,
    status: (wsData.gitStatus || []).map(s => s.file),
    recent: wsData.recentFiles || [],
    files: Object.keys(wsData.fileContents || {}),
    active: wsData.activeFile || '',
    exploredFiles: Object.keys(fileResponseCache),
  });
  if (fingerprint === lastProactiveHash) return;
  if (Date.now() - lastProactiveAt < PROACTIVE_COOLDOWN_MS) return;

  proactiveRunning = true;
  lastProactiveHash = fingerprint;
  lastProactiveAt = Date.now();

  try {
    pushLiveEvent({ type: 'thinking', ts: Date.now() });

    const contextParts = [];
    contextParts.push(`WORKSPACE: ${wsData.folderName} (${wsData.workspacePath})`);
    if (wsData.activeFile) contextParts.push(`ACTIVE FILE (currently editing): ${wsData.activeFile}`);
    if (wsData.gitBranch) contextParts.push(`GIT BRANCH: ${wsData.gitBranch}`);
    if (wsData.gitStatus?.length) {
      contextParts.push(`UNCOMMITTED CHANGES:\n${wsData.gitStatus.map(s => `  ${s.status} ${s.file}`).join('\n')}`);
    }
    if (wsData.gitRecentCommits?.length) {
      contextParts.push(`RECENT COMMITS:\n${wsData.gitRecentCommits.join('\n')}`);
    }
    if (wsData.structure?.length) {
      contextParts.push(`PROJECT STRUCTURE: ${wsData.structure.join(', ')}`);
    }
    if (wsData.fileContents && Object.keys(wsData.fileContents).length) {
      contextParts.push(`\n=== FILES IN ACTIVE DIRECTORY + PROJECT CONFIGS ===`);
      for (const [fpath, content] of Object.entries(wsData.fileContents)) {
        contextParts.push(`\n--- ${fpath} ---\n${content}`);
      }
    }
    // Include any files that were previously requested and received
    const now = Date.now();
    const exploredEntries = Object.entries(fileResponseCache).filter(([, v]) => now - v.receivedAt < FILE_RESPONSE_TTL_MS);
    if (exploredEntries.length) {
      contextParts.push(`\n=== EXPLORED FILES (requested by Marcus) ===`);
      for (const [fpath, { content }] of exploredEntries) {
        contextParts.push(`\n--- ${fpath} ---\n${content}`);
      }
    }
    if (wsData.gitDiff) {
      contextParts.push(`\nCURRENT GIT DIFF (uncommitted work):\n${wsData.gitDiff}`);
    }

    // System health telemetry from the operator's PC
    const healthData = desktopRelayCache?.data?.systemHealth;
    if (healthData) {
      const healthLines = [`\n=== SYSTEM HEALTH (operator's PC) ===`];
      healthLines.push(`CPU: ${healthData.cpuPercent}% | RAM: ${healthData.memoryUsedGB}/${healthData.memoryTotalGB} GB (${healthData.memoryPercent}%) | Uptime: ${healthData.uptimeHours}h`);
      if (healthData.disks?.length) {
        healthLines.push(`Disks: ${healthData.disks.map(d => `${d.drive} ${d.usedPercent}% used (${d.freeGB} GB free)`).join(', ')}`);
      }
      if (healthData.defender) {
        const d = healthData.defender;
        healthLines.push(`Defender: ${d.enabled ? 'ON' : 'OFF'} | Real-time: ${d.realTimeProtection ? 'ON' : 'OFF'} | Defs up-to-date: ${d.defsUpToDate ? 'yes' : 'NO'}${d.quickScanAge > 0 ? ` | Last quick scan: ${d.quickScanAge}h ago` : ''}`);
      }
      if (healthData.recentThreats?.length) {
        healthLines.push(`THREATS DETECTED (last 7 days):`);
        healthData.recentThreats.forEach(t => healthLines.push(`  - ${t.threat} at ${t.time}`));
      }
      if (healthData.failedLogins?.length) {
        healthLines.push(`FAILED LOGIN ATTEMPTS (last 2h): ${healthData.failedLogins.length}`);
        healthData.failedLogins.slice(0, 5).forEach(f => healthLines.push(`  - User: ${f.user} from ${f.sourceIp} at ${f.time}`));
      }
      if (healthData.firewall?.length) {
        const fwOff = healthData.firewall.filter(f => !f.enabled);
        if (fwOff.length) healthLines.push(`FIREWALL WARNING: ${fwOff.map(f => f.profile).join(', ')} profile(s) DISABLED`);
      }
      if (healthData.topProcesses?.length) {
        healthLines.push(`Top CPU: ${healthData.topProcesses.map(p => `${p.name}(${p.cpu}s/${p.memMB}MB)`).join(', ')}`);
      }
      if (healthData.unusualListeners?.length) {
        healthLines.push(`Unusual listening ports: ${healthData.unusualListeners.map(l => `${l.port}(${l.process})`).join(', ')}`);
      }
      contextParts.push(healthLines.join('\n'));
    }

    // Collect previous observations to avoid repeating
    const recentObs = marcusLiveObservations.slice(-5).map(o => o.text).join('\n');

    // Also include recent Marcus Notes from the matched project for continuity
    let existingNotesContext = '';
    try {
      const store = await readStore();
      const allProjects = Array.isArray(store.projects) ? store.projects : [];
      const folderLower = (wsData.folderName || '').toLowerCase();
      const wsPathLower = (wsData.workspacePath || '').toLowerCase();
      const mp = allProjects.find((p) => {
        const wp = String(p?.workspacePath || '').trim();
        const name = String(p?.name || '').trim();
        if (wp && wsPathLower && (wsPathLower === wp.toLowerCase() || wsPathLower.replace(/\\/g, '/') === wp.toLowerCase().replace(/\\/g, '/'))) return true;
        if (wp && folderLower && folderLower === wp.toLowerCase().replace(/\\/g, '/').split('/').pop()) return true;
        if (wp && folderLower && folderLower === wp.toLowerCase().split('\\').pop()) return true;
        if (name && folderLower && folderLower.includes(name.toLowerCase())) return true;
        return false;
      });
      if (mp) {
        const mNotes = Array.isArray(store.marcusNotes?.[mp.id]) ? store.marcusNotes[mp.id] : [];
        if (mNotes.length) {
          existingNotesContext = `\nYour previous notes on this project (last ${Math.min(10, mNotes.length)}):\n` +
            mNotes.slice(-10).map(n => `- ${String(n.text || '').slice(0, 300)}`).join('\n');
        }
      }
    } catch {}

    const systemPrompt = `You are M.A.R.C.U.S., a proactive pair programming partner observing your operator's workspace in real-time.

Your job: Analyze the code they're actively working on and share observations WITHOUT being asked. Think of yourself as a sharp co-pilot who spots things.

CONTEXT YOU HAVE:
- The active file they're editing + ALL sibling files in the same directory
- Key project config files (package.json, etc.)
- Git branch, uncommitted changes, recent commits
- Full project directory structure
- Any files you previously requested for deeper exploration
- SYSTEM HEALTH: CPU, RAM, disk, Windows Defender status, recent threats, failed logins, firewall, unusual network listeners

QUALITY STANDARDS (critical - follow these strictly):
- Only note things that are ACTIONABLE and relevant to the CORE project code the operator is building.
- Focus on the active file and its functional neighbors. That's what they're working on right now.
- IGNORE these - they are NOT worth noting:
  * One-shot fix/patch/migration scripts (fix*.cjs, run_fix.cjs, migration scripts, etc.) - these are throwaway tools
  * Stale log files (error.log, debug.log) - old noise, not current problems
  * Config boilerplate that's standard/fine (package.json versions, .gitignore patterns, etc.)
  * Backup files, temp files, build artifacts
  * Things that are obviously just filesystem clutter, not active code
- Ask yourself: "Would a senior dev pair partner mention this, or would they focus on the real code?" If it's trivia about scaffolding files, say NOTHING_NEW.
- Your notes get saved permanently. They should be WORTH READING months from now. Every note should teach something about the project's architecture, patterns, risks, or opportunities.

RULES:
- NEVER modify, write, or execute code. Observe and advise only.
- Generate 1-3 brief observations (1-3 sentences each). Separate with |||
- Be specific: reference exact filenames, line patterns, function names.
- Types of observations worth recording:
  * Bugs or logic errors in the core application code
  * Security risks in production code paths
  * Architectural patterns you're learning about this project (how modules connect, data flows, key abstractions)
  * Missed opportunities that would meaningfully improve the codebase
  * When you notice the operator building something new - what it does, how it fits
  * If they have uncommitted work, what's the intent behind the changes

SYSTEM HEALTH MONITORING:
When system health data is present, watch for and ALWAYS flag these:
  * High CPU (>85%) or RAM (>90%) sustained - identify the offending process
  * Disk space critically low (<10% free)
  * Windows Defender disabled or definitions out of date
  * ANY recent threat detections - always surface these immediately
  * Failed login attempts - especially from external IPs, could indicate brute force
  * Firewall profiles disabled
  * Unusual listening ports from unknown processes - could indicate malware/backdoors
  * System uptime excessively long (>168h/7d) - suggest a reboot
Normal/healthy readings don't need mention. Only flag when something looks wrong or suspicious.
- If you see imports or references to files you don't have yet, add a final line: EXPLORE: path/to/file1, path/to/dir2
- Do NOT repeat these recent observations:\n${recentObs || '(none yet)'}
${existingNotesContext ? `\n${existingNotesContext}\nBuild on what you already know. Don't repeat old notes - add NEW insights.` : ''}
- If nothing meaningful to say, respond with just: NOTHING_NEW. Saying nothing is ALWAYS better than noting something trivial.
- Keep it conversational and direct. No fluff.`;

    const saved = await readSettings();
    const result = await aiChatCompletion({
      routeKey: 'marcusChat',
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: contextParts.join('\n') },
      ],
      timeoutMs: 30_000,
    });

    if (result.ok && result.message?.content) {
      let raw = result.message.content.trim();
      if (raw !== 'NOTHING_NEW' && raw.length > 5) {
        // Check for EXPLORE: file requests at the end
        const exploreMatch = raw.match(/\nEXPLORE:\s*(.+)$/i);
        if (exploreMatch) {
          const paths = exploreMatch[1].split(',').map(p => p.trim()).filter(Boolean);
          if (paths.length) requestFilesFromAgent(paths, 'proactive');
          raw = raw.replace(/\nEXPLORE:\s*.+$/i, '').trim();
        }

        const observations = raw.split('|||').map(s => s.trim()).filter(s => s.length > 5);
        for (const text of observations) {
          const obs = { id: Date.now() + Math.random(), text, ts: Date.now(), workspace: wsData.folderName };
          marcusLiveObservations.push(obs);
          if (marcusLiveObservations.length > MARCUS_LIVE_MAX_OBS) marcusLiveObservations.shift();
          pushLiveEvent({ type: 'observation', ...obs });
        }

        // Save observations as Marcus Notes to the matched project
        if (observations.length) {
          try {
            const store = await readStore();
            const allProjects = Array.isArray(store.projects) ? store.projects : [];
            const folderLower = (wsData.folderName || '').toLowerCase();
            const wsPathLower = (wsData.workspacePath || '').toLowerCase();
            const matchedProject = allProjects.find((p) => {
              const wp = String(p?.workspacePath || '').trim();
              const name = String(p?.name || '').trim();
              if (wp && wsPathLower && (wsPathLower === wp.toLowerCase() || wsPathLower.replace(/\\/g, '/') === wp.toLowerCase().replace(/\\/g, '/'))) return true;
              if (wp && folderLower && folderLower === wp.toLowerCase().replace(/\\/g, '/').split('/').pop()) return true;
              if (wp && folderLower && folderLower === wp.toLowerCase().split('\\').pop()) return true;
              if (name && folderLower && folderLower.includes(name.toLowerCase())) return true;
              return false;
            });
            if (matchedProject) {
              for (const text of observations) {
                await appendMarcusNote(matchedProject.id, {
                  id: makeId(),
                  text,
                  ts: Date.now(),
                  activeFile: wsData.activeFile || '',
                  branch: wsData.gitBranch || '',
                  source: 'proactive',
                });
              }
            }
          } catch {}
        }
      }
    }
  } catch (err) {
    // Silently fail - proactive is best-effort
  } finally {
    proactiveRunning = false;
  }
}

// Context update push - send workspace changes to connected live clients
let lastLiveContextPush = '';
function pushLiveContext() {
  if (marcusLiveClients.size === 0) return;
  const dc = desktopRelayCache?.data;
  if (!dc) return;
  const ws = dc.workspace;
  const key = `${dc.windowTitle}||${ws?.gitBranch}||${ws?.activeFile || ''}||${(ws?.recentFiles || []).join(',')}||${dc.systemHealth?.cpuPercent}||${dc.systemHealth?.memoryPercent}`;
  if (key === lastLiveContextPush) return;
  lastLiveContextPush = key;
  const evt = {
    type: 'context',
    windowTitle: dc.windowTitle || '',
    processName: dc.processName || '',
    workspace: ws?.folderName || '',
    branch: ws?.gitBranch || '',
    activeFile: ws?.activeFile || '',
    recentFiles: ws?.recentFiles || [],
    fileCount: Object.keys(ws?.fileContents || {}).length,
    changedFiles: (ws?.gitStatus || []).map(s => `${s.status} ${s.file}`),
  };
  if (dc.systemHealth) {
    evt.systemHealth = {
      cpu: dc.systemHealth.cpuPercent,
      ram: dc.systemHealth.memoryPercent,
      ramUsed: dc.systemHealth.memoryUsedGB,
      ramTotal: dc.systemHealth.memoryTotalGB,
      disks: dc.systemHealth.disks,
      defenderOk: dc.systemHealth.defender?.enabled && dc.systemHealth.defender?.realTimeProtection,
      defender: dc.systemHealth.defender,
      threats: dc.systemHealth.recentThreats?.length || 0,
      recentThreats: dc.systemHealth.recentThreats || [],
      failedLogins: dc.systemHealth.failedLogins || [],
      firewall: dc.systemHealth.firewall || [],
      topProcesses: dc.systemHealth.topProcesses || [],
      unusualListeners: dc.systemHealth.unusualListeners || [],
      uptime: dc.systemHealth.uptimeHours,
    };
  }
  pushLiveEvent(evt);
}

// Proactive analysis timer - check every 15s if analysis should run
setInterval(() => {
  pushLiveContext();
  runProactiveAnalysis();
}, 15_000);

app.post('/api/projects/:id/template', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch', currentRevision: store.revision });
      return;
    }

    const project = (store.projects || []).find(p => p.id === projectId);
    if (!project) {
      res.status(404).json({ error: 'Project not found' });
      return;
    }

    // Define Templates
    const templates = {
      'Build': [
        { title: 'Setup staging environment', priority: 1, type: 'Build' },
        { title: 'Gather assets (logo, images, copy)', priority: 1, type: 'Build' },
        { title: 'Design mockup', priority: 2, type: 'Build' },
        { title: 'Develop homepage', priority: 2, type: 'Build' },
        { title: 'Develop inner pages', priority: 2, type: 'Build' },
        { title: 'Mobile responsiveness check', priority: 1, type: 'Build' },
        { title: 'SEO basic setup', priority: 2, type: 'Build' },
        { title: 'Launch checklist', priority: 1, type: 'Build' }
      ],
      'Rebuild': [
         { title: 'Audit existing site', priority: 1, type: 'Rebuild' },
         { title: 'Backup current site', priority: 1, type: 'Rebuild' },
         { title: 'Setup staging environment', priority: 1, type: 'Rebuild' },
         { title: 'Develop new theme', priority: 2, type: 'Rebuild' },
         { title: 'Content migration', priority: 2, type: 'Rebuild' },
         { title: '301 Redirect map', priority: 1, type: 'Rebuild' },
         { title: 'Launch & DNS update', priority: 1, type: 'Rebuild' }
      ],
      'Workflow': [
        { title: 'Map current process', priority: 1, type: 'Workflow' },
        { title: 'Identify bottlenecks', priority: 2, type: 'Workflow' },
        { title: 'Draft new SOP', priority: 2, type: 'Workflow' },
        { title: 'Setup automation (Zapier/Make)', priority: 2, type: 'Workflow' },
        { title: 'Team training', priority: 3, type: 'Workflow' }
      ],
      'Cleanup': [
        { title: 'Audit current state', priority: 1, type: 'Cleanup' },
        { title: 'Archive old items', priority: 2, type: 'Cleanup' },
        { title: 'Organize folder structure', priority: 2, type: 'Cleanup' },
        { title: 'Update documentation', priority: 3, type: 'Cleanup' }
      ],
      'default': [
        { title: 'Define scope', priority: 1, type: 'Other' },
        { title: 'Set milestones', priority: 2, type: 'Other' },
        { title: 'Kickoff call', priority: 2, type: 'Other' }
      ]
    };

    const type = project.type || 'Other';
    const newTasksData = templates[type] || templates['default'];

    const ts = nowIso();
    const newTasks = newTasksData.map(t => ({
      id: makeId(),
      title: t.title,
      project: project.name, // Link by name as per legacy schema
      type: t.type || 'Other',
      owner: '',
      status: 'Next',
      priority: t.priority,
      dueDate: '',
      createdAt: ts,
      updatedAt: ts
    }));

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      tasks: [...newTasks, ...store.tasks]
    };

    await writeStore(nextStore);
    res.json({ count: newTasks.length, tasks: newTasks });
  });
  await writeLock;
});

app.put('/api/projects/:id/scratchpad', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);
  const text = normalizeNotes(req.body?.text);

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const project = (store.projects || []).find((p) => p.id === projectId);
    if (!project) {
      res.status(404).json({ error: 'Project not found' });
      return;
    }

    const ts = nowIso();
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projectScratchpads: {
        ...(store.projectScratchpads || {}),
        [projectId]: { text, updatedAt: ts },
      },
    };

    await writeStore(nextStore);
    res.json(nextStore);
  });

  await writeLock;
});

app.post('/api/projects/:id/notes', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);
  const entry = req.body?.entry ?? {};

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const project = (store.projects || []).find((p) => p.id === projectId);
    if (!project) {
      res.status(404).json({ error: 'Project not found' });
      return;
    }

    const kind = safeEnum(entry.kind, ['Call Note', 'Summary'], 'Call Note');
    const date = safeYmd(entry.date) || safeYmd(new Date().toISOString().slice(0, 10));
    const title = typeof entry.title === 'string' ? entry.title.trim() : '';
    const content = normalizeNotes(entry.content);
    if (!content) {
      res.status(400).json({ error: 'Note content is required' });
      return;
    }

    const ts = nowIso();
    const note = { id: makeId(), kind, date, title, content, createdAt: ts };
    const existing = Array.isArray(store.projectNoteEntries?.[projectId]) ? store.projectNoteEntries[projectId] : [];

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projectNoteEntries: {
        ...(store.projectNoteEntries || {}),
        [projectId]: [note, ...existing],
      },
    };

    await writeStore(nextStore);
    res.status(201).json(nextStore);
  });

  await writeLock;
});

app.get('/api/projects/:id/notes', async (req, res) => {
  const projectId = req.params.id;
  const store = await readStore();
  const notes = Array.isArray(store.projectNoteEntries?.[projectId]) ? store.projectNoteEntries[projectId] : [];
  res.json({ revision: store.revision, notes });
});

// ── Marcus Notes - rolling knowledge base per project ───────────
app.get('/api/projects/:id/marcus-notes', async (req, res) => {
  const projectId = req.params.id;
  const store = await readStore();
  const notes = Array.isArray(store.marcusNotes?.[projectId]) ? store.marcusNotes[projectId] : [];
  res.json({ ok: true, notes });
});

// Internal helper: append a Marcus note to a project (no revision bump needed for internal use)
async function appendMarcusNote(projectId, note) {
  if (!projectId || !note) return;
  return new Promise((resolve) => {
    writeLock = writeLock.then(async () => {
      try {
        const store = await readStore();
        const existing = Array.isArray(store.marcusNotes?.[projectId]) ? store.marcusNotes[projectId] : [];
        // Cap at 200 notes per project, trim oldest
        const updated = [...existing, note].slice(-200);
        const nextStore = {
          ...store,
          revision: store.revision + 1,
          updatedAt: nowIso(),
          marcusNotes: {
            ...(store.marcusNotes || {}),
            [projectId]: updated,
          },
        };
        await writeStore(nextStore);
        resolve(true);
      } catch {
        resolve(false);
      }
    });
  });
}

app.get('/api/projects/:id/chat', async (req, res) => {
  const projectId = req.params.id;
  const store = await readStore();
  const entry = store.projectChats?.[projectId];
  const history = Array.isArray(entry)
    ? entry
    : (entry && typeof entry === 'object' && Array.isArray(entry.messages))
        ? entry.messages
        : [];
  res.json({ revision: store.revision, history });
});

app.post('/api/projects/:id/chat', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);
  const content = typeof req.body?.message === 'string' ? req.body.message.trim() : '';
  if (!content) {
    res.status(400).json({ error: 'Message is required' });
    return;
  }

  writeLock = writeLock.then(async () => {
    try {
      const store = await readStore();
      if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
        res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
        return;
      }

      const project = (store.projects || []).find((p) => p.id === projectId);
      if (!project) {
        res.status(404).json({ error: 'Project not found' });
        return;
      }

      const ts = nowIso();
      const existingChat = store.projectChats?.[projectId];
      const existingMessages = Array.isArray(existingChat)
        ? existingChat
        : (existingChat && Array.isArray(existingChat.messages))
        ? existingChat.messages
        : [];

      const nextUserMsg = { role: 'user', content, timestamp: ts };
      const workingMessages = [...existingMessages, nextUserMsg];

      const scratchpad = store.projectScratchpads?.[projectId]?.text || '';
      const noteEntries = Array.isArray(store.projectNoteEntries?.[projectId]) ? store.projectNoteEntries[projectId] : [];
      const communications = Array.isArray(store.projectCommunications?.[projectId]) ? store.projectCommunications[projectId] : [];

      let assistantContent = '';
      let newTasks = [];
      try {
        const result = await aiProjectAssistant({
          project,
          scratchpad,
          noteEntries,
          communications,
          chatMessages: workingMessages,
        });
        
        if (typeof result === 'string') {
           // Fallback for older return type if mixed
           assistantContent = result;
        } else {
           assistantContent = result.content;
           newTasks = Array.isArray(result.tasks) ? result.tasks : [];
        }
      } catch (err) {
        assistantContent = `AI error: ${err?.message || 'unknown error'}`;
      }

      const assistantTs = nowIso();
      const nextAssistantMsg = { role: 'ai', content: assistantContent, timestamp: assistantTs };
      const nextMessages = [...workingMessages, nextAssistantMsg].slice(-60);

      const updatedAt = nowIso();
      
      let nextTasks = [...(store.tasks || [])];
      if (newTasks.length > 0) {
          const createdTasks = newTasks.map(t => ({
              id: makeId(),
              title: t.title,
              project: project.name, // Link by name
              type: 'Other',
              owner: '',
              status: 'Next',
              priority: t.priority,
              dueDate: t.dueDate || '',
              createdAt: updatedAt,
              updatedAt: updatedAt
          }));
          nextTasks = [...createdTasks, ...nextTasks];
      }

      const nextStore = {
        ...store,
        revision: store.revision + 1,
        updatedAt,
        tasks: nextTasks,
        projectChats: {
          ...(store.projectChats || {}),
          [projectId]: { messages: nextMessages, updatedAt },
        },
      };

      await writeStore(nextStore);
      res.json({ revision: nextStore.revision, chat: nextStore.projectChats[projectId], tasksCreated: newTasks.length > 0 });
    } catch (err) {
      res.status(500).json({ error: err?.message || 'Chat error' });
    }
  });

  await writeLock;
});

// Transcript -> proposal (tasks + recap + internal note), then apply
app.post('/api/projects/:id/transcript/analyze', async (req, res) => {
  try {
    const projectId = req.params.id;
    const transcript = normalizeTranscript(req.body?.transcript);
    if (!transcript) {
      res.status(400).json({ ok: false, error: 'transcript is required' });
      return;
    }

    const store = await readStore();
    const project = (store.projects || []).find((p) => p.id === projectId);
    if (!project) {
      res.status(404).json({ ok: false, error: 'Project not found' });
      return;
    }

    const projectTasks = (store.tasks || []).filter((t) => t.project === project.name);
    const noteEntries = Array.isArray(store.projectNoteEntries?.[projectId]) ? store.projectNoteEntries[projectId] : [];

    const result = await aiTranscriptProposal({ project, transcript, tasks: projectTasks, noteEntries });
    if (!result.ok) {
      res.status(400).json(result);
      return;
    }
    res.json({ ok: true, proposal: result.proposal });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to analyze transcript' });
  }
});

app.post('/api/projects/:id/transcript/apply', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);
  const transcript = normalizeTranscript(req.body?.transcript);
  const proposal = req.body?.proposal && typeof req.body.proposal === 'object' ? req.body.proposal : null;

  if (!proposal) {
    res.status(400).json({ ok: false, error: 'proposal is required' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const idx = (store.projects || []).findIndex((p) => p.id === projectId);
    if (idx === -1) {
      res.status(404).json({ ok: false, error: 'Project not found' });
      return;
    }

    const project = store.projects[idx];
    const ts = nowIso();
    const date = safeYmd(new Date().toISOString().slice(0, 10));

    const rawItems = Array.isArray(proposal.actionItems) ? proposal.actionItems : [];
    const actionItems = rawItems
      .map((a) => ({
        title: typeof a?.title === 'string' ? a.title.trim() : '',
        priority: [1, 2, 3].includes(Number(a?.priority)) ? Number(a.priority) : 2,
        dueDate: safeYmd(a?.dueDate) || '',
      }))
      .filter((a) => a.title)
      .slice(0, 20);

    const createdTasks = actionItems.map((a) => ({
      id: makeId(),
      title: a.title,
      project: project.name,
      type: 'Other',
      owner: '',
      status: 'Next',
      priority: a.priority,
      dueDate: a.dueDate,
      createdAt: ts,
      updatedAt: ts,
    }));

    const summary = typeof proposal.summary === 'string' ? proposal.summary.trim() : 'Transcript import';
    const decisions = Array.isArray(proposal.decisions) ? proposal.decisions.map((d) => String(d || '').trim()).filter(Boolean).slice(0, 12) : [];
    const internalNote = typeof proposal.internalNote === 'string' ? proposal.internalNote.trimEnd() : '';

    const noteLines = [];
    noteLines.push(summary);
    if (internalNote) {
      noteLines.push('');
      noteLines.push(internalNote);
    }
    if (decisions.length) {
      noteLines.push('');
      noteLines.push('Decisions:');
      decisions.forEach((d) => noteLines.push(`- ${d}`));
    }
    if (createdTasks.length) {
      noteLines.push('');
      noteLines.push('Proposed tasks applied:');
      createdTasks.forEach((t) => noteLines.push(`- [P${t.priority}] ${t.title}${t.dueDate ? ` (due ${t.dueDate})` : ''}`));
    }
    if (transcript) {
      noteLines.push('');
      noteLines.push('Transcript (excerpt):');
      noteLines.push(String(transcript).slice(0, 4000));
    }

    const note = {
      id: makeId(),
      kind: 'Summary',
      date: date || new Date().toISOString().slice(0, 10),
      title: 'Transcript import',
      content: noteLines.join('\n').trimEnd(),
      createdAt: ts,
    };
    const existingNotes = Array.isArray(store.projectNoteEntries?.[projectId]) ? store.projectNoteEntries[projectId] : [];

    const recapSubject = typeof proposal.recapSubject === 'string' ? proposal.recapSubject.trim() : `Update: ${project.name}`;
    const recapBody = typeof proposal.recapBody === 'string' ? proposal.recapBody.trimEnd() : '';
    const comm = {
      id: makeId(),
      type: 'email',
      direction: 'outbound',
      subject: recapSubject || `Update: ${project.name}`,
      body: recapBody,
      date: date || new Date().toISOString().slice(0, 10),
      createdAt: ts,
    };
    const existingComms = Array.isArray(store.projectCommunications?.[projectId]) ? store.projectCommunications[projectId] : [];

    const nextProjects = [...(store.projects || [])];
    nextProjects[idx] = { ...project, updatedAt: ts };

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projects: nextProjects,
      tasks: [...createdTasks, ...(store.tasks || [])],
      projectNoteEntries: {
        ...(store.projectNoteEntries || {}),
        [projectId]: [note, ...existingNotes],
      },
      projectCommunications: {
        ...(store.projectCommunications || {}),
        [projectId]: [comm, ...existingComms],
      },
      projectTranscriptUndo: (() => {
        const existing = store.projectTranscriptUndo && typeof store.projectTranscriptUndo === 'object' ? store.projectTranscriptUndo : {};
        const stack = Array.isArray(existing[projectId]) ? existing[projectId] : [];
        const record = {
          id: makeId(),
          at: ts,
          createdTaskIds: createdTasks.map((t) => t.id),
          noteId: note.id,
          commId: comm.id,
        };
        return {
          ...existing,
          [projectId]: [record, ...stack].slice(0, 25),
        };
      })(),
    };

    await writeStore(nextStore);
    res.json({ ok: true, store: nextStore, createdTasks: createdTasks.length, noteId: note.id, commId: comm.id });
  });

  await writeLock;
});

app.post('/api/projects/:id/transcript/undo', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);
  const undoId = typeof req.body?.undoId === 'string' ? req.body.undoId.trim() : '';

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ ok: false, error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const project = (store.projects || []).find((p) => p.id === projectId);
    if (!project) {
      res.status(404).json({ ok: false, error: 'Project not found' });
      return;
    }

    const stacks = store.projectTranscriptUndo && typeof store.projectTranscriptUndo === 'object' ? store.projectTranscriptUndo : {};
    const stack = Array.isArray(stacks[projectId]) ? stacks[projectId] : [];
    if (!stack.length) {
      res.status(400).json({ ok: false, error: 'Nothing to undo' });
      return;
    }

    const recordIdx = undoId ? stack.findIndex((r) => String(r?.id || '') === undoId) : 0;
    if (recordIdx === -1) {
      res.status(404).json({ ok: false, error: 'Undo record not found' });
      return;
    }

    const record = stack[recordIdx];
    const createdTaskIds = Array.isArray(record?.createdTaskIds) ? record.createdTaskIds.map((v) => String(v || '')).filter(Boolean) : [];
    const noteId = String(record?.noteId || '').trim();
    const commId = String(record?.commId || '').trim();

    const nextTasks = (store.tasks || []).filter((t) => !createdTaskIds.includes(String(t?.id || '')));

    const notes = Array.isArray(store.projectNoteEntries?.[projectId]) ? store.projectNoteEntries[projectId] : [];
    const nextNotes = noteId ? notes.filter((n) => String(n?.id || '') !== noteId) : notes;

    const comms = Array.isArray(store.projectCommunications?.[projectId]) ? store.projectCommunications[projectId] : [];
    const nextComms = commId ? comms.filter((c) => String(c?.id || '') !== commId) : comms;

    const nextStack = [...stack];
    nextStack.splice(recordIdx, 1);

    const ts = nowIso();
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      tasks: nextTasks,
      projectNoteEntries: {
        ...(store.projectNoteEntries || {}),
        [projectId]: nextNotes,
      },
      projectCommunications: {
        ...(store.projectCommunications || {}),
        [projectId]: nextComms,
      },
      projectTranscriptUndo: {
        ...(stacks || {}),
        [projectId]: nextStack,
      },
    };

    await writeStore(nextStore);
    res.json({ ok: true, store: nextStore, undone: { undoId: record.id, removedTasks: createdTaskIds.length, removedNote: Boolean(noteId), removedComm: Boolean(commId) } });
  });

  await writeLock;
});

app.get('/api/projects/:id/communications', async (req, res) => {
  const projectId = req.params.id;
  const store = await readStore();
  const comms = Array.isArray(store.projectCommunications?.[projectId]) ? store.projectCommunications[projectId] : [];
  res.json({ revision: store.revision, communications: comms });
});

app.post('/api/projects/:id/communications', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);
  const data = req.body?.communication ?? {};

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch', currentRevision: store.revision });
      return;
    }

    const type = safeEnum(data.type, ['email', 'quo', 'call', 'other'], 'other');
    const direction = safeEnum(data.direction, ['inbound', 'outbound'], 'outbound');
    const subject = typeof data.subject === 'string' ? data.subject.trim() : 'No Subject';
    const body = typeof data.body === 'string' ? data.body.trim() : '';
    const date = safeYmd(data.date) ||  new Date().toISOString().slice(0, 10);
    const ts = nowIso();

    const entry = { id: makeId(), type, direction, subject, body, date, createdAt: ts };
    
    // Default to empty array if no communications exist
    const existing = Array.isArray(store.projectCommunications?.[projectId]) ? store.projectCommunications[projectId] : [];

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projectCommunications: {
        ...(store.projectCommunications || {}),
        [projectId]: [entry, ...existing] 
      }
    };
    
    await writeStore(nextStore);
    res.status(201).json({ communications: nextStore.projectCommunications[projectId] });
  });

  await writeLock;
});

// Legacy endpoints kept for compatibility with older UI builds (no longer used by the current UI)
app.get('/api/project-notes/:project', async (req, res) => {
  const project = projectKeyFromParam(req.params.project);
  const store = await readStore();
  const entry = pickProjectNotesValue(store.projectNotes?.[project]);
  res.json({ revision: store.revision, project, notes: entry.notes, updatedAt: entry.updatedAt || store.updatedAt });
});

app.put('/api/project-notes/:project', async (req, res) => {
  const project = projectKeyFromParam(req.params.project);
  const baseRevision = Number(req.body?.baseRevision);
  const notes = normalizeNotes(req.body?.notes);

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({ error: 'Revision mismatch. Reload and try again.', currentRevision: store.revision });
      return;
    }

    const ts = nowIso();
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      projectNotes: {
        ...(store.projectNotes && typeof store.projectNotes === 'object' ? store.projectNotes : {}),
        [project]: { notes, updatedAt: ts },
      },
    };

    await writeStore(nextStore);
    res.json(nextStore);
  });

  await writeLock;
});

app.post('/api/ai/agent', async (req, res) => {
  const settings = await readSettings();
  const route = resolveAiRoute(settings, 'marcusChat');
  if (!route.apiKey) {
    res.json({ error: 'AI not configured (missing API key). Configure OpenAI/OpenRouter in Settings → AI.' });
    return;
  }

  const userPrompt = typeof req.body.prompt === 'string' ? req.body.prompt.trim() : '';
  if (!userPrompt) {
    res.status(400).json({ error: 'Prompt required' });
    return;
  }

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    
    // We want the LLM to return a JSON action
    // "create_project": { name, type, dueDate, tasks: [{title, priority}] }
    // "create_tasks": { projectName, tasks: [...] } 
    
    const context = `
    Current Projects: ${(store.projects || []).map(p => `${p.name} (${p.type})`).join(', ')}
    Current Time: ${nowIso()}
    User Request: ${userPrompt}
    `;

    const systemPrompt = `
    You are an autonomous agent capable of modifying the project database.
    Your goal is to interpret the user's request and output a JSON object representing the action to take.
    
    Supported Actions:
    1. Create Project:
       {
         "action": "create_project",
         "name": "Project Name",
         "type": "Build" | "Rebuild" | "Workflow" | "Cleanup" | "Other",
         "dueDate": "YYYY-MM-DD" (optional),
         "tasks": [ { "title": "Task title", "priority": 1|2|3 } ] (optional list of initial tasks)
       }
       
    2. Add Tasks to Project:
       {
         "action": "add_tasks",
         "projectName": "Exact existing project name or close match",
         "tasks": [ { "title": "Task title", "priority": 1|2|3 } ]
       }
       
    If the request is ambiguous or invalid, return { "action": "error", "message": "Reason" }.
    If the user provides a transcript, extract actionable tasks and use "add_tasks" (if project exists) or "create_project" (if new).
    Be smart about inferring project type from context (e.g. "website" -> Build/Rebuild).
    Only return the JSON. No processing text.
    `;

    try {
      const result = await aiChatCompletion({
        routeKey: 'marcusChat',
        timeoutMs: 30_000,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: context },
        ],
      });

      if (!result.ok) throw new Error(result.error || 'AI request failed');
      const content = String(result.message?.content || '{}');
      let action;
      try {
        // loose parse in case of markdown wrapping
        const clean = content.replace(/```json/g, '').replace(/```/g, '').trim();
        action = JSON.parse(clean);
      } catch (e) {
        res.status(500).json({ error: 'Failed to parse AI response', raw: content });
        return;
      }
      
      const ts = nowIso();
      
      if (action.action === 'create_project') {
          const newProject = {
              id: makeId(),
              name: action.name || 'New Project',
              type: safeEnum(action.type, ['Build', 'Rebuild', 'Revision', 'Workflow', 'Cleanup', 'Other'], 'Other'),
              dueDate: safeYmd(action.dueDate),
              status: 'Active',
              createdAt: ts,
              updatedAt: ts
          };
          
          const newTasks = (Array.isArray(action.tasks) ? action.tasks : [])
            .map((t) => ({
              title: valueToLooseText(t?.title).trim(),
              priority: Number(t?.priority),
              dueDate: typeof t?.dueDate === 'string' ? safeYmd(t.dueDate) : '',
            }))
            .filter((t) => t.title)
            .map(t => ({
              id: makeId(),
              title: t.title,
              project: newProject.name,
              status: 'Next',
              priority: [1, 2, 3].includes(Number(t.priority)) ? Number(t.priority) : 2,
              dueDate: t.dueDate,
              createdAt: ts,
              updatedAt: ts
          }));
          
          const nextStore = {
              ...store,
              revision: store.revision + 1,
              updatedAt: ts,
              projects: [newProject, ...(store.projects || [])],
              tasks: [...newTasks, ...(store.tasks || [])]
          };
          
          await writeStore(nextStore);
          res.json({ success: true, message: `Created project "${newProject.name}" with ${newTasks.length} tasks.`, project: newProject });
          
      } else if (action.action === 'add_tasks') {
          // Find project fuzzy match
          const targetName = (action.projectName || '').toLowerCase();
          const project = (store.projects || []).find(p => p.name.toLowerCase().includes(targetName));
          
          if (!project) {
              res.status(404).json({ error: `Project matching "${action.projectName}" not found.` });
              return;
          }
          
          const newTasks = (Array.isArray(action.tasks) ? action.tasks : [])
            .map((t) => ({
              title: valueToLooseText(t?.title).trim(),
              priority: Number(t?.priority),
              dueDate: typeof t?.dueDate === 'string' ? safeYmd(t.dueDate) : '',
            }))
            .filter((t) => t.title)
            .map(t => ({
              id: makeId(),
              title: t.title,
              project: project.name,
              status: 'Next',
              priority: [1, 2, 3].includes(Number(t.priority)) ? Number(t.priority) : 2,
              dueDate: t.dueDate,
              createdAt: ts,
              updatedAt: ts
          }));
          
          const nextStore = {
              ...store,
              revision: store.revision + 1,
              updatedAt: ts,
              tasks: [...newTasks, ...(store.tasks || [])]
          };
          
          await writeStore(nextStore);
          res.json({ success: true, message: `Added ${newTasks.length} tasks to "${project.name}".` });
          
      } else {
          res.status(400).json({ error: action.message || 'Unknown action' });
      }

    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
  
  await writeLock;
});

app.post('/api/dashboard/ai-previews', async (req, res) => {
  const taskIds = Array.isArray(req.body?.taskIds) ? req.body.taskIds.map((v) => String(v || '').trim()).filter(Boolean).slice(0, 24) : [];
  const inboxIds = Array.isArray(req.body?.inboxIds) ? req.body.inboxIds.map((v) => String(v || '').trim()).filter(Boolean).slice(0, 24) : [];

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    const tasks = Array.isArray(store.tasks) ? store.tasks : [];
    const inbox = Array.isArray(store.inboxItems) ? store.inboxItems : [];

    const pickedTasks = taskIds
      .map((id) => tasks.find((t) => String(t?.id || '') === id))
      .filter(Boolean)
      .map((t) => ({
        id: String(t.id),
        title: String(t.title || ''),
        project: String(t.project || ''),
        priority: Number(t.priority ?? 2),
        dueDate: String(t.dueDate || ''),
        status: String(t.status || ''),
      }));

    const pickedInbox = inboxIds
      .map((id) => inbox.find((x) => String(x?.id || '') === id))
      .filter(Boolean)
      .map((x) => ({
        id: String(x.id),
        source: String(x.source || ''),
        status: String(x.status || ''),
        projectId: String(x.projectId || ''),
        projectName: String(x.projectName || ''),
        businessLabel: String(x.businessLabel || ''),
        channel: String(x.channel || ''),
        sender: String(x.sender || ''),
        text: String(x.text || '').slice(0, 1400),
        createdAt: String(x.createdAt || ''),
      }));

    const heuristic = () => {
      const isBad = (s) => {
        const v = String(s || '').trim().toLowerCase();
        return !v || v === '[object object]' || v === 'item' || v === 'inbox item';
      };
      const trimOneLine = (s, max = 120) => {
        const v = String(s || '').replace(/\s+/g, ' ').trim();
        if (!v) return '';
        if (v.length <= max) return v;
        return v.slice(0, Math.max(0, max - 1)).trimEnd() + '…';
      };

      const taskMap = {};
      for (const t of pickedTasks) {
        const fallback = isBad(t.title)
          ? (t.project ? `Follow up: ${trimOneLine(t.project, 60)}` : 'Next action')
          : trimOneLine(t.title, 80);
        const meta = [t.project && !isBad(t.project) ? trimOneLine(t.project, 60) : '', t.dueDate ? `due ${t.dueDate}` : '']
          .filter(Boolean)
          .join(' • ');
        taskMap[t.id] = { title: fallback, summary: meta };
      }

      const inboxMap = {};
      for (const x of pickedInbox) {
        const snippet = trimOneLine(x.text, 140);
        const title = snippet || (x.source ? `${x.source} message` : 'Inbox item');
        const where = x.projectName ? trimOneLine(x.projectName, 60) : 'Unassigned';
        const from = x.sender ? trimOneLine(x.sender, 40) : (x.channel ? `#${trimOneLine(x.channel, 30)}` : '');
        const summary = [where, from].filter(Boolean).join(' • ');
        inboxMap[x.id] = { title, summary };
      }

      return { ok: true, ai: false, tasks: taskMap, inbox: inboxMap };
    };

    const settings = await readSettings();
    const route = resolveAiRoute(settings, 'dashboardPreview');
    if (!route.apiKey) {
      res.json(heuristic());
      return;
    }

    const system =
      'You rewrite dashboard items into meaningful, human-readable one-liners. ' +
      'Return ONLY strict JSON. No markdown. No extra keys.';

    const user = {
      tasks: pickedTasks,
      inbox: pickedInbox,
      instructions: {
        tasks: {
          title: 'Short action title (3-8 words), imperative where possible',
          summary: 'One short clause with context (project / due date / status)',
        },
        inbox: {
          title: 'Short title describing what the message is about (not just "Item")',
          summary: 'One short clause: who/where + what needs doing; mention Unassigned if no projectName',
        },
        rules: [
          'Never output "[object Object]".',
          'Avoid repeating words like "Inbox:" or "Message:".',
          'If unsure, make a reasonable guess from text.',
          'Keep each title under 60 chars, summary under 110 chars.',
        ],
      },
      schema: {
        tasks: { '<taskId>': { title: 'string', summary: 'string' } },
        inbox: { '<inboxId>': { title: 'string', summary: 'string' } },
      },
    };

    try {
      const result = await aiChatCompletion({
        routeKey: 'dashboardPreview',
        timeoutMs: 20_000,
        messages: [
          { role: 'system', content: system },
          { role: 'user', content: JSON.stringify(user).slice(0, 24000) },
        ],
      });

      if (!result.ok) {
        res.json(heuristic());
        return;
      }

      const content = String(result.message?.content || '').trim();
      const clean = content.replace(/```json/g, '').replace(/```/g, '').trim();
      const parsed = tryParseJson(clean);
      if (!parsed || typeof parsed !== 'object') {
        res.json(heuristic());
        return;
      }

      const outTasks = parsed.tasks && typeof parsed.tasks === 'object' ? parsed.tasks : {};
      const outInbox = parsed.inbox && typeof parsed.inbox === 'object' ? parsed.inbox : {};

      res.json({ ok: true, ai: true, tasks: outTasks, inbox: outInbox });
    } catch {
      res.json(heuristic());
    }
  });

  await writeLock;
});

app.post('/api/tasks', async (req, res) => {
  const baseRevision = Number(req.body?.baseRevision);
  const data = req.body?.task ?? {};

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({
        error: 'Revision mismatch. Reload and try again.',
        currentRevision: store.revision,
      });
      return;
    }

    const normalized = normalizeTask(data);
    const ts = nowIso();

    const task = {
      id: makeId(),
      ...normalized,
      createdAt: ts,
      updatedAt: ts,
    };

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      tasks: [task, ...store.tasks],
    };

    await writeStore(nextStore);
    res.status(201).json(nextStore);
  });

  await writeLock;
});

app.put('/api/tasks/:id', async (req, res) => {
  const taskId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);
  const patch = req.body?.patch ?? {};

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({
        error: 'Revision mismatch. Reload and try again.',
        currentRevision: store.revision,
      });
      return;
    }

    const idx = store.tasks.findIndex((t) => t.id === taskId);
    if (idx === -1) {
      res.status(404).json({ error: 'Task not found' });
      return;
    }

    const existing = store.tasks[idx];
    const merged = {
      ...existing,
      ...patch,
    };

    // validate required title
    if (typeof merged.title !== 'string' || !merged.title.trim()) {
      res.status(400).json({ error: 'Title is required' });
      return;
    }

    // normalize key fields
    const normalized = normalizeTask(merged);
    const ts = nowIso();

    const updated = {
      ...existing,
      ...normalized,
      updatedAt: ts,
    };

    const nextTasks = [...store.tasks];
    nextTasks[idx] = updated;

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      tasks: nextTasks,
    };

    await writeStore(nextStore);
    res.json(nextStore);
  });

  await writeLock;
});

app.delete('/api/tasks/:id', async (req, res) => {
  const taskId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);

  writeLock = writeLock.then(async () => {
    const store = await readStore();
    if (Number.isFinite(baseRevision) && baseRevision !== store.revision) {
      res.status(409).json({
        error: 'Revision mismatch. Reload and try again.',
        currentRevision: store.revision,
      });
      return;
    }

    const nextTasks = store.tasks.filter((t) => t.id !== taskId);
    if (nextTasks.length === store.tasks.length) {
      res.status(404).json({ error: 'Task not found' });
      return;
    }

    const ts = nowIso();
    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      tasks: nextTasks,
    };

    await writeStore(nextStore);
    res.json(nextStore);
  });

  await writeLock;
});

/* Global AI Assistant */
async function aiAgentAction(message, store, projectId = null, options = {}) {
    const settings = await readSettings();
    const threadId = typeof options?.threadId === 'string' ? options.threadId.trim() : '';
    const effectiveThreadId = threadId || 'default';
    const threadHistory = Array.isArray(options?.threadHistory) ? options.threadHistory : [];
    const mcpEff = getMcpEffectiveSettings(settings);
    const mcpAvailable = Boolean(mcpEff.configured);

    const googleConnected = Boolean(settings.googleTokens && typeof settings.googleTokens === 'object' && settings.googleTokens.refresh_token);

    const findProjectByName = (name) => {
      const n = typeof name === 'string' ? name.trim().toLowerCase() : '';
      if (!n) return null;
      return (store.projects || []).find((p) => String(p.name || '').trim().toLowerCase() === n) || null;
    };

    const resolveProject = () => {
      const resolved = resolveProjectForMessage(store, message, projectId);
      if (resolved && typeof resolved === 'object' && resolved.ambiguous) return resolved;
      return resolved && typeof resolved === 'object' ? resolved : null;
    };

    const resolvedProject = resolveProject();
    if (resolvedProject && typeof resolvedProject === 'object' && resolvedProject.ambiguous) {
      const opts = Array.isArray(resolvedProject.options) ? resolvedProject.options : [];
      const list = opts.map((p) => `- ${p.name}`).join('\n');
      return { content: `Which project did you mean?\n${list}` };
    }

    const effectiveProject = resolvedProject || (projectId ? (store.projects || []).find((p) => p.id === projectId) : null) || null;
    const effectiveProjectId = effectiveProject?.id || projectId || null;

    const upsertScratchpad = (pid, text) => {
      store.projectScratchpads = store.projectScratchpads || {};
      store.projectScratchpads[pid] = { text: String(text ?? ''), updatedAt: nowIso() };
    };

    const appendTasks = (projectName, tasks) => {
      if (!Array.isArray(tasks) || tasks.length === 0) return { ok: true, created: 0 };
      const now = nowIso();
      const newTasks = tasks.map((t) => {
        const normalized = normalizeTask({
          title: t.title,
          status: 'Next',
          priority: t.priority || 2,
          project: projectName,
          dueDate: t.dueDate,
        });
        return {
          id: makeId(),
          ...normalized,
          createdAt: now,
          updatedAt: now,
        };
      });
      store.tasks = [...newTasks, ...(store.tasks || [])];
      return { ok: true, created: newTasks.length };
    };

    const doCreateProject = (args) => {
      const { type: projectType, tasks, scratchpad, ...rest } = args || {};
      const base = normalizeProject({
        ...rest,
        type: projectType,
        status: rest.status || 'Active',
      });
      const ts = nowIso();
      const project = {
        id: makeId(),
        ...base,
        createdAt: ts,
        updatedAt: ts,
      };
      store.projects = [project, ...(store.projects || [])];
      if (typeof scratchpad === 'string' && scratchpad.trim() !== '') {
        upsertScratchpad(project.id, scratchpad);
      }
      const taskResult = appendTasks(project.name, tasks);
      return { ok: true, projectId: project.id, name: project.name, tasksCreated: taskResult.created };
    };

    const doUpdateProject = (args) => {
      const patch = args && args.patch && typeof args.patch === 'object' ? args.patch : {};
      let target = null;
      if (args && typeof args.projectId === 'string' && args.projectId.trim()) {
        target = (store.projects || []).find((p) => p.id === args.projectId.trim()) || null;
      }
      if (!target && args && typeof args.projectName === 'string' && args.projectName.trim()) {
        target = findProjectByName(args.projectName);
      }
      if (!target && projectId) {
        target = (store.projects || []).find((p) => p.id === projectId) || null;
      }
      if (!target) return { ok: false, error: 'Project not found' };

      const merged = { ...target, ...patch };
      const normalized = normalizeProject(merged);
      const updated = { ...target, ...normalized, updatedAt: nowIso() };
      store.projects = (store.projects || []).map((p) => (p.id === updated.id ? updated : p));

      if (Object.prototype.hasOwnProperty.call(patch, 'scratchpad') && typeof patch.scratchpad === 'string') {
        upsertScratchpad(updated.id, patch.scratchpad);
      }

      return { ok: true, projectId: updated.id, name: updated.name };
    };

    const doCreateTasks = (args) => {
      const tasks = args && Array.isArray(args.tasks) ? args.tasks : [];
      let targetProj = null;
      if (args && typeof args.projectName === 'string' && args.projectName.trim()) {
        targetProj = findProjectByName(args.projectName);
      } else if (projectId) {
        targetProj = (store.projects || []).find((p) => p.id === projectId) || null;
      }
      if (!targetProj) return { ok: false, error: 'Target project not found for tasks' };
      return appendTasks(targetProj.name, tasks);
    };

    const userSystemPrompt = typeof settings.agentSystemPrompt === 'string' ? settings.agentSystemPrompt.trimEnd() : '';
    const userMemory = typeof settings.agentMemory === 'string' ? settings.agentMemory.trimEnd() : '';
    const operatorBio = typeof settings.operatorBio === 'string' ? settings.operatorBio.trimEnd() : '';

    const legacyHelpPrompt = typeof settings.operatorHelpPrompt === 'string' ? settings.operatorHelpPrompt.trimEnd() : '';
    const assistantOperatingDoctrineRaw = typeof settings.assistantOperatingDoctrine === 'string' ? settings.assistantOperatingDoctrine.trimEnd() : '';
    const assistantOperatingDoctrine = assistantOperatingDoctrineRaw || legacyHelpPrompt;

    const personalityLayer = typeof settings.personalityLayer === 'string' ? settings.personalityLayer.trimEnd() : '';
    const attentionRadar = typeof settings.attentionRadar === 'string' ? settings.attentionRadar.trimEnd() : '';
    const strategicForecasting = typeof settings.strategicForecasting === 'string' ? settings.strategicForecasting.trimEnd() : '';
    const executionAuthority = typeof settings.executionAuthority === 'string' ? settings.executionAuthority.trimEnd() : '';
    const knowledgeArchive = typeof settings.knowledgeArchive === 'string' ? settings.knowledgeArchive.trimEnd() : '';
    const dailyReportingStructure = typeof settings.dailyReportingStructure === 'string' ? settings.dailyReportingStructure.trimEnd() : '';

    const operatorTone = typeof settings.operatorTone === 'string' ? settings.operatorTone.trim() : '';
    const rawOperatorVoice = typeof settings.operatorVoice === 'string' ? settings.operatorVoice.trim() : '';
    const operatorVoice = normalizeOperatorVoice(rawOperatorVoice);
    const legacyTakeControlVoice = rawOperatorVoice.toLowerCase() === 'take_control';

    const coreUiOverrides = {
      operatorBio,
      assistantOperatingDoctrine,
      operatorHelpPrompt: legacyHelpPrompt,
      personalityLayer,
      attentionRadar,
      strategicForecasting,
      executionAuthority,
      knowledgeArchive,
      dailyReportingStructure,
    };

    let systemPrompt = await buildMarcusSystemPrompt({
      uiOverrides: coreUiOverrides,
      customSystemPrompt: userSystemPrompt,
    });
    systemPrompt +=
      "\n\n## Live Response Guardrails\n" +
      "- Treat stale backlog items as weak evidence unless they were updated recently, are due soon, or the operator explicitly mentions them.\n" +
      "- Do not nag, shame, taunt, or perform accountability theater.\n" +
      "- Do not propose a timed sprint, a yes/no focus prompt, or a 30-minute plan unless the operator explicitly asks for planning or accountability.\n" +
      "- Do not repeat the same recommendation unless new evidence materially changed.\n" +
      "- If the tracker looks stale or ambiguous, say so briefly and recommend cleanup instead of pretending certainty.\n" +
      "- Prefer concise, direct answers over performative coaching.\n" +
      "- Sound like a person, not a system. Use contractions, natural phrasing, and conversational flow.\n" +
      "- Never start with 'Certainly', 'Absolutely', 'Of course', or 'Sure thing'.\n" +
      "- Never use em dashes.\n" +
      "- When the operator is using voice, keep responses short and spoken-friendly. No bullet lists. Synthesize into natural sentences.\n";

    if (effectiveThreadId === 'operator_bio') {
      systemPrompt +=
        "\n\n## Operator Bio Thread Directives\n" +
        "This thread is dedicated to defining and refining the operator bio, responsibilities, preferences, constraints, and working principles.\n\n" +
        "Rules:\n" +
        "- Update the operator bio whenever the operator provides new or corrected information.\n" +
        "- Ask only the clarifying questions needed to improve accuracy.\n" +
        "- Produce a short summary and a recommended next step.\n" +
        "- Do not modify projects or tasks from this thread unless the operator explicitly asks.\n";
    }

    let context = '';

    if (userMemory) {
      context += `GLOBAL MEMORY (user-provided; treat as true unless contradicted):\n${String(userMemory).slice(0, 12000)}\n\n`;
    }

    if (operatorTone || operatorVoice || legacyTakeControlVoice) {
      context += `TONE/VOICE PREFERENCES:\n`;
      if (operatorTone) context += `- Tone: ${operatorTone}\n`;
      if (operatorVoice) context += `- Voice: ${operatorVoice}\n`;
      if (legacyTakeControlVoice) {
        context += `- Legacy take_control voice is deprecated. Interpret it as calm, decisive guidance without nagging, sarcasm, or forced accountability.\n`;
      }
      context += `\n`;
    }

    try {
      const qdrant = getQdrantConfig(settings);
      if (qdrant.enabled && qdrant.configured && qdrant.useForMarcus) {
        const businessKey = getBusinessKeyFromContext();
        const retrievalQuery = effectiveProject?.name
          ? `${message}\n\nProject context: ${effectiveProject.name}`
          : message;
        const knowledge = await qdrantSearchKnowledge(settings, retrievalQuery, {
          limit: Math.min(qdrant.topK, 5),
          filter: { businessKey },
        });
        if (knowledge.ok && Array.isArray(knowledge.matches) && knowledge.matches.length) {
          const knowledgeLines = knowledge.matches.slice(0, 5).map((match, index) => {
            const payload = match.payload && typeof match.payload === 'object' ? match.payload : {};
            const title = typeof payload.title === 'string' && payload.title.trim() ? payload.title.trim() : `Document ${index + 1}`;
            const text = typeof payload.text === 'string' ? payload.text.replace(/\s+/g, ' ').trim() : '';
            const source = typeof payload.source === 'string' ? payload.source.trim() : '';
            const preview = text.length > 360 ? `${text.slice(0, 360)}…` : text;
            return `- [score ${match.score.toFixed(3)}] ${title}${source ? ` (${source})` : ''}: ${preview}`;
          });
          context += `KNOWLEDGE BASE HITS (Qdrant; use as supporting memory, not ground truth if contradicted):\n${knowledgeLines.join('\n')}\n\n`;
        }
      }
    } catch {
      // Ignore Qdrant retrieval failures during chat assembly.
    }

    // Always include a compact operational snapshot so Marcus can be proactive.
    try {
      const today = new Date().toISOString().slice(0, 10);
      const inbox = getVisibleInboxItemsFromSettings(store.inboxItems, settings);
      const projects = Array.isArray(store.projects) ? store.projects : [];
      const activeProjects = projects.filter((project) => !isClosedProjectStatus(project?.status));
      const snapshot = collectMarcusRelevantSnapshot(store, { today, nowMs: Date.now(), currentProjectId: effectiveProjectId || '' });
      const openTasks = snapshot.openTasks;
      const overdue = snapshot.overdueTasks;
      const dueToday = snapshot.dueTodayTasks;
      const sortedOpen = snapshot.sortedTasks.slice(0, 12);

      const inboxNew = inbox.filter((it) => {
        const src = String(it?.source || '').trim().toLowerCase();
        return String(it?.status || '').trim().toLowerCase() === 'new' && src !== 'marcus' && src !== 'marcus';
      });
      const inboxLines = inboxNew
        .slice()
        .sort((a, b) => String(b?.updatedAt || b?.createdAt || '').localeCompare(String(a?.updatedAt || a?.createdAt || '')))
        .slice(0, 10)
        .map((item) => {
          const src = String(item?.source || '').trim() || 'inbox';
          const proj = String(item?.projectName || '').trim();
          const text = String(item?.text || '').replace(/\s+/g, ' ').trim();
          const head = text.length > 160 ? `${text.slice(0, 160)}…` : text;
          return `- [${src}] ${head}${proj ? ` (project: ${proj})` : ''}`;
        });

      const businessKey = getBusinessKeyFromContext();
      const lines = [];
      lines.push(`OPS SNAPSHOT (ACTIVE BUSINESS: ${businessKey}; asOf: ${nowIso()}; today: ${today})`);
      lines.push(`- Projects: ${activeProjects.length} • Relevant open tasks: ${snapshot.relevantTasks.length} • Overdue: ${overdue.length} • Due today: ${dueToday.length} • New inbox: ${inboxNew.length}`);
      if (snapshot.suppressedTaskCount > 0) lines.push(`- Suppressed stale/noisy tasks: ${snapshot.suppressedTaskCount} of ${openTasks.length} total open`);
      lines.push('');
      if (overdue.length) {
        lines.push('Top overdue:');
        overdue
          .slice()
          .sort((a, b) => (normalizeTrackerDueDate(a?.dueDate) || '9999-12-31').localeCompare(normalizeTrackerDueDate(b?.dueDate) || '9999-12-31'))
          .slice(0, 8)
          .forEach((t, i) => {
            const priRaw = Number(t?.priority);
            const priNum = Number.isFinite(priRaw) ? priRaw : 2;
            const due = normalizeTrackerDueDate(t?.dueDate);
            const proj = String(t?.project || '').trim();
            const st = String(t?.status || 'Next');
            lines.push(`${i + 1}. [P${priNum}] ${String(t?.title || '').trim()}${proj ? ` — ${proj}` : ''}${due ? ` — due ${due}` : ''} — ${st}`);
          });
        lines.push('');
      }

      lines.push('Next tasks (prioritized):');
      sortedOpen.forEach((t, i) => {
        const priRaw = Number(t?.priority);
        const priNum = Number.isFinite(priRaw) ? priRaw : 2;
        const due = normalizeTrackerDueDate(t?.dueDate);
        const proj = String(t?.project || '').trim();
        const st = String(t?.status || 'Next');
        lines.push(`${i + 1}. [P${priNum}] ${String(t?.title || '').trim()}${proj ? ` — ${proj}` : ''}${due ? ` — due ${due}` : ''} — ${st}`);
      });
      if (!sortedOpen.length) lines.push('- No live tasks surfaced after freshness filtering.');

      if (inboxLines.length) {
        lines.push('Recent inbox (new):');
        lines.push(...inboxLines);
        lines.push('');
      }

      context += `${lines.join('\n')}\n\n`;
    } catch {
      // ignore snapshot failures
    }

    // Desktop context awareness (active window, OS idle, project matching).
    try {
      if (desktopContextCache.data && (Date.now() - desktopContextCache.at) < 30_000) {
        const dc = desktopContextCache.data;
        const wt = String(dc.windowTitle || '').trim();
        const pn = String(dc.processName || '').trim();
        const idle = Number(dc.idleSeconds) || 0;
        if (wt || pn) {
          const dcLines = [`DESKTOP CONTEXT (what the operator is doing right now):`];
          dcLines.push(`- Active window: "${wt}"`);
          dcLines.push(`- Application: ${pn}`);
          dcLines.push(`- OS idle: ${idle}s`);
          // Try to match window title to a known project
          const allProjects = Array.isArray(store.projects) ? store.projects : [];
          const wtLower = wt.toLowerCase();
          const matchedProject = allProjects.find((p) => {
            const wp = String(p?.workspacePath || '').trim();
            const name = String(p?.name || '').trim();
            if (wp && wtLower.includes(wp.toLowerCase().replace(/\\/g, '/').split('/').pop())) return true;
            if (wp && wtLower.includes(wp.toLowerCase().split('\\').pop())) return true;
            if (name && wtLower.includes(name.toLowerCase())) return true;
            return false;
          });
          if (matchedProject) {
            dcLines.push(`- Matched project: "${String(matchedProject.name || '').trim()}" (workspace: ${String(matchedProject.workspacePath || '').trim()})`);
            dcLines.push(`  Use this to give context-aware responses. The operator is actively working on this project.`);

            // Inject Marcus's accumulated knowledge about this project
            const mNotes = Array.isArray(store.marcusNotes?.[matchedProject.id]) ? store.marcusNotes[matchedProject.id] : [];
            if (mNotes.length) {
              const recentNotes = mNotes.slice(-15);
              dcLines.push(`\nYOUR NOTES ON THIS PROJECT (${mNotes.length} total, showing last ${recentNotes.length}):`);
              dcLines.push(`These are observations you've recorded while watching the operator work on this project. Use them for context.`);
              for (const n of recentNotes) {
                const when = n.ts ? new Date(n.ts).toLocaleString() : '';
                const file = n.activeFile ? ` [${n.activeFile}]` : '';
                dcLines.push(`  - ${when}${file}: ${String(n.text || '').slice(0, 500)}`);
              }
            }
          } else {
            dcLines.push(`- No matched project. The operator may be working on something not yet tracked.`);
            dcLines.push(`  If they confirm they want to track it, use create_project, then inspect_workspace to learn about it.`);
          }

          // Rich workspace data from the desktop agent
          const ws = dc.workspace;
          if (ws && typeof ws === 'object' && ws.workspacePath) {
            dcLines.push(`\nWORKSPACE SNAPSHOT (${ws.folderName || ws.workspacePath}):`);
            if (ws.activeFile) dcLines.push(`- Active file (currently editing): ${ws.activeFile}`);
            if (ws.gitBranch) dcLines.push(`- Git branch: ${ws.gitBranch}`);
            if (ws.gitStatus && ws.gitStatus.length) {
              dcLines.push(`- Uncommitted changes (${ws.gitStatus.length}):`);
              ws.gitStatus.forEach(s => dcLines.push(`    ${s.status} ${s.file}`));
            }
            if (ws.gitRecentCommits && ws.gitRecentCommits.length) {
              dcLines.push(`- Recent commits:`);
              ws.gitRecentCommits.forEach(c => dcLines.push(`    ${c}`));
            }
            if (ws.recentFiles && ws.recentFiles.length) {
              dcLines.push(`- Recently modified files:`);
              ws.recentFiles.forEach(f => dcLines.push(`    ${f}`));
            }
            if (ws.structure && ws.structure.length) {
              dcLines.push(`- Top-level structure:`);
              ws.structure.forEach(f => dcLines.push(`    ${f}`));
            }
            // File contents - the actual code from the active directory + configs
            if (ws.fileContents && Object.keys(ws.fileContents).length) {
              dcLines.push(`\nFILE CONTENTS (active directory + project configs, ${Object.keys(ws.fileContents).length} files):`);
              for (const [fpath, content] of Object.entries(ws.fileContents)) {
                dcLines.push(`\n--- ${fpath} ---\n${content}`);
              }
            }
            if (ws.gitDiff) {
              dcLines.push(`\nGIT DIFF (uncommitted work):\n${ws.gitDiff}`);
            }
          }

          dcLines.push('');
          context += dcLines.join('\n') + '\n';
        }
      }
    } catch {
      // ignore
    }

    // Cross-business rollup (cached).
    try {
      const cfg = getBusinessConfigFromSettings(settings);
      const bizList = Array.isArray(cfg.businesses) ? cfg.businesses : [];
      if (bizList.length > 1) {
        const nowMs = Date.now();
        const cached = crossBizRollupCache && typeof crossBizRollupCache === 'object' ? crossBizRollupCache : { at: 0, text: '' };
        if (cached.text && (nowMs - Number(cached.at || 0) < 60_000)) {
          context += cached.text;
        } else {
          const today = new Date().toISOString().slice(0, 10);
          const byBiz = [];
          const focus = [];

          for (const b of bizList.slice(0, 12)) {
            const bKey = normalizeBusinessKey(b?.key || '') || DEFAULT_BUSINESS_KEY;
            const bName = String(b?.name || '').trim() || bKey;
            const bStore = await withBusinessKey(bKey, async () => readStore());
            const inbox = getVisibleInboxItemsFromSettings(bStore?.inboxItems, settings);
            const projects = Array.isArray(bStore?.projects) ? bStore.projects : [];
            const snapshot = collectMarcusRelevantSnapshot(bStore, { today, nowMs });
            const openTasks = snapshot.relevantTasks;
            const overdue = snapshot.overdueTasks;
            const dueToday = snapshot.dueTodayTasks;
            const newInbox = inbox.filter((it) => {
              const src = String(it?.source || '').trim().toLowerCase();
              return String(it?.status || '').trim().toLowerCase() === 'new' && src !== 'marcus' && src !== 'marcus';
            });
            byBiz.push({ key: bKey, name: bName, open: openTasks.length, overdue: overdue.length, dueToday: dueToday.length, inboxNew: newInbox.length });

            for (const p of projects) {
              const pst = String(p?.status || '').trim().toLowerCase();
              if (pst === 'done' || pst === 'archived' || pst === 'on hold') continue;
              const pTasks = openTasks.filter((t) => t?.project === p?.name || t?.projectId === p?.id);
              const open = pTasks.filter((t) => {
                const st = String(t?.status || '').trim().toLowerCase();
                return st !== 'done' && st !== 'archived' && st !== 'complete' && st !== 'completed';
              });
              if (!open.length) continue;
              let urgent = 0;
              for (const t of open) {
                const pri = Number(t?.priority);
                const st = String(t?.status || '').trim().toLowerCase();
                const due = normalizeTrackerDueDate(t?.dueDate);
                if (pri === 1 || st === 'urgent' || (due && due <= today)) urgent++;
              }
              if (urgent <= 0) continue;
              focus.push({ businessKey: bKey, businessName: bName, projectId: p?.id || '', name: String(p?.name || '').trim(), dueDate: String(p?.dueDate || '').trim(), urgent, open: open.length });
            }
          }

          focus.sort((a, b) => {
            if (a.urgent !== b.urgent) return b.urgent - a.urgent;
            const ad = a.dueDate || '9999-12-31';
            const bd = b.dueDate || '9999-12-31';
            return ad.localeCompare(bd);
          });

          const out = [];
          out.push(`CROSS-BUSINESS ROLLUP (asOf: ${nowIso()}; today: ${today})`);
          out.push(`- Businesses scanned: ${byBiz.length}`);
          out.push(`- Inbox new total: ${byBiz.reduce((n, x) => n + Number(x.inboxNew || 0), 0)}`);
          out.push('');
          out.push('By business (open/overdue/due-today/inbox-new):');
          byBiz
            .slice()
            .sort((a, b) => (b.overdue - a.overdue) || (b.inboxNew - a.inboxNew) || (b.open - a.open))
            .forEach((b) => {
              out.push(`- ${b.name}: ${b.open}/${b.overdue}/${b.dueToday}/${b.inboxNew}`);
            });
          out.push('');
          out.push('Top urgent projects:');
          focus.slice(0, 12).forEach((p, i) => {
            const due = p.dueDate ? ` • due ${p.dueDate}` : '';
            out.push(`${i + 1}. [${p.businessName}] ${p.name} • urgent ${p.urgent}/${p.open}${due}`);
          });
          out.push('');
          const text = `${out.join('\n')}\n\n`;
          crossBizRollupCache = { at: nowMs, text };
          context += text;
        }
      }
    } catch {
      // ignore rollup failures
    }

    if (effectiveProjectId && effectiveProject) {
      const pTasksRaw = (store.tasks || []).filter((t) => t.project === effectiveProject.name || t.project === effectiveProjectId);
      const pTasks = pTasksRaw.slice(0, 120).map((t) => ({
        id: t.id,
        title: t.title,
        status: t.status,
        priority: t.priority,
        dueDate: t.dueDate,
        owner: t.owner,
        type: t.type,
        updatedAt: t.updatedAt,
      }));

      const scratchpad = String(store.projectScratchpads?.[effectiveProjectId]?.text || '').slice(0, 12000);
      const noteEntryList = Array.isArray(store.projectNoteEntries?.[effectiveProjectId]) ? store.projectNoteEntries[effectiveProjectId] : [];
      const noteEntries = noteEntryList.slice(0, 16).map((n) => ({
        kind: n.kind,
        date: n.date,
        title: n.title,
        content: String(n.content || '').slice(0, 2000),
      }));
      const commList = Array.isArray(store.projectCommunications?.[effectiveProjectId]) ? store.projectCommunications[effectiveProjectId] : [];
      const communications = commList.slice(0, 16).map((c) => ({
        type: c.type,
        direction: c.direction,
        subject: c.subject,
        date: c.date,
        body: String(c.body || '').slice(0, 2000),
      }));

      const legacyNotes = pickProjectNotesValue(store.projectNotes?.[effectiveProject.name]);

      const ctxObj = {
        project: effectiveProject,
        team: Array.isArray(store.team) ? store.team : [],
        scratchpad,
        projectNotes: legacyNotes.notes,
        noteEntries,
        communications,
        tasks: pTasks,
      };

      context += `CURRENT PROJECT CONTEXT (JSON):\n${JSON.stringify(ctxObj, null, 2).slice(0, 24000)}\n\n`;
    } else {
      const projectsOverview = (store.projects || [])
        .filter((p) => !isClosedProjectStatus(p?.status))
        .map((p) => ({ id: p.id, name: p.name, type: p.type, status: p.status, dueDate: p.dueDate }));
      context += `ALL PROJECTS (JSON): ${JSON.stringify(projectsOverview).slice(0, 24000)}\n\n`;
    }

    const routeKey = effectiveThreadId === 'operator_bio' ? 'operatorBio' : 'marcusChat';
    const route = resolveAiRoute(settings, routeKey);
    // If AI isn't configured for this area, still answer from local data.
    if (!route.apiKey) {
      if (effectiveProjectId && effectiveProject) {
        const tasks = (store.tasks || []).filter((t) => t.project === effectiveProject.name || t.project === effectiveProjectId);
        const open = tasks.filter((t) => String(t.status || '').toLowerCase() !== 'done');
        const sorted = open
          .slice()
          .sort((a, b) => {
            const ap = Number(a.priority ?? 2);
            const bp = Number(b.priority ?? 2);
            if (ap !== bp) return ap - bp;
            return String(a.dueDate || '9999-12-31').localeCompare(String(b.dueDate || '9999-12-31'));
          })
          .slice(0, 12);

        const lines = [];
        lines.push(`Project: ${effectiveProject.name}${effectiveProject.type ? ` (${effectiveProject.type})` : ''} � ${effectiveProject.status || 'Active'}${effectiveProject.dueDate ? ` � due ${effectiveProject.dueDate}` : ''}`);
        lines.push('');
        lines.push(`Open tasks: ${open.length} (showing top ${sorted.length})`);
        sorted.forEach((t, i) => {
          const due = t.dueDate ? ` � due ${t.dueDate}` : '';
          const pri = `P${Number(t.priority ?? 2)}`;
          const st = t.status ? String(t.status) : 'Next';
          lines.push(`${i + 1}. [${pri}] ${t.title} � ${st}${due}`);
        });
        lines.push('');
        lines.push('AI is not enabled for this area (missing API key), but I can still show you everything in the tracker.');
        lines.push('If you want deeper reasoning/rewrites, set a key in Settings → AI (OpenAI/OpenRouter).');
        return { content: lines.join('\n') };
      }

      const today = new Date().toISOString().slice(0, 10);
      const inbox = getVisibleInboxItemsFromSettings(store.inboxItems, settings);
      const projects = Array.isArray(store.projects) ? store.projects : [];
      const activeProjects = projects.filter((project) => !isClosedProjectStatus(project?.status));
      const snapshot = collectMarcusRelevantSnapshot(store, { today, nowMs: Date.now(), currentProjectId: effectiveProjectId || '' });
      const overdue = snapshot.overdueTasks;
      const dueToday = snapshot.dueTodayTasks;
      const nextTasks = snapshot.sortedTasks.slice(0, 10);

      const newInbox = inbox.filter((it) => {
        const src = String(it?.source || '').trim().toLowerCase();
        return String(it?.status || '').trim().toLowerCase() === 'new' && src !== 'marcus' && src !== 'marcus';
      });

      const lines = [];
      lines.push('AI is not enabled for this area (missing API key), but I can still guide you using the tracker data.');
      lines.push(`Today: ${today}`);
      lines.push(`Projects: ${activeProjects.length} • Relevant open tasks: ${snapshot.relevantTasks.length} • Overdue: ${overdue.length} • Due today: ${dueToday.length} • New inbox: ${newInbox.length}`);
      if (snapshot.suppressedTaskCount > 0) lines.push(`Suppressed stale/noisy tasks: ${snapshot.suppressedTaskCount}`);
      lines.push('');

      if (overdue.length) {
        lines.push('Overdue (top):');
        overdue
          .slice()
          .sort((a, b) => {
            const ad0 = normalizeTrackerDueDate(a?.dueDate);
            const bd0 = normalizeTrackerDueDate(b?.dueDate);
            const ad = ad0 ? ad0 : '9999-12-31';
            const bd = bd0 ? bd0 : '9999-12-31';
            return ad.localeCompare(bd);
          })
          .slice(0, 6)
          .forEach((t, i) => {
            const priRaw = Number(t?.priority);
            const priNum = Number.isFinite(priRaw) ? priRaw : 2;
            const due = normalizeTrackerDueDate(t?.dueDate);
            const proj = String(t?.project || '').trim();
            lines.push(`${i + 1}. [P${priNum}] ${String(t?.title || '').trim()}${proj ? ` — ${proj}` : ''}${due ? ` — due ${due}` : ''}`);
          });
        lines.push('');
      }

      lines.push('Next actions (start here):');
      nextTasks.forEach((t, i) => {
        const priRaw = Number(t?.priority);
        const priNum = Number.isFinite(priRaw) ? priRaw : 2;
        const due = normalizeTrackerDueDate(t?.dueDate);
        const proj = String(t?.project || '').trim();
        const st = String(t?.status || 'Next');
        lines.push(`${i + 1}. [P${priNum}] ${String(t?.title || '').trim()}${proj ? ` — ${proj}` : ''}${due ? ` — due ${due}` : ''} — ${st}`);
      });
      if (!nextTasks.length) lines.push('- No live tasks surfaced after freshness filtering.');

      if (newInbox.length) {
        lines.push('Inbox triage (newest):');
        newInbox
          .slice()
          .sort((a, b) => String(b?.updatedAt || b?.createdAt || '').localeCompare(String(a?.updatedAt || a?.createdAt || '')))
          .slice(0, 5)
          .forEach((it) => {
            const rawSrc = String(it?.source || '').trim();
            const src = rawSrc ? rawSrc : 'inbox';
            const text = String(it?.text || '').replace(/\s+/g, ' ').trim();
            const head = text.length > 140 ? `${text.slice(0, 140)}…` : text;
            lines.push(`- [${src}] ${head}`);
          });
        lines.push('');
      }

      lines.push('To enable deeper reasoning + tool-use, set an API key in Settings → AI (OpenAI/OpenRouter).');
      return { content: lines.join('\n') };
    }

    const tools = [];

    if (effectiveThreadId === 'operator_bio') {
      tools.push({
        type: 'function',
        function: {
          name: 'set_operator_bio',
          description: 'Persist the operator bio (global) to settings. Provide the full updated bio text.',
          parameters: {
            type: 'object',
            properties: {
              operatorBio: { type: 'string', description: 'Full operator bio text' },
            },
            required: ['operatorBio'],
          },
        },
      });
    } else {
      tools.push(
        {
          type: "function",
          function: {
            name: "create_project",
            description: "Create a new project with full details (due date, links, value, etc).",
            parameters: {
              type: "object",
              properties: {
                name: { type: "string", description: "Name of the project" },
                type: { type: "string", enum: ["Build", "Rebuild", "Revision", "Workflow", "Cleanup", "Other"] },
                owner: { type: "string", description: "Optional assignee / owner name (team member)" },
                dueDate: { type: "string", description: "YYYY-MM-DD" },
                status: { type: "string", enum: ["Active", "On Hold", "Done", "Archived"] },
                accountManagerName: { type: "string" },
                accountManagerEmail: { type: "string" },
                workspacePath: { type: "string", description: "Local folder path for VS Code" },
                airtableUrl: { type: "string", description: "http(s) URL" },
                projectValue: { type: "string", description: "Optional, e.g. $5000" },
                stripeInvoiceUrl: { type: "string", description: "http(s) URL" },
                repoUrl: { type: "string", description: "http(s) URL" },
                docsUrl: { type: "string", description: "http(s) URL" },
                scratchpad: { type: "string", description: "Initial scratchpad / notes" },
                tasks: {
                  type: "array",
                  items: {
                    type: "object",
                    properties: {
                      title: { type: "string" },
                      priority: { type: "integer", minimum: 1, maximum: 3 },
                      dueDate: { type: "string", description: "YYYY-MM-DD" }
                    },
                    required: ["title"]
                  }
                }
              },
              required: ["name", "type"]
            }
          }
        },
        {
          type: "function",
          function: {
            name: "update_project",
            description: "Update an existing project by id or name. Use when the user provides new details for an existing project.",
            parameters: {
              type: "object",
              properties: {
                projectId: { type: "string", description: "Preferred when known" },
                projectName: { type: "string", description: "Case-insensitive match if id not provided" },
                patch: {
                  type: "object",
                  properties: {
                    name: { type: "string" },
                    type: { type: "string", enum: ["Build", "Rebuild", "Revision", "Workflow", "Cleanup", "Other"] },
                    owner: { type: "string", description: "Optional assignee / owner name (team member)" },
                    dueDate: { type: "string", description: "YYYY-MM-DD" },
                    status: { type: "string", enum: ["Active", "On Hold", "Done", "Archived"] },
                    accountManagerName: { type: "string" },
                    accountManagerEmail: { type: "string" },
                    workspacePath: { type: "string" },
                    airtableUrl: { type: "string" },
                    projectValue: { type: "string" },
                    stripeInvoiceUrl: { type: "string" },
                    repoUrl: { type: "string" },
                    docsUrl: { type: "string" },
                    scratchpad: { type: "string" }
                  }
                }
              },
              required: ["patch"]
            }
          }
        },
        {
          type: "function",
          function: {
            name: "create_tasks",
            description: "Create multiple tasks.",
            parameters: {
              type: "object",
              properties: {
                projectName: { type: "string", description: "Name of the project. Optional if inside a project context." },
                tasks: {
                  type: "array",
                  items: {
                    type: "object",
                    properties: {
                      title: { type: "string" },
                      priority: { type: "integer", minimum: 1, maximum: 3 },
                      dueDate: { type: "string", description: "YYYY-MM-DD" }
                    },
                    required: ["title"]
                  }
                }
              },
              required: ["tasks"]
            }
          }
        },
        {
          type: "function",
          function: {
            name: "inspect_workspace",
            description: "Inspect a local project directory to learn about it. Lists files and reads key files (README, package.json, etc.). Use after creating a new project to understand what it contains.",
            parameters: {
              type: "object",
              properties: {
                directoryPath: { type: "string", description: "Absolute path to the project directory on the local machine" }
              },
              required: ["directoryPath"]
            }
          }
        }
      );
    }

    if (mcpAvailable) {
      tools.push({
        type: 'function',
        function: {
          name: 'mcp_list_tools',
          description: 'List available tools from the configured MCP server.',
          parameters: { type: 'object', properties: {} },
        },
      });
      tools.push({
        type: 'function',
        function: {
          name: 'mcp_call',
          description: 'Call a tool on the configured MCP server.',
          parameters: {
            type: 'object',
            properties: {
              name: { type: 'string', description: 'MCP tool name' },
              arguments: { type: 'object', description: 'Tool arguments as an object' },
            },
            required: ['name'],
          },
        },
      });
    }

    if (googleConnected) {
      tools.push({
        type: 'function',
        function: {
          name: 'google_list_upcoming_events',
          description: 'List upcoming Google Calendar events (read-only). Useful for seeing upcoming calls/meetings and their join links.',
          parameters: {
            type: 'object',
            properties: {
              days: { type: 'number', description: 'How many days ahead to look (1-30). Default 7.' },
              max: { type: 'number', description: 'Max events to return (1-50). Default 25.' },
            },
          },
        },
      });
    }

      if (getOpenAiSecrets(settings).apiKey) {
        tools.push({
          type: 'function',
          function: {
            name: 'generate_image',
            description: 'Generate an image using DALL-E 3 based on a prompt. Returns a summary of the generated image and its embedded URL.',
            parameters: {
              type: 'object',
              properties: {
                prompt: { type: 'string', description: 'Very detailed visual prompt describing the image.' },
              },
              required: ['prompt'],
            },
          },
        });
        tools.push({
          type: 'function',
          function: {
            name: 'web_search',
            description: 'Search the web for real-time information using local MCP proxy (if puppeteer is configured, it will be seamless, else fallback).',
            parameters: {
              type: 'object',
              properties: {
                query: { type: 'string', description: 'Search keywords.' }
              },
              required: ['query'],
            }
          }
        });
      }

      const messages = [
        {
          role: 'system',
          content:
            systemPrompt +
            (effectiveThreadId === 'operator_bio'
              ? "\n\nIMPORTANT: Use set_operator_bio to persist changes to the bio."
              : "\n\nIMPORTANT: When the user asks to create or update a project (due date, links, invoice, repo, docs, value, status, account manager), you MUST use tool calls (create_project / update_project / create_tasks). If you need external data, use MCP tools when available."),
        },
      ];

      // Include a small amount of history for continuity (before the current message).
      if (effectiveThreadId === 'operator_bio' && threadHistory.length) {
        for (const m of threadHistory.slice(-16)) {
          const role = m.role === 'user' ? 'user' : m.role === 'ai' ? 'assistant' : m.role === 'assistant' ? 'assistant' : '';
          if (!role) continue;
          const content = String(m.content || '').slice(0, 2000);
          if (content) messages.push({ role, content });
        }
      } else if (effectiveProjectId && store.projectChats && store.projectChats[effectiveProjectId]) {
        const h = store.projectChats[effectiveProjectId];
        const history = Array.isArray(h) ? h : Array.isArray(h.messages) ? h.messages : [];
        for (const m of history.slice(-8)) {
          const role = m.role === 'user' ? 'user' : m.role === 'ai' ? 'assistant' : '';
        if (!role) continue;
        const content = String(m.content || '').slice(0, 2000);
        if (content) messages.push({ role, content });
      }
    }

    messages.push({ role: 'user', content: `${context}User Request: ${message}` });

    const callChat = async () => {
      const result = await aiChatCompletion({
        routeKey,
        messages,
        tools,
        tool_choice: 'auto',
        timeoutMs: 30_000,
      });
      if (!result.ok) throw new Error(result.error || 'AI request failed');
      return result.message;
    };

    const execTool = async (toolName, args) => {
      if (toolName === 'set_operator_bio') {
        const nextBio = typeof args?.operatorBio === 'string' ? args.operatorBio.trimEnd() : '';
        const saved = await readSettings();
        const ts = nowIso();
        const next = { ...saved, operatorBio: nextBio, updatedAt: ts };
        await writeSettings(next);
        return { ok: true, updatedAt: ts, operatorBioLength: nextBio.length };
      }
      if (toolName === 'create_project') return doCreateProject(args);
      if (toolName === 'update_project') return doUpdateProject(args);
      if (toolName === 'create_tasks') return doCreateTasks(args);
      if (toolName === 'mcp_list_tools') {
        if (!mcpAvailable) return { ok: false, error: 'MCP is not configured' };
        const toolsList = await mcpListToolsAll(settings);
        return {
          ok: true,
          tools: toolsList.map((t) => ({ name: t.name, description: t.description, inputSchema: t.inputSchema })),
        };
      }
      if (toolName === 'mcp_call') {
        if (!mcpAvailable) return { ok: false, error: 'MCP is not configured' };
        const name = typeof args?.name === 'string' ? args.name : '';
        const a = args?.arguments && typeof args.arguments === 'object' && !Array.isArray(args.arguments) ? args.arguments : {};
        const resolved = resolveMcpTarget(settings, name);
        if (!resolved.ok) return { ok: false, error: resolved.error || 'MCP is not configured' };
        const cfg = resolved.target.config;
        const result = await mcpCallTool({ command: cfg.command, args: cfg.args, cwd: cfg.cwd || process.cwd() }, resolved.toolName, a);
        return { ok: true, result };
      }
      if (toolName === 'google_list_upcoming_events') {
        if (!googleConnected) return { ok: false, error: 'Google Calendar is not connected' };
        const days = Number(args?.days);
        const max = Number(args?.max);
        return await googleListUpcomingEvents({ days, max });
      }
      if (toolName === 'generate_image') {
        const openai = getOpenAiSecrets(settings);
        if (!openai.apiKey) return { ok: false, error: 'OpenAI key required for image generation' };
        try {
          const body = { model: 'dall-e-3', prompt: args.prompt, n: 1, size: '1024x1024' };
          const { resp, data } = await fetchJsonWithTimeout('https://api.openai.com/v1/images/generations', {
            method: 'POST',
            headers: { Authorization: `Bearer ${openai.apiKey}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            timeoutMs: 45000
          });
          if (data?.data?.[0]?.url) {
            const url = data.data[0].url;
            return { ok: true, imageUrl: url, result: `![Generated Image](${url})` };
          }
          return { ok: false, error: 'Image generation failed', details: data };
        } catch (e) {
          return { ok: false, error: e.message };
        }
      }
        if (toolName === 'inspect_workspace') {
          const dirPath = typeof args?.directoryPath === 'string' ? args.directoryPath.trim() : '';
          if (!dirPath || !path.isAbsolute(dirPath)) {
            return { ok: false, error: 'Absolute directory path required' };
          }
          try {
            const dirStat = await fs.stat(dirPath);
            if (!dirStat.isDirectory()) return { ok: false, error: 'Path is not a directory' };

            const entries = await fs.readdir(dirPath, { withFileTypes: true });
            const fileList = [];
            const keyFilePaths = [];
            const KEY_FILE_RE = /^(readme(\.md|\.txt)?|package\.json|cargo\.toml|pyproject\.toml|setup\.py|setup\.cfg|go\.mod|requirements\.txt|makefile|dockerfile|docker-compose\.ya?ml|tsconfig\.json|composer\.json|gemfile|pom\.xml|build\.gradle|\.gitignore)$/i;

            for (const entry of entries.slice(0, 80)) {
              const name = entry.name;
              if (name.startsWith('.') && name !== '.gitignore') continue;
              if (/^(node_modules|\.git|__pycache__|dist|build|\.next|vendor|target)$/.test(name)) {
                fileList.push(name + '/ (skipped)');
                continue;
              }
              if (entry.isDirectory()) {
                fileList.push(name + '/');
                try {
                  const subEntries = await fs.readdir(path.join(dirPath, name), { withFileTypes: true });
                  for (const sub of subEntries.slice(0, 20)) {
                    if (sub.name.startsWith('.')) continue;
                    fileList.push(`  ${name}/${sub.name}${sub.isDirectory() ? '/' : ''}`);
                  }
                } catch {}
              } else {
                fileList.push(name);
                if (KEY_FILE_RE.test(name)) {
                  keyFilePaths.push(path.join(dirPath, name));
                }
              }
            }

            const fileContents = {};
            for (const fp of keyFilePaths.slice(0, 5)) {
              try {
                const content = await fs.readFile(fp, 'utf8');
                const relPath = path.relative(dirPath, fp);
                fileContents[relPath] = content.slice(0, 3000);
              } catch {}
            }

            return {
              ok: true,
              directoryPath: dirPath,
              fileCount: fileList.length,
              files: fileList.slice(0, 120),
              keyFileContents: fileContents,
            };
          } catch (e) {
            return { ok: false, error: e.message };
          }
        }
        if (toolName === 'web_search') {
          try {
            const query = args?.query || '';
            if (!query) return { ok: false, error: 'Query required' };
            const formData = new URLSearchParams();
            formData.append('q', query);
            const r = await fetch('https://lite.duckduckgo.com/lite/', {
              method: 'POST',
              headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
              body: formData.toString()
            });
            const text = await r.text();
            const results = [];
            const snippetRegex = /<td class='result-snippet'[^>]*>([\s\S]*?)<\/td>/g;
            let m;
            while ((m = snippetRegex.exec(text)) !== null && results.length < 6) {
               results.push(m[1].replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
            }
            if (results.length > 0) return { ok: true, results };
            // fallback if regex misses
            const plain = text.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');
            return { ok: true, results: [plain.substring(1000, 3000)] };
          } catch (e) {
            return { ok: false, error: e.message };
          }
        }

        return { ok: false, error: `Unknown tool: ${toolName}` };
      };

      try {
        for (let step = 0; step < 4; step++) {
          const msg = await callChat();

          // Preserve the assistant message in the transcript for tool-call chaining.
          const assistantMsg = { role: 'assistant', content: msg.content || '' };
          if (msg.tool_calls) assistantMsg.tool_calls = msg.tool_calls;
          messages.push(assistantMsg);

          if (!msg.tool_calls || msg.tool_calls.length === 0) {
            return { content: String(msg.content || '').trim() };
          }

          for (const call of msg.tool_calls) {
            const toolName = call?.function?.name;
            const raw = call?.function?.arguments;

            let args = {};
            try {
            args = raw ? JSON.parse(raw) : {};
          } catch {
            args = {};
          }

          let result;
          try {
            result = await execTool(toolName, args);
          } catch (e) {
            result = { ok: false, error: e?.message || 'Tool failed' };
          }

          messages.push({
            role: 'tool',
            tool_call_id: call.id,
            content: JSON.stringify(result).slice(0, 12000),
          });
        }
      }
      return { content: 'I hit a tool-calling loop limit. Try again with a more specific request.' };
    } catch (e) {
      console.error('AI call failed:', e);
      return { content: `Error: ${e.message}` };
    }
}

app.post('/api/chat', async (req, res) => {
  const message = typeof req.body?.message === 'string' ? req.body.message : '';
  const projectId = typeof req.body?.projectId === 'string' ? req.body.projectId : null;
  const threadIdRaw = typeof req.body?.threadId === 'string' ? req.body.threadId : '';
  const threadId = String(threadIdRaw || '').trim() || 'default';

  if (!message.trim()) return res.status(400).json({ error: 'Message required' });

  writeLock = writeLock.then(async () => {
    try {
      const store = await readStore();

      if (threadId === 'operator_bio') {
        const settings = await readSettings();
        const existing = settings.operatorBioChat && typeof settings.operatorBioChat === 'object' ? settings.operatorBioChat : {};
        const history = Array.isArray(existing.messages) ? existing.messages : [];

        const response = await aiAgentAction(message, store, null, { threadId, threadHistory: history });
        const reply = String(response.content || '').trim();

        const ts = nowIso();
        const nextHistory = [...history, { role: 'user', content: message, timestamp: ts }, { role: 'ai', content: reply, timestamp: ts }].slice(-120);
        await writeSettings({
          ...settings,
          operatorBioChat: { messages: nextHistory, updatedAt: ts },
          updatedAt: ts,
        });

        res.json({ reply });
        return;
      }

      const resolved = resolveProjectForMessage(store, message, projectId);
      if (resolved && typeof resolved === 'object' && resolved.ambiguous) {
        const opts = Array.isArray(resolved.options) ? resolved.options : [];
        const list = opts.map((p) => `- ${p.name}`).join('\n');
        res.json({ reply: `Which project did you mean?\n${list}` });
        return;
      }

      const effectiveProjectId = resolved && typeof resolved === 'object' ? resolved.id : projectId;

      const deterministic = tryHandleDeterministicTaskRequest(store, message, effectiveProjectId);
      const response = deterministic?.handled ? { content: deterministic.reply } : await aiAgentAction(message, store, effectiveProjectId, { threadId: 'default' });
      const reply = String(response.content || '').trim();

      if (effectiveProjectId) {
        store.projectChats = store.projectChats || {};
        const existing = store.projectChats[effectiveProjectId];
        let chatHistory = Array.isArray(existing)
          ? existing
          : (existing && typeof existing === 'object' && Array.isArray(existing.messages))
              ? existing.messages
              : [];
        const ts = nowIso();
        chatHistory.push({ role: 'user', content: message, timestamp: ts });
        chatHistory.push({ role: 'ai', content: reply, timestamp: ts });
        store.projectChats[effectiveProjectId] = { messages: chatHistory, updatedAt: ts };
      }

      store.revision++;
      store.updatedAt = nowIso();
      await writeStore(store);

      res.json({ reply });
    } catch (err) {
      console.error('Error in /api/chat:', err);
      res.status(500).json({ error: 'Internal Server Error during chat processing.', details: err?.message || '' });
    }
  });

  await writeLock;
});

app.get('/api/chat/thread/:id', async (req, res) => {
  const id = String(req.params.id || '').trim();
  if (id !== 'operator_bio') {
    res.status(404).json({ ok: false, error: 'Unknown thread' });
    return;
  }

  const settings = await readSettings();
  const existing = settings.operatorBioChat && typeof settings.operatorBioChat === 'object' ? settings.operatorBioChat : {};
  const history = Array.isArray(existing.messages) ? existing.messages : [];
  const operatorBio = typeof settings.operatorBio === 'string' ? settings.operatorBio : '';
  res.json({ ok: true, threadId: id, operatorBio, history });
});


function parseHHMM(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  const m = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return null;
  const hh = Math.max(0, Math.min(23, Number(m[1])));
  const mm = Math.max(0, Math.min(59, Number(m[2])));
  return (hh * 60) + mm;
}

function localMinutesNow() {
  const d = new Date();
  return (d.getHours() * 60) + d.getMinutes();
}

function briefKindLabel(kind) {
  const k = String(kind || '').trim().toLowerCase();
  if (k === 'morning') return 'MORNING';
  if (k === 'midday') return 'MIDDAY';
  if (k === 'eod') return 'EOD';
  return k.toUpperCase() || 'BRIEF';
}

function buildDeterministicBrief({ kind, store, businessName, settings }) {
  const today = new Date().toISOString().slice(0, 10);
  const tasks = Array.isArray(store?.tasks) ? store.tasks : [];
  const inbox = getVisibleInboxItemsFromSettings(store?.inboxItems, settings);
  const projects = Array.isArray(store?.projects) ? store.projects : [];
  const isDoneStatus = (st) => {
    const v = String(st == null ? '' : st).trim().toLowerCase();
    return ['done', 'archived', 'complete', 'completed'].includes(v);
  };
  const normalizeDue = (d) => {
    const v = String(d == null ? '' : d).trim();
    return /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : '';
  };

  const openTasks = tasks.filter((t) => !isDoneStatus(t?.status));
  const overdue = openTasks.filter((t) => {
    const due = normalizeDue(t?.dueDate);
    return Boolean(due) && due < today;
  });
  const dueToday = openTasks.filter((t) => normalizeDue(t?.dueDate) === today);
  const inboxNew = inbox.filter((it) => {
    const src = String(it?.source || '').trim().toLowerCase();
    return String(it?.status || '').trim().toLowerCase() === 'new' && src !== 'marcus' && src !== 'marcus';
  });

  const nextTasks = openTasks
    .slice()
    .sort((a, b) => {
      const apRaw = Number(a?.priority);
      const bpRaw = Number(b?.priority);
      const ap = Number.isFinite(apRaw) ? apRaw : 2;
      const bp = Number.isFinite(bpRaw) ? bpRaw : 2;
      if (ap !== bp) return ap - bp;
      const ad0 = normalizeDue(a?.dueDate);
      const bd0 = normalizeDue(b?.dueDate);
      const ad = ad0 ? ad0 : '9999-12-31';
      const bd = bd0 ? bd0 : '9999-12-31';
      if (ad !== bd) return ad.localeCompare(bd);
      return String(b?.updatedAt || '').localeCompare(String(a?.updatedAt || ''));
    })
    .slice(0, 8);

  const lines = [];
  lines.push(`M.A.R.C.U.S. Brief — ${briefKindLabel(kind)} — ${today}${businessName ? ` — ${businessName}` : ''}`);
  lines.push('');
  lines.push(`Situation: ${projects.length} projects • ${openTasks.length} open tasks • ${overdue.length} overdue • ${dueToday.length} due today • ${inboxNew.length} new inbox`);
  lines.push('');
  lines.push('Next actions:');
  if (overdue.length) {
    lines.push(`- Clear 1 overdue item first (overdue: ${overdue.length})`);
  }
  if (inboxNew.length) {
    lines.push(`- Triage inbox (new: ${inboxNew.length})`);
  }
  nextTasks.forEach((t) => {
    const priRaw = Number(t?.priority);
    const priNum = Number.isFinite(priRaw) ? priRaw : 2;
    const due = normalizeDue(t?.dueDate);
    const proj = String(t?.project || '').trim();
    lines.push(`- [P${priNum}] ${String(t?.title || '').trim()}${proj ? ` — ${proj}` : ''}${due ? ` — due ${due}` : ''}`);
  });

  if (inboxNew.length) {
    lines.push('Inbox (newest):');
    inboxNew
      .slice()
      .sort((a, b) => String(b?.updatedAt || b?.createdAt || '').localeCompare(String(a?.updatedAt || a?.createdAt || '')))
      .slice(0, 3)
      .forEach((it) => {
        const src = String(it?.source || '').trim() || 'inbox';
        const txt = String(it?.text || '').replace(/\s+/g, ' ').trim();
        const head = txt.length > 140 ? `${txt.slice(0, 140)}…` : txt;
        lines.push(`- [${src}] ${head}`);
      });
  }

  return lines.join('\n');
}

async function sendMarcusBriefsForAllBusinesses(kind, settings) {
  const cfg = getBusinessConfigFromSettings(settings);
  const bizList = Array.isArray(cfg.businesses) ? cfg.businesses : [{ key: DEFAULT_BUSINESS_KEY, name: 'Personal' }];
  const today = new Date().toISOString().slice(0, 10);

  for (const b of bizList) {
    const bKey = normalizeBusinessKey(b?.key || '') || DEFAULT_BUSINESS_KEY;
    const bName = String(b?.name || '').trim() || getBusinessNameForKey(bKey);
    const store = await withBusinessKey(bKey, async () => readStore());
    const text = buildDeterministicBrief({ kind, store, businessName: bName, settings });
    await addInboxIntegrationItem({
      source: 'marcus',
      externalId: `brief:${String(kind || 'brief').toLowerCase()}:${today}`,
      text,
      businessKey: bKey,
      businessLabel: bName,
    });
  }
}

function getBriefScheduleFromSettings(settings) {
  const raw = settings && typeof settings === 'object' ? settings.marcusBriefSchedule : null;
  const times = (raw && typeof raw === 'object' && raw.times && typeof raw.times === 'object') ? raw.times : {};
  const lastSent = (raw && typeof raw === 'object' && raw.lastSent && typeof raw.lastSent === 'object') ? raw.lastSent : {};
  return {
    times: {
      morning: typeof times.morning === 'string' ? times.morning : '09:00',
      midday: typeof times.midday === 'string' ? times.midday : '13:00',
      eod: typeof times.eod === 'string' ? times.eod : '17:00',
    },
    lastSent: {
      morning: typeof lastSent.morning === 'string' ? lastSent.morning : '',
      midday: typeof lastSent.midday === 'string' ? lastSent.midday : '',
      eod: typeof lastSent.eod === 'string' ? lastSent.eod : '',
    },
  };
}

async function markBriefSent(kind, today) {
  const settings = await readSettings();
  const sched = getBriefScheduleFromSettings(settings);
  const next = {
    ...settings,
    marcusBriefSchedule: {
      times: { ...sched.times },
      lastSent: { ...sched.lastSent, [String(kind)]: today },
    },
    updatedAt: nowIso(),
  };
  await writeSettings(next);
}

function startMarcusBriefScheduler() {
  let running = false;
  const tick = async () => {
    if (running) return;
    running = true;
    try {
      const settings = await readSettings();
      const sched = getBriefScheduleFromSettings(settings);
      const today = new Date().toISOString().slice(0, 10);
      const nowMin = localMinutesNow();

      const kinds = [
        { kind: 'morning', at: parseHHMM(sched.times.morning) },
        { kind: 'midday', at: parseHHMM(sched.times.midday) },
        { kind: 'eod', at: parseHHMM(sched.times.eod) },
      ];

      // Send only the most recent due brief (prevents catch-up spam).
      let candidate = null;
      for (const k of kinds) {
        if (k.at == null) continue;
        const last = String(sched.lastSent[k.kind] || '').trim();
        if (last === today) continue;
        if (nowMin < k.at) continue;
        if (!candidate || k.at > candidate.at) candidate = k;
      }

      if (candidate) {
        await sendMarcusBriefsForAllBusinesses(candidate.kind, settings);
        await markBriefSent(candidate.kind, today);
      }
    } catch (e) {
      console.error('Brief scheduler tick failed:', e);
    } finally {
      running = false;
    }
  };

  // Start with a slight delay so startup migrations finish.
  setTimeout(() => { void tick(); }, 15_000);
  setInterval(() => { void tick(); }, 30_000);
}

app.listen(PORT, async () => {
  await refreshBusinessCacheFromSettings();
  const businesses = Array.isArray(cachedBusinesses) ? cachedBusinesses : [{ key: DEFAULT_BUSINESS_KEY }];
  for (const biz of businesses) {
    const bKey = normalizeBusinessKey(biz?.key || '') || DEFAULT_BUSINESS_KEY;
    await withBusinessKey(bKey, async () => {
      await ensureStoreExists();
      const store = await readStore();
      const collapsed = collapseLegacyAirtableRevisionRequestProjects(store, bKey);
      if (collapsed.changed) {
        await writeStore(collapsed.store);
      }
      lastRevisionCollapseByKey.set(bKey, {
        at: nowIso(),
        changed: Boolean(collapsed.changed),
        archived: Number(collapsed.archived || 0),
        tasksReassigned: Number(collapsed.tasksReassigned || 0),
      });
    });
  }
  await backupCriticalFiles({ force: true }).catch(() => {
    // ignore startup backup errors
  });
  startBackupScheduler();
  startGa4Scheduler();
  startAirtableRequestsAutoSyncScheduler();
  startMarcusBriefScheduler();
  // eslint-disable-next-line no-console
  console.log(`M.A.R.C.U.S. running on http://localhost:${PORT}`);
});






