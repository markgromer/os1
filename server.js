import crypto from 'node:crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import fs from 'node:fs/promises';
import { exec } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';

import express from 'express';
import { google } from 'googleapis';

import { mcpCallTool, mcpListTools } from './mcpClient.js';

const app = express();
// When running behind SiteGround / reverse proxies, trust forwarded headers.
app.set('trust proxy', true);
const PORT = process.env.PORT ? Number(process.env.PORT) : 3030;

const DEFAULT_BUSINESS_KEY = 'personal';
const requestContext = new AsyncLocalStorage();

let cachedActiveBusinessKey = DEFAULT_BUSINESS_KEY;
let cachedBusinesses = [{ key: DEFAULT_BUSINESS_KEY, name: 'Personal', phoneNumbers: [] }];

const lastRevisionCollapseByKey = new Map();

const DEBUG_WEBHOOKS = String(process.env.DEBUG_WEBHOOKS || '').trim().toLowerCase() === 'true';

// Capture the raw request bytes so we can verify webhook signatures (Slack/Twilio/etc).
app.use(express.json({
  limit: '256kb',
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

const APP_NAME = 'Task Tracker';

function getDefaultSettingsDir() {
  const home = os.homedir();
  if (process.platform === 'win32') {
    const appData = typeof process.env.APPDATA === 'string' ? process.env.APPDATA.trim() : '';
    return path.join(appData || path.join(home, 'AppData', 'Roaming'), APP_NAME);
  }
  if (process.platform === 'darwin') {
    return path.join(home, 'Library', 'Application Support', APP_NAME);
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
  const queryToken = typeof req.query?.token === 'string' ? req.query.token.trim() : '';
  return queryToken;
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
    res.status(401).json({ error: 'Unauthorized' });
  } catch {
    res.status(401).json({ error: 'Unauthorized' });
  }
});

app.get('/api/auth/status', (req, res) => {
  if (!ADMIN_TOKEN) {
    res.json({ ok: true, authRequired: false, authenticated: true });
    return;
  }
  const token = extractBearerToken(req);
  const authenticated = Boolean(token && safeTimingEqual(token, ADMIN_TOKEN));
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
  team: [],
  projectNotes: {},
  projectScratchpads: {},
  projectNoteEntries: {},
  projectChats: {},
  projectCommunications: {},
  inboxItems: [],
  projectTranscriptUndo: {},
};

let writeLock = Promise.resolve();

async function readSettings() {
  try {
    await fs.mkdir(SETTINGS_DIR, { recursive: true });
    const raw = await fs.readFile(SETTINGS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return {};
    return parsed;
  } catch {
    return {};
  }
}

async function writeSettings(next) {
  await fs.mkdir(SETTINGS_DIR, { recursive: true });
  const tmpFile = `${SETTINGS_FILE}.tmp-${crypto.randomBytes(6).toString('hex')}`;
  await fs.writeFile(tmpFile, JSON.stringify(next, null, 2) + '\n', 'utf8');
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
  return clone;
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

function previewTextServer(text, maxLen = 140) {
  const s = String(text || '').replace(/\s+/g, ' ').trim();
  if (!s) return '';
  if (s.length <= maxLen) return s;
  return `${s.slice(0, Math.max(0, maxLen - 1)).trimEnd()}…`;
}

function summarizeRadarGroupText(texts) {
  const list = Array.isArray(texts) ? texts.map((t) => String(t || '')).filter(Boolean) : [];
  if (!list.length) return '';

  const stop = new Set([
    'this', 'that', 'with', 'from', 'your', 'youre', 'have', 'will', 'just', 'like', 'thanks', 'thank', 'hello',
    'sent', 'text', 'message', 'sms', 'call', 'email', 'slack', 'team', 'please', 'need', 'needed', 'needed',
    'client', 'project', 'title', 'link', 'airtable', 'http', 'https', 'www',
  ]);

  const counts = new Map();
  for (const raw of list.slice(0, 8)) {
    const s = raw
      .replace(/https?:\/\/\S+/gi, ' ')
      .replace(/\+?\d[\d\s().-]{7,}\d/g, ' ')
      .replace(/[^a-zA-Z0-9\s]/g, ' ')
      .toLowerCase();
    for (const w of s.split(/\s+/g)) {
      if (!w || w.length < 4) continue;
      if (stop.has(w)) continue;
      counts.set(w, (counts.get(w) || 0) + 1);
    }
  }

  const top = Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 3)
    .map(([w]) => w);

  if (top.length) return top.join(' · ');

  const first = list.find((t) => String(t || '').trim()) || '';
  return previewTextServer(first, 80);
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

async function addInboxIntegrationItem({ source, externalId, text, projectId = '', projectName = '', businessKey = '', businessLabel = '', toNumber = '', fromNumber = '', channel = '' }) {
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
    if (cleanExternalId && list.some((x) => String(x?.id || '') === id)) {
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
      summary: 'Task Tracker',
      description: 'Project due dates synced from Task Tracker',
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
  return name ? `[TT] ${name}` : '[TT] Project';
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
      description: 'Synced from Task Tracker (project due date)',
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
  await fs.mkdir(path.dirname(file), { recursive: true });
  try {
    await fs.access(file);
  } catch {
    await fs.writeFile(file, JSON.stringify(EMPTY_STORE, null, 2) + '\n', 'utf8');
  }
}

async function readStore() {
  await ensureStoreExists();
  const file = getStoreFileForBusiness(getBusinessKeyFromContext());
  const raw = await fs.readFile(file, 'utf8');
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== 'object') return structuredClone(EMPTY_STORE);

  const revision = Number(parsed.revision);
  const updatedAt = typeof parsed.updatedAt === 'string' ? parsed.updatedAt : new Date(0).toISOString();
  const projects = Array.isArray(parsed.projects) ? parsed.projects : [];
  const clients = Array.isArray(parsed.clients) ? parsed.clients : [];
  const tasks = (Array.isArray(parsed.tasks) ? parsed.tasks : []).map(sanitizeTaskRecord);
  const team = Array.isArray(parsed.team) ? parsed.team : [];
  const projectNotes = parsed.projectNotes && typeof parsed.projectNotes === 'object' ? parsed.projectNotes : {};
  const projectScratchpads = parsed.projectScratchpads && typeof parsed.projectScratchpads === 'object' ? parsed.projectScratchpads : {};
  const projectNoteEntries = parsed.projectNoteEntries && typeof parsed.projectNoteEntries === 'object' ? parsed.projectNoteEntries : {};
  const projectChats = parsed.projectChats && typeof parsed.projectChats === 'object' ? parsed.projectChats : {};
  const projectCommunications = parsed.projectCommunications && typeof parsed.projectCommunications === 'object' ? parsed.projectCommunications : {};
  const inboxItems = Array.isArray(parsed.inboxItems) ? parsed.inboxItems : [];
  const projectTranscriptUndo = parsed.projectTranscriptUndo && typeof parsed.projectTranscriptUndo === 'object' ? parsed.projectTranscriptUndo : {};

  return {
    revision: Number.isFinite(revision) && revision > 0 ? revision : 1,
    updatedAt,
    projects,
    clients,
    tasks,
    team,
    projectNotes,
    projectScratchpads,
    projectNoteEntries,
    projectChats,
    projectCommunications,
    inboxItems,
    projectTranscriptUndo,
  };
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
    const channel = typeof i.channel === 'string' ? i.channel.trim().slice(0, 32) : '';

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
  await ensureStoreExists();

  const file = getStoreFileForBusiness(getBusinessKeyFromContext());
  const tmpFile = `${file}.tmp-${crypto.randomBytes(6).toString('hex')}`;
  await fs.writeFile(tmpFile, JSON.stringify(nextStore, null, 2) + '\n', 'utf8');
  await fs.rename(tmpFile, file);
  backupCriticalFiles().catch(() => {
    // backup is best-effort
  });
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

  const scored = [];
  for (const p of projects) {
    const name = String(p?.name || '').trim();
    if (!name) continue;
    const nameKey = normKey(name);
    if (!nameKey) continue;
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
      .filter((p) => String(p?.status || '') !== 'Done')
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
  const { apiKey, model } = await getAiConfig();
  if (!apiKey) {
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
    out.push('If you want real AI suggestions, set OPENAI_API_KEY and restart the server.');
    return out.join('\n');
  }

  // model resolved above

  const safeNotes = String(notes || '').slice(0, 8000);
  const safeTasks = (Array.isArray(tasks) ? tasks : []).slice(0, 60).map((t) => ({
    title: t.title,
    status: t.status,
    priority: t.priority,
    dueDate: t.dueDate,
    owner: t.owner,
    type: t.type,
  }));

  const payload = {
    model,

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
  };

  const resp = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`AI request failed (${resp.status}). ${text}`.slice(0, 400));
  }

  const json = await resp.json();
  const content = json?.choices?.[0]?.message?.content;
  if (typeof content !== 'string' || !content.trim()) {
    throw new Error('AI returned no content');
  }
  return content.trim();
}

async function aiProjectAssistant({ project, scratchpad, noteEntries, communications, chatMessages }) {
  const { apiKey, model } = await getAiConfig();

  const projectName = project?.name || '';
  const projectType = project?.type || '';
  const projectDue = project?.dueDate || '';
  const projectStatus = project?.status || 'Active';
  const accountManagerName = project?.accountManagerName || '';

  const recentNotes = Array.isArray(noteEntries) ? noteEntries.slice(0, 6) : [];
  const recentComms = Array.isArray(communications) ? communications.slice(0, 8) : [];
  const recentChat = Array.isArray(chatMessages) ? chatMessages.slice(-16) : [];

  if (!apiKey) {
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
    lines.push('To enable real AI, set OPENAI_API_KEY and restart the server.');
    return { content: lines.join('\n'), tasks: [] };
  }

  // model resolved above

  const context = {
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

  const payload = {
    model,

    messages,
    tools,
    tool_choice: 'auto',
  };

  const resp = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`AI request failed (${resp.status}). ${text}`.slice(0, 400));
  }

  const json = await resp.json();
  const choice = json?.choices?.[0];
  const msg = choice?.message;
  
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
  const { apiKey, model } = await getAiConfig();

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

  if (!apiKey) {
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

  const payload = {
    model,

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
  };

  const resp = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    return { ok: false, error: `AI request failed (${resp.status}). ${text}`.slice(0, 400) };
  }

  const json = await resp.json().catch(() => ({}));
  const content = json?.choices?.[0]?.message?.content;
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
      meta: { source: 'openai' },
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

  const mcpEff = getMcpEffectiveSettings(settings);
  const mcpEnabled = Boolean(mcpEff.enabled);
  const mcpConfigured = Boolean(mcpEff.configured);

  res.json({
    ...safe,
    aiEnabled: Boolean(apiKey),
    openaiModel: model,
    openaiKeyHint: keyHint,
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
    mcpEnabled,
    mcpConfigured,
  });
});

app.put('/api/settings', async (req, res) => {
  const body = req.body || {};
  
  writeLock = writeLock.then(async () => {
    const saved = await readSettings();
    const next = { ...saved, ...body, updatedAt: nowIso() };
    await writeSettings(next);
    // Never echo settings back (could include secrets).
    res.json({ ok: true });
  });
  
  await writeLock;
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
      <h1>Google Calendar connected.</h1>
      <p>You can close this tab and return to Task Tracker.</p>
    </body></html>`);
  } catch (err) {
    res.status(500).send(`OAuth failed: ${err?.message || 'unknown error'}`);
  }
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

// Integrations: Fireflies ingestion (meeting summaries into inbox; optional project note linkage)
// Expected payload: { projectId?, projectName?, date?: 'YYYY-MM-DD', title?: string, summary: string, transcriptUrl?: string, meetingId?: string }
app.post('/api/integrations/fireflies/ingest', async (req, res) => {
  const secret = typeof req.headers['x-fireflies-secret'] === 'string' ? req.headers['x-fireflies-secret'].trim() : '';
  const saved = await readSettings();
  const expected =
    (typeof process.env.FIREFLIES_SECRET === 'string' ? process.env.FIREFLIES_SECRET.trim() : '') ||
    (typeof process.env.FIREFLIES_WEBHOOK_SECRET === 'string' ? process.env.FIREFLIES_WEBHOOK_SECRET.trim() : '') ||
    (typeof saved.firefliesSecret === 'string' ? saved.firefliesSecret.trim() : '');
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
      <p>You can close this tab and return to Task Tracker.</p>
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

    const routing = resolveBusinessForInbound({ settings, toNumber: to });

    const { matched, finalProjectName, fromLabel } = await withBusinessKey(routing.businessKey, async () => {
      const businessStore = await readStore();
      const match = matchProjectFromText(businessStore, body);
      let projName = match?.name || '';
      let label = from || '';

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

    const lines = [];
    lines.push(`📱 SMS • ${routing.businessLabel}`);
    lines.push(`From: ${fromLabel}`);
    lines.push(`To: ${to}`);
    lines.push(``);
    lines.push(body);

    await addInboxIntegrationItem({
      source: 'sms',
      externalId: `sms:${sid || crypto.createHash('sha1').update(`${from}|${to}|${body}`).digest('hex')}`,
      text: lines.join('\n'),
      projectId: matched?.id || '',
      projectName: finalProjectName,
      businessKey: routing.businessKey,
      businessLabel: routing.businessLabel,
      toNumber: to,
      fromNumber: from,
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
      openaiModel: openaiModel || 'gpt-4.1-mini',
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
    const store = await readStore();
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
  res.json(outStore || structuredClone(EMPTY_STORE));
});

// Inbox (global capture)
app.get('/api/inbox', async (req, res) => {
  const store = await readStore();
  const items = Array.isArray(store.inboxItems) ? store.inboxItems : [];
  res.json({ revision: store.revision, updatedAt: store.updatedAt, items });
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
        const existingBiz = businessGroupsByKey.get(bKey);
        if (!existingBiz) {
          businessGroupsByKey.set(bKey, {
            businessKey: bKey,
            businessLabel: bLabel,
            count: 1,
            latestAt: t,
            latestMs: ms,
            sample: preview ? [preview] : [],
            summary: '',
          });
        } else {
          existingBiz.count += 1;
          if (ms > existingBiz.latestMs) {
            existingBiz.latestMs = ms;
            existingBiz.latestAt = t;
          }
          if (preview && existingBiz.sample.length < 3) existingBiz.sample.push(preview);
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

    const businessGroups = Array.from(businessGroupsByKey.values()).map((g) => ({
      ...g,
      summary: summarizeRadarGroupText(g.sample),
    }));
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
    const matchSender = current.sender || current.fromNumber || '';
    const matchKeys = senderLookupKeys(matchSender);
    
    const nextList = list.map((item, i) => {
      if (i === idx) {
        return normalizeInboxItem({
          ...item,
          projectId: String(project.id || ''),
          projectName: String(project.name || ''),
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
    };

    await writeStore(nextStore);
    const updated = nextList[idx] || null;
    res.json({ ok: true, store: nextStore, item: updated, project });
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

    const nextItem = normalizeInboxItem({
      ...item,
      projectId: createdProject.id,
      projectName: createdProject.name,
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
    });

    const nextStore = {
      ...store,
      revision: store.revision + 1,
      updatedAt: ts,
      inboxItems: nextList,
      tasks: nextTasks,
      projectNoteEntries: nextProjectNoteEntries,
      projectCommunications: nextProjectComms,
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
  const { apiKey, model } = await getAiConfig();
  if (!apiKey) {
    res.json({ error: 'AI not configured' });
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
      const resp = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: context }
          ],

        }),
      });
      
      if (!resp.ok) {
        throw new Error(`AI error: ${resp.status}`);
      }
      
      const json = await resp.json();
      const content = json.choices?.[0]?.message?.content || '{}';
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

    const { apiKey, model } = await getAiConfig();
    if (!apiKey) {
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
      const { resp, data } = await fetchJsonWithTimeout('https://api.openai.com/v1/chat/completions', {
        timeoutMs: 20_000,
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model,
          messages: [
            { role: 'system', content: system },
            { role: 'user', content: JSON.stringify(user).slice(0, 24000) },
          ],
        }),
      });

      if (!resp.ok) {
        res.json(heuristic());
        return;
      }

      const content = String(data?.choices?.[0]?.message?.content || '').trim();
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
async function aiAgentAction(message, store, projectId = null) {
  const { apiKey, model } = await getAiConfig();

    const settings = await readSettings();
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

    let context = '';
    const baseSystemPrompt =
      "You are an intelligent project assistant for OS.1 (Operator System 1). Stay concise and action-oriented. You DO have access to the user's OS.1 project data provided in context. Never claim you can't access tasks/notes; if something isn't present in context, ask for it.";
    let systemPrompt = userSystemPrompt ? `${userSystemPrompt}\n\n---\n${baseSystemPrompt}` : baseSystemPrompt;

    if (userMemory) {
      context += `GLOBAL MEMORY (user-provided; treat as true unless contradicted):\n${String(userMemory).slice(0, 12000)}\n\n`;
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
      const projectsOverview = (store.projects || []).map((p) => ({ id: p.id, name: p.name, type: p.type, status: p.status, dueDate: p.dueDate }));
      context += `ALL PROJECTS (JSON): ${JSON.stringify(projectsOverview).slice(0, 24000)}\n\n`;
    }

    // If OpenAI isn't configured, still answer from local data.
    if (!apiKey) {
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
        lines.push('AI is not enabled (OPENAI_API_KEY not set), but I can still show you everything in the tracker.');
        lines.push('If you want deeper reasoning/rewrites, set the key in Settings ? OpenAI.');
        return { content: lines.join('\n') };
      }

      return {
        content:
          "AI is not enabled (OPENAI_API_KEY not set). Tell me a project name and I can summarize its tasks/notes/scratchpad from the tracker, or enable OpenAI in Settings ? OpenAI for full conversational reasoning.",
      };
    }

    const tools = [
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
      }
    ];

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

      if (apiKey) {
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
            "\n\nIMPORTANT: When the user asks to create or update a project (due date, links, invoice, repo, docs, value, status, account manager), you MUST use tool calls (create_project / update_project / create_tasks). If you need external data, use MCP tools when available.",
        },
      ];

      // Include a small amount of history for continuity (before the current message).
      if (effectiveProjectId && store.projectChats && store.projectChats[effectiveProjectId]) {
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

    const callOpenAi = async () => {
      // Some newer models reject non-default sampling params; omit temperature entirely.
      const body = {
        model,
        messages,
        tools,
        tool_choice: 'auto',
      };

      const { resp, data } = await fetchJsonWithTimeout('https://api.openai.com/v1/chat/completions', {
        timeoutMs: 30_000,
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });

      if (!resp.ok) {
        const detail = typeof data?.error?.message === 'string' ? data.error.message : JSON.stringify(data);
        throw new Error(`AI request failed (${resp.status}). model=${model}. ${detail}`.slice(0, 600));
      }
      const msg = data?.choices?.[0]?.message;
      if (!msg) throw new Error('AI returned no message');
      return msg;
    };

    const execTool = async (toolName, args) => {
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
        if (!apiKey) return { ok: false, error: 'No API key' };
        try {
          const body = { model: 'dall-e-3', prompt: args.prompt, n: 1, size: '1024x1024' };
          const { resp, data } = await fetchJsonWithTimeout('https://api.openai.com/v1/images/generations', {
            method: 'POST',
            headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
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
      return { ok: false, error: `Unknown tool: ${toolName}` };
    };

    try {
      for (let step = 0; step < 4; step++) {
        const msg = await callOpenAi();

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

  if (!message.trim()) return res.status(400).json({ error: 'Message required' });

  writeLock = writeLock.then(async () => {
    try {
      const store = await readStore();

      const resolved = resolveProjectForMessage(store, message, projectId);
      if (resolved && typeof resolved === 'object' && resolved.ambiguous) {
        const opts = Array.isArray(resolved.options) ? resolved.options : [];
        const list = opts.map((p) => `- ${p.name}`).join('\n');
        res.json({ reply: `Which project did you mean?\n${list}` });
        return;
      }

      const effectiveProjectId = resolved && typeof resolved === 'object' ? resolved.id : projectId;

      const deterministic = tryHandleDeterministicTaskRequest(store, message, effectiveProjectId);
      const response = deterministic?.handled ? { content: deterministic.reply } : await aiAgentAction(message, store, effectiveProjectId);
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
  // eslint-disable-next-line no-console
  console.log(`Task Tracker running on http://localhost:${PORT}`);
});






