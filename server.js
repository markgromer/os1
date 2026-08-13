import crypto from 'node:crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import fs from 'node:fs/promises';
import fsSync from 'node:fs';
import { exec, execFile } from 'node:child_process';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import tls from 'node:tls';

import express from 'express';
import compression from 'compression';
import { google } from 'googleapis';
import { ImapFlow } from 'imapflow';
import { simpleParser } from 'mailparser';
import nodemailer from 'nodemailer';

import { mcpCallTool, mcpListTools } from './mcpClient.js';
import { registerOperationsRoutes } from './marcus/api/operations_routes.js';
import { registerMissionMemoryRoutes } from './marcus/api/mission_memory_routes.js';
import { registerProjectEvidenceRoutes } from './marcus/api/project_evidence_routes.js';
import { scopeAuthorizedPublishActions } from './marcus/approvals/publish_safeguard.js';
import { buildMarcusSystemPrompt } from './marcus/core/build_system_prompt.js';
import { explicitlyDefersCodexStart, explicitlyDefersProjectAudit, withoutProjectExecutionDeferrals } from './marcus/core/request_intent.js';
import { ProjectEvidenceService } from './marcus/evidence/project_evidence_service.js';
import {
  executeMarcusProjectActivityTool,
  getMarcusProjectActivityToolDefinitions,
  isMarcusProjectActivityTool,
} from './marcus/evidence/marcus_project_activity_tools.js';
import { buildActiveBrief as buildOperationalActiveBrief } from './marcus/intelligence/active_brief.js';
import { formatMissionMemoryForPrompt, MissionMemoryStore } from './marcus/memory/mission_memory_store.js';
import { createOperationsEngine } from './marcus/operations/operation_engine.js';
import { discoverDurableBackupSources } from './marcus/operations/operation_backups.js';
import { DesktopActionQueue } from './marcus/operations/desktop_action_queue.js';
import { startOperationMonitor } from './marcus/operations/operation_monitor.js';
import { extractExplicitGitHubRepositories, ProjectOperatorService } from './marcus/operators/project_operator_service.js';
import { DesktopCodexAdapter } from './marcus/providers/desktop_codex_adapter.js';
import { createGitHubActionsCodexAdapterFromEnv } from './marcus/providers/github_actions_codex_adapter.js';
import { createHttpCodexAdapterFromEnv } from './marcus/providers/http_codex_adapter.js';
import { RoutedCodexAdapter } from './marcus/providers/routed_codex_adapter.js';
import {
  buildMarcusRealtimeClientSecretRequest,
  DEFAULT_MARCUS_REALTIME_MODEL,
  DEFAULT_MARCUS_REALTIME_VOICE,
} from './marcus/voice/realtime_session.js';
import { RealtimeTelemetryStore } from './marcus/voice/realtime_telemetry.js';
import {
  executeMarcusOperationTool,
  formatOperationStatusForMarcus,
  getMarcusOperationToolDefinitions,
  isMarcusOperationTool,
  shouldCreateDurableOperationForRequest,
} from './marcus/operations/marcus_operation_tools.js';

const app = express();
app.use(compression());
// When running behind SiteGround / reverse proxies, trust forwarded headers.
const PORT = process.env.PORT ? Number(process.env.PORT) : 3030;
const IS_HOSTED_RUNTIME = String(process.env.NODE_ENV || '').toLowerCase() === 'production'
  || Boolean(process.env.RENDER || process.env.RENDER_SERVICE_ID || process.env.RENDER_EXTERNAL_URL);
const ALLOW_UNAUTHENTICATED_LOCAL = String(process.env.MARCUS_ALLOW_UNAUTHENTICATED_LOCAL || '').trim().toLowerCase() === 'true';
const SERVER_HOST = String(process.env.MARCUS_HOST || '').trim() || (IS_HOSTED_RUNTIME ? '0.0.0.0' : '127.0.0.1');
app.set('trust proxy', IS_HOSTED_RUNTIME ? 1 : false);

const DEFAULT_BUSINESS_KEY = 'personal';
const requestContext = new AsyncLocalStorage();

let cachedActiveBusinessKey = DEFAULT_BUSINESS_KEY;
let cachedBusinesses = [{ key: DEFAULT_BUSINESS_KEY, name: 'Personal', phoneNumbers: [] }];

const lastRevisionCollapseByKey = new Map();

const TRANSIENT_RENAME_CODES = new Set(['EACCES', 'EBUSY', 'EPERM']);

async function replaceFileAtomically(tmpFile, destinationFile) {
  const maxAttempts = process.platform === 'win32' ? 6 : 1;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await fs.rename(tmpFile, destinationFile);
      return;
    } catch (error) {
      if (attempt === maxAttempts || !TRANSIENT_RENAME_CODES.has(error?.code)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 20 * attempt));
    }
  }
}

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
  limit: '512kb',
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
  return normalizeBusinessKey(headerKey);
}

// Attach a per-request business context.
// - If client sends X-Business-Key, we honor it.
// - Otherwise we fall back to the server's saved active business key.
app.use((req, res, next) => {
  const incoming = getBusinessKeyFromRequest(req);
  const allowed = new Set((Array.isArray(cachedBusinesses) ? cachedBusinesses : []).map((item) => normalizeBusinessKey(item?.key || '')).filter(Boolean));
  if (incoming && !allowed.has(incoming)) {
    res.status(403).json({ ok: false, error: 'Business is not available to the authenticated operator.' });
    return;
  }
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
const MARCUS_OPERATIONAL_CONTROLS_FILE = path.join(DATA_DIR, 'marcus-operational-controls.json');
const MARCUS_SESSION_STATE_FILE = path.join(DATA_DIR, 'marcus-session-state.json');
const realtimeTelemetryStore = new RealtimeTelemetryStore({ dataDir: DATA_DIR });
const missionMemoryStore = new MissionMemoryStore({ dataDir: DATA_DIR });

const BUSINESS_DATA_DIR = path.join(DATA_DIR, 'businesses');
const desktopActionQueue = new DesktopActionQueue({
  dataDir: DATA_DIR,
  leaseMs: process.env.MARCUS_DESKTOP_ACTION_LEASE_MS,
});
const desktopCodexFlag = String(process.env.MARCUS_DESKTOP_CODEX_ENABLED || '').trim().toLowerCase();
const desktopCodexEnabled = desktopCodexFlag === 'true' || (IS_HOSTED_RUNTIME && desktopCodexFlag !== 'false');
const desktopCodexAdapter = desktopCodexEnabled ? new DesktopCodexAdapter({
  dataDir: DATA_DIR,
  queueAction: async (action) => queueDesktopAction(action),
  monitorBaseUrl: process.env.RENDER_EXTERNAL_URL || process.env.MARCUS_PUBLIC_URL || '',
}) : null;
const fallbackCodexAdapter = createHttpCodexAdapterFromEnv(process.env)
  || createGitHubActionsCodexAdapterFromEnv(process.env);
const directCodexAdapter = desktopCodexAdapter || fallbackCodexAdapter
  ? new RoutedCodexAdapter({ desktopAdapter: desktopCodexAdapter, fallbackAdapter: fallbackCodexAdapter })
  : null;

const operationsEngine = createOperationsEngine({
  dataDir: DATA_DIR,
  getLegacyProjects: async (businessKey) => {
    const store = await readStoreForBusiness(businessKey);
    return Array.isArray(store?.projects) ? store.projects : [];
  },
  getDesktopContext: async () => desktopRelayCache?.data || desktopContextCache?.data || {},
  queueDesktopAction: async (action) => queueDesktopAction(action),
  githubReadAdapter: async (input) => githubOperationsReadAdapter(input),
  githubWriteAdapter: async (input) => githubOperationsWriteAdapter(input),
  cloudflareWriteAdapter: async (input) => cloudflareOperationsWriteAdapter(input),
  directCodexAdapter,
  reviewCodexResult: async ({ messages, timeoutMs, responseFormat }) => aiChatCompletion({
    routeKey: 'marcusChat',
    messages,
    timeoutMs,
    response_format: responseFormat,
  }),
  allowedWorkspaceRoots: String(process.env.MARCUS_ALLOWED_WORKSPACE_ROOTS || '')
    .split(path.delimiter).map((value) => value.trim()).filter(Boolean),
});

const projectEvidenceService = new ProjectEvidenceService({
  dataDir: DATA_DIR,
  listProjects: (businessKey) => operationsEngine.listProjectRegistry(businessKey),
  listOperations: (businessKey, filters) => operationsEngine.listOperations(businessKey, filters),
  getLegacyStore: (businessKey) => readStoreForBusiness(businessKey),
  getSettings: () => readSettings(),
  githubApi: (pathPart) => githubApi(pathPart),
  renderApi: (pathPart) => renderApi(pathPart),
  cloudflareApi: (pathPart) => cloudflareApi(pathPart),
});
operationsEngine.setCodexLifecycleRecorder((event) => projectEvidenceService.recordCodexLifecycle(event));

const projectOperatorService = new ProjectOperatorService({
  operationsEngine,
  projectEvidenceService,
  getLegacyStore: (businessKey) => readStoreForBusiness(businessKey),
  getDesktopContext: async () => desktopRelayCache?.data || desktopContextCache?.data || {},
  getMissionMemory: (businessKey, request) => missionMemoryStore.relevant(businessKey, request),
  githubApi: (pathPart, options) => githubApi(pathPart, options),
});

async function createOrReuseDurableOperationForMessage(message, { projectId = '', projectName = '', source = 'marcus_chat' } = {}) {
  const businessKey = getBusinessKeyFromContext();
  const originalRequest = String(message || '').trim();
  const created = await operationsEngine.createFromRequest(businessKey, {
    originalRequest,
    projectId,
    projectName,
    requestedBy: 'mark',
    source,
    autoPlan: true,
    autoStart: true,
  });
  return { ...created, reused: false };
}

function isExplicitOperationApprovalMessage(message) {
  const text = String(message || '').trim().toLowerCase();
  if (!text) return false;
  return /\b(approve|approved|approval granted|go ahead|proceed|do it|get it done|yes do it|yeah do it|yep do it|run it|start it)\b/i.test(text);
}

function operationApprovalTargetsMessage({ message, operation, approval }) {
  const text = String(message || '').trim().toLowerCase();
  return [operation?.id, operation?.title, operation?.projectName, approval?.action]
    .map((value) => String(value || '').trim().toLowerCase())
    .filter((value) => value.length >= 3)
    .some((value) => text.includes(value));
}

async function maybeApprovePendingOperationFromMessage(message, { approvalAuthorized = false } = {}) {
  if (!isExplicitOperationApprovalMessage(message)) return null;
  const businessKey = getBusinessKeyFromContext();
  const operations = await operationsEngine.listOperations(businessKey, { limit: 100 });
  const candidates = [];
  for (const operation of operations) {
    if (!['waiting_for_approval', 'queued', 'running', 'blocked', 'paused', 'awaiting_provider', 'recovery_required'].includes(operation.status)) continue;
    for (const approval of operation.approvals || []) {
      if (approval.status === 'pending') candidates.push({ operation, approval });
    }
  }
  if (!candidates.length) return null;

  const targeted = candidates.filter((candidate) => operationApprovalTargetsMessage({ message, ...candidate }));
  const selected = targeted.length === 1 ? targeted[0] : (candidates.length === 1 ? candidates[0] : null);
  if (!selected) {
    const choices = candidates.slice(0, 8).map(({ operation, approval }) =>
      `- ${operation.id}: ${operation.projectName || operation.title || 'Operation'} - ${approval.action} (${approval.riskLevel})`).join('\n');
    return {
      ok: false,
      approvalRequired: true,
      reply: `I need which pending approval you want me to approve.\n${choices}`,
    };
  }

  if (!approvalAuthorized) {
    return {
      ok: false,
      approvalRequired: true,
      reauthenticationRequired: true,
      operation: selected.operation,
      reply: 'This approval requires the paired Marcus app or durable admin authentication. No action was authorized.',
    };
  }

  const operation = await operationsEngine.approveOperationStep(businessKey, selected.operation.id, selected.approval.id, {
    approvedBy: 'mark',
    message,
    runCycle: true,
  });
  return {
    ok: true,
    operation,
    reply: `Approved ${selected.approval.action} for ${operation.projectName || operation.title}.\n${formatOperationStatusForMarcus(operation)}`,
  };
}

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
  const shouldDesktopActions = force || shouldCreateBackupForKey('desktop-actions');

  // Personal/legacy store
  if (shouldTasks) {
    const ok = await writeBackupSnapshot({ sourceFile: DATA_FILE, prefix: 'tasks' });
    if (ok) markBackupForKey('tasks');
  }

  if (shouldDesktopActions) {
    const ok = await writeBackupSnapshot({ sourceFile: desktopActionQueue.file, prefix: 'desktop-actions' });
    if (ok) markBackupForKey('desktop-actions');
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

    const durableSources = await discoverDurableBackupSources({
      businessDataDir: BUSINESS_DATA_DIR,
      configuredBusinessKeys: (Array.isArray(cfg.businesses) ? cfg.businesses : []).map((business) => business.key),
    });
    for (const source of durableSources) {
      const cacheKey = `durable:${source.prefix}`;
      if (!force && !shouldCreateBackupForKey(cacheKey)) continue;
      const ok = await writeBackupSnapshot({ sourceFile: source.sourceFile, prefix: source.prefix });
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
const LOOPBACK_BINDING = SERVER_HOST === 'localhost' || SERVER_HOST === '::1' || /^127(?:\.\d{1,3}){3}$/.test(SERVER_HOST);
if (!ADMIN_TOKEN && (IS_HOSTED_RUNTIME || !ALLOW_UNAUTHENTICATED_LOCAL || !LOOPBACK_BINDING)) {
  throw new Error('ADMIN_TOKEN is required outside explicit loopback development. For local-only development set MARCUS_ALLOW_UNAUTHENTICATED_LOCAL=true and bind MARCUS_HOST to a loopback address.');
}
const ELEVENLABS_API_KEY = typeof process.env.ELEVENLABS_API_KEY === 'string' ? process.env.ELEVENLABS_API_KEY.trim() : '';
const ELEVENLABS_VOICE_ID = typeof process.env.ELEVENLABS_VOICE_ID === 'string' ? process.env.ELEVENLABS_VOICE_ID.trim() : '';
const ELEVENLABS_MODEL_ID = typeof process.env.ELEVENLABS_MODEL_ID === 'string' ? process.env.ELEVENLABS_MODEL_ID.trim() : 'eleven_flash_v2_5';
const ELEVENLABS_OUTPUT_FORMAT = typeof process.env.ELEVENLABS_OUTPUT_FORMAT === 'string' ? process.env.ELEVENLABS_OUTPUT_FORMAT.trim() : 'mp3_44100_128';
const MARCUS_REALTIME_MODEL = typeof process.env.MARCUS_REALTIME_MODEL === 'string' && process.env.MARCUS_REALTIME_MODEL.trim()
  ? process.env.MARCUS_REALTIME_MODEL.trim()
  : DEFAULT_MARCUS_REALTIME_MODEL;
const MARCUS_REALTIME_VOICE = typeof process.env.MARCUS_REALTIME_VOICE === 'string' && process.env.MARCUS_REALTIME_VOICE.trim()
  ? process.env.MARCUS_REALTIME_VOICE.trim()
  : DEFAULT_MARCUS_REALTIME_VOICE;
const AUTH_COOKIE_NAME = 'ops_admin_token';
const AUTH_COOKIE_MAX_AGE_SEC = 60 * 60 * 24 * 30;
const MOBILE_PAIRING_CODE_TTL_MS = 10 * 60 * 1000;
const MOBILE_PAIRING_ATTEMPT_WINDOW_MS = 10 * 60 * 1000;
const MOBILE_PAIRING_MAX_FAILURES = 8;
const MOBILE_PAIRING_FILE = path.join(DATA_DIR, 'mobile-pairing.json');
const MOBILE_PAIRING_LOCK_FILE = `${MOBILE_PAIRING_FILE}.lock`;
const mobilePairingAttempts = new Map();

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

function pairingCodeHash(code) {
  return crypto.createHmac('sha256', ADMIN_TOKEN || 'marcus-local-pairing')
    .update(String(code || ''))
    .digest('hex');
}

function pruneMobilePairingState() {
  const now = Date.now();
  for (const [key, record] of mobilePairingAttempts.entries()) {
    if (!Number.isFinite(record?.startedAt) || record.startedAt + MOBILE_PAIRING_ATTEMPT_WINDOW_MS <= now) {
      mobilePairingAttempts.delete(key);
    }
  }
}

function mobilePairingAttemptKey(req) {
  return String(req.ip || req.socket?.remoteAddress || 'unknown');
}

function recordMobilePairingFailure(req) {
  pruneMobilePairingState();
  const key = mobilePairingAttemptKey(req);
  const current = mobilePairingAttempts.get(key);
  const next = current && current.startedAt + MOBILE_PAIRING_ATTEMPT_WINDOW_MS > Date.now()
    ? { ...current, count: current.count + 1 }
    : { startedAt: Date.now(), count: 1 };
  mobilePairingAttempts.set(key, next);
  return next.count;
}

function mobilePairingIsRateLimited(req) {
  pruneMobilePairingState();
  return (mobilePairingAttempts.get(mobilePairingAttemptKey(req))?.count || 0) >= MOBILE_PAIRING_MAX_FAILURES;
}

async function withMobilePairingFileLock(callback) {
  await fs.mkdir(DATA_DIR, { recursive: true });
  let lockHandle = null;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      lockHandle = await fs.open(MOBILE_PAIRING_LOCK_FILE, 'wx');
      break;
    } catch (error) {
      if (error?.code !== 'EEXIST') throw error;
      try {
        const stat = await fs.stat(MOBILE_PAIRING_LOCK_FILE);
        if (Date.now() - stat.mtimeMs > 30_000) await fs.unlink(MOBILE_PAIRING_LOCK_FILE);
      } catch (statError) {
        if (statError?.code !== 'ENOENT') throw statError;
      }
      await new Promise((resolve) => setTimeout(resolve, 20 + (attempt * 5)));
    }
  }
  if (!lockHandle) throw Object.assign(new Error('Mobile pairing is busy. Request a new code shortly.'), { statusCode: 503 });
  try {
    return await callback();
  } finally {
    await lockHandle.close().catch(() => {});
    await fs.unlink(MOBILE_PAIRING_LOCK_FILE).catch(() => {});
  }
}

async function readMobilePairingRecord() {
  try {
    const parsed = JSON.parse(await fs.readFile(MOBILE_PAIRING_FILE, 'utf8'));
    const codeHash = typeof parsed?.codeHash === 'string' ? parsed.codeHash : '';
    const expiresAt = Number(parsed?.expiresAt);
    if (!/^[a-f0-9]{64}$/i.test(codeHash) || !Number.isFinite(expiresAt)) return null;
    return { codeHash: codeHash.toLowerCase(), expiresAt };
  } catch (error) {
    if (error?.code === 'ENOENT' || error instanceof SyntaxError) return null;
    throw error;
  }
}

async function createMobilePairingCode() {
  return withMobilePairingFileLock(async () => {
    const code = String(crypto.randomInt(0, 1_000_000)).padStart(6, '0');
    const expiresAt = Date.now() + MOBILE_PAIRING_CODE_TTL_MS;
    const tmpFile = `${MOBILE_PAIRING_FILE}.tmp-${crypto.randomBytes(6).toString('hex')}`;
    await fs.writeFile(tmpFile, JSON.stringify({
      version: 1,
      codeHash: pairingCodeHash(code),
      expiresAt,
      createdAt: nowIso(),
    }, null, 2) + '\n', 'utf8');
    await replaceFileAtomically(tmpFile, MOBILE_PAIRING_FILE);
    return { code, expiresAt };
  });
}

async function consumeMobilePairingCode(code) {
  const submittedHash = /^\d{6}$/.test(code) ? pairingCodeHash(code) : '';
  if (!submittedHash) return false;
  return withMobilePairingFileLock(async () => {
    const record = await readMobilePairingRecord();
    if (!record || record.expiresAt <= Date.now()) {
      if (record) await fs.unlink(MOBILE_PAIRING_FILE).catch(() => {});
      return false;
    }
    if (!safeTimingEqual(submittedHash, record.codeHash)) return false;
    await fs.unlink(MOBILE_PAIRING_FILE);
    return true;
  });
}

function isPublicApiRoute(req) {
  const method = String(req.method || '').toUpperCase();
  const p = String(req.path || '');
  if (method === 'POST' && p === '/api/auth/login') return true;
  if (method === 'POST' && p === '/api/auth/pair') return true;
  if (method === 'POST' && p === '/api/auth/logout') return true;
  if (method === 'GET' && p === '/api/auth/status') return true;
  if (method === 'GET' && p === '/api/health') return true;
  if (method === 'GET' && /^\/api\/codex-monitor\/jobs\/[^/]+$/.test(p)) return true;
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
  const cookieToken = extractAuthCookieToken(req);
  if (cookieToken) return cookieToken;
  const liveToken = typeof req.query?.liveToken === 'string' ? req.query.liveToken.trim() : '';
  if (liveToken) return liveToken;
  const queryToken = typeof req.query?.token === 'string' ? req.query.token.trim() : '';
  return queryToken;
}

function extractAuthCookieToken(req) {
  return String(parseCookies(req)[AUTH_COOKIE_NAME] || '').trim();
}

function hasDurableAdminAuthentication(req) {
  if (!ADMIN_TOKEN) return true;
  const token = extractBearerToken(req);
  const cookieToken = extractAuthCookieToken(req);
  return Boolean(
    (token && safeTimingEqual(token, ADMIN_TOKEN))
    || (cookieToken && safeTimingEqual(cookieToken, ADMIN_TOKEN)),
  );
}

const MARCUS_LIVE_SESSION_TTL_MS = 10 * 60 * 1000;
const marcusLiveSessionTokens = new Map();

function pruneMarcusLiveSessionTokens() {
  const now = Date.now();
  for (const [token, session] of marcusLiveSessionTokens.entries()) {
    if (!Number.isFinite(session?.expiresAt) || session.expiresAt <= now) marcusLiveSessionTokens.delete(token);
  }
}

function createMarcusLiveSessionToken() {
  pruneMarcusLiveSessionTokens();
  const token = crypto.randomBytes(24).toString('base64url');
  const expiresAt = Date.now() + MARCUS_LIVE_SESSION_TTL_MS;
  marcusLiveSessionTokens.set(token, { expiresAt, businessKey: getBusinessKeyFromContext() });
  return { token, expiresAt };
}

function isValidMarcusLiveSessionToken(token) {
  const t = typeof token === 'string' ? token.trim() : '';
  if (!t) return false;
  pruneMarcusLiveSessionTokens();
  return Boolean(marcusLiveSessionTokens.has(t));
}

function getMarcusLiveSession(token) {
  const value = isValidMarcusLiveSessionToken(token) ? marcusLiveSessionTokens.get(String(token || '').trim()) : null;
  return value || null;
}

function isMarcusLiveSessionRoute(req) {
  const p = String(req?.path || '');
  return (p === '/api/operations/summary' && String(req?.method || '').toUpperCase() === 'GET')
    || p === '/api/marcus/live'
    || p === '/api/marcus/live/action'
    || p === '/api/marcus/active-brief'
    || p === '/api/marcus/live/chat'
    || p === '/api/marcus/live/dashboard'
    || p === '/api/marcus/operator-health'
    || p === '/api/marcus/project-operator'
    || p === '/api/marcus/project-bootstrap'
    || p === '/api/marcus/live/performance'
    || p === '/api/marcus/live/session-status'
    || p === '/api/marcus/live/voice/status'
    || p === '/api/marcus/live/voice/speak'
    || p === '/api/marcus/realtime/client-secret'
    || p === '/api/marcus/realtime/telemetry'
    || p === '/api/marcus/realtime/acceptance'
    || p === '/api/marcus/acceptance'
    || p === '/api/marcus/transcribe'
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
    const liveSession = isMarcusLiveSessionRoute(req) ? getMarcusLiveSession(token) : null;
    if (liveSession && liveSession.businessKey === getBusinessKeyFromContext()) return next();
    const cookieToken = extractAuthCookieToken(req);
    if (cookieToken && safeTimingEqual(cookieToken, ADMIN_TOKEN)) return next();
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
  const cookieToken = extractAuthCookieToken(req);
  const authenticated = Boolean(
    (token && (safeTimingEqual(token, ADMIN_TOKEN) || isValidMarcusLiveSessionToken(token)))
    || (cookieToken && safeTimingEqual(cookieToken, ADMIN_TOKEN)),
  );
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

app.post('/api/auth/pairing-code', async (req, res) => {
  if (!ADMIN_TOKEN) return res.status(400).json({ ok: false, error: 'Mobile pairing requires admin authentication.' });
  try {
    const { code, expiresAt } = await createMobilePairingCode();
    res.setHeader('Cache-Control', 'no-store');
    res.status(201).json({ ok: true, code, expiresAt, ttlMs: MOBILE_PAIRING_CODE_TTL_MS });
  } catch (error) {
    res.status(Number(error?.statusCode) || 500).json({ ok: false, error: String(error?.message || error) });
  }
});

app.post('/api/auth/pair', async (req, res) => {
  if (!ADMIN_TOKEN) return res.json({ ok: true, authRequired: false, authenticated: true });
  if (mobilePairingIsRateLimited(req)) {
    return res.status(429).json({ ok: false, error: 'Too many pairing attempts. Request a new code later.' });
  }
  const code = typeof req.body?.code === 'string' ? req.body.code.trim() : '';
  try {
    if (!await consumeMobilePairingCode(code)) {
      recordMobilePairingFailure(req);
      return res.status(401).json({ ok: false, error: 'Invalid or expired pairing code.' });
    }
  } catch (error) {
    return res.status(Number(error?.statusCode) || 500).json({ ok: false, error: String(error?.message || error) });
  }
  mobilePairingAttempts.delete(mobilePairingAttemptKey(req));
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Set-Cookie', buildAuthCookie({ req, token: ADMIN_TOKEN, remember: true }));
  res.json({ ok: true, authRequired: true, authenticated: true });
});

app.post('/api/auth/logout', (req, res) => {
  res.setHeader('Set-Cookie', buildAuthCookie({ req, token: '', clear: true }));
  res.json({ ok: true });
});

registerOperationsRoutes(app, {
  engine: operationsEngine,
  getBusinessKey: () => getBusinessKeyFromContext(),
});

registerMissionMemoryRoutes(app, {
  store: missionMemoryStore,
  getBusinessKey: () => getBusinessKeyFromContext(),
});

registerProjectEvidenceRoutes(app, {
  service: projectEvidenceService,
  getBusinessKey: () => getBusinessKeyFromContext(),
});

app.post('/api/desktop-context/codex-updates', async (req, res) => {
  if (!desktopCodexAdapter) return res.status(404).json({ ok: false, error: 'Desktop Codex is not enabled.' });
  try {
    const job = await desktopCodexAdapter.ingestUpdate({
      ...(req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {}),
      desktopAgentId: String(req.body?.desktopAgentId || req.body?.agentId || '').trim().slice(0, 200),
    });
    res.json({ ok: true, job });
  } catch (error) {
    const status = error?.code === 'CODEX_JOB_NOT_FOUND' ? 404 : error?.code === 'CODEX_AGENT_MISMATCH' ? 403 : 400;
    res.status(status).json({ ok: false, error: String(error?.message || error), code: error?.code || 'CODEX_UPDATE_REJECTED' });
  }
});

app.get('/api/codex-monitor/jobs/:jobId', async (req, res) => {
  if (!desktopCodexAdapter) return res.status(404).json({ ok: false, error: 'Desktop Codex is not enabled.' });
  const token = String(req.query?.monitorToken || '').trim();
  const after = Math.max(0, Number(req.query?.after) || 0);
  const job = await desktopCodexAdapter.getPublicJob(req.params.jobId, token, { after });
  if (!job) return res.status(401).json({ ok: false, error: 'Invalid or expired Codex monitor link.' });
  res.setHeader('Cache-Control', 'no-store');
  res.json({ ok: true, job });
});

app.get('/api/codex/jobs', async (_req, res) => {
  if (!desktopCodexAdapter) return res.json({ ok: true, enabled: false, jobs: [] });
  res.json({ ok: true, enabled: true, jobs: await desktopCodexAdapter.listJobs({ limit: 50 }) });
});

app.post('/api/codex/jobs/:jobId/followup', async (req, res) => {
  if (!desktopCodexAdapter) return res.status(404).json({ ok: false, error: 'Desktop Codex is not enabled.' });
  const message = typeof req.body?.message === 'string' ? req.body.message.trim().slice(0, 8_000) : '';
  if (!message) return res.status(400).json({ ok: false, error: 'Follow-up message is required.' });
  try {
    const job = await desktopCodexAdapter.sendFollowup({ jobId: req.params.jobId }, message);
    res.status(202).json({ ok: true, job });
  } catch (error) {
    res.status(error?.code === 'CODEX_JOB_NOT_FOUND' ? 404 : 400).json({ ok: false, error: String(error?.message || error) });
  }
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
    airtableDerivedStatusSync: parsed.airtableDerivedStatusSync === true,
    agentSystemPrompt: normalizeAgentSystemPrompt(parsed.agentSystemPrompt),
    operatorVoice: normalizeOperatorVoice(parsed.operatorVoice),
    automationConfig: normalizeAutomationConfig(parsed.automationConfig),
    automationDigestQueue: normalizeAutomationDigestQueue(parsed.automationDigestQueue),
    externalActionDrafts: normalizeExternalActionDrafts(parsed.externalActionDrafts),
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
  await replaceFileAtomically(tmpFile, SETTINGS_FILE);
  refreshBusinessCacheFromSettings().catch(() => {
    // best-effort
  });
  backupCriticalFiles().catch(() => {
    // backup is best-effort
  });
}

function normalizeProactiveModeServer(mode) {
  const value = typeof mode === 'string' ? mode.trim().toLowerCase() : '';
  return ['quiet', 'normal', 'aggressive', 'focus', 'away'].includes(value) ? value : 'normal';
}

function normalizeControlMap(input) {
  const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const out = {};
  for (const [key, value] of Object.entries(raw)) {
    const id = String(key || '').trim().slice(0, 240);
    if (!id || !value || typeof value !== 'object' || Array.isArray(value)) continue;
    out[id] = {
      ...value,
      updatedAt: typeof value.updatedAt === 'string' ? value.updatedAt : '',
    };
  }
  return out;
}

function normalizeMarcusOperationalControls(input = {}) {
  const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  return {
    ok: true,
    version: 1,
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : '',
    proactiveMode: normalizeProactiveModeServer(raw.proactiveMode),
    signals: normalizeControlMap(raw.signals),
    memory: normalizeControlMap(raw.memory),
    actions: normalizeControlMap(raw.actions),
    projects: normalizeControlMap(raw.projects),
    focus: normalizeControlMap(raw.focus),
  };
}

async function readMarcusOperationalControls() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    const raw = await fs.readFile(MARCUS_OPERATIONAL_CONTROLS_FILE, 'utf8');
    return normalizeMarcusOperationalControls(JSON.parse(raw));
  } catch {
    return normalizeMarcusOperationalControls({});
  }
}

async function writeMarcusOperationalControls(next) {
  const normalized = normalizeMarcusOperationalControls({
    ...(next && typeof next === 'object' ? next : {}),
    updatedAt: nowIso(),
  });
  await fs.mkdir(DATA_DIR, { recursive: true });
  const tmpFile = `${MARCUS_OPERATIONAL_CONTROLS_FILE}.tmp-${crypto.randomBytes(6).toString('hex')}`;
  await fs.writeFile(tmpFile, JSON.stringify(normalized, null, 2) + '\n', 'utf8');
  await replaceFileAtomically(tmpFile, MARCUS_OPERATIONAL_CONTROLS_FILE);
  backupCriticalFiles().catch(() => {
    // backup is best-effort
  });
  return normalized;
}

function normalizeMarcusSessionState(input = {}) {
  const raw = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const lastCheckInAt = typeof raw.lastCheckInAt === 'string' && Date.parse(raw.lastCheckInAt) ? raw.lastCheckInAt : '';
  const lastOpenedAt = typeof raw.lastOpenedAt === 'string' && Date.parse(raw.lastOpenedAt) ? raw.lastOpenedAt : '';
  return {
    ok: true,
    version: 1,
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : '',
    lastCheckInAt,
    lastOpenedAt,
    lastBriefHash: typeof raw.lastBriefHash === 'string' ? raw.lastBriefHash.slice(0, 80) : '',
    checkInCount: Number.isFinite(Number(raw.checkInCount)) ? Math.max(0, Math.floor(Number(raw.checkInCount))) : 0,
  };
}

async function readMarcusSessionState() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    const raw = await fs.readFile(MARCUS_SESSION_STATE_FILE, 'utf8');
    return normalizeMarcusSessionState(JSON.parse(raw));
  } catch {
    return normalizeMarcusSessionState({});
  }
}

async function writeMarcusSessionState(next) {
  const normalized = normalizeMarcusSessionState({
    ...(next && typeof next === 'object' ? next : {}),
    updatedAt: nowIso(),
  });
  await fs.mkdir(DATA_DIR, { recursive: true });
  const tmpFile = `${MARCUS_SESSION_STATE_FILE}.tmp-${crypto.randomBytes(6).toString('hex')}`;
  await fs.writeFile(tmpFile, JSON.stringify(normalized, null, 2) + '\n', 'utf8');
  await replaceFileAtomically(tmpFile, MARCUS_SESSION_STATE_FILE);
  backupCriticalFiles().catch(() => {
    // backup is best-effort
  });
  return normalized;
}

function mergeControlPatch(existing, section, id, patch) {
  const safeSection = ['signals', 'memory', 'actions', 'projects', 'focus'].includes(section) ? section : '';
  const safeId = String(id || '').trim().slice(0, 240);
  if (!safeSection || !safeId) throw new Error('Invalid control section or id');
  const cleanPatch = patch && typeof patch === 'object' && !Array.isArray(patch) ? patch : {};
  const next = normalizeMarcusOperationalControls(existing);
  next[safeSection] = normalizeControlMap(next[safeSection]);
  if (safeSection === 'focus' && cleanPatch.primary) {
    for (const [key, value] of Object.entries(next.focus || {})) {
      if (value && typeof value === 'object') next.focus[key] = { ...value, primary: false };
    }
  }
  next[safeSection][safeId] = {
    ...(next[safeSection][safeId] || {}),
    ...cleanPatch,
    updatedAt: nowIso(),
  };
  return next;
}

function normalizeSnakeValue(value) {
  return String(value || '').trim().toLowerCase().replace(/[-\s]+/g, '_');
}

function normalizeActionLifecycle(value) {
  const normalized = normalizeSnakeValue(value);
  return ['suggested_action', 'draft_action', 'approved_action', 'completed_action', 'dismissed_action'].includes(normalized)
    ? normalized
    : '';
}

function normalizeProjectControlState(value) {
  const normalized = normalizeSnakeValue(value);
  return ['active', 'pinned', 'reactivated', 'known_history', 'dormant', 'archived', 'complete', 'completed'].includes(normalized)
    ? (normalized === 'completed' ? 'complete' : normalized)
    : '';
}

function transitionActionControl(existing, actionId, lifecycle, extra = {}) {
  const id = String(actionId || '').trim().slice(0, 240);
  const nextLifecycle = normalizeActionLifecycle(lifecycle);
  if (!id) throw new Error('Action id is required');
  if (!nextLifecycle) throw new Error('Invalid action lifecycle');
  const next = normalizeMarcusOperationalControls(existing);
  const previous = next.actions[id] && typeof next.actions[id] === 'object' ? next.actions[id] : {};
  const executionStatus = nextLifecycle === 'approved_action'
    ? 'approved_pending_execution'
    : nextLifecycle === 'completed_action'
      ? 'manually_completed'
      : nextLifecycle === 'dismissed_action'
        ? 'dismissed'
        : (previous.executionStatus || 'not_executable');
  next.actions[id] = {
    ...previous,
    ...(extra && typeof extra === 'object' && !Array.isArray(extra) ? extra : {}),
    lifecycle: nextLifecycle,
    executionStatus,
    executionDeferred: nextLifecycle === 'approved_action',
    approvedAt: nextLifecycle === 'approved_action' ? nowIso() : previous.approvedAt || '',
    completedAt: nextLifecycle === 'completed_action' ? nowIso() : previous.completedAt || '',
    dismissedAt: nextLifecycle === 'dismissed_action' ? nowIso() : previous.dismissedAt || '',
    updatedAt: nowIso(),
  };
  return next;
}

function createManualActionControl(existing, input = {}) {
  const clean = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const title = String(clean.title || clean.summary || 'Review manual action').replace(/\s+/g, ' ').trim().slice(0, 180);
  const summary = String(clean.summary || clean.body || title).replace(/\s+/g, ' ').trim().slice(0, 500);
  const type = String(clean.type || 'manual_action').trim().slice(0, 80);
  const now = nowIso();
  const seed = `${title}|${summary}|${now}|${crypto.randomBytes(4).toString('hex')}`;
  const id = `action:manual:${crypto.createHash('sha1').update(seed).digest('hex').slice(0, 16)}`;
  const next = normalizeMarcusOperationalControls(existing);
  next.actions[id] = {
    id,
    title,
    summary,
    body: String(clean.body || summary).trim().slice(0, 1200),
    type,
    source: 'manual-command',
    lifecycle: normalizeActionLifecycle(clean.lifecycle) || 'suggested_action',
    executionStatus: 'not_executable',
    executionDeferred: true,
    requiresApproval: clean.requiresApproval !== false,
    approvalRequired: clean.requiresApproval !== false,
    payload: clean.payload && typeof clean.payload === 'object' && !Array.isArray(clean.payload) ? clean.payload : undefined,
    createdAt: now,
    updatedAt: now,
    changedBy: String(clean.changedBy || 'command').trim().slice(0, 80),
  };
  return { next, action: next.actions[id] };
}

function createProjectDraftActionControl(existing, input = {}) {
  const clean = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const name = String(clean.projectName || clean.title || 'New project').replace(/\s+/g, ' ').trim().slice(0, 180);
  if (!name) throw new Error('Project name is required');
  return createManualActionControl(existing, {
    title: `Create project: ${name}`,
    summary: String(clean.summary || `Review and create a project record for ${name}.`).replace(/\s+/g, ' ').trim().slice(0, 500),
    body: String(clean.body || clean.summary || `Project draft requested from command: ${name}`).trim().slice(0, 1200),
    type: 'create_project_draft',
    lifecycle: 'draft_action',
    requiresApproval: true,
    changedBy: clean.changedBy || 'command',
    payload: {
      projectName: name,
      sourceText: String(clean.sourceText || '').trim().slice(0, 1000),
      proposedState: 'idea',
    },
  });
}

function createManualFocusControl(existing, input = {}) {
  const clean = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const title = String(clean.title || clean.name || 'Current focus').replace(/\s+/g, ' ').trim().slice(0, 180);
  if (!title) throw new Error('Focus title is required');
  const summary = String(clean.summary || clean.detail || 'Pinned from command.').replace(/\s+/g, ' ').trim().slice(0, 500);
  const now = nowIso();
  const idSeed = `${title}|${summary}`.toLowerCase();
  const id = `focus:manual:${crypto.createHash('sha1').update(idSeed).digest('hex').slice(0, 16)}`;
  const next = normalizeMarcusOperationalControls(existing);
  for (const [key, value] of Object.entries(next.focus || {})) {
    if (value && typeof value === 'object' && value.primary) {
      next.focus[key] = { ...value, primary: false, updatedAt: now };
    }
  }
  next.focus[id] = {
    ...(next.focus[id] || {}),
    id,
    title,
    name: title,
    summary,
    detail: summary,
    type: 'manual_focus',
    state: 'active',
    pinned: true,
    primary: true,
    source: 'command',
    confidence: 1,
    createdAt: next.focus[id]?.createdAt || now,
    updatedAt: now,
    changedBy: String(clean.changedBy || 'command').trim().slice(0, 80),
  };
  return { next, focus: next.focus[id] };
}

function getOperationalItemIds(item) {
  const ids = new Set();
  if (!item || typeof item !== 'object') return ids;
  for (const key of ['id', 'sourceSignalId', 'signalId', 'target', 'targetId', 'projectId', 'threadId', 'messageId', 'taskId']) {
    const value = String(item[key] || '').trim();
    if (value) ids.add(value);
  }
  if (item.id) ids.add(`action:${String(item.id).trim()}`);
  return ids;
}

function findControlForItem(item, controls = {}) {
  const map = controls && typeof controls === 'object' && !Array.isArray(controls) ? controls : {};
  const ids = getOperationalItemIds(item);
  for (const id of ids) {
    if (map[id]) return { id, control: map[id] };
  }
  return { id: '', control: null };
}

function isSignalControlledOut(item, signalControls = {}, now = Date.now()) {
  const { control } = findControlForItem(item, signalControls);
  if (!control) return false;
  if (control.dismissed || control.convertedToAction) return true;
  const snoozedUntil = Number(control.snoozedUntil || 0);
  return Boolean(snoozedUntil && snoozedUntil > now);
}

function filterControlledSignals(rows, signalControls, now) {
  const list = Array.isArray(rows) ? rows : [];
  return list.filter((item) => !isSignalControlledOut(item, signalControls, now));
}

function applyFocusControlsToBrief(out, focusControls = {}) {
  const controls = normalizeControlMap(focusControls);
  const focusRows = Object.values(controls)
    .filter((control) => control && typeof control === 'object' && !control.dismissed && !control.archived && !control.forgotten)
    .map((control) => ({
      id: String(control.id || control.targetId || control.target || '').trim(),
      title: String(control.title || control.name || control.summary || 'Pinned focus').trim(),
      name: String(control.name || control.title || control.summary || 'Pinned focus').trim(),
      summary: String(control.summary || control.detail || '').trim(),
      detail: String(control.detail || control.summary || '').trim(),
      type: String(control.type || 'focus_control').trim(),
      state: String(control.state || 'active').trim(),
      source: String(control.source || 'operational-controls').trim(),
      confidence: Number.isFinite(Number(control.confidence)) ? Number(control.confidence) : 1,
      pinned: Boolean(control.pinned || control.primary),
      primary: Boolean(control.primary),
      target: String(control.target || control.targetId || '').trim(),
      targetId: String(control.targetId || control.target || '').trim(),
      businessKey: String(control.businessKey || '').trim(),
      businessName: String(control.businessName || '').trim(),
      updatedAt: String(control.updatedAt || '').trim(),
      controlled: true,
    }))
    .filter((control) => control.title || control.name || control.targetId)
    .sort((a, b) => Number(b.primary) - Number(a.primary) || String(b.updatedAt).localeCompare(String(a.updatedAt)));

  if (!focusRows.length) {
    out.focusPolicy = {
      controlledFocusCount: 0,
      primaryFocusId: String(out.currentFocus?.id || out.activeProject?.id || '').trim(),
      source: out.currentFocus ? 'inferred' : 'none',
    };
    return out;
  }

  const primary = focusRows.find((row) => row.primary) || focusRows[0];
  const activeProjects = Array.isArray(out.projectActivity) ? out.projectActivity : [];
  const pinnedTargets = new Set(focusRows.map((row) => row.targetId || row.target || row.id).filter(Boolean));
  const matchedProjects = activeProjects
    .filter((project) => {
      const ids = getOperationalItemIds(project);
      for (const id of ids) if (pinnedTargets.has(id)) return true;
      return false;
    })
    .map((project) => ({ ...project, focusPinned: true, controlled: true }));

  out.focusLanes = Array.from(new Map([...focusRows, ...matchedProjects].map((row) => [String(row.id || row.targetId || row.title || row.name), row])).values());
  out.currentFocus = {
    id: primary.id || primary.targetId || `focus:${crypto.createHash('sha1').update(primary.title || primary.name).digest('hex').slice(0, 12)}`,
    title: primary.title || primary.name,
    name: primary.name || primary.title,
    detail: primary.detail || primary.summary,
    summary: primary.summary || primary.detail,
    type: primary.type,
    source: primary.source,
    confidence: primary.confidence,
    pinned: true,
    controlled: true,
    businessKey: primary.businessKey,
    businessName: primary.businessName,
    updatedAt: primary.updatedAt,
  };
  out.activeProject = out.currentFocus;
  out.focusPolicy = {
    controlledFocusCount: focusRows.length,
    primaryFocusId: out.currentFocus.id,
    source: 'operational-controls',
  };
  return out;
}

function isCriticalAttentionItem(item) {
  const score = Number(item?.score || 0);
  const priority = String(item?.priority || item?.status || '').toLowerCase();
  const bucket = String(item?.bucket || item?.queue || '').toLowerCase();
  return score >= 78 || priority === 'critical' || bucket === 'interrupt_now';
}

function matchesFocusAttentionItem(item, brief) {
  if (isCriticalAttentionItem(item)) return true;
  const focus = brief?.activeProject || brief?.currentFocus || {};
  const terms = [
    focus?.id,
    focus?.target,
    focus?.name,
    focus?.title,
    item?.bucket === 'waiting' ? 'waiting' : '',
  ].map((value) => String(value || '').trim().toLowerCase()).filter(Boolean);
  if (!terms.length) return false;
  const text = JSON.stringify(item || {}).toLowerCase();
  return terms.some((term) => text.includes(term));
}

function applyProactiveAttentionPolicy(items, brief, mode) {
  const list = Array.isArray(items) ? items : [];
  if (mode === 'quiet') return list.filter(isCriticalAttentionItem);
  if (mode === 'away') return list.filter((item) => Number(item?.score || 0) >= 86 || String(item?.priority || '').toLowerCase() === 'critical');
  if (mode === 'focus') return list.filter((item) => matchesFocusAttentionItem(item, brief));
  return list;
}

function applyMemoryControlsToPulse(memoryPulse, memoryControls = {}) {
  const pulse = memoryPulse && typeof memoryPulse === 'object' ? memoryPulse : {};
  const overlayRecord = (record) => {
    const { control } = findControlForItem(record, memoryControls);
    if (!control) return record;
    return {
      ...record,
      ...control,
      pinned: Boolean(control.important || control.pinned || record?.pinned),
      status: control.archived
        ? 'archived'
        : control.important && !control.outdated && !control.forgotten ? 'important' : (control.status || record?.status),
      controlled: true,
    };
  };
  const visible = (rows) => (Array.isArray(rows) ? rows : [])
    .map(overlayRecord)
    .filter((record) => !record.forgotten && !record.archived)
    .sort((a, b) => Number(Boolean(b.important)) - Number(Boolean(a.important)) || Number(Boolean(b.pinned)) - Number(Boolean(a.pinned)));
  return {
    ...pulse,
    records: visible(pulse.records),
    newFacts: visible(pulse.newFacts),
    staleAssumptions: visible(pulse.staleAssumptions),
    uncertain: visible(pulse.uncertain),
    controlledRecords: Object.entries(memoryControls || {}).map(([id, control]) => ({ id, ...control })),
  };
}

function applyActionControlsToQueue(actionQueue, controls = {}) {
  const rows = Array.isArray(actionQueue) ? actionQueue : [];
  const seen = new Set();
  const overlaid = rows.map((action) => {
    const { id, control } = findControlForItem(action, controls);
    if (id) seen.add(id);
    if (!control) return action;
    return {
      ...action,
      ...control,
      id: action.id || id,
      lifecycle: normalizeActionLifecycle(control.lifecycle) || action.lifecycle || 'suggested_action',
      controlled: true,
    };
  });
  for (const [id, control] of Object.entries(controls || {})) {
    if (seen.has(id)) continue;
    if (!control?.sourceSignalId && !control?.title && !control?.summary) continue;
    overlaid.push({
      id,
      title: control.title || `Review converted signal: ${control.sourceSignalId}`,
      summary: control.summary || (control.sourceSignalId ? 'Converted from an operational signal. Review before execution.' : 'Manual action created from command.'),
      sourceSignalId: control.sourceSignalId,
      source: control.source || (control.sourceSignalId ? 'signal-control' : 'manual-command'),
      type: control.type || (control.sourceSignalId ? 'converted_signal' : 'manual_action'),
      requiresApproval: control.requiresApproval !== false,
      approvalRequired: control.approvalRequired !== false && control.requiresApproval !== false,
      lifecycle: normalizeActionLifecycle(control.lifecycle) || 'suggested_action',
      executionStatus: control.executionStatus || 'not_executable',
      executionDeferred: control.executionDeferred !== false,
      controlled: true,
      ...control,
    });
  }
  return overlaid;
}

function decisionForApprovalAction(action) {
  if (!action || typeof action !== 'object') return null;
  const id = String(action.id || '').trim();
  if (!id || !(action.approvalRequired || action.requiresApproval)) return null;
  return {
    id: `decision:${id}`,
    type: 'Decision',
    title: action.title || action.summary || 'Approve prepared action',
    summary: action.summary || action.body || 'Prepared action is waiting for Mark approval.',
    question: 'Should this prepared action be approved, revised, or dismissed?',
    source: action.source || 'action-queue',
    sourceActionId: id,
    businessKey: action.businessKey || '',
    target: action.target || id,
    targetType: action.type || 'ActionDraft',
    score: Number(action.score || 62),
    urgency: Number(action.urgency || 0.5),
    confidence: Number(action.confidence || 0.72),
    reasons: [action.approvalReason || 'External execution remains approval-gated.'],
    recommendedAction: action.suggestedButtonLabel || 'Review and approve or dismiss.',
    requiresMark: true,
    approvalRequired: true,
  };
}

function applyProjectControlsToActivity(projectActivity, controls = {}) {
  const rows = Array.isArray(projectActivity) ? projectActivity : [];
  return rows.map((project) => {
    const { control } = findControlForItem(project, controls);
    if (!control) return project;
    const state = normalizeProjectControlState(control.state || control.activityStatus || control.status);
    const next = {
      ...project,
      ...control,
      controlled: true,
      controlState: state || control.controlState || '',
    };
    if (state === 'active' || state === 'pinned' || state === 'reactivated') {
      next.activityStatus = 'active';
      next.status = 'active';
      next.projectState = 'active';
      next.pinned = true;
      next.operationalOverride = 'keep_active';
      next.reason = control.reason || 'Kept active by operator control.';
    } else if (state === 'archived' || state === 'complete') {
      next.activityStatus = state === 'complete' ? 'complete' : 'archived';
      next.status = next.activityStatus;
      next.projectState = state === 'complete' ? 'complete' : 'archived';
      next.operationalOverride = state;
      next.reason = control.reason || (state === 'complete' ? 'Marked complete by operator control.' : 'Archived by operator control.');
    } else if (state === 'known_history' || state === 'dormant') {
      next.activityStatus = 'historical';
      next.status = 'historical';
      next.projectState = 'dormant';
      next.operationalOverride = 'known_history';
      next.reason = control.reason || 'Kept in known history by operator control.';
    }
    return next;
  });
}

function recomputeCommunicationCounts(comms) {
  const out = comms && typeof comms === 'object' ? { ...comms } : {};
  out.counts = {
    waitingOnMark: Array.isArray(out.waitingOnMark) ? out.waitingOnMark.length : 0,
    waitingOnOthers: Array.isArray(out.waitingOnOthers) ? out.waitingOnOthers.length : 0,
    draftableReplies: Array.isArray(out.draftableReplies) ? out.draftableReplies.length : 0,
    followUpsDue: Array.isArray(out.followUpsDue) ? out.followUpsDue.length : 0,
    unusualSilence: Array.isArray(out.unusualSilence) ? out.unusualSilence.length : 0,
    highValueMissedOpportunities: Array.isArray(out.highValueMissedOpportunities) ? out.highValueMissedOpportunities.length : 0,
  };
  return out;
}

function hashMarcusBriefForSession(brief) {
  const payload = {
    top: (Array.isArray(brief?.topPriorities) ? brief.topPriorities : []).slice(0, 8).map((item) => item?.id || item?.title),
    actions: (Array.isArray(brief?.activeActionQueue) ? brief.activeActionQueue : Array.isArray(brief?.actionQueue) ? brief.actionQueue : []).slice(0, 8).map((item) => item?.id || item?.title),
    systems: (Array.isArray(brief?.systemHealth?.items) ? brief.systemHealth.items : []).map((item) => `${item?.id || item?.title}:${item?.status || ''}`),
  };
  return crypto.createHash('sha1').update(JSON.stringify(payload)).digest('hex');
}

function itemChangedAtMs(item) {
  const candidates = [
    item?.changedAt,
    item?.lastSeenAt,
    item?.lastActivityAt,
    item?.updatedAt,
    item?.createdAt,
    item?.date,
  ];
  for (const value of candidates) {
    const ms = Date.parse(String(value || ''));
    if (Number.isFinite(ms)) return ms;
  }
  return 0;
}

function sessionChangeRow(item, category, fallbackAt = '') {
  if (!item || typeof item !== 'object') return null;
  const changedMs = itemChangedAtMs(item);
  const changedAt = changedMs ? new Date(changedMs).toISOString() : fallbackAt;
  return {
    id: String(item.id || item.target || `${category}:${item.title || item.name || ''}`).trim().slice(0, 240),
    category,
    title: String(item.title || item.name || item.summary || 'Changed item').trim().slice(0, 180),
    summary: String(item.detail || item.summary || item.recommendedAction || item.nextAction || '').trim().slice(0, 260),
    source: String(item.source || item.channel || item.businessKey || category).trim().slice(0, 80),
    changedAt,
    score: Number.isFinite(Number(item.score)) ? Math.round(Number(item.score)) : null,
    confidence: Number.isFinite(Number(item.confidence)) ? Math.round(Number(item.confidence) * 100) : null,
  };
}

function buildSessionContextForBrief(brief, rawSessionState) {
  const state = normalizeMarcusSessionState(rawSessionState);
  const lastMs = Date.parse(state.lastCheckInAt || '') || 0;
  const generatedAt = String(brief?.generatedAt || nowIso());
  const candidates = [
    ...(Array.isArray(brief?.topPriorities) ? brief.topPriorities.map((item) => [item, 'attention']) : []),
    ...(Array.isArray(brief?.operationalSignals) ? brief.operationalSignals.map((item) => [item, 'signal']) : []),
    ...(Array.isArray(brief?.activeActionQueue) ? brief.activeActionQueue.map((item) => [item, 'action']) : []),
    ...(Array.isArray(brief?.actionQueue) ? brief.actionQueue.map((item) => [item, 'action']) : []),
    ...(Array.isArray(brief?.memoryPulse?.records) ? brief.memoryPulse.records.map((item) => [item, 'memory']) : []),
    ...(Array.isArray(brief?.systemHealth?.items) ? brief.systemHealth.items.map((item) => [item, 'system']) : []),
    ...(Array.isArray(brief?.projectActivity) ? brief.projectActivity.map((item) => [item, 'work']) : []),
  ];
  const seen = new Set();
  const changed = [];
  for (const [item, category] of candidates) {
    const row = sessionChangeRow(item, category, generatedAt);
    if (!row?.title) continue;
    const key = row.id || `${row.category}:${row.title}`;
    if (seen.has(key)) continue;
    const realChangedMs = itemChangedAtMs(item);
    const changedMs = Date.parse(row.changedAt || '') || 0;
    if (lastMs && changedMs && changedMs <= lastMs) continue;
    if (lastMs && !realChangedMs) continue;
    seen.add(key);
    changed.push(row);
  }
  changed.sort((a, b) => (Date.parse(b.changedAt || '') || 0) - (Date.parse(a.changedAt || '') || 0) || Number(b.score || 0) - Number(a.score || 0));
  const topChanged = changed.slice(0, 8);
  const counts = {};
  for (const row of changed) counts[row.category] = Number(counts[row.category] || 0) + 1;
  return {
    ...state,
    currentBriefHash: hashMarcusBriefForSession(brief),
    generatedAt,
    hasPriorCheckIn: Boolean(state.lastCheckInAt),
    changedSinceLastCheckIn: topChanged,
    changedSinceLastCheckInCount: changed.length,
    changedSinceLastCheckInCounts: counts,
    briefingLine: state.lastCheckInAt
      ? (topChanged.length ? `${topChanged.length} notable change${topChanged.length === 1 ? '' : 's'} since ${state.lastCheckInAt}.` : `No notable changes since ${state.lastCheckInAt}.`)
      : 'No prior check-in recorded; using the current active brief as the starting baseline.',
  };
}

function applyOperationalControlsToBrief(brief, rawControls) {
  const controls = normalizeMarcusOperationalControls(rawControls);
  const now = Date.now();
  const mode = controls.proactiveMode;
  const out = { ...(brief && typeof brief === 'object' ? brief : {}) };
  out.projectActivity = applyProjectControlsToActivity(out.projectActivity, controls.projects);
  applyFocusControlsToBrief(out, controls.focus);
  const closedProjectIds = new Set((Array.isArray(out.projectActivity) ? out.projectActivity : [])
    .filter((project) => ['archived', 'complete'].includes(normalizeProjectControlState(project?.controlState || project?.activityStatus || project?.status)) || ['archived', 'complete'].includes(String(project?.operationalOverride || '').toLowerCase()))
    .map((project) => String(project?.id || project?.target || '').trim())
    .filter(Boolean));
  const activeOverrideIds = new Set((Array.isArray(out.projectActivity) ? out.projectActivity : [])
    .filter((project) => ['active', 'pinned', 'reactivated'].includes(normalizeProjectControlState(project?.controlState || project?.activityStatus || project?.status)) || project?.operationalOverride === 'keep_active')
    .map((project) => String(project?.id || project?.target || '').trim())
    .filter(Boolean));
  const isClosedProjectItem = (item) => {
    const ids = getOperationalItemIds(item);
    for (const id of ids) if (closedProjectIds.has(id)) return true;
    return false;
  };
  const controlledSignalKeys = [
    'topPriorities',
    'urgentInterrupts',
    'waitingOnMark',
    'waitingOnClients',
    'waitingOnTeam',
    'stalledProjects',
    'risks',
    'opportunities',
    'decisionQueue',
    'ignoreQueue',
    'operationalSignals',
  ];
  let suppressedByControlsCount = 0;
  for (const key of controlledSignalKeys) {
    if (!Array.isArray(out[key])) continue;
    const filtered = filterControlledSignals(out[key], controls.signals, now).filter((item) => !isClosedProjectItem(item));
    suppressedByControlsCount += out[key].length - filtered.length;
    out[key] = filtered;
  }

  const rawTop = Array.isArray(out.topPriorities) ? out.topPriorities : [];
  const rawUrgent = Array.isArray(out.urgentInterrupts) ? out.urgentInterrupts : [];
  const controlledTop = applyProactiveAttentionPolicy(rawTop, out, mode);
  const controlledUrgent = applyProactiveAttentionPolicy(rawUrgent, out, mode);
  const attentionQueue = Array.from(new Map([...controlledUrgent, ...controlledTop].map((item) => [String(item?.id || item?.title || JSON.stringify(item)), item])).values());
  const suppressedByModeCount = Math.max(0, rawTop.length - controlledTop.length) + Math.max(0, rawUrgent.length - controlledUrgent.length);

  out.controlledAttention = {
    topPriorities: controlledTop,
    urgentInterrupts: controlledUrgent,
    attentionQueue,
  };
  out.topPriorities = controlledTop;
  out.urgentInterrupts = controlledUrgent;

  const modeDescriptions = {
    quiet: 'Only critical or interrupt-grade items are surfaced.',
    normal: 'Ranked attention is shown after explicit controls are applied.',
    aggressive: 'All generated attention above the base threshold is surfaced.',
    focus: 'Focus-related work and critical blockers are surfaced.',
    away: 'Only critical items remain visible for later catch-up.',
  };
  out.attentionPolicy = {
    mode,
    description: modeDescriptions[mode] || modeDescriptions.normal,
    interruptThreshold: mode === 'away' ? 86 : mode === 'quiet' ? 78 : mode === 'focus' ? 78 : 0,
    visibleAttentionCount: attentionQueue.length,
    suppressedByControlsCount,
    suppressedByModeCount,
  };

  const actionQueue = applyActionControlsToQueue(out.actionQueue || out.preparedActions || [], controls.actions);
  out.actionQueue = actionQueue;
  out.preparedActions = applyActionControlsToQueue(out.preparedActions || [], controls.actions);
  out.activeActionQueue = actionQueue.filter((action) => !['completed_action', 'dismissed_action'].includes(normalizeActionLifecycle(action.lifecycle)));
  const existingDecisionIds = new Set((Array.isArray(out.decisionQueue) ? out.decisionQueue : []).map((decision) => String(decision?.id || '').trim()).filter(Boolean));
  const actionDecisions = out.activeActionQueue
    .map(decisionForApprovalAction)
    .filter((decision) => decision && !existingDecisionIds.has(String(decision.id || '').trim()));
  out.decisionQueue = [...(Array.isArray(out.decisionQueue) ? out.decisionQueue : []), ...actionDecisions]
    .sort((a, b) => Number(b?.score || 0) - Number(a?.score || 0))
    .slice(0, 20);

  out.memoryPulse = applyMemoryControlsToPulse(out.memoryPulse, controls.memory);
  const activeProjects = (Array.isArray(out.projectActivity) ? out.projectActivity : []).filter((project) => ['active', 'waiting', 'warming'].includes(String(project?.activityStatus || project?.status || '').toLowerCase()));
  out.projects = activeProjects.slice(0, 3);
  const activeProject = activeProjects[0] || null;
  if (activeProject && out.focusPolicy?.source !== 'operational-controls') {
    out.activeProject = {
      id: activeProject.id,
      name: activeProject.name || activeProject.title,
      businessKey: activeProject.businessKey || '',
      businessName: activeProject.businessName || activeProject.businessKey || '',
      activityStatus: activeProject.activityStatus || activeProject.status,
      controlled: Boolean(activeProject.controlled),
    };
    out.currentFocus = out.activeProject;
  }
  out.projectControlPolicy = {
    activeOverrideCount: activeOverrideIds.size,
    closedOverrideCount: closedProjectIds.size,
    controlledProjectCount: Object.keys(controls.projects || {}).length,
  };
  if (out.sessionBriefing && typeof out.sessionBriefing === 'object') {
    out.sessionBriefing = {
      ...out.sessionBriefing,
      needsAttention: filterControlledSignals(out.sessionBriefing.needsAttention, controls.signals, now),
      waitingOnMark: filterControlledSignals(out.sessionBriefing.waitingOnMark, controls.signals, now),
      risks: filterControlledSignals(out.sessionBriefing.risks, controls.signals, now),
      opportunities: filterControlledSignals(out.sessionBriefing.opportunities, controls.signals, now),
      decisions: filterControlledSignals(out.sessionBriefing.decisions, controls.signals, now),
      ignoreQueue: filterControlledSignals(out.sessionBriefing.ignoreQueue, controls.signals, now),
      topActions: out.activeActionQueue.slice(0, 5),
    };
  }
  if (out.communicationIntelligence && typeof out.communicationIntelligence === 'object') {
    const comms = { ...out.communicationIntelligence };
    for (const key of ['waitingOnMark', 'waitingOnOthers', 'draftableReplies', 'followUpsDue', 'unusualSilence', 'highValueMissedOpportunities']) {
      comms[key] = filterControlledSignals(comms[key], controls.signals, now);
    }
    out.communicationIntelligence = recomputeCommunicationCounts(comms);
  }
  out.operationalControls = controls;
  return out;
}

function applyMarcusSessionContextToBrief(brief, sessionState) {
  const out = { ...(brief && typeof brief === 'object' ? brief : {}) };
  const sessionContext = buildSessionContextForBrief(out, sessionState);
  out.sessionContext = sessionContext;
  if (out.sessionBriefing && typeof out.sessionBriefing === 'object') {
    out.sessionBriefing = {
      ...out.sessionBriefing,
      lastCheckInAt: sessionContext.lastCheckInAt,
      changedSinceLastTime: sessionContext.changedSinceLastCheckIn,
      changedSinceLastTimeCount: sessionContext.changedSinceLastCheckInCount,
      changedSummary: sessionContext.briefingLine,
    };
  }
  return out;
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

function getGitHubCloudConfig(saved = {}) {
  const token = typeof process.env.GITHUB_TOKEN === 'string' ? process.env.GITHUB_TOKEN.trim() : '';
  const savedToken = typeof saved.githubToken === 'string' ? saved.githubToken.trim() : '';
  const owner = typeof process.env.GITHUB_OWNER === 'string' ? process.env.GITHUB_OWNER.trim() : '';
  const savedOwner = typeof saved.githubOwner === 'string' ? saved.githubOwner.trim() : '';
  const effectiveToken = token || savedToken;
  const effectiveOwner = owner || savedOwner;
  return { token: effectiveToken, owner: effectiveOwner, configured: Boolean(effectiveToken), tokenHint: maskSecretHint(effectiveToken), source: token ? 'env' : savedToken ? 'settings' : 'none' };
}

function getCloudflareConfig(saved = {}) {
  const token = typeof process.env.CLOUDFLARE_API_TOKEN === 'string' ? process.env.CLOUDFLARE_API_TOKEN.trim() : '';
  const savedToken = typeof saved.cloudflareApiToken === 'string' ? saved.cloudflareApiToken.trim() : '';
  const accountId = typeof process.env.CLOUDFLARE_ACCOUNT_ID === 'string' ? process.env.CLOUDFLARE_ACCOUNT_ID.trim() : '';
  const savedAccountId = typeof saved.cloudflareAccountId === 'string' ? saved.cloudflareAccountId.trim() : '';
  const defaultZoneId = typeof process.env.CLOUDFLARE_DEFAULT_ZONE_ID === 'string' ? process.env.CLOUDFLARE_DEFAULT_ZONE_ID.trim() : '';
  const savedDefaultZoneId = typeof saved.cloudflareDefaultZoneId === 'string' ? saved.cloudflareDefaultZoneId.trim() : '';
  const effectiveToken = token || savedToken;
  return {
    token: effectiveToken,
    accountId: accountId || savedAccountId,
    defaultZoneId: defaultZoneId || savedDefaultZoneId,
    configured: Boolean(effectiveToken),
    tokenHint: maskSecretHint(effectiveToken),
    source: token ? 'env' : savedToken ? 'settings' : 'none',
  };
}

function getQuoOutboundConfig(saved = {}) {
  const envApiKey = firstNonEmptyString(process.env, ['QUO_API_KEY', 'OPENPHONE_API_KEY']);
  const savedApiKey = typeof saved.quoApiKey === 'string' ? saved.quoApiKey.trim() : '';
  const apiKey = envApiKey || savedApiKey;
  const defaultPhoneNumberId = firstNonEmptyString(process.env, ['QUO_DEFAULT_PHONE_NUMBER_ID', 'OPENPHONE_DEFAULT_PHONE_NUMBER_ID'])
    || (typeof saved.quoDefaultPhoneNumberId === 'string' ? saved.quoDefaultPhoneNumberId.trim() : '');
  const from = firstNonEmptyString(process.env, ['QUO_FROM_NUMBER', 'OPENPHONE_FROM_NUMBER'])
    || (typeof saved.quoFromNumber === 'string' ? saved.quoFromNumber.trim() : '');
  const userId = firstNonEmptyString(process.env, ['QUO_USER_ID', 'OPENPHONE_USER_ID'])
    || (typeof saved.quoUserId === 'string' ? saved.quoUserId.trim() : '');
  const configuredBaseUrl = firstNonEmptyString(process.env, ['QUO_API_BASE_URL', 'OPENPHONE_API_BASE_URL'])
    || (typeof saved.quoBaseUrl === 'string' ? saved.quoBaseUrl.trim() : '');
  const baseUrl = normalizeBaseUrl(configuredBaseUrl) || 'https://api.openphone.com';
  return {
    apiKey,
    baseUrl,
    defaultPhoneNumberId,
    from,
    userId,
    configured: Boolean(apiKey && (defaultPhoneNumberId || from)),
    source: envApiKey ? 'env' : savedApiKey ? 'settings' : 'none',
    keyHint: maskSecretHint(apiKey),
  };
}

function normalizeExternalRecipient(type, value) {
  const recipient = String(value || '').trim();
  if (type === 'text' && !/^\+[1-9]\d{7,14}$/.test(recipient)) {
    throw new Error('Text recipient must use E.164 format, for example +15555555555.');
  }
  if (type === 'email' && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(recipient)) {
    throw new Error('Email recipient must be a valid email address.');
  }
  return recipient;
}

function normalizeEmailFromAddress(value) {
  const fromAddress = String(value || '').trim();
  const displayNameMatch = fromAddress.match(/^[^<>\r\n]{1,200}\s*<([^<>\r\n]+)>$/);
  normalizeExternalRecipient('email', displayNameMatch ? displayNameMatch[1].trim() : fromAddress);
  return fromAddress;
}

async function resolveQuoSender(config) {
  if (!config?.apiKey) throw new Error('Quo API key is not configured.');
  if (config.from && config.userId) return { from: config.from, userId: config.userId, phoneNumberId: config.defaultPhoneNumberId };
  const { resp, data } = await fetchJsonWithTimeout(`${config.baseUrl}/v1/phone-numbers`, {
    method: 'GET',
    headers: { Authorization: config.apiKey, 'Content-Type': 'application/json' },
    timeoutMs: 20_000,
  });
  if (!resp.ok) {
    const detail = String(data?.message || data?.error?.message || data?.error || '').trim();
    throw new Error(`Quo phone-number lookup failed (${resp.status})${detail ? `: ${detail}` : ''}`);
  }
  const numbers = Array.isArray(data?.data) ? data.data : [];
  const normalizedFrom = String(config.from || '').replace(/[^\d+]/g, '');
  const selected = numbers.find((item) => config.defaultPhoneNumberId && item?.id === config.defaultPhoneNumberId)
    || numbers.find((item) => normalizedFrom && [item?.formattedNumber, item?.number].some((value) => String(value || '').replace(/[^\d+]/g, '') === normalizedFrom))
    || (numbers.length === 1 ? numbers[0] : null);
  if (!selected) throw new Error('Quo sender could not be resolved. Configure QUO_DEFAULT_PHONE_NUMBER_ID or QUO_FROM_NUMBER.');
  const from = [selected.number, config.from, selected.formattedNumber]
    .map((value) => {
      const raw = String(value || '').trim();
      if (!raw) return '';
      const digits = raw.replace(/\D/g, '');
      if (raw.startsWith('+')) return `+${digits}`;
      if (digits.length === 10) return `+1${digits}`;
      return digits.length >= 8 && digits.length <= 15 ? `+${digits}` : '';
    })
    .find((value) => /^\+[1-9]\d{7,14}$/.test(value)) || '';
  const userId = String(config.userId || selected.users?.[0]?.id || '').trim();
  if (!/^\+[1-9]\d{7,14}$/.test(from)) throw new Error('Quo sender phone number is missing or invalid.');
  if (!userId) throw new Error('Quo sender user could not be resolved. Configure QUO_USER_ID.');
  return { from, userId, phoneNumberId: String(selected.id || config.defaultPhoneNumberId || '').trim() };
}

async function sendQuoText(config, { to, content }) {
  const recipient = normalizeExternalRecipient('text', to);
  const text = String(content || '').trim();
  if (!text || text.length > 1_600) throw new Error('Text content must contain 1 to 1600 characters.');
  const sender = await resolveQuoSender(config);
  const { resp, data } = await fetchJsonWithTimeout(`${config.baseUrl}/v1/messages`, {
    method: 'POST',
    headers: { Authorization: config.apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      content: text,
      from: sender.from,
      to: [recipient],
      userId: sender.userId,
      ...(sender.phoneNumberId ? { phoneNumberId: sender.phoneNumberId } : {}),
    }),
    timeoutMs: 25_000,
  });
  if (!resp.ok) {
    const detail = String(data?.message || data?.error?.message || data?.error || '').trim();
    throw new Error(`Quo message send failed (${resp.status})${detail ? `: ${detail}` : ''}`);
  }
  return {
    provider: 'quo',
    messageId: String(data?.data?.id || '').trim(),
    status: String(data?.data?.status || 'queued').trim(),
    accepted: true,
  };
}

function normalizeExternalActionDraft(input = {}) {
  const raw = input && typeof input === 'object' ? input : {};
  const type = safeEnum(raw.type, ['email', 'text'], 'email');
  const to = normalizeExternalRecipient(type, String(raw.to || raw.recipient || '').trim().slice(0, 500));
  const subject = String(raw.subject || '').trim().slice(0, 300);
  const body = String(raw.body || raw.text || raw.message || '').trim().slice(0, 8_000);
  const projectId = String(raw.projectId || '').trim().slice(0, 160);
  const projectName = String(raw.projectName || '').trim().slice(0, 300);
  const reason = String(raw.reason || raw.approvalReason || '').trim().slice(0, 1_000);
  if (!to) throw new Error('Recipient is required.');
  if (!body) throw new Error('Message body is required.');
  if (type === 'email' && !subject) throw new Error('Email subject is required.');
  const now = nowIso();
  return {
    id: makeId(),
    type,
    to,
    subject,
    body,
    projectId,
    projectName,
    reason: reason || 'External communication requires Mark approval before sending.',
    status: 'pending_approval',
    requiresApproval: true,
    createdAt: now,
    updatedAt: now,
    createdBy: 'marcus',
  };
}

function normalizeExternalActionDrafts(input) {
  const list = Array.isArray(input) ? input : [];
  return list
    .map((item) => item && typeof item === 'object' ? item : null)
    .filter(Boolean)
    .map((item) => ({
      id: String(item.id || '').trim().slice(0, 120) || makeId(),
      type: safeEnum(item.type, ['email', 'text'], 'email'),
      to: String(item.to || '').trim().slice(0, 500),
      subject: String(item.subject || '').trim().slice(0, 300),
      body: String(item.body || '').trim().slice(0, 8_000),
      projectId: String(item.projectId || '').trim().slice(0, 160),
      projectName: String(item.projectName || '').trim().slice(0, 300),
      reason: String(item.reason || '').trim().slice(0, 1_000),
      status: safeEnum(item.status, ['pending_approval', 'approved', 'sending', 'rejected', 'sent', 'failed'], 'pending_approval'),
      requiresApproval: true,
      createdAt: typeof item.createdAt === 'string' ? item.createdAt : nowIso(),
      updatedAt: typeof item.updatedAt === 'string' ? item.updatedAt : '',
      createdBy: String(item.createdBy || 'marcus').trim().slice(0, 100),
      approvedAt: typeof item.approvedAt === 'string' ? item.approvedAt : '',
      approvedBy: String(item.approvedBy || '').trim().slice(0, 100),
      approvalMessage: String(item.approvalMessage || '').trim().slice(0, 1_000),
      rejectedAt: typeof item.rejectedAt === 'string' ? item.rejectedAt : '',
      rejectedBy: String(item.rejectedBy || '').trim().slice(0, 100),
      rejectionMessage: String(item.rejectionMessage || '').trim().slice(0, 1_000),
      sentAt: typeof item.sentAt === 'string' ? item.sentAt : '',
      sendResult: item.sendResult && typeof item.sendResult === 'object' ? item.sendResult : {},
    }))
    .filter((item) => item.to && item.body)
    .slice(-200);
}

async function createExternalActionDraft(input = {}) {
  const draft = normalizeExternalActionDraft(input);
  let created = null;
  writeLock = writeLock.catch(() => {}).then(async () => {
    const settings = await readSettings();
    const existing = normalizeExternalActionDrafts(settings.externalActionDrafts);
    await writeSettings({
      ...settings,
      externalActionDrafts: [...existing, draft].slice(-200),
      updatedAt: nowIso(),
    });
    created = draft;
  });
  await writeLock;
  return created;
}

async function updateExternalAction(id, updater) {
  let updated = null;
  writeLock = writeLock.catch(() => {}).then(async () => {
    const settings = await readSettings();
    const actions = normalizeExternalActionDrafts(settings.externalActionDrafts);
    const index = actions.findIndex((item) => item.id === id);
    if (index < 0) throw Object.assign(new Error('External action draft not found.'), { statusCode: 404 });
    const next = await updater(actions[index], settings);
    if (!next) {
      updated = actions[index];
      return;
    }
    actions[index] = { ...next, updatedAt: nowIso() };
    await writeSettings({ ...settings, externalActionDrafts: actions, updatedAt: nowIso() });
    updated = actions[index];
  });
  await writeLock;
  return updated;
}

async function approveExternalAction(id, message) {
  const approvalMessage = String(message || '').trim().slice(0, 1_000);
  if (!isExternalActionApprovalMessage(approvalMessage)) {
    throw Object.assign(new Error('Approval message must explicitly approve this external action.'), { statusCode: 409, approvalRequired: true });
  }
  return updateExternalAction(id, (action) => {
    if (action.status !== 'pending_approval') {
      throw Object.assign(new Error(`External action cannot be approved from ${action.status}.`), { statusCode: 409 });
    }
    return { ...action, status: 'approved', approvedAt: nowIso(), approvedBy: 'mark', approvalMessage };
  });
}

async function persistExternalActionSendResult(id, status, sendResult) {
  return updateExternalAction(id, (action) => ({
    ...action,
    status,
    ...(status === 'sent' ? { sentAt: nowIso() } : {}),
    sendResult: sendResult && typeof sendResult === 'object' ? sendResult : {},
  }));
}

async function sendApprovedExternalAction(id) {
  const settings = await readSettings();
  const existing = normalizeExternalActionDrafts(settings.externalActionDrafts).find((item) => item.id === id);
  if (!existing) throw Object.assign(new Error('External action draft not found.'), { statusCode: 404 });
  if (existing.status === 'sent') return { action: existing, reused: true };
  const retryingApprovedFailure = existing.status === 'failed'
    && Boolean(existing.approvedAt && existing.approvalMessage);
  if (existing.status !== 'approved' && !retryingApprovedFailure) {
    throw Object.assign(new Error(`External action cannot be sent from ${existing.status}. Explicit approval is required first.`), { statusCode: 409, approvalRequired: true });
  }

  const email = getEmailConfig(settings);
  const quo = getQuoOutboundConfig(settings);
  if (existing.type === 'email' && !email.smtpConfigured) {
    throw Object.assign(new Error('SMTP email sending is not configured.'), { statusCode: 503 });
  }
  if (existing.type === 'text' && !quo.configured) {
    throw Object.assign(new Error('Quo outbound text sending is not configured.'), { statusCode: 503 });
  }

  const claimed = await updateExternalAction(id, (action) => {
    if (action.status === 'sent') return null;
    const retryableFailure = action.status === 'failed'
      && Boolean(action.approvedAt && action.approvalMessage);
    if (action.status !== 'approved' && !retryableFailure) {
      throw Object.assign(new Error(`External action cannot be claimed from ${action.status}.`), { statusCode: 409 });
    }
    return { ...action, status: 'sending', sendResult: {} };
  });
  if (claimed.status === 'sent') return { action: claimed, reused: true };

  try {
    let result;
    if (claimed.type === 'email') {
      const smtpResult = await withSmtpTransport(email, async (transport) => transport.sendMail({
        from: email.fromAddress,
        to: claimed.to,
        subject: claimed.subject,
        text: claimed.body,
      }));
      result = {
        provider: 'smtp',
        messageId: String(smtpResult.value?.messageId || '').trim(),
        accepted: Array.isArray(smtpResult.value?.accepted) ? smtpResult.value.accepted : [],
        rejected: Array.isArray(smtpResult.value?.rejected) ? smtpResult.value.rejected : [],
        response: String(smtpResult.value?.response || '').slice(0, 500),
        profile: describeSmtpProfile(smtpResult.profile),
      };
    } else {
      result = await sendQuoText(quo, { to: claimed.to, content: claimed.body });
    }
    const action = await persistExternalActionSendResult(id, 'sent', result);
    return { action, result, reused: false };
  } catch (error) {
    const failure = { provider: claimed.type === 'email' ? 'smtp' : 'quo', error: String(error?.message || error).slice(0, 1_000) };
    await persistExternalActionSendResult(id, 'failed', failure);
    throw Object.assign(new Error(failure.error), { statusCode: 502 });
  }
}

function isExternalCommunicationRequest(message) {
  const text = String(message || '').trim();
  return /\b(email|e-mail|text|sms|message)\b/i.test(text)
    && /\b(send|write|draft|reply|respond|compose|prepare)\b/i.test(text);
}

function isExternalActionApprovalMessage(message) {
  const text = String(message || '').trim();
  return /\b(approve|approved|go ahead|proceed|send it|send this|send that|send (?:the )?(?:email|text|message))\b/i.test(text);
}

function externalActionTargetsMessage(message, action) {
  const text = String(message || '').trim().toLowerCase();
  return [action.id, action.to, action.projectName, action.subject]
    .map((value) => String(value || '').trim().toLowerCase())
    .filter((value) => value.length >= 3)
    .some((value) => text.includes(value));
}

async function maybeApproveAndSendExternalActionFromMessage(message, { approvalAuthorized = false } = {}) {
  if (!isExternalActionApprovalMessage(message)) return null;
  const settings = await readSettings();
  const candidates = normalizeExternalActionDrafts(settings.externalActionDrafts)
    .filter((action) => ['pending_approval', 'approved'].includes(action.status));
  if (!candidates.length) return null;
  const targeted = candidates.filter((action) => externalActionTargetsMessage(message, action));
  const selected = targeted.length === 1 ? targeted[0] : (candidates.length === 1 ? candidates[0] : null);
  if (!selected) {
    const choices = candidates.slice(-8).reverse().map((action) => `- ${action.id}: ${action.type} to ${action.to}${action.subject ? ` - ${action.subject}` : ''}`).join('\n');
    return { ok: false, approvalRequired: true, reply: `I need which message you want me to approve and send.\n${choices}` };
  }
  if (!approvalAuthorized) {
    return {
      ok: false,
      approvalRequired: true,
      reauthenticationRequired: true,
      externalAction: selected,
      reply: 'Sending requires the paired Marcus app or durable admin authentication. The draft remains unsent.',
    };
  }
  try {
    if (selected.status === 'pending_approval') await approveExternalAction(selected.id, message);
    const sent = await sendApprovedExternalAction(selected.id);
    return {
      ok: true,
      externalAction: sent.action,
      reply: `${sent.reused ? 'Already sent' : 'Sent'} ${sent.action.type} to ${sent.action.to}.${sent.action.sendResult?.messageId ? ` Provider receipt: ${sent.action.sendResult.messageId}.` : ''}`,
    };
  } catch (error) {
    return {
      ok: false,
      externalAction: selected,
      providerConfigurationRequired: Number(error?.statusCode) === 503,
      reply: `I recorded the approval, but did not send the ${selected.type}: ${String(error?.message || error)}`,
    };
  }
}

function getExternalMessageDraftToolDefinition() {
  return {
    type: 'function',
    function: {
      name: 'draft_external_message',
      description: 'Create an approval-gated email or text draft. This never sends the message. Use whenever Mark asks to draft, write, reply, email, text, or send an external message.',
      parameters: {
        type: 'object',
        properties: {
          type: { type: 'string', enum: ['email', 'text'] },
          to: { type: 'string', description: 'Email address or E.164 phone number.' },
          subject: { type: 'string', description: 'Required for email; omit for text.' },
          body: { type: 'string' },
          projectId: { type: 'string' },
          projectName: { type: 'string' },
          reason: { type: 'string', description: 'Why this external communication should be sent.' },
        },
        required: ['type', 'to', 'body'],
      },
    },
  };
}

function getRenderCloudConfig(saved = {}) {
  const token = typeof process.env.RENDER_API_KEY === 'string' ? process.env.RENDER_API_KEY.trim() : '';
  const savedToken = typeof saved.renderApiKey === 'string' ? saved.renderApiKey.trim() : '';
  const effectiveToken = token || savedToken;
  return { token: effectiveToken, configured: Boolean(effectiveToken), tokenHint: maskSecretHint(effectiveToken), source: token ? 'env' : savedToken ? 'settings' : 'none' };
}

async function buildMarcusOperatorHealth() {
  const [settings, readiness, missionMemory] = await Promise.all([
    readSettings(),
    operationsEngine.readiness(getBusinessKeyFromContext()),
    missionMemoryStore.list(getBusinessKeyFromContext(), { status: 'active', limit: 100 }),
  ]);
  const ai = await getAiConfig();
  const email = getEmailConfig(settings);
  const quoOutbound = getQuoOutboundConfig(settings);
  const providerConfiguration = getMarcusProviderConfiguration(settings);
  const github = getGitHubCloudConfig(settings);
  const cloudflare = getCloudflareConfig(settings);
  const render = getRenderCloudConfig(settings);
  const desktopAgeMs = desktopRelayCache?.at ? Date.now() - desktopRelayCache.at : null;
  const desktopOnline = Number.isFinite(desktopAgeMs) && desktopAgeMs <= DESKTOP_RELAY_TTL_MS;
  const quoWebhookConfigured = Boolean(
    (typeof process.env.TWILIO_AUTH_TOKEN === 'string' && process.env.TWILIO_AUTH_TOKEN.trim())
    || (typeof process.env.QUO_WEBHOOK_TOKEN === 'string' && process.env.QUO_WEBHOOK_TOKEN.trim())
    || (typeof settings.quoAuthToken === 'string' && settings.quoAuthToken.trim())
  );
  const canAuditAndPrepareCodex = Boolean(readiness.operationEngineInitialized && readiness.projectRegistryAvailable);
  const directCodex = readiness.codex?.directAdapterConfigured === true;
  const codexResultReviewReady = directCodex
    && readiness.codex?.authoritativeResultEvidence === true
    && readiness.codex?.independentResultReviewerConfigured === true
    && Boolean(ai.apiKey);
  const activeMissionMemories = missionMemory.memories || [];
  const missionMemoryReady = activeMissionMemories.some((item) => item.kind === 'mission')
    && activeMissionMemories.some((item) => item.kind === 'standing_instruction');
  return {
    ok: true,
    businessKey: getBusinessKeyFromContext(),
    generatedAt: nowIso(),
    capabilities: {
      missionMemory: {
        available: missionMemoryReady,
        activeCount: activeMissionMemories.length,
        revision: missionMemory.revision,
        updatedAt: missionMemory.updatedAt,
        businessScoped: true,
        persisted: true,
      },
      projectOperator: {
        available: canAuditAndPrepareCodex,
        mode: directCodex ? 'direct_codex' : 'codex_handoff',
        provider: readiness.codex?.provider || readiness.codex?.mode || 'unknown',
        canAuditProjectContext: canAuditAndPrepareCodex,
        canCreateDurableOperation: Boolean(readiness.operationStoreAvailable),
        canStartCodexDirectly: directCodex,
        canPrepareCodexHandoff: canAuditAndPrepareCodex,
        pendingExternalCodexCount: readiness.pendingExternalCodexCount,
      },
      codexResultReview: {
        available: codexResultReviewReady,
        authoritativeEvidence: readiness.codex?.authoritativeResultEvidence === true,
        evidenceSource: readiness.codex?.authoritativeResultEvidence === true ? 'github_api' : 'not_configured',
        independentReviewerConfigured: readiness.codex?.independentResultReviewerConfigured === true,
        failClosed: true,
        targetChecksRemainIndependent: true,
      },
      github: {
        backendTokenConfigured: github.configured,
        ownerConfigured: Boolean(github.owner),
        tokenHint: github.tokenHint,
        source: github.source,
        access: github.configured ? 'server_api' : 'not_configured_for_server',
        readAccess: github.configured,
        approvedMutationPathAvailable: github.configured,
        approvedMutationActions: ['merge_pull_request'],
        mergeSafety: 'registered_repository_exact_head_sha_checks_and_readback',
      },
      cloudflare: {
        backendTokenConfigured: cloudflare.configured,
        accountIdConfigured: Boolean(cloudflare.accountId),
        defaultZoneConfigured: Boolean(cloudflare.defaultZoneId),
        tokenHint: cloudflare.tokenHint,
        source: cloudflare.source,
        access: cloudflare.configured ? 'server_api' : 'not_configured_for_server',
        readAccess: cloudflare.configured,
        approvedMutationPathAvailable: Boolean(cloudflare.configured && cloudflare.accountId),
        approvedMutationActions: ['upsert_dns_record', 'delete_dns_record', 'deploy_worker_version'],
        mutationSafety: 'registered_project_target_drift_check_idempotency_and_readback',
      },
      render: {
        backendTokenConfigured: render.configured,
        tokenHint: render.tokenHint,
        source: render.source,
      },
      openai: {
        configured: Boolean(ai.apiKey),
        source: ai.source,
        model: ai.model,
        keyHint: ai.keyHint,
        realtimeVoice: {
          configured: Boolean(ai.apiKey),
          provider: 'openai_realtime',
          model: MARCUS_REALTIME_MODEL,
          voice: MARCUS_REALTIME_VOICE,
        },
      },
      desktopAgent: {
        relayOnline: desktopOnline,
        relayAgeMs: desktopAgeMs,
        actionQueueInitialized: readiness.desktopQueueInitialized,
      },
      communication: {
        emailReadConfigured: email.imapConfigured,
        emailSendConfigured: email.smtpConfigured,
        emailProviderVerified: providerConfiguration.email.verification?.verified === true,
        emailProviderVerifiedAt: providerConfiguration.email.verification?.verifiedAt || '',
        textSendConfigured: quoOutbound.configured,
        textProviderVerified: providerConfiguration.text.verification?.verified === true,
        textProviderVerifiedAt: providerConfiguration.text.verification?.verifiedAt || '',
        textProvider: quoOutbound.configured ? 'quo' : 'none',
        textWebhookConfigured: quoWebhookConfigured,
        externalSendRequiresApproval: true,
      },
    },
    blockers: [
      !missionMemoryReady ? 'Durable mission memory is missing an active mission or standing instruction.' : '',
      !github.configured ? 'GITHUB_TOKEN is not configured for the Marcus server; GitHub reads rely on route/user tooling instead of backend provider access.' : '',
      !cloudflare.configured ? 'CLOUDFLARE_API_TOKEN is not configured for the Marcus server; Cloudflare reads rely on route/user tooling instead of backend provider access.' : '',
      !ai.apiKey ? 'OpenAI is not configured; AI chat, transcription, and model-assisted drafting will be limited.' : '',
      !directCodex ? 'No direct Codex launch adapter is configured; Marcus can prepare durable handoffs and track registered Codex results, but cannot honestly claim a real session started.' : '',
      directCodex && !codexResultReviewReady ? 'Independent Codex result review is not fully configured with authoritative GitHub evidence and AI review.' : '',
      !desktopOnline ? 'Desktop agent relay is not currently online; local workspace context/actions may be stale or unavailable.' : '',
      !email.smtpConfigured ? 'SMTP is not configured; Marcus can draft and approve email but cannot send it yet.' : '',
      !quoOutbound.configured ? 'Quo outbound API credentials are not configured; Marcus can draft and approve text messages but cannot send them yet.' : '',
    ].filter(Boolean),
  };
}

async function buildMarcusAcceptanceReport({ sessionId = '' } = {}) {
  const health = await buildMarcusOperatorHealth();
  const settings = await readSettings();
  const voice = await realtimeTelemetryStore.acceptance(getBusinessKeyFromContext(), {
    sessionId,
    limit: sessionId ? 1 : 50,
  });
  const voiceSession = sessionId
    ? voice.latest
    : (voice.sessions.find((session) => session.acceptedOnPhysicalDevice) || voice.latest);
  const communication = health.capabilities?.communication || {};
  const acceptedSend = (type, verifiedAt) => {
    const verifiedMs = Date.parse(String(verifiedAt || ''));
    if (!Number.isFinite(verifiedMs)) return null;
    return normalizeExternalActionDrafts(settings.externalActionDrafts)
      .filter((action) => action.type === type && action.status === 'sent' && Date.parse(action.sentAt) >= verifiedMs)
      .filter((action) => type === 'text'
        ? action.sendResult?.provider === 'quo' && action.sendResult?.accepted === true
        : action.sendResult?.provider === 'smtp'
          && (Boolean(action.sendResult?.messageId) || (Array.isArray(action.sendResult?.accepted) && action.sendResult.accepted.length > 0)))
      .sort((a, b) => Date.parse(b.sentAt) - Date.parse(a.sentAt))[0] || null;
  };
  const textSend = acceptedSend('text', communication.textProviderVerifiedAt);
  const emailSend = acceptedSend('email', communication.emailProviderVerifiedAt);
  const gates = {
    missionMemoryReady: health.capabilities?.missionMemory?.available === true,
    projectOperatorReady: health.capabilities?.projectOperator?.canStartCodexDirectly === true,
    codexResultReviewReady: health.capabilities?.codexResultReview?.available === true,
    githubReady: health.capabilities?.github?.backendTokenConfigured === true,
    cloudflareReady: health.capabilities?.cloudflare?.backendTokenConfigured === true,
    openaiReady: health.capabilities?.openai?.configured === true,
    desktopRelayReady: health.capabilities?.desktopAgent?.relayOnline === true,
    textProviderVerified: communication.textSendConfigured === true && communication.textProviderVerified === true,
    emailProviderVerified: communication.emailSendConfigured === true && communication.emailProviderVerified === true,
    approvedTextSendAccepted: Boolean(textSend),
    approvedEmailSendAccepted: Boolean(emailSend),
    physicalAndroidVoiceAccepted: voiceSession?.acceptedOnPhysicalDevice === true,
  };
  const labels = {
    missionMemoryReady: 'Durable mission memory',
    projectOperatorReady: 'Direct Codex operator',
    codexResultReviewReady: 'Independent Codex result review',
    githubReady: 'GitHub',
    cloudflareReady: 'Cloudflare',
    openaiReady: 'OpenAI',
    desktopRelayReady: 'Desktop relay',
    textProviderVerified: 'Quo text provider',
    emailProviderVerified: 'SMTP email provider',
    approvedTextSendAccepted: 'Approved Quo test send',
    approvedEmailSendAccepted: 'Approved SMTP test send',
    physicalAndroidVoiceAccepted: 'Physical Android voice acceptance',
  };
  return {
    ok: true,
    ready: Object.values(gates).every(Boolean),
    generatedAt: nowIso(),
    gates,
    missing: Object.entries(gates).filter(([, value]) => !value).map(([key]) => ({ key, label: labels[key] })),
    voice: {
      privacy: voice.privacy,
      session: voiceSession || null,
    },
    providers: {
      text: {
        configured: communication.textSendConfigured === true,
        verified: communication.textProviderVerified === true,
        verifiedAt: communication.textProviderVerifiedAt || '',
        acceptedSend: textSend ? { actionId: textSend.id, sentAt: textSend.sentAt, provider: 'quo' } : null,
      },
      email: {
        configured: communication.emailSendConfigured === true,
        verified: communication.emailProviderVerified === true,
        verifiedAt: communication.emailProviderVerifiedAt || '',
        acceptedSend: emailSend ? { actionId: emailSend.id, sentAt: emailSend.sentAt, provider: 'smtp' } : null,
      },
    },
  };
}

async function githubApi(pathPart, { method = 'GET', body, timeoutMs = 20_000 } = {}) {
  const cfg = getGitHubCloudConfig(await readSettings());
  if (!cfg.token) throw new Error('GITHUB_TOKEN is not configured.');
  const cleanPath = String(pathPart || '').startsWith('/') ? pathPart : `/${pathPart || ''}`;
  const testBase = process.env.NODE_ENV === 'test' ? String(process.env.MARCUS_TEST_GITHUB_API_BASE_URL || '').trim().replace(/\/$/, '') : '';
  const githubApiBase = /^http:\/\/(?:127\.0\.0\.1|localhost)(?::\d+)?(?:\/|$)/.test(testBase) ? testBase : 'https://api.github.com';
  const { resp, data } = await fetchJsonWithTimeout(`${githubApiBase}${cleanPath}`, {
    timeoutMs,
    method,
    headers: {
      Authorization: `Bearer ${cfg.token}`,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  if (!resp.ok) {
    const error = new Error(data?.message || `GitHub API failed (${resp.status}).`);
    error.status = resp.status;
    throw error;
  }
  return data;
}

async function githubOperationsReadAdapter({ repository, action, input = {} }) {
  const [owner, repo] = String(repository || '').split('/');
  if (!/^[A-Za-z0-9_.-]+$/.test(owner || '') || !/^[A-Za-z0-9_.-]+$/.test(repo || '')) throw new Error('Registered GitHub repository is invalid.');
  const base = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`;
  const ref = String(input.ref || '').trim();
  let data;
  if (action === 'repository_metadata' || action === 'default_branch') {
    data = await githubApi(base);
    return action === 'default_branch'
      ? { repository, defaultBranch: data?.default_branch || '' }
      : { repository, id: data?.id, name: data?.name, fullName: data?.full_name, private: Boolean(data?.private), defaultBranch: data?.default_branch, archived: Boolean(data?.archived), updatedAt: data?.updated_at, pushedAt: data?.pushed_at, htmlUrl: data?.html_url };
  }
  if (action === 'branch_metadata') {
    data = await githubApi(`${base}/branches/${encodeURIComponent(ref)}`);
    return { repository, name: data?.name, protected: Boolean(data?.protected), commit: { sha: data?.commit?.sha, url: data?.commit?.html_url } };
  }
  if (action === 'commit_metadata') {
    data = await githubApi(`${base}/commits/${encodeURIComponent(ref)}`);
    return { repository, sha: data?.sha, htmlUrl: data?.html_url, message: String(data?.commit?.message || '').slice(0, 8_000), author: data?.commit?.author, committer: data?.commit?.committer, files: (Array.isArray(data?.files) ? data.files : []).slice(0, 100).map((file) => ({ filename: file.filename, status: file.status, additions: file.additions, deletions: file.deletions, changes: file.changes })) };
  }
  if (action === 'repository_file') {
    const filePath = String(input.path || '').split('/').map(encodeURIComponent).join('/');
    const qs = ref ? `?ref=${encodeURIComponent(ref)}` : '';
    data = await githubApi(`${base}/contents/${filePath}${qs}`);
    if (Array.isArray(data)) return { repository, type: 'directory', entries: data.slice(0, 100).map((item) => ({ name: item.name, path: item.path, type: item.type, size: item.size, sha: item.sha })) };
    const encoded = String(data?.content || '').replace(/\s+/g, '');
    return { repository, type: data?.type || 'file', path: data?.path || input.path, sha: data?.sha, size: data?.size || 0, content: encoded ? Buffer.from(encoded, 'base64').toString('utf8').slice(0, 80_000) : '' };
  }
  if (action === 'compare_refs') {
    data = await githubApi(`${base}/compare/${encodeURIComponent(input.base)}...${encodeURIComponent(input.head)}`);
    return { repository, status: data?.status, aheadBy: data?.ahead_by, behindBy: data?.behind_by, totalCommits: data?.total_commits, commits: (Array.isArray(data?.commits) ? data.commits : []).slice(0, 100).map((commit) => ({ sha: commit.sha, message: String(commit?.commit?.message || '').slice(0, 2_000), htmlUrl: commit.html_url })), files: (Array.isArray(data?.files) ? data.files : []).slice(0, 100).map((file) => ({ filename: file.filename, status: file.status, additions: file.additions, deletions: file.deletions, changes: file.changes })) };
  }
  if (action === 'pull_request_metadata') {
    data = await githubApi(`${base}/pulls/${Number(input.pullNumber)}`);
    return { repository, number: data?.number, title: data?.title, state: data?.state, draft: Boolean(data?.draft), merged: Boolean(data?.merged), mergeable: data?.mergeable, base: data?.base?.ref, head: data?.head?.ref, headSha: data?.head?.sha, author: data?.user?.login, htmlUrl: data?.html_url, updatedAt: data?.updated_at };
  }
  if (action === 'workflow_status') {
    const query = new URLSearchParams({ per_page: String(Math.max(1, Math.min(100, Number(input.limit) || 50))) });
    if (ref) query.set('branch', ref);
    data = await githubApi(`${base}/actions/runs?${query}`);
    return { repository, totalCount: data?.total_count || 0, runs: (Array.isArray(data?.workflow_runs) ? data.workflow_runs : []).slice(0, 100).map((run) => ({ id: run.id, name: run.name, event: run.event, status: run.status, conclusion: run.conclusion, branch: run.head_branch, sha: run.head_sha, htmlUrl: run.html_url, createdAt: run.created_at, updatedAt: run.updated_at })) };
  }
  throw new Error('Unsupported GitHub read action.');
}

function githubChecksAreSettled(checks, statuses) {
  const checkRuns = Array.isArray(checks?.check_runs) ? checks.check_runs : [];
  const contexts = Array.isArray(statuses?.statuses) ? statuses.statuses : [];
  const unsuccessfulChecks = checkRuns.filter((run) => run?.status !== 'completed'
    || !['success', 'neutral', 'skipped'].includes(String(run?.conclusion || '').toLowerCase()));
  const unsuccessfulStatuses = contexts.filter((status) => String(status?.state || '').toLowerCase() !== 'success');
  return {
    settled: unsuccessfulChecks.length === 0 && unsuccessfulStatuses.length === 0,
    totalCheckRuns: checkRuns.length,
    totalStatuses: contexts.length,
    unsuccessfulChecks: unsuccessfulChecks.map((run) => ({ id: run.id, name: run.name, status: run.status, conclusion: run.conclusion })),
    unsuccessfulStatuses: unsuccessfulStatuses.map((status) => ({ id: status.id, context: status.context, state: status.state })),
  };
}

function providerStateUnknown(message, cause) {
  const error = new Error(message);
  error.code = 'PROVIDER_STATE_UNKNOWN';
  if (cause) error.cause = cause;
  return error;
}

async function githubOperationsWriteAdapter({ repository, action, input = {}, operationId = '' }) {
  if (action === 'create_repository') {
    const cfg = getGitHubCloudConfig(await readSettings());
    const owner = String(input.owner || '').trim();
    const name = String(input.name || '').trim();
    if (!cfg.owner || owner.toLowerCase() !== String(cfg.owner).toLowerCase()) {
      throw new Error('The approved repository owner does not match the configured GitHub account.');
    }
    if (!/^[A-Za-z0-9_.-]+$/.test(name)) throw new Error('The approved GitHub repository name is invalid.');
    let existing = null;
    try { existing = await githubApi(`/repos/${encodeURIComponent(owner)}/${encodeURIComponent(name)}`); }
    catch (error) { if (Number(error?.status) !== 404) throw error; }
    if (existing) throw new Error(`GitHub repository ${owner}/${name} already exists; Marcus refused to adopt or overwrite it as a new project.`);
    await githubApi('/user/repos', {
        method: 'POST',
        body: {
          name,
          description: String(input.description || '').trim().slice(0, 1_000),
          private: input.private !== false,
          has_issues: true,
          has_projects: true,
          has_wiki: false,
          auto_init: false,
        },
    });
    const verified = await githubApi(`/repos/${encodeURIComponent(owner)}/${encodeURIComponent(name)}`);
    if (!verified?.id || String(verified.full_name || '').toLowerCase() !== `${owner}/${name}`.toLowerCase()
      || Boolean(verified.private) !== (input.private !== false)) {
      throw new Error('GitHub did not confirm the exact approved repository after creation.');
    }
    return {
      action,
      operationId,
      verified: true,
      idempotentReplay: false,
      repository: verified.full_name,
      repositoryId: verified.id,
      private: Boolean(verified.private),
      defaultBranch: verified.default_branch || 'main',
      htmlUrl: verified.html_url,
      cloneUrl: verified.clone_url,
    };
  }
  if (action !== 'merge_pull_request') throw new Error('Unsupported GitHub write action.');
  const [owner, repo] = String(repository || '').split('/');
  if (!/^[A-Za-z0-9_.-]+$/.test(owner || '') || !/^[A-Za-z0-9_.-]+$/.test(repo || '')) throw new Error('Registered GitHub repository is invalid.');
  const pullNumber = Number(input.pullNumber);
  const expectedHeadSha = String(input.expectedHeadSha || '').toLowerCase();
  const base = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`;
  const pullPath = `${base}/pulls/${pullNumber}`;
  let pull = await githubApi(pullPath);
  if (String(pull?.head?.sha || '').toLowerCase() !== expectedHeadSha) {
    throw new Error(`Pull request head changed. Expected ${expectedHeadSha}, found ${pull?.head?.sha || 'unknown'}; merge was refused.`);
  }
  if (pull?.merged === true) {
    return {
      action, repository, pullNumber, expectedHeadSha, idempotentReplay: true, verified: true,
      merged: true, mergeCommitSha: pull.merge_commit_sha, htmlUrl: pull.html_url,
    };
  }
  if (pull?.state !== 'open' || pull?.draft === true) throw new Error('The pull request is not an open, non-draft merge target.');
  if (pull?.mergeable === null) {
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    pull = await githubApi(pullPath);
  }
  if (pull?.mergeable === false) throw new Error(`GitHub reports that pull request #${pullNumber} is not mergeable.`);
  const [checks, statuses] = await Promise.all([
    githubApi(`${base}/commits/${encodeURIComponent(expectedHeadSha)}/check-runs?per_page=100`),
    githubApi(`${base}/commits/${encodeURIComponent(expectedHeadSha)}/status?per_page=100`),
  ]);
  const checkEvidence = githubChecksAreSettled(checks, statuses);
  if (!checkEvidence.settled) {
    throw new Error(`Pull request checks are not all settled successfully: ${JSON.stringify(checkEvidence)}`);
  }
  const mergeBody = {
    sha: expectedHeadSha,
    merge_method: ['merge', 'squash', 'rebase'].includes(input.mergeMethod) ? input.mergeMethod : 'squash',
    ...(input.commitTitle ? { commit_title: input.commitTitle } : {}),
    ...(input.commitMessage ? { commit_message: input.commitMessage } : {}),
  };
  const merged = await githubApi(`${pullPath}/merge`, { method: 'PUT', body: mergeBody });
  if (merged?.merged !== true || !merged?.sha) throw new Error(merged?.message || 'GitHub did not confirm the pull request merge.');
  let verified;
  try { verified = await githubApi(pullPath); }
  catch (error) { throw providerStateUnknown('GitHub accepted the merge but its final state could not be read back.', error); }
  if (verified?.merged !== true || verified?.merge_commit_sha !== merged.sha) {
    throw new Error('GitHub accepted the merge request but authoritative read-back did not confirm the same merge commit.');
  }
  return {
    action, repository, pullNumber, expectedHeadSha, operationId, idempotentReplay: false, verified: true,
    merged: true, mergeCommitSha: verified.merge_commit_sha, htmlUrl: verified.html_url, checks: checkEvidence,
  };
}

async function cloudflareApi(pathPart, { method = 'GET', body, timeoutMs = 20_000 } = {}) {
  const cfg = getCloudflareConfig(await readSettings());
  if (!cfg.token) throw new Error('CLOUDFLARE_API_TOKEN is not configured.');
  const cleanPath = String(pathPart || '').startsWith('/') ? pathPart : `/${pathPart || ''}`;
  const testBase = process.env.NODE_ENV === 'test' ? String(process.env.MARCUS_TEST_CLOUDFLARE_API_BASE_URL || '').trim().replace(/\/$/, '') : '';
  const cloudflareApiBase = /^http:\/\/(?:127\.0\.0\.1|localhost)(?::\d+)?(?:\/|$)/.test(testBase) ? testBase : 'https://api.cloudflare.com/client/v4';
  const { resp, data } = await fetchJsonWithTimeout(`${cloudflareApiBase}${cleanPath}`, {
    timeoutMs,
    method,
    headers: {
      Authorization: `Bearer ${cfg.token}`,
      'Content-Type': 'application/json',
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  if (!resp.ok || data?.success === false) {
    const msg = Array.isArray(data?.errors) && data.errors[0]?.message ? data.errors[0].message : `Cloudflare API failed (${resp.status}).`;
    const error = new Error(msg);
    error.status = resp.status;
    throw error;
  }
  return data;
}

function cloudflareResultRows(data, nestedKey = '') {
  const result = data?.result;
  if (Array.isArray(result)) return result;
  if (nestedKey && Array.isArray(result?.[nestedKey])) return result[nestedKey];
  return Array.isArray(result?.items) ? result.items : [];
}

function cloudflareDnsRecordView(record) {
  if (!record) return null;
  return {
    id: record.id, type: record.type, name: record.name, content: record.content,
    ttl: record.ttl, proxied: Boolean(record.proxied), priority: Number(record.priority || 0),
    comment: record.comment || '', modifiedOn: record.modified_on,
  };
}

function cloudflareDnsStateMatches(record, desired) {
  return Boolean(record)
    && String(record.type || '').toUpperCase() === desired.recordType
    && String(record.name || '').toLowerCase().replace(/\.$/, '') === desired.name
    && String(record.content || '') === desired.content
    && Number(record.ttl || 1) === desired.ttl
    && Boolean(record.proxied) === desired.proxied
    && (!['MX'].includes(desired.recordType) || Number(record.priority || 0) === desired.priority);
}

function urlHostname(value) {
  try { return new URL(value).hostname.toLowerCase(); } catch { return ''; }
}

async function validateCloudflareZoneTarget(input, registryTarget) {
  const deployments = registryTarget?.deployments || {};
  const zoneData = await cloudflareApi(`/zones/${encodeURIComponent(input.zoneId)}`);
  const zone = zoneData?.result;
  const zoneName = String(zone?.name || '').toLowerCase();
  const registeredZoneId = String(deployments.cloudflareZoneId || '');
  const registeredZoneName = String(deployments.cloudflareZoneName || '').toLowerCase();
  const productionHost = urlHostname(deployments.productionUrl);
  const bound = (registeredZoneId && registeredZoneId === input.zoneId)
    || (registeredZoneName && registeredZoneName === zoneName)
    || (productionHost && (productionHost === zoneName || productionHost.endsWith(`.${zoneName}`)));
  if (!zone?.id || !zoneName || !bound) throw new Error('The Cloudflare zone is not bound to the resolved project registry target.');
  if (!(input.name === zoneName || input.name.endsWith(`.${zoneName}`))) throw new Error('The DNS record name is outside the bound Cloudflare zone.');
  return { id: zone.id, name: zone.name, accountId: zone.account?.id || '' };
}

async function cloudflareDnsMutation({ action, input, registryTarget }) {
  const zone = await validateCloudflareZoneTarget(input, registryTarget);
  const query = new URLSearchParams({ type: input.recordType, name: input.name, per_page: '100' });
  const listed = await cloudflareApi(`/zones/${encodeURIComponent(input.zoneId)}/dns_records?${query}`);
  const matches = cloudflareResultRows(listed).filter((record) =>
    String(record.type || '').toUpperCase() === input.recordType
    && String(record.name || '').toLowerCase().replace(/\.$/, '') === input.name);
  let current = input.recordId ? matches.find((record) => record.id === input.recordId) : (matches.length === 1 ? matches[0] : null);
  if (!input.recordId && matches.length > 1) throw new Error('Multiple DNS records match this type and name; an exact record ID is required.');
  if (action === 'upsert_dns_record' && input.recordId && !current) {
    throw new Error('The exact DNS record ID no longer exists under the approved type and name; update was refused instead of creating a replacement.');
  }

  if (action === 'delete_dns_record') {
    if (!current) {
      const expectedStillExists = matches.some((record) => String(record.content || '') === input.content);
      if (expectedStillExists) throw new Error('The expected DNS value still exists under a different record ID; deletion was refused.');
      return { action, zone, recordId: input.recordId, idempotentReplay: true, verified: true, deleted: true };
    }
    if (!cloudflareDnsStateMatches(current, input)) throw new Error('The DNS record no longer matches the exact approved type, name, content, TTL, proxy, and priority state.');
    const before = cloudflareDnsRecordView(current);
    await cloudflareApi(`/zones/${encodeURIComponent(input.zoneId)}/dns_records/${encodeURIComponent(input.recordId)}`, { method: 'DELETE' });
    let afterList;
    try { afterList = await cloudflareApi(`/zones/${encodeURIComponent(input.zoneId)}/dns_records?${query}`); }
    catch (error) { throw providerStateUnknown('Cloudflare accepted the DNS deletion but its final state could not be read back.', error); }
    const remains = cloudflareResultRows(afterList).some((record) => record.id === input.recordId);
    if (remains) throw new Error('Cloudflare accepted the delete request but read-back still found the record.');
    return { action, zone, before, recordId: input.recordId, idempotentReplay: false, verified: true, deleted: true };
  }

  if (current && cloudflareDnsStateMatches(current, input)) {
    return { action, zone, before: cloudflareDnsRecordView(current), after: cloudflareDnsRecordView(current), idempotentReplay: true, verified: true };
  }
  const body = {
    type: input.recordType, name: input.name, content: input.content, ttl: input.ttl, proxied: input.proxied,
    ...(input.comment ? { comment: input.comment } : {}),
    ...(input.recordType === 'MX' ? { priority: input.priority } : {}),
  };
  const before = cloudflareDnsRecordView(current);
  const changed = current
    ? await cloudflareApi(`/zones/${encodeURIComponent(input.zoneId)}/dns_records/${encodeURIComponent(current.id)}`, { method: 'PUT', body })
    : await cloudflareApi(`/zones/${encodeURIComponent(input.zoneId)}/dns_records`, { method: 'POST', body });
  const changedId = changed?.result?.id;
  if (!changedId) throw new Error('Cloudflare did not return a DNS record ID.');
  let readBack;
  try { readBack = await cloudflareApi(`/zones/${encodeURIComponent(input.zoneId)}/dns_records/${encodeURIComponent(changedId)}`); }
  catch (error) { throw providerStateUnknown('Cloudflare accepted the DNS change but its final state could not be read back.', error); }
  if (!cloudflareDnsStateMatches(readBack?.result, input)) throw new Error('Cloudflare accepted the DNS change but read-back did not match the approved state.');
  return { action, zone, before, after: cloudflareDnsRecordView(readBack.result), idempotentReplay: false, verified: true };
}

async function cloudflareWorkerDeployment({ input, registryTarget, operationId }) {
  const cfg = getCloudflareConfig(await readSettings());
  const deployments = registryTarget?.deployments || {};
  if (!cfg.accountId || input.accountId !== cfg.accountId) throw new Error('The Worker account does not match Marcus\'s configured Cloudflare account.');
  if (deployments.cloudflareAccountId && input.accountId !== deployments.cloudflareAccountId) throw new Error('The Worker account does not match the project registry target.');
  const registeredScript = String(deployments.cloudflareProject || '').toLowerCase();
  const productionHost = urlHostname(deployments.productionUrl);
  const bound = registeredScript === input.scriptName
    || (productionHost.endsWith('.workers.dev') && productionHost.split('.')[0] === input.scriptName);
  if (!bound) throw new Error('The Worker script is not bound to the resolved project registry target.');

  const root = `/accounts/${encodeURIComponent(input.accountId)}/workers/scripts/${encodeURIComponent(input.scriptName)}`;
  const [versionData, currentData] = await Promise.all([
    cloudflareApi(`${root}/versions/${encodeURIComponent(input.versionId)}`),
    cloudflareApi(`${root}/deployments`),
  ]);
  if (versionData?.result?.id !== input.versionId) throw new Error('The target Worker version could not be verified.');
  const current = cloudflareResultRows(currentData, 'deployments')[0];
  if (!current?.id) throw new Error('The current Worker deployment could not be determined.');
  if (current.id !== input.expectedCurrentDeploymentId) {
    throw new Error(`The Worker deployment changed after preparation. Expected ${input.expectedCurrentDeploymentId}, found ${current.id}; deployment was refused.`);
  }
  const currentVersions = Array.isArray(current.versions) ? current.versions : [];
  if (currentVersions.length === 1 && currentVersions[0]?.version_id === input.versionId && Number(currentVersions[0]?.percentage) === 100) {
    return { action: 'deploy_worker_version', scriptName: input.scriptName, deploymentId: current.id, versionId: input.versionId, idempotentReplay: true, verified: true };
  }
  const created = await cloudflareApi(`${root}/deployments`, {
    method: 'POST',
    body: {
      strategy: 'percentage', versions: [{ version_id: input.versionId, percentage: 100 }],
      annotations: {
        'workers/message': input.message || `Marcus operation ${operationId}`,
        'workers/triggered_by': 'marcus-approved-operation',
      },
    },
  });
  const deploymentId = created?.result?.id;
  if (!deploymentId) throw new Error('Cloudflare did not return a Worker deployment ID.');
  let readBack;
  try { readBack = await cloudflareApi(`${root}/deployments/${encodeURIComponent(deploymentId)}`); }
  catch (error) { throw providerStateUnknown('Cloudflare accepted the Worker deployment but its final state could not be read back.', error); }
  const versions = Array.isArray(readBack?.result?.versions) ? readBack.result.versions : [];
  if (readBack?.result?.id !== deploymentId || versions.length !== 1
    || versions[0]?.version_id !== input.versionId || Number(versions[0]?.percentage) !== 100) {
    throw new Error('Cloudflare accepted the deployment but read-back did not confirm the approved version at 100%.');
  }
  return { action: 'deploy_worker_version', scriptName: input.scriptName, priorDeploymentId: current.id, deploymentId, versionId: input.versionId, idempotentReplay: false, verified: true };
}

async function cloudflareOperationsWriteAdapter({ action, input = {}, registryTarget = {}, operationId = '' }) {
  if (action === 'deploy_worker_version') return cloudflareWorkerDeployment({ input, registryTarget, operationId });
  if (action === 'upsert_dns_record' || action === 'delete_dns_record') return cloudflareDnsMutation({ action, input, registryTarget });
  throw new Error('Unsupported Cloudflare write action.');
}

async function inspectGitHubPullRequest(ownerValue, repoValue, pullNumberValue) {
  const owner = String(ownerValue || '').trim();
  const repo = String(repoValue || '').trim();
  const pullNumber = Number(pullNumberValue);
  if (!/^[A-Za-z0-9_.-]+$/.test(owner) || !/^[A-Za-z0-9_.-]+$/.test(repo) || !Number.isSafeInteger(pullNumber) || pullNumber < 1) {
    throw new Error('A valid owner, repository, and pull request number are required.');
  }
  const base = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`;
  const pull = await githubApi(`${base}/pulls/${pullNumber}`);
  const headSha = String(pull?.head?.sha || '');
  const [checks, statuses] = headSha ? await Promise.all([
    githubApi(`${base}/commits/${encodeURIComponent(headSha)}/check-runs?per_page=100`),
    githubApi(`${base}/commits/${encodeURIComponent(headSha)}/status?per_page=100`),
  ]) : [{ check_runs: [] }, { statuses: [] }];
  return {
    repository: `${owner}/${repo}`, pullNumber, title: pull?.title || '', state: pull?.state,
    draft: Boolean(pull?.draft), merged: Boolean(pull?.merged), mergeable: pull?.mergeable,
    mergeableState: pull?.mergeable_state, base: pull?.base?.ref, head: pull?.head?.ref, headSha,
    htmlUrl: pull?.html_url, checks: githubChecksAreSettled(checks, statuses),
  };
}

async function cloudflareWorkerInspection(kind, scriptNameValue = '') {
  const cfg = getCloudflareConfig(await readSettings());
  if (!cfg.accountId) throw new Error('CLOUDFLARE_ACCOUNT_ID is not configured.');
  if (kind === 'workers') {
    const data = await cloudflareApi(`/accounts/${encodeURIComponent(cfg.accountId)}/workers/scripts`);
    return {
      accountId: cfg.accountId,
      workers: cloudflareResultRows(data).slice(0, 200).map((worker) => ({
        id: worker.id, createdOn: worker.created_on, modifiedOn: worker.modified_on,
        compatibilityDate: worker.compatibility_date, usageModel: worker.usage_model,
      })),
    };
  }
  const scriptName = String(scriptNameValue || '').trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9_-]{0,62}$/.test(scriptName)) throw new Error('A valid Worker script name is required.');
  const root = `/accounts/${encodeURIComponent(cfg.accountId)}/workers/scripts/${encodeURIComponent(scriptName)}`;
  if (kind === 'versions') {
    const data = await cloudflareApi(`${root}/versions?deployable=true&per_page=50`);
    return {
      accountId: cfg.accountId, scriptName,
      versions: cloudflareResultRows(data).slice(0, 50).map((version) => ({
        id: version.id, number: version.number, createdOn: version.metadata?.created_on,
        source: version.metadata?.source, authorEmail: version.metadata?.author_email,
        message: version.annotations?.['workers/message'] || version.metadata?.annotations?.['workers/message'] || '',
      })),
    };
  }
  if (kind === 'deployments') {
    const data = await cloudflareApi(`${root}/deployments`);
    return {
      accountId: cfg.accountId, scriptName,
      deployments: cloudflareResultRows(data, 'deployments').slice(0, 50).map((deployment) => ({
        id: deployment.id, createdOn: deployment.created_on, source: deployment.source,
        versions: Array.isArray(deployment.versions) ? deployment.versions.map((version) => ({ versionId: version.version_id, percentage: version.percentage })) : [],
        message: deployment.annotations?.['workers/message'] || '',
      })),
    };
  }
  throw new Error('Unsupported Cloudflare Worker inspection.');
}

async function renderApi(pathPart, { method = 'GET', body, timeoutMs = 20_000 } = {}) {
  const cfg = getRenderCloudConfig(await readSettings());
  if (!cfg.token) throw new Error('RENDER_API_KEY is not configured.');
  const cleanPath = String(pathPart || '').startsWith('/') ? pathPart : `/${pathPart || ''}`;
  const { resp, data } = await fetchJsonWithTimeout(`https://api.render.com/v1${cleanPath}`, {
    timeoutMs,
    method,
    headers: {
      Authorization: `Bearer ${cfg.token}`,
      Accept: 'application/json',
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  if (!resp.ok) throw new Error(data?.message || `Render API failed (${resp.status}).`);
  return data;
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
    disableFileAccess: true,
    disableUrlAccess: true,
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

async function aiChatCompletion({ routeKey, messages, tools, tool_choice, response_format, timeoutMs = 30_000 }) {
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
  if (response_format) body.response_format = response_format;

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
  delete clone.quoApiKey;
  delete clone.ghlApiKey;
  delete clone.githubToken;
  delete clone.cloudflareApiToken;
  delete clone.renderApiKey;
  delete clone.externalActionDrafts;
  delete clone.marcusProviderVerification;
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

  writeLock = writeLock.catch(() => {}).then(() => withBusinessKey(targetBusinessKey, async () => {
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
  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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
  await replaceFileAtomically(tmpFile, file);
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

function findProjectForDesktopContext(store, desktopData) {
  const projects = Array.isArray(store?.projects) ? store.projects : [];
  const ws = desktopData?.workspace && typeof desktopData.workspace === 'object' ? desktopData.workspace : {};
  const title = String(desktopData?.windowTitle || '').toLowerCase();
  const wsPath = String(ws?.workspacePath || '').trim().toLowerCase().replace(/\\/g, '/');
  const folder = String(ws?.folderName || '').trim().toLowerCase();

  for (const project of projects) {
    const pPath = String(project?.workspacePath || '').trim().toLowerCase().replace(/\\/g, '/');
    if (pPath && wsPath && (pPath === wsPath || wsPath.endsWith('/' + pPath.split('/').pop()))) return project;
  }
  for (const project of projects) {
    const name = String(project?.name || '').trim().toLowerCase();
    if (!name) continue;
    if (folder && (folder.includes(name) || name.includes(folder))) return project;
    if (title && title.includes(name)) return project;
  }
  return null;
}

function buildTeamMessageDraft({ project, signal, teamMember }) {
  const projectName = String(project?.name || '').trim() || 'the project';
  const owner = String(teamMember?.name || project?.accountManagerName || '').trim();
  const opener = owner ? `${owner},` : 'Team,';
  const ask = String(signal || '').replace(/\s+/g, ' ').trim();
  const shortAsk = ask ? previewTextServer(ask, 160) : `Please check ${projectName} and confirm the next move.`;
  return `${opener} quick check on ${projectName}: ${shortAsk}\n\nCan you confirm owner, next step, and ETA?`;
}

function buildMarcusActiveBriefForStore({ store, settings, businessKey, businessName, desktopData, nowMs = Date.now() }) {
  const s = store && typeof store === 'object' ? store : EMPTY_STORE;
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const tasks = Array.isArray(s.tasks) ? s.tasks : [];
  const inbox = Array.isArray(s.inboxItems) ? s.inboxItems : [];
  const today = new Date(nowMs).toISOString().slice(0, 10);
  const realConversationSources = new Set(['fireflies', 'zoom', 'openphone', 'slack', 'email', 'sms', 'quo']);
  const currentConversationCutoffMs = nowMs - (10 * MS_PER_DAY);
  const freshConversationCutoffMs = nowMs - (3 * MS_PER_DAY);
  const recentProjectCutoffMs = nowMs - (14 * MS_PER_DAY);
  const activeProject = findProjectForDesktopContext(s, desktopData);
  const activeProjectId = String(activeProject?.id || '').trim();
  const snapshot = collectMarcusRelevantSnapshot(s, { today, nowMs, currentProjectId: activeProjectId });

  const openByProject = new Map();
  for (const task of tasks) {
    if (isClosedTaskStatus(task?.status)) continue;
    const key = String(task?.projectId || task?.project || '').trim();
    if (!key) continue;
    const list = openByProject.get(key) || [];
    list.push(task);
    openByProject.set(key, list);
  }

  const projectCards = [];
  for (const project of projects) {
    if (isClosedProjectStatus(project?.status)) continue;
    const linkedTasks = getLinkedProjectTasks(s, project).filter((task) => !isClosedTaskStatus(task?.status));
    const linkedInbox = getLinkedProjectInboxItems(s, project).filter((item) => {
      const status = String(item?.status || '').trim().toLowerCase();
      return status === 'new' || status === 'triaged' || !status;
    });
    const activityMs = computeProjectLastActivityMs(s, project, linkedTasks, linkedInbox);
    const recentLinkedInbox = linkedInbox.filter((item) => {
      const source = String(item?.source || item?.channel || '').trim().toLowerCase();
      const rawText = String(item?.text || item?.body || '').toLowerCase();
      if (rawText.includes('inboxcreate') || rawText.includes('smoke')) return false;
      if (source && !realConversationSources.has(source)) return false;
      const ts = Math.max(parseTrackerTime(item?.updatedAt), parseTrackerTime(item?.createdAt));
      return ts >= currentConversationCutoffMs;
    });
    const dueDate = normalizeTrackerDueDate(project?.dueDate);
    const overdue = Boolean(dueDate && dueDate < today);
    const dueSoon = Boolean(dueDate && dueDate >= today && dueDate <= (addDaysToYmd(today, 7) || today));
    const urgentTasks = linkedTasks.filter((task) => {
      const due = normalizeTrackerDueDate(task?.dueDate);
      return Number(task?.priority) === 1 || (due && due <= today) || String(task?.status || '').toLowerCase() === 'urgent';
    });
    const isActive = activeProjectId && String(project?.id || '') === activeProjectId;
    const staleDays = activityMs > 0 ? Math.floor((nowMs - activityMs) / MS_PER_DAY) : 999;
    const hasRecentProjectActivity = activityMs >= recentProjectCutoffMs;
    const hasRecentConversation = recentLinkedInbox.length > 0;
    const belongsInNow = Boolean(
      isActive ||
      hasRecentProjectActivity ||
      hasRecentConversation ||
      (dueSoon && (hasRecentProjectActivity || hasRecentConversation)) ||
      (urgentTasks.length && staleDays <= 14)
    );
    if (!belongsInNow) continue;

    let score = 0;
    if (isActive) score += 100;
    if (overdue && (isActive || hasRecentConversation || staleDays <= 14)) score += 18;
    if (dueSoon) score += 24;
    score += Math.min(28, urgentTasks.length * 9);
    score += Math.min(24, recentLinkedInbox.length * 8);
    if (activityMs > 0) score += Math.max(0, 18 - Math.floor((nowMs - activityMs) / MS_PER_DAY));

    projectCards.push({
      id: String(project?.id || ''),
      name: String(project?.name || '').trim(),
      status: String(project?.status || '').trim(),
      dueDate,
      businessKey,
      businessName,
      score,
      reason: isActive
        ? 'Active in your current window'
        : hasRecentConversation
          ? 'Recent client conversation needs attention'
          : urgentTasks.length
            ? 'Current task pressure'
            : dueSoon
              ? 'Due soon and recently active'
              : 'Recent project activity',
      openTasks: linkedTasks.length,
      urgentTasks: urgentTasks.length,
      inboxCount: recentLinkedInbox.length,
      lastActivityAt: activityMs ? new Date(activityMs).toISOString() : '',
    });
  }

  projectCards.sort((a, b) => (b.score - a.score) || String(a.dueDate || '9999-12-31').localeCompare(String(b.dueDate || '9999-12-31')));

  const conversationCards = [];
  for (const item of inbox) {
    const status = String(item?.status || '').trim().toLowerCase();
    if (status && !['new', 'triaged'].includes(status)) continue;
    const sourceLower = String(item?.source || '').trim().toLowerCase();
    const idLower = String(item?.id || '').trim().toLowerCase();
    const rawTextLower = String(item?.text || item?.body || '').trim().toLowerCase();
    if (sourceLower === 'marty' || sourceLower === 'marcus' || idLower.includes(':brief:') || idLower.includes('smoke') || rawTextLower.includes('smoke')) continue;
    if (rawTextLower.includes('inboxcreate')) continue;
    if (shouldSuppressInboxRadarItem(item, settings || {})) continue;
    const signal = extractInboxSignalText(item);
    if (!signal) continue;
    const projectId = String(item?.projectId || '').trim();
    const project = projects.find((p) => String(p?.id || '') === projectId) || null;
    const recommendation = buildMarcusInboxRecommendation(s, item);
    const whoName = String(item?.contactName || item?.fromName || item?.sender || item?.fromNumber || recommendation?.who?.name || '').trim();
    if (!whoName && !projectId && !realConversationSources.has(sourceLower)) continue;
    const needsAction = hasActionCueInText(signal);
    const isActiveConversationProject = Boolean(activeProjectId && projectId === activeProjectId);
    const ts = Math.max(parseTrackerTime(item?.updatedAt), parseTrackerTime(item?.createdAt));
    const ageHours = ts ? Math.max(0, (nowMs - ts) / (60 * 60 * 1000)) : 999;
    const isCurrent = ts >= currentConversationCutoffMs;
    const isFresh = ts >= freshConversationCutoffMs;
    if (!isActiveConversationProject && !isCurrent) continue;
    if (!isActiveConversationProject && !needsAction && !isFresh) continue;
    if (!isActiveConversationProject && /unknown\s+sender|unknown\s+contact/i.test(whoName || signal)) continue;
    const activeBoost = isActiveConversationProject ? 30 : 0;
    const score = activeBoost + (needsAction ? 38 : 12) + Math.max(0, 24 - Math.floor(ageHours / 3)) + (projectId ? 8 : 0);
    conversationCards.push({
      id: String(item?.id || ''),
      projectId,
      projectName: String(item?.projectName || project?.name || recommendation?.project?.projectName || '').trim(),
      businessKey,
      businessName,
      who: whoName || (sourceLower === 'fireflies' ? 'Meeting transcript' : sourceLower === 'slack' ? 'Slack' : 'Client'),
      source: String(item?.source || item?.channel || '').trim(),
      preview: previewTextServer(signal, 180),
      score,
      needsAction,
      updatedAt: ts ? new Date(ts).toISOString() : '',
      recommendation,
    });
  }
  conversationCards.sort((a, b) => (b.score - a.score) || String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')));

  const messageDrafts = conversationCards.slice(0, 1).map((conversation) => {
    const project = projects.find((p) => String(p?.id || '') === String(conversation.projectId || '')) || null;
    const team = Array.isArray(s.team) ? s.team : [];
    const delegate = conversation?.recommendation?.delegate || suggestDelegateForInboxItem(s, conversation.preview, conversation.recommendation?.project);
    const member = team.find((m) => String(m?.id || '') === String(delegate?.teamId || '')) || team.find((m) => String(m?.name || '') === String(project?.accountManagerName || '')) || null;
    return {
      id: `draft:${conversation.id}`,
      conversationId: conversation.id,
      businessKey,
      businessName,
      projectId: conversation.projectId,
      projectName: conversation.projectName,
      to: String(member?.name || project?.accountManagerName || 'Team').trim(),
      reason: conversation.needsAction ? 'Client conversation appears actionable' : 'Conversation may need an internal follow-up',
      body: buildTeamMessageDraft({ project, signal: conversation.preview, teamMember: member }),
    };
  });

  const priorityTasks = snapshot.sortedTasks.slice(0, 3).map((task) => ({
    id: String(task?.id || ''),
    title: String(task?.title || '').trim(),
    project: String(task?.project || '').trim(),
    owner: String(task?.owner || '').trim(),
    priority: Number(task?.priority) || 2,
    dueDate: normalizeTrackerDueDate(task?.dueDate),
    status: String(task?.status || '').trim(),
    businessKey,
    businessName,
  }));

  return {
    businessKey,
    businessName,
    activeProjectId,
    activeProjectName: String(activeProject?.name || '').trim(),
    projects: projectCards.slice(0, 2),
    conversations: conversationCards.slice(0, 4),
    tasks: priorityTasks,
    messageDrafts,
    stats: {
      openTasks: snapshot.openTasks.length,
      relevantTasks: snapshot.relevantTasks.length,
      overdueTasks: snapshot.overdueTasks.length,
      dueTodayTasks: snapshot.dueTodayTasks.length,
      inboxActionable: conversationCards.filter((c) => c.needsAction).length,
    },
  };
}

async function buildMarcusActiveBrief() {
  const settings = await readSettings();
  const cfg = getBusinessConfigFromSettings(settings);
  const businesses = Array.isArray(cfg.businesses) && cfg.businesses.length
    ? cfg.businesses
    : [{ key: DEFAULT_BUSINESS_KEY, name: 'Personal' }];
  const desktopData = desktopRelayCache?.data || desktopContextCache?.data || null;
  const stores = [];
  for (const business of businesses) {
    const key = normalizeBusinessKey(business?.key || '') || DEFAULT_BUSINESS_KEY;
    const name = String(business?.name || key).trim();
    try {
      const store = await readStoreForBusiness(key);
      const activeProject = findProjectForDesktopContext(store, desktopData);
      stores.push({
        businessKey: key,
        businessName: name,
        store,
        activeProjectId: String(activeProject?.id || '').trim(),
      });
    } catch {}
  }

  // Doctrine guides behavior, but intelligence state drives the UI.
  // The ActiveBrief is structured first; chat/prompting explains or acts on it afterward.
  const brief = buildOperationalActiveBrief({
    stores,
    settings,
    desktopData,
    nowMs: Date.now(),
  });
  let projectActivity = null;
  try {
    projectActivity = await projectEvidenceService.getActivity(getBusinessKeyFromContext());
  } catch {
    // The legacy brief remains available while evidence storage recovers.
  }
  return {
    ...brief,
    projectEvidenceActivity: projectActivity,
    evidenceFocus: projectActivity?.currentFocus || null,
  };
}

function getMarcusProviderConfiguration(saved = {}) {
  const text = getQuoOutboundConfig(saved);
  const email = getEmailConfig(saved);
  const verification = saved.marcusProviderVerification && typeof saved.marcusProviderVerification === 'object'
    ? saved.marcusProviderVerification
    : {};
  const textVerification = verification.text?.verified === true
    && verification.text.fingerprint === marcusProviderConfigurationFingerprint('text', saved) ? {
    verified: true,
    provider: 'quo',
    verifiedAt: String(verification.text.verifiedAt || '').slice(0, 40),
    phoneNumberId: String(verification.text.phoneNumberId || '').slice(0, 200),
    fromNumber: String(verification.text.fromNumber || '').slice(0, 30),
    userId: String(verification.text.userId || '').slice(0, 200),
  } : null;
  const emailVerification = verification.email?.verified === true
    && verification.email.fingerprint === marcusProviderConfigurationFingerprint('email', saved) ? {
    verified: true,
    provider: 'smtp',
    verifiedAt: String(verification.email.verifiedAt || '').slice(0, 40),
    fromAddress: String(verification.email.fromAddress || '').slice(0, 320),
    profile: verification.email.profile && typeof verification.email.profile === 'object'
      ? {
          host: String(verification.email.profile.host || '').slice(0, 253),
          port: normalizeNetworkPort(verification.email.profile.port, 465),
          secure: verification.email.profile.secure === true,
          label: String(verification.email.profile.label || '').slice(0, 80),
        }
      : null,
  } : null;
  const emailEnvConfigured = Boolean(
    String(process.env.SMTP_HOST || '').trim()
    || String(process.env.SMTP_USERNAME || '').trim()
    || String(process.env.SMTP_PASSWORD || '').trim()
    || String(process.env.SMTP_FROM_ADDRESS || '').trim()
  );
  const emailSavedConfigured = Boolean(
    String(saved.smtpHost || '').trim()
    || String(saved.smtpUsername || '').trim()
    || String(saved.smtpPassword || '').trim()
    || String(saved.smtpFromAddress || '').trim()
  );

  return {
    ok: true,
    text: {
      provider: 'quo',
      configured: text.configured,
      apiKeyConfigured: Boolean(text.apiKey),
      apiKeyHint: text.keyHint,
      source: text.source,
      defaultPhoneNumberId: text.defaultPhoneNumberId,
      fromNumber: text.from,
      userId: text.userId,
      verification: textVerification,
    },
    email: {
      provider: 'smtp',
      configured: email.smtpConfigured,
      passwordConfigured: Boolean(email.smtp.password),
      passwordHint: maskSecretHint(email.smtp.password),
      source: emailEnvConfigured && emailSavedConfigured ? 'mixed' : emailEnvConfigured ? 'env' : emailSavedConfigured ? 'settings' : 'none',
      host: email.smtp.host,
      port: email.smtp.port,
      secure: email.smtp.secure,
      username: email.smtp.username,
      fromAddress: email.fromAddress,
      verification: emailVerification,
    },
  };
}

function marcusProviderConfigurationFingerprint(type, saved = {}) {
  if (type === 'text') {
    const config = getQuoOutboundConfig(saved);
    return crypto.createHash('sha256').update(JSON.stringify({
      provider: 'quo',
      apiKey: config.apiKey,
      baseUrl: config.baseUrl,
      defaultPhoneNumberId: config.defaultPhoneNumberId,
      from: config.from,
      userId: config.userId,
    })).digest('hex');
  }
  const email = getEmailConfig(saved);
  return crypto.createHash('sha256').update(JSON.stringify({
    provider: 'smtp',
    host: email.smtp.host,
    port: email.smtp.port,
    secure: email.smtp.secure,
    username: email.smtp.username,
    password: email.smtp.password,
    fromAddress: email.fromAddress,
  })).digest('hex');
}

function normalizeProviderTextField(value, label, maxLength = 320) {
  const normalized = String(value ?? '').trim();
  if (normalized.length > maxLength || /[\u0000-\u001f\u007f]/.test(normalized)) {
    throw new Error(`${label} is invalid.`);
  }
  return normalized;
}

function normalizeSmtpHost(value) {
  const host = normalizeProviderTextField(value, 'SMTP host', 253);
  if (host && (!/^[a-z0-9.-]+$/i.test(host) || host.startsWith('.') || host.endsWith('.') || host.includes('..'))) {
    throw new Error('SMTP host must be a hostname or IPv4 address without a URL scheme.');
  }
  return host;
}

async function updateMarcusProviderConfiguration(input = {}) {
  const text = input?.text && typeof input.text === 'object' && !Array.isArray(input.text) ? input.text : null;
  const email = input?.email && typeof input.email === 'object' && !Array.isArray(input.email) ? input.email : null;
  if (!text && !email) throw new Error('Text or email provider settings are required.');

  writeLock = writeLock.catch(() => {}).then(async () => {
    const saved = await readSettings();
    const next = { ...saved, updatedAt: nowIso() };
    const previousFingerprints = {
      text: marcusProviderConfigurationFingerprint('text', saved),
      email: marcusProviderConfigurationFingerprint('email', saved),
    };

    if (text) {
      const apiKey = normalizeProviderTextField(text.apiKey, 'Quo API key', 1_000);
      if (text.clearApiKey === true) next.quoApiKey = '';
      else if (apiKey) next.quoApiKey = apiKey;
      if (Object.hasOwn(text, 'defaultPhoneNumberId')) {
        next.quoDefaultPhoneNumberId = normalizeProviderTextField(text.defaultPhoneNumberId, 'Quo phone-number ID', 200);
      }
      if (Object.hasOwn(text, 'fromNumber')) {
        const fromNumber = normalizeProviderTextField(text.fromNumber, 'Quo sender number', 30);
        if (fromNumber) normalizeExternalRecipient('text', fromNumber);
        next.quoFromNumber = fromNumber;
      }
      if (Object.hasOwn(text, 'userId')) {
        next.quoUserId = normalizeProviderTextField(text.userId, 'Quo user ID', 200);
      }
    }

    if (email) {
      const password = normalizeProviderTextField(email.password, 'SMTP password', 1_000);
      if (email.clearPassword === true) next.smtpPassword = '';
      else if (password) next.smtpPassword = password;
      if (Object.hasOwn(email, 'host')) next.smtpHost = normalizeSmtpHost(email.host);
      if (Object.hasOwn(email, 'port')) {
        const port = Number(email.port);
        if (!Number.isInteger(port) || port < 1 || port > 65_535) throw new Error('SMTP port must be between 1 and 65535.');
        next.smtpPort = port;
      }
      if (Object.hasOwn(email, 'secure')) next.smtpSecure = email.secure === true;
      if (Object.hasOwn(email, 'username')) {
        next.smtpUsername = normalizeProviderTextField(email.username, 'SMTP username', 320);
      }
      if (Object.hasOwn(email, 'fromAddress')) {
        const fromAddress = normalizeProviderTextField(email.fromAddress, 'SMTP from address', 320);
        if (fromAddress) normalizeEmailFromAddress(fromAddress);
        next.smtpFromAddress = fromAddress;
      }
    }

    const verification = { ...(next.marcusProviderVerification || {}) };
    if (text && marcusProviderConfigurationFingerprint('text', next) !== previousFingerprints.text) {
      verification.text = null;
    }
    if (email && marcusProviderConfigurationFingerprint('email', next) !== previousFingerprints.email) {
      verification.email = null;
    }
    next.marcusProviderVerification = verification;

    await writeSettings(next);
  });
  await writeLock;
  return getMarcusProviderConfiguration(await readSettings());
}

async function persistResolvedQuoSender(sender = {}) {
  const envPhoneNumberId = firstNonEmptyString(process.env, ['QUO_DEFAULT_PHONE_NUMBER_ID', 'OPENPHONE_DEFAULT_PHONE_NUMBER_ID']);
  const envFrom = firstNonEmptyString(process.env, ['QUO_FROM_NUMBER', 'OPENPHONE_FROM_NUMBER']);
  const envUserId = firstNonEmptyString(process.env, ['QUO_USER_ID', 'OPENPHONE_USER_ID']);
  writeLock = writeLock.catch(() => {}).then(async () => {
    const saved = await readSettings();
    const next = { ...saved };
    let changed = false;
    if (!envPhoneNumberId && !String(saved.quoDefaultPhoneNumberId || '').trim() && sender.phoneNumberId) {
      next.quoDefaultPhoneNumberId = sender.phoneNumberId;
      changed = true;
    }
    if (!envFrom && !String(saved.quoFromNumber || '').trim() && sender.from) {
      next.quoFromNumber = sender.from;
      changed = true;
    }
    if (!envUserId && !String(saved.quoUserId || '').trim() && sender.userId) {
      next.quoUserId = sender.userId;
      changed = true;
    }
    if (changed) await writeSettings({ ...next, updatedAt: nowIso() });
  });
  await writeLock;
}

async function persistMarcusProviderVerification(type, details = {}) {
  writeLock = writeLock.catch(() => {}).then(async () => {
    const saved = await readSettings();
    const existing = saved.marcusProviderVerification && typeof saved.marcusProviderVerification === 'object'
      ? saved.marcusProviderVerification
      : {};
    await writeSettings({
      ...saved,
      marcusProviderVerification: {
        ...existing,
        [type]: {
          verified: true,
          provider: type === 'text' ? 'quo' : 'smtp',
          verifiedAt: nowIso(),
          fingerprint: marcusProviderConfigurationFingerprint(type, saved),
          ...details,
        },
      },
      updatedAt: nowIso(),
    });
  });
  await writeLock;
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
  const githubCfg = getGitHubCloudConfig(settings);
  const cloudflareCfg = getCloudflareConfig(settings);
  const renderCfg = getRenderCloudConfig(settings);

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
    githubConfigured: githubCfg.configured,
    githubOwner: githubCfg.owner,
    githubTokenHint: githubCfg.tokenHint,
    githubSource: githubCfg.source,
    cloudflareConfigured: cloudflareCfg.configured,
    cloudflareAccountIdConfigured: Boolean(cloudflareCfg.accountId),
    cloudflareDefaultZoneConfigured: Boolean(cloudflareCfg.defaultZoneId),
    cloudflareTokenHint: cloudflareCfg.tokenHint,
    cloudflareSource: cloudflareCfg.source,
    renderConfigured: renderCfg.configured,
    renderTokenHint: renderCfg.tokenHint,
    renderSource: renderCfg.source,
  });
});

app.put('/api/settings', async (req, res) => {
  const body = req.body || {};
  
  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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
    cachedBusinesses = finalCfg.businesses;
    cachedActiveBusinessKey = finalCfg.activeBusinessKey;
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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
    cachedBusinesses = finalCfg.businesses;
    cachedActiveBusinessKey = finalCfg.activeBusinessKey;
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

    writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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
  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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
  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

let desktopActionResults = [];      // [{id,type,ok,error?,completedAt}]
const DESKTOP_ACTION_RESULT_TTL_MS = 10 * 60_000;
const LOCAL_CODEX_DESKTOP_ACTIONS = new Set([
  'start-local-codex-job', 'followup-local-codex-job', 'cancel-local-codex-job',
]);

function pruneDesktopActionResults() {
  const cutoff = Date.now() - DESKTOP_ACTION_RESULT_TTL_MS;
  desktopActionResults = desktopActionResults.filter((r) => Number(r?.completedAt || 0) >= cutoff).slice(-500);
}

async function queueDesktopAction(action) {
  return desktopActionQueue.enqueue(action);
}

function launchVsCodeNative(projectPath, cb) {
  const commands = ['code', 'code.cmd'];
  const tryNext = () => {
    const cmd = commands.shift();
    if (!cmd) {
      cb(new Error('VS Code command not found. Make sure the code command is installed.'));
      return;
    }
    try {
      execFile(cmd, [projectPath], { windowsHide: true }, (error) => {
        if (!error) {
          cb(null);
          return;
        }
        tryNext();
      });
    } catch {
      tryNext();
    }
  };
  tryNext();
}

app.post('/api/launch', async (req, res) => {
  const projectPath = typeof req.body?.path === 'string' ? req.body.path.trim() : '';
  if (!projectPath) return res.status(400).json({ error: 'Path required' });

  if (process.platform === 'win32') {
    launchVsCodeNative(projectPath, (error) => {
      if (error) {
        console.error(`launch error: ${error}`);
        return res.status(500).json({ error: 'Failed to launch VS Code. Make sure the code command is installed.' });
      }
      res.json({ ok: true, success: true, mode: 'native' });
    });
    return;
  }

  try {
    const action = await queueDesktopAction({
      type: 'open-vscode',
      payload: { path: projectPath },
      requestedBy: 'ui',
    });
    res.status(202).json({ ok: true, queued: true, mode: 'desktop-agent', actionId: action.id });
  } catch (error) {
    res.status(503).json({ ok: false, error: 'The desktop action queue is unavailable.', code: error?.code || 'DESKTOP_ACTION_QUEUE_FAILED' });
  }
});

app.get('/api/desktop-context/actions', async (req, res) => {
  const relayAgentId = typeof req.query?.agentId === 'string' ? req.query.agentId.trim().slice(0, 200) : '';
  try {
    const actions = await desktopActionQueue.claim(relayAgentId);
    res.json({ ok: true, actions });
  } catch (error) {
    res.status(503).json({ ok: false, error: 'The desktop action queue is unavailable.', code: error?.code || 'DESKTOP_ACTION_QUEUE_FAILED' });
  }
});

app.post('/api/desktop-context/action-results', async (req, res) => {
  const results = Array.isArray(req.body?.results) ? req.body.results.slice(0, 100) : [];
  const relayAgentId = typeof req.body?.agentId === 'string' ? req.body.agentId.trim().slice(0, 200) : '';
  const now = Date.now();
  let count = 0;
  const rejected = [];
  for (const raw of results) {
    if (!raw || typeof raw !== 'object') continue;
    const id = typeof raw.id === 'string' ? raw.id.trim().slice(0, 80) : '';
    const type = typeof raw.type === 'string' ? raw.type.trim().slice(0, 80) : '';
    if (!id || !type) continue;
    const businessKey = normalizeBusinessKey(raw.businessKey || '');
    const details = (() => {
      try {
        const encoded = JSON.stringify(raw.details ?? null);
        return encoded.length <= 20_000 ? JSON.parse(encoded) : { truncated: true, preview: encoded.slice(0, 20_000) };
      } catch { return null; }
    })();
    const result = {
      id,
      type,
      jobId: typeof raw.jobId === 'string' ? raw.jobId.trim().slice(0, 300) : '',
      businessKey,
      operationId: typeof raw.operationId === 'string' ? raw.operationId.trim().slice(0, 120) : '',
      stepId: typeof raw.stepId === 'string' ? raw.stepId.trim().slice(0, 120) : '',
      projectRegistryId: typeof raw.projectRegistryId === 'string' ? raw.projectRegistryId.trim().slice(0, 160) : '',
      desktopAgentId: relayAgentId || (typeof raw.desktopAgentId === 'string' ? raw.desktopAgentId.trim().slice(0, 200) : ''),
      idempotencyKey: typeof raw.idempotencyKey === 'string' ? raw.idempotencyKey.trim().slice(0, 240) : '',
      attemptNumber: Number.isFinite(Number(raw.attemptNumber)) ? Number(raw.attemptNumber) : null,
      ok: raw.ok === true,
      error: typeof raw.error === 'string' ? raw.error.trim().slice(0, 4_000) : '',
      details,
      completedAt: now,
    };
    desktopActionResults.push(result);
    let accepted = false;
    if (businessKey && result.projectRegistryId) {
      try {
        if (type === 'validate-workspace') {
          await operationsEngine.attestProjectWorkspace(businessKey, result.projectRegistryId, {
            ...result,
            challengeId: id,
            registeredPath: typeof details?.registeredPath === 'string' ? details.registeredPath : '',
            canonicalPath: typeof details?.canonicalPath === 'string' ? details.canonicalPath : '',
          });
          count++;
          accepted = true;
        } else if (LOCAL_CODEX_DESKTOP_ACTIONS.has(type)) {
          count++;
          accepted = true;
          if (!result.ok && desktopCodexAdapter) {
            await desktopCodexAdapter.ingestUpdate({
              jobId: result.jobId || (typeof details?.jobId === 'string' ? details.jobId : id),
              desktopAgentId: result.desktopAgentId,
              status: 'failed',
              error: result.error || `Desktop action ${type} failed.`,
              events: [{ type: 'desktop_codex.launch_failed', data: { actionType: type, error: result.error } }],
            }).catch(() => {});
          }
        } else {
          const reconciled = await operationsEngine.reconcileDesktopResult(result, { runCycle: false });
          count++;
          accepted = true;
          if (type === 'create-project-workspace' && result.ok) {
            await operationsEngine.approveCreatedProjectWorkspace(businessKey, result.projectRegistryId, {
              desktopAgentId: result.desktopAgentId,
              registeredPath: typeof details?.registeredPath === 'string' ? details.registeredPath : '',
              canonicalPath: typeof details?.canonicalPath === 'string' ? details.canonicalPath : '',
            });
          }
          if (type === 'deploy-cloudflare-project' && result.ok && typeof details?.deploymentUrl === 'string') {
            const deploymentUrl = new URL(details.deploymentUrl).toString();
            if (!/\.(?:workers|pages)\.dev$/i.test(new URL(deploymentUrl).hostname)) {
              throw new Error('Desktop deployment result did not contain a verified Cloudflare URL.');
            }
            const current = await operationsEngine.registry.get(businessKey, result.projectRegistryId);
            const deployments = { ...(current?.deployments || {}), productionUrl: deploymentUrl };
            await operationsEngine.registry.update(businessKey, result.projectRegistryId, { deployments });
            if (reconciled.operation?.id) {
              await operationsEngine.store.update(businessKey, reconciled.operation.id, (draft) => {
                const metadata = draft.metadata && typeof draft.metadata === 'object' ? draft.metadata : {};
                draft.metadata = {
                  ...metadata,
                  executionTarget: { ...(metadata.executionTarget || {}), deployments },
                  projectSnapshot: { ...(metadata.projectSnapshot || {}), deployments },
                };
                return draft;
              });
            }
          }
          if (reconciled.operation?.status === 'queued') setImmediate(() => operationsEngine.tick(businessKey, reconciled.operation.id).catch(() => {}));
        }
      } catch (error) {
        rejected.push({ id, code: error?.code || 'DESKTOP_RESULT_REJECTED' });
      }
    } else {
      count++;
      accepted = true;
    }
    if (accepted) {
      if (businessKey && result.projectRegistryId && type !== 'validate-workspace' && !LOCAL_CODEX_DESKTOP_ACTIONS.has(type)) {
        try {
          await projectEvidenceService.recordDesktopActionResult(businessKey, result);
        } catch {
          // Desktop action reconciliation remains authoritative if evidence recording fails.
        }
      }
      try {
        await desktopActionQueue.acknowledge({ id, agentId: result.desktopAgentId, type, idempotencyKey: result.idempotencyKey });
      } catch (error) {
        rejected.push({ id, code: error?.code || 'DESKTOP_ACTION_ACK_FAILED' });
      }
    }
  }
  pruneDesktopActionResults();
  res.json({ ok: true, received: count, rejected });
});

app.get('/api/desktop-context/action-results', (req, res) => {
  pruneDesktopActionResults();
  res.json({ ok: true, results: desktopActionResults.slice(-50) });
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

  const codexWorkspaces = (Array.isArray(req.body?.codexWorkspaces) ? req.body.codexWorkspaces : [])
    .slice(0, 12)
    .map((raw) => {
      const item = raw && typeof raw === 'object' ? raw : {};
      return {
        sessionId: typeof item.sessionId === 'string' ? item.sessionId.trim().slice(0, 160) : '',
        workspacePath: typeof item.workspacePath === 'string' ? item.workspacePath.trim().slice(0, 512) : '',
        folderName: typeof item.folderName === 'string' ? item.folderName.trim().slice(0, 128) : '',
        projectName: typeof item.projectName === 'string' ? item.projectName.trim().slice(0, 160) : '',
        modifiedAt: typeof item.modifiedAt === 'string' ? item.modifiedAt.trim().slice(0, 40) : '',
        source: typeof item.source === 'string' ? item.source.trim().slice(0, 80) : '',
        originator: typeof item.originator === 'string' ? item.originator.trim().slice(0, 120) : '',
        gitBranch: typeof item.gitBranch === 'string' ? item.gitBranch.trim().slice(0, 128) : '',
        gitRemote: typeof item.gitRemote === 'string' ? item.gitRemote.trim().slice(0, 512) : '',
        gitStatusCount: Math.max(0, Math.min(10_000, Number(item.gitStatusCount) || 0)),
        gitStatus: Array.isArray(item.gitStatus) ? item.gitStatus.slice(0, 30).map((entry) => ({
          status: typeof entry?.status === 'string' ? entry.status.slice(0, 4) : '',
          file: typeof entry?.file === 'string' ? entry.file.slice(0, 256) : '',
        })) : [],
        gitRecentCommits: Array.isArray(item.gitRecentCommits)
          ? item.gitRecentCommits.slice(0, 3).map((entry) => typeof entry === 'string' ? entry.slice(0, 240) : '').filter(Boolean)
          : [],
      };
    })
    .filter((item) => item.workspacePath && item.folderName);

  const desktopAuthorizationInput = req.body?.desktopAuthorization && typeof req.body.desktopAuthorization === 'object'
    ? req.body.desktopAuthorization
    : {};
  const desktopAuthorization = {
    agentId: typeof req.body?.agentId === 'string' ? req.body.agentId.trim().slice(0, 200) : '',
    scope: desktopAuthorizationInput.scope === 'full_pc' && desktopAuthorizationInput.broadWorkspaceRootsAllowed === true
      ? 'full_pc'
      : 'workspace_roots',
    broadWorkspaceRootsAllowed: desktopAuthorizationInput.broadWorkspaceRootsAllowed === true,
    allowedRoots: Array.isArray(desktopAuthorizationInput.allowedRoots)
      ? desktopAuthorizationInput.allowedRoots.slice(0, 40).map((item) => typeof item === 'string' ? item.trim().slice(0, 512) : '').filter(Boolean)
      : [],
    newProjectRoot: typeof desktopAuthorizationInput.newProjectRoot === 'string'
      ? desktopAuthorizationInput.newProjectRoot.trim().slice(0, 512)
      : '',
  };
  const data = { ok: true, windowTitle: wt, processName: pn, idleSeconds: idle, source: 'relay', workspace, codexWorkspaces, desktopAuthorization };

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

  const relayAgentId = typeof req.body?.agentId === 'string' ? req.body.agentId.trim().slice(0, 200) : '';
  void projectEvidenceService.recordDesktopContext(getBusinessKeyFromContext(), {
    agentId: relayAgentId,
    context: data,
  }).catch(() => {
    // Desktop context delivery must not fail because evidence aggregation is unavailable.
  });

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
let marcusLiveActions = [];            // recent HUD actions taken by the operator
const MARCUS_LIVE_MAX_ACTIONS = 80;
let lastProactiveHash = '';
let lastProactiveAt = 0;
const PROACTIVE_COOLDOWN_MS = 20_000;  // keep up when Mark bounces between projects
let proactiveRunning = false;
const MARCUS_LIVE_CONVERSATION_MAX_MESSAGES = 80;
const MARCUS_LIVE_CONTEXT_WINDOW_MS = 45 * 60_000;
const MARCUS_LIVE_PROJECT_MEMORY_MAX = 40;
const MARCUS_LIVE_PROJECT_REQUIREMENTS_MAX = 12;

function conversationProjectReference(project = {}) {
  return {
    projectRegistryId: String(project.projectRegistryId || project.registryId || project.id || '').trim().slice(0, 160),
    projectId: String(project.projectId || '').trim().slice(0, 160),
    name: String(project.name || project.canonicalName || project.projectName || '').trim().slice(0, 300),
    repo: String(project.repo || project.fullName || '').trim().replace(/\.git$/i, '').slice(0, 500),
  };
}

function normalizeMarcusLiveConversation(input) {
  const raw = input && typeof input === 'object' ? input : {};
  const messages = (Array.isArray(raw.messages) ? raw.messages : [])
    .map((message) => ({
      role: message?.role === 'assistant' ? 'assistant' : 'user',
      content: String(message?.content || '').trim().slice(0, 4_000),
      timestamp: typeof message?.timestamp === 'string' ? message.timestamp : nowIso(),
      metadata: message?.metadata && typeof message.metadata === 'object' ? message.metadata : {},
    }))
    .filter((message) => message.content)
    .slice(-MARCUS_LIVE_CONVERSATION_MAX_MESSAGES);
  const active = raw.activeProject && typeof raw.activeProject === 'object' ? raw.activeProject : {};
  const projectMemories = (Array.isArray(raw.projectMemories) ? raw.projectMemories : [])
    .map((memory) => ({
      project: conversationProjectReference(memory?.project),
      requirements: [...new Set((Array.isArray(memory?.requirements) ? memory.requirements : [])
        .map((item) => String(item || '').replace(/\s+/g, ' ').trim().slice(0, 500))
        .filter(Boolean))].slice(-MARCUS_LIVE_PROJECT_REQUIREMENTS_MAX),
      updatedAt: typeof memory?.updatedAt === 'string' ? memory.updatedAt : '',
    }))
    .filter((memory) => Object.values(normalizeConversationProject(memory.project)).some(Boolean) && memory.requirements.length)
    .slice(-MARCUS_LIVE_PROJECT_MEMORY_MAX);
  return {
    messages,
    activeProject: conversationProjectReference(active),
    projectMemories,
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : '',
  };
}

async function readMarcusLiveConversation() {
  const settings = await readSettings();
  return normalizeMarcusLiveConversation(settings.marcusLiveConversation);
}

async function recordMarcusLiveExchange(message, reply, metadata = {}) {
  writeLock = writeLock.catch(() => {}).then(async () => {
    const settings = await readSettings();
    const conversation = normalizeMarcusLiveConversation(settings.marcusLiveConversation);
    const timestamp = nowIso();
    const rawMetadata = metadata && typeof metadata === 'object' ? metadata : {};
    const { requirements: suppliedRequirements, ...exchangeMetadata } = rawMetadata;
    conversation.messages.push(
      { role: 'user', content: String(message || '').trim().slice(0, 4_000), timestamp, metadata: exchangeMetadata },
      { role: 'assistant', content: String(reply || '').trim().slice(0, 4_000), timestamp, metadata: exchangeMetadata },
    );
    conversation.messages = conversation.messages.filter((item) => item.content).slice(-MARCUS_LIVE_CONVERSATION_MAX_MESSAGES);
    const project = metadata?.project && typeof metadata.project === 'object' ? metadata.project : null;
    if (project && (project.id || project.projectRegistryId || project.name)) {
      conversation.activeProject = conversationProjectReference(project);
      const requirements = Array.isArray(suppliedRequirements)
        ? suppliedRequirements
        : extractMarcusLiveRequirementSentences([message], { limit: MARCUS_LIVE_PROJECT_REQUIREMENTS_MAX });
      mergeMarcusLiveProjectMemory(conversation, conversation.activeProject, requirements, timestamp);
    }
    conversation.updatedAt = timestamp;
    await writeSettings({ ...settings, marcusLiveConversation: conversation, updatedAt: timestamp });
  });
  await writeLock;
}

function recentMarcusLiveMessages(conversation, nowMs = Date.now()) {
  return (Array.isArray(conversation?.messages) ? conversation.messages : []).filter((message) => {
    const timestamp = Date.parse(message.timestamp || '');
    return Number.isFinite(timestamp) && nowMs - timestamp <= MARCUS_LIVE_CONTEXT_WINDOW_MS;
  }).slice(-16);
}

function normalizeConversationProject(project = {}) {
  return {
    projectRegistryId: String(project.projectRegistryId || project.registryId || project.id || '').trim().toLowerCase(),
    projectId: String(project.projectId || '').trim().toLowerCase(),
    name: String(project.name || project.canonicalName || '').trim().toLowerCase(),
    repo: String(project.repo || project.fullName || '').trim().replace(/\.git$/i, '').toLowerCase(),
  };
}

function mergeMarcusLiveProjectMemory(conversation, project, requirements, updatedAt = nowIso()) {
  const reference = conversationProjectReference(project);
  if (!Object.values(normalizeConversationProject(reference)).some(Boolean)) return;
  const incoming = (Array.isArray(requirements) ? requirements : [])
    .map((item) => String(item || '').replace(/\s+/g, ' ').trim().slice(0, 500))
    .filter(Boolean);
  if (!incoming.length) return;
  const memories = Array.isArray(conversation.projectMemories) ? conversation.projectMemories : [];
  const index = memories.findIndex((memory) => conversationProjectsMatch(memory.project, reference));
  const existing = index >= 0 ? memories.splice(index, 1)[0] : { project: reference, requirements: [] };
  const merged = [];
  const seen = new Set();
  for (const requirement of [...(existing.requirements || []), ...incoming]) {
    const key = requirement.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    merged.push(requirement);
  }
  memories.push({
    project: reference,
    requirements: merged.slice(-MARCUS_LIVE_PROJECT_REQUIREMENTS_MAX),
    updatedAt,
  });
  conversation.projectMemories = memories.slice(-MARCUS_LIVE_PROJECT_MEMORY_MAX);
}

function conversationProjectsMatch(left, right) {
  const a = normalizeConversationProject(left);
  const b = normalizeConversationProject(right);
  return ['projectRegistryId', 'projectId', 'repo', 'name'].some((key) => a[key] && b[key] && a[key] === b[key]);
}

function rememberedMarcusLiveProjectRequirements(conversation, targetProject) {
  const memories = Array.isArray(conversation?.projectMemories) ? conversation.projectMemories : [];
  return memories.find((memory) => conversationProjectsMatch(memory.project, targetProject))?.requirements || [];
}

function scopedMarcusLiveUserMessages(conversation, targetProject = conversation?.activeProject || {}) {
  const messages = recentMarcusLiveMessages(conversation);
  const hasTargetProject = Object.values(normalizeConversationProject(targetProject)).some(Boolean);
  const scoped = [];
  for (let index = 0; index < messages.length; index += 1) {
    const item = messages[index];
    if (item.role !== 'user') continue;
    const ownProject = item.metadata?.project;
    const pairedProject = messages[index + 1]?.role === 'assistant'
      && messages[index + 1]?.timestamp === item.timestamp
      ? messages[index + 1]?.metadata?.project
      : null;
    const project = ownProject || pairedProject;
    if (!hasTargetProject || (project && conversationProjectsMatch(project, targetProject))) scoped.push(item.content);
  }
  return scoped;
}

function extractMarcusLiveRequirementSentences(messages, { limit = 3 } = {}) {
  const candidates = [];
  let sequence = 0;
  for (const message of (Array.isArray(messages) ? messages : [])) {
    const cleaned = withoutProjectExecutionDeferrals(message);
    const sentences = cleaned.split(/(?<=[.!?])\s+|[\r\n]+/).map((item) => item.trim()).filter(Boolean);
    for (const sentence of sentences) {
      sequence += 1;
      if (/\b(read[- ]only|acceptance test)\b/i.test(sentence)) continue;
      if (/^(?:tell|identify|repeat|show|list|what|which|audit|inspect|review|check|prepare|start|launch|run|open|get it|do it)\b/i.test(sentence)) continue;
      const signals = sentence.match(/\b(?:need|needs|must|should|require|requires|required|requirement|feature|popup|modal|setting|button|collect|block|verify|verified|token|slug|trigger)\w*\b/gi) || [];
      if (signals.length < 2) continue;
      const text = sentence.replace(/\s+/g, ' ').trim().slice(0, 500);
      const key = text.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
      candidates.push({ text, key, score: signals.length, sequence });
    }
  }
  const selected = [];
  for (const candidate of candidates.sort((a, b) => b.score - a.score || b.sequence - a.sequence)) {
    const duplicate = selected.some((item) => item.key === candidate.key
      || (item.key.length >= 30 && candidate.key.length >= 30 && (item.key.includes(candidate.key) || candidate.key.includes(item.key))));
    if (!duplicate) selected.push(candidate);
    if (selected.length >= Math.max(1, Math.min(MARCUS_LIVE_PROJECT_REQUIREMENTS_MAX, Number(limit) || 3))) break;
  }
  return selected.sort((a, b) => a.sequence - b.sequence).map((item) => item.text);
}

function summarizeMarcusLiveRequirements(conversation, currentMessage, targetProject, additionalMessages = [], { limit = 3 } = {}) {
  const messages = [
    ...rememberedMarcusLiveProjectRequirements(conversation, targetProject),
    ...(Array.isArray(additionalMessages) ? additionalMessages : []),
    ...scopedMarcusLiveUserMessages(conversation, targetProject),
    String(currentMessage || '').trim(),
  ].filter(Boolean);
  return extractMarcusLiveRequirementSentences(messages, { limit });
}

async function collectMarcusLiveProjectRequirements(businessKey, conversation, currentMessage, targetProject, { limit = 8 } = {}) {
  let operationRequests = [];
  if (Object.values(normalizeConversationProject(targetProject)).some(Boolean)) {
    try {
      const operations = await operationsEngine.listOperations(businessKey, { limit: 100 });
      operationRequests = operations
        .filter((operation) => conversationProjectsMatch({
          projectRegistryId: operation.projectRegistryId,
          projectId: operation.projectId,
          name: operation.projectName,
        }, targetProject))
        .map((operation) => operation.originalRequest)
        .filter(Boolean);
    } catch {}
  }
  return summarizeMarcusLiveRequirements(conversation, currentMessage, targetProject, operationRequests, { limit });
}

function isMarcusLiveAcceptanceOnlyMessage(message) {
  const text = String(message || '');
  return /\b(read[- ]only|acceptance test)\b/i.test(text) && /\b(tell|identify|repeat|show|do not|don't)\b/i.test(text);
}

function buildMarcusLiveProjectRequest(conversation, message, targetProject = conversation?.activeProject || {}, retainedRequirements = []) {
  const current = String(message || '').trim();
  const prior = scopedMarcusLiveUserMessages(conversation, targetProject)
    .slice(-7)
    .map((item) => withoutProjectExecutionDeferrals(item))
    .filter((item) => item && !isMarcusLiveAcceptanceOnlyMessage(item));
  const active = targetProject || {};
  const projectLine = [active.name, active.repo].filter(Boolean).length
    ? `Active project from this conversation: ${[active.name, active.repo].filter(Boolean).join(' / ')}.`
    : '';
  const requirements = (Array.isArray(retainedRequirements) ? retainedRequirements : [])
    .map((item) => String(item || '').trim())
    .filter(Boolean);
  const requirementsBlock = requirements.length
    ? `Durable project requirements:\n${requirements.map((item) => `- ${item}`).join('\n')}`
    : '';
  return [projectLine, requirementsBlock, ...prior, current].filter(Boolean).join('\n').slice(-12_000);
}

function parseMissionMemoryCommand(message) {
  const text = String(message || '').replace(/\s+/g, ' ').trim();
  if (!text) return null;
  const queryMatch = text.match(/^(?:what|which|show|list|tell me)\b.{0,45}\b(?:remember|memory|mission|standing instructions?|preferences?)\b(?:\s+(?:about|for|on)\s+(.+))?[?.!]*$/i);
  if (queryMatch) return { action: 'list', query: String(queryMatch[1] || '').trim() };

  let content = '';
  let kind = 'standing_instruction';
  let title = 'Standing instruction from Mark';
  const missionMatch = text.match(/^(?:please\s+)?(?:your|our|marcus(?:'s)?)\s+mission\s+(?:is|should be|:)\s*(.+)$/i);
  const preferenceMatch = text.match(/^(?:please\s+)?(?:remember\s+(?:that\s+)?)?(?:my|our)\s+preference\s+(?:is|:)\s*(.+)$/i);
  const fromNowOnMatch = text.match(/^(?:please\s+)?from now on[, :]\s*(.+)$/i);
  const rememberMatch = text.match(/^(?:please\s+)?remember(?!\s+to\b)(?:\s+that)?[,:]?\s+(.+)$/i);
  if (missionMatch) {
    content = missionMatch[1];
    kind = 'mission';
    title = 'Mission from Mark';
  } else if (preferenceMatch) {
    content = preferenceMatch[1];
    kind = 'preference';
    title = 'Preference from Mark';
  } else if (fromNowOnMatch) {
    content = fromNowOnMatch[1];
  } else if (rememberMatch) {
    content = rememberMatch[1];
    if (/^(?:i|we)\s+prefer\b/i.test(content)) {
      kind = 'preference';
      title = 'Preference from Mark';
    } else if (/^(?:we|you|marcus)\s+(?:need|must|should)\b/i.test(content)) {
      kind = 'standing_instruction';
    } else {
      kind = 'fact';
      title = 'Remembered fact from Mark';
    }
  }
  content = String(content || '').trim();
  return content ? { action: 'add', kind, title, content } : null;
}

async function handleMissionMemoryCommand(businessKey, command, source = 'marcus_live_explicit_command') {
  if (!command) return null;
  if (command.action === 'list') {
    const memories = await missionMemoryStore.relevant(businessKey, command.query, { limit: 8 });
    return {
      ok: true,
      status: 'mission_memory_read',
      memories,
      reply: memories.length
        ? `Here is the durable mission memory I am using:\n${memories.map((memory) => `- [${memory.kind}] ${memory.title}: ${memory.content}`).join('\n')}`
        : 'I do not have a matching durable mission memory yet.',
    };
  }
  try {
    const result = await missionMemoryStore.add(businessKey, command, {
      actor: 'mark',
      source,
    });
    return {
      ok: true,
      status: result.created ? 'mission_memory_created' : 'mission_memory_confirmed',
      memory: result.memory,
      reply: `${result.created ? 'I added' : 'I reconfirmed'} this durable ${result.memory.kind.replaceAll('_', ' ')}: ${result.memory.content}`,
    };
  } catch (error) {
    return {
      ok: false,
      status: 'mission_memory_rejected',
      reply: `I did not store that in mission memory: ${String(error?.message || error)}`,
    };
  }
}

function isProjectContextDeclaration(message) {
  const text = String(message || '').trim();
  const explicitRepo = extractExplicitGitHubRepositories(text).length > 0;
  return explicitRepo || (/\b(project|repo|repository)\b/i.test(text) && /\bgithub\b/i.test(text));
}

function isProjectSwitchRequest(message) {
  const text = String(message || '').trim();
  return /^(?:please\s+)?(?:switch|change|move)(?:\s+(?:the\s+)?(?:active\s+)?project)?\s+(?:to\s+)?[^.!?]{2,160}[.!?]*$/i.test(text)
    || /^(?:please\s+)?(?:work\s+on|open)\s+(?:the\s+)?[^.!?]{2,160}?\s+(?:project|repo|repository)[.!?]*$/i.test(text);
}

function isNewProjectBootstrapRequest(message) {
  const text = String(message || '').trim();
  if (/\b(?:do not|don't|dont|never)\b[^.!?]{0,80}\b(?:create|start|make|build)\b/i.test(text)) return false;
  return /\b(?:create|start|make|build)\b[^.!?]{0,120}\b(?:new project|project from scratch|empty project|new app|new application)\b/i.test(text)
    || /\b(?:new project|project from scratch|empty project|new app|new application)\b[^.!?]{0,120}\b(?:create|start|make|build)\b/i.test(text);
}

function projectNameFromBootstrapRequest(message, explicitName = '') {
  const supplied = String(explicitName || '').replace(/\s+/g, ' ').trim();
  if (supplied) return supplied.slice(0, 120);
  const text = String(message || '').replace(/\s+/g, ' ').trim();
  const quoted = text.match(/\b(?:called|named)\s+["']([^"']{2,120})["']/i);
  if (quoted) return quoted[1].trim();
  const named = text.match(/\b(?:called|named)\s+([A-Za-z0-9][A-Za-z0-9 &._-]{1,119}?)(?=\s+(?:that|which|for|from|to|with|and\s+(?:publish|deploy|create))\b|[,.!?]|$)/i);
  if (named) return named[1].trim();
  const beforeType = text.match(/\bnew\s+([A-Za-z0-9][A-Za-z0-9 &._-]{1,80}?)\s+(?:project|app|application)\b/i);
  if (beforeType && !/^(?:empty|software|web|mobile)$/i.test(beforeType[1].trim())) return beforeType[1].trim();
  return '';
}

function projectSlug(value) {
  return String(value || '').normalize('NFKD').replace(/[^A-Za-z0-9]+/g, '-').replace(/^-+|-+$/g, '').toLowerCase().slice(0, 80);
}

async function prepareNewProjectBootstrap(message, { projectName = '', source = 'marcus_live' } = {}) {
  const businessKey = getBusinessKeyFromContext();
  const request = String(message || '').trim().slice(0, 12_000);
  const name = projectNameFromBootstrapRequest(request, projectName);
  if (!name) {
    return { ok: true, status: 'needs_project_name', reply: 'What should I call the new project? Give me the project name and I will create its folder, start the watched Codex build, and prepare the GitHub and Cloudflare approvals.' };
  }
  const desktop = desktopRelayCache?.data || desktopContextCache?.data || {};
  const authorization = desktop.desktopAuthorization && typeof desktop.desktopAuthorization === 'object' ? desktop.desktopAuthorization : {};
  if (authorization.scope !== 'full_pc' || authorization.broadWorkspaceRootsAllowed !== true || !authorization.agentId) {
    return { ok: false, status: 'desktop_authorization_required', reply: 'The desktop agent has not yet reported the full-PC authorization. I will not create a folder or start local Codex until that exact machine grant is active.' };
  }
  const slug = projectSlug(name);
  if (!slug) return { ok: false, status: 'invalid_project_name', reply: 'That project name cannot produce a safe folder and repository name.' };
  const root = String(authorization.newProjectRoot || '').trim();
  if (!/^[A-Za-z]:\\/.test(root)) return { ok: false, status: 'project_root_required', reply: 'The desktop agent has not reported a valid Windows root for new Marcus projects.' };
  const workspacePath = path.win32.join(root, slug);
  const github = getGitHubCloudConfig(await readSettings());
  if (!github.configured || !github.owner) {
    return { ok: false, status: 'github_required', reply: 'GitHub is not connected on the Marcus server, so I cannot create the full project workflow yet.' };
  }
  const projects = await operationsEngine.listProjectRegistry(businessKey);
  const duplicate = projects.find((project) => project.canonicalName.toLowerCase() === name.toLowerCase()
    || String(project.localWorkspace?.path || '').toLowerCase() === workspacePath.toLowerCase());
  if (duplicate) {
    const existingOperations = await operationsEngine.listOperations(businessKey, { projectRegistryId: duplicate.id, limit: 20 });
    const existingOperation = existingOperations.find((operation) => operation.metadata?.projectBootstrap);
    if (existingOperation) {
      return {
        ok: true,
        status: 'project_bootstrap_started',
        project: duplicate,
        operation: existingOperation,
        reply: `${duplicate.canonicalName} already has durable operation ${existingOperation.id}. I resumed that exact workflow instead of creating a duplicate.`,
      };
    }
    const isRecoverableOrphan = duplicate.metadata?.projectBootstrap === true
      && String(duplicate.localWorkspace?.path || '').toLowerCase() === workspacePath.toLowerCase()
      && duplicate.localWorkspace?.desktopAgentId === authorization.agentId
      && String(duplicate.metadata?.requestedRepository || '').toLowerCase() === `${github.owner}/${slug}`.toLowerCase();
    if (!isRecoverableOrphan) {
      return { ok: false, status: 'project_exists', project: duplicate, reply: `${duplicate.canonicalName} already exists in Marcus. Say "switch to ${duplicate.canonicalName}" or give the new project a different name.` };
    }
  }
  const deployCloudflare = !/\b(?:do not|don't|dont|without|no)\b[^.!?]{0,50}\b(?:cloudflare|deploy|publish|live)\b/i.test(request);
  const repositoryPrivate = !/\bpublic\s+(?:repo|repository|project)\b/i.test(request);
  let project = duplicate || await operationsEngine.createProjectRegistryRecord(businessKey, {
      canonicalName: name,
      aliases: [slug, `${name} project`, `${name} app`],
      description: `Project created from Mark's request through the Marcus desktop and local Codex workflow.`,
      localWorkspace: { path: workspacePath, platform: 'win32', desktopAgentId: authorization.agentId },
      metadata: {
        projectBootstrap: true,
        bootstrapSource: source,
        requestedRepository: `${github.owner}/${slug}`,
        requestedCloudflareDeployment: deployCloudflare,
      },
    });
  if (!project.localWorkspace?.approvalChallenge?.id && project.localWorkspace?.trustStatus !== 'approved') {
    project = await operationsEngine.prepareNewProjectWorkspace(businessKey, project.id, { desktopAgentId: authorization.agentId });
  }
  const steps = [
    {
      id: 'context', title: 'Prepare new project context', type: 'internal', provider: 'internal',
      toolName: 'prepare_operation_context', description: 'Bind the new project name, workspace, repository target, build request, and approval boundaries.',
      input: {}, maxAttempts: 2,
    },
    {
      id: 'workspace', title: 'Create the local project workspace', type: 'desktop', provider: 'desktop',
      toolName: 'create-project-workspace', description: `Create ${workspacePath}, initialize Git, and open it visibly on Mark's PC.`,
      dependsOn: ['context'], input: { projectName: name, initializeGit: true, openInVsCode: true }, maxAttempts: 2,
    },
    {
      id: 'codex', title: 'Build the project with local Codex', type: 'codex', provider: 'codex',
      toolName: 'codex_implementation', description: `${request}\n\nBuild a complete working application in the new workspace. Include a production-ready Wrangler configuration so the approved Cloudflare deployment can run without manual packaging.`,
      dependsOn: ['workspace'], input: { providerMode: 'desktop_codex' }, maxAttempts: 2,
      verificationRequirements: ['artifact_present'],
    },
    {
      id: 'github', title: `Create GitHub repository ${github.owner}/${slug}`, type: 'github_write', provider: 'github_write',
      toolName: 'create_repository', description: `Create the exact ${repositoryPrivate ? 'private' : 'public'} GitHub repository after Mark approves it.`,
      dependsOn: ['codex'], input: {
        owner: github.owner, name: slug, description: `${name} - created by Marcus`, private: repositoryPrivate,
        environment: 'production', approvalTarget: `${github.owner}/${slug} (${repositoryPrivate ? 'private' : 'public'})`,
      }, maxAttempts: 1,
    },
    {
      id: 'origin', title: 'Connect the local project to GitHub', type: 'desktop', provider: 'desktop',
      toolName: 'connect-github-repository', description: 'Bind the verified new repository as the local Git origin.',
      dependsOn: ['github'], input: {}, maxAttempts: 2,
    },
    {
      id: 'push', title: 'Commit and push the built project', type: 'desktop', provider: 'desktop',
      toolName: 'publish-project-changes', description: 'Commit the reviewed local build and push main to the exact new repository after Mark approves it.',
      dependsOn: ['origin'], input: {
        commitMessage: `Build ${name}`, environment: 'production', approvalTarget: `${github.owner}/${slug}:main`,
      }, maxAttempts: 1,
    },
    ...(deployCloudflare ? [{
      id: 'cloudflare', title: 'Deploy the project to Cloudflare', type: 'desktop', provider: 'desktop',
      toolName: 'deploy-cloudflare-project', description: 'Run the project Wrangler deployment and record the verified workers.dev or pages.dev URL after Mark approves it.',
      dependsOn: ['push'], input: { environment: 'production', approvalTarget: `${slug} to Cloudflare production` }, maxAttempts: 1,
    }] : []),
    {
      id: 'verify', title: 'Verify the delivered project', type: 'verification', provider: 'verification',
      toolName: 'verify_operation', description: 'Require recorded implementation evidence before the durable workflow can complete.',
      dependsOn: [deployCloudflare ? 'cloudflare' : 'push'], input: { requirements: [{ type: 'artifact_present', required: true }] }, maxAttempts: 2,
    },
  ];
  const created = await operationsEngine.createFromRequest(businessKey, {
    originalRequest: request,
    objective: `Create ${name} from scratch as a working application, publish its new GitHub repository,${deployCloudflare ? ' deploy it to Cloudflare,' : ''} and verify the result.`,
    projectRegistryId: project.id,
    projectName: name,
    requestedBy: 'mark',
    source,
    riskLevel: 'medium',
    autoPlan: true,
    autoStart: true,
    plan: { steps },
    acceptanceCriteria: [
      `${name} exists in a new local Git workspace and implements Mark's request.`,
      'The Codex run is visible on Mark\'s PC and its events and final result are durably recorded.',
      `The exact GitHub repository ${github.owner}/${slug} is created only after explicit approval and receives the project commit.`,
      ...(deployCloudflare ? ['Cloudflare reports a live deployment URL after explicit production approval.'] : []),
      'Marcus retains evidence for each completed stage and does not infer completion from a model claim.',
    ],
    metadata: { projectBootstrap: { slug, workspacePath, githubOwner: github.owner, deployCloudflare, repositoryPrivate } },
  });
  const reply = `I created the durable ${name} build workflow and queued the exact local workspace on your PC. Local Codex will open in the live monitor after the folder is attested. GitHub creation, push, and${deployCloudflare ? ' Cloudflare production deployment' : ' publishing'} each remain tied to exact approvals. Operation: ${created.operation.id}.`;
  return { ok: true, status: 'project_bootstrap_started', project, operation: created.operation, reply };
}

function rememberMarcusLiveAction(action) {
  const entry = {
    id: makeId(),
    itemId: String(action?.itemId || '').slice(0, 180),
    action: String(action?.action || '').slice(0, 40),
    label: String(action?.label || '').slice(0, 240),
    kind: String(action?.kind || '').slice(0, 40),
    target: String(action?.target || '').slice(0, 512),
    ts: Date.now(),
  };
  marcusLiveActions.push(entry);
  if (marcusLiveActions.length > MARCUS_LIVE_MAX_ACTIONS) {
    marcusLiveActions = marcusLiveActions.slice(-MARCUS_LIVE_MAX_ACTIONS);
  }
  return entry;
}

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

app.get('/api/integrations/github/status', async (req, res) => {
  const cfg = getGitHubCloudConfig(await readSettings());
  res.json({
    ok: true,
    configured: Boolean(cfg.configured),
    owner: cfg.owner,
    tokenHint: cfg.tokenHint,
    source: cfg.source,
  });
});

app.get('/api/integrations/github/repos', async (req, res) => {
  try {
    const cfg = getGitHubCloudConfig(await readSettings());
    const owner = String(req.query?.owner || cfg.owner || '').trim();
    const limit = Math.max(1, Math.min(100, Number(req.query?.limit) || 30));
    const pathPart = owner
      ? `/users/${encodeURIComponent(owner)}/repos?per_page=${limit}&sort=updated`
      : `/user/repos?per_page=${limit}&sort=updated&affiliation=owner,collaborator,organization_member`;
    const repos = await githubApi(pathPart);
    res.json({
      ok: true,
      repos: (Array.isArray(repos) ? repos : []).map((repo) => ({
        id: repo.id,
        name: repo.name,
        fullName: repo.full_name,
        private: Boolean(repo.private),
        defaultBranch: repo.default_branch,
        htmlUrl: repo.html_url,
        updatedAt: repo.updated_at,
        pushedAt: repo.pushed_at,
      })),
    });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to list GitHub repos' });
  }
});

app.get('/api/integrations/github/repo-file', async (req, res) => {
  try {
    const owner = String(req.query?.owner || '').trim();
    const repo = String(req.query?.repo || '').trim();
    const filePath = String(req.query?.path || '').trim();
    const ref = String(req.query?.ref || '').trim();
    if (!owner || !repo || !filePath) return res.status(400).json({ ok: false, error: 'owner, repo, and path are required.' });
    const qs = ref ? `?ref=${encodeURIComponent(ref)}` : '';
    const data = await githubApi(`/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/contents/${filePath.split('/').map(encodeURIComponent).join('/')}${qs}`);
    if (Array.isArray(data)) {
      return res.json({ ok: true, type: 'dir', entries: data.map((item) => ({ name: item.name, path: item.path, type: item.type, size: item.size })) });
    }
    const encoded = String(data?.content || '').replace(/\s+/g, '');
    const content = encoded ? Buffer.from(encoded, 'base64').toString('utf8') : '';
    res.json({ ok: true, type: data?.type || 'file', name: data?.name || '', path: data?.path || filePath, size: data?.size || 0, encoding: data?.encoding || '', content: content.slice(0, 80_000) });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to read GitHub repo file' });
  }
});

app.get('/api/integrations/cloudflare/status', async (req, res) => {
  const cfg = getCloudflareConfig(await readSettings());
  res.json({
    ok: true,
    configured: Boolean(cfg.configured),
    accountIdConfigured: Boolean(cfg.accountId),
    defaultZoneIdConfigured: Boolean(cfg.defaultZoneId),
    tokenHint: cfg.tokenHint,
    source: cfg.source,
  });
});

app.get('/api/integrations/github/pull-request', async (req, res) => {
  try {
    const result = await inspectGitHubPullRequest(req.query?.owner, req.query?.repo, req.query?.pullNumber);
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to inspect GitHub pull request.' });
  }
});

app.get('/api/integrations/cloudflare/zones', async (req, res) => {
  try {
    const data = await cloudflareApi('/zones?per_page=50');
    res.json({
      ok: true,
      zones: (Array.isArray(data?.result) ? data.result : []).map((zone) => ({
        id: zone.id,
        name: zone.name,
        status: zone.status,
        paused: Boolean(zone.paused),
        type: zone.type,
      })),
    });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to list Cloudflare zones' });
  }
});

app.get('/api/integrations/cloudflare/dns-records', async (req, res) => {
  try {
    const cfg = getCloudflareConfig(await readSettings());
    const zoneId = String(req.query?.zoneId || cfg.defaultZoneId || '').trim();
    if (!zoneId) return res.status(400).json({ ok: false, error: 'zoneId is required or CLOUDFLARE_DEFAULT_ZONE_ID must be configured.' });
    const data = await cloudflareApi(`/zones/${encodeURIComponent(zoneId)}/dns_records?per_page=100`);
    res.json({
      ok: true,
      zoneId,
      records: (Array.isArray(data?.result) ? data.result : []).map((record) => ({
        id: record.id,
        type: record.type,
        name: record.name,
        content: record.content,
        proxied: Boolean(record.proxied),
        ttl: record.ttl,
        modifiedOn: record.modified_on,
      })),
    });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to list Cloudflare DNS records' });
  }
});

app.get('/api/integrations/render/status', async (req, res) => {
  const cfg = getRenderCloudConfig(await readSettings());
  res.json({ ok: true, configured: Boolean(cfg.configured), tokenHint: cfg.tokenHint, source: cfg.source });
});

app.get('/api/integrations/cloudflare/workers', async (req, res) => {
  try {
    res.json({ ok: true, ...await cloudflareWorkerInspection('workers') });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to list Cloudflare Workers.' });
  }
});

app.get('/api/integrations/cloudflare/worker-versions', async (req, res) => {
  try {
    res.json({ ok: true, ...await cloudflareWorkerInspection('versions', req.query?.scriptName) });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to list Cloudflare Worker versions.' });
  }
});

app.get('/api/integrations/cloudflare/worker-deployments', async (req, res) => {
  try {
    res.json({ ok: true, ...await cloudflareWorkerInspection('deployments', req.query?.scriptName) });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to list Cloudflare Worker deployments.' });
  }
});

app.get('/api/integrations/render/services', async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(100, Number(req.query?.limit) || 50));
    const data = await renderApi(`/services?limit=${limit}`);
    const rows = Array.isArray(data) ? data : Array.isArray(data?.services) ? data.services : [];
    res.json({
      ok: true,
      services: rows.map((row) => {
        const service = row.service || row;
        return {
          id: service.id,
          name: service.name,
          type: service.type,
          repo: service.repo,
          branch: service.branch,
          serviceDetails: service.serviceDetails ? { plan: service.serviceDetails.plan, region: service.serviceDetails.region } : undefined,
          updatedAt: service.updatedAt || service.updated_at,
        };
      }),
    });
  } catch (err) {
    res.status(400).json({ ok: false, error: err?.message || 'Failed to list Render services' });
  }
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
  res.write(`data: ${JSON.stringify({
    type: 'init',
    observations: marcusLiveObservations.slice(-20),
    actions: marcusLiveActions.slice(-20),
  })}\n\n`);
  const currentContext = buildMarcusLiveContextEvent();
  if (currentContext) {
    res.write(`data: ${JSON.stringify(currentContext)}\n\n`);
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

function buildMarcusLiveContextEvent() {
  const dc = desktopRelayCache?.data;
  if (!dc) return null;
  const ws = dc.workspace;
  const evt = {
    type: 'context',
    windowTitle: dc.windowTitle || '',
    processName: dc.processName || '',
    workspace: ws?.folderName || '',
    workspacePath: ws?.workspacePath || '',
    branch: ws?.gitBranch || '',
    activeFile: ws?.activeFile || '',
    recentFiles: ws?.recentFiles || [],
    fileCount: Object.keys(ws?.fileContents || {}).length,
    changedFiles: (ws?.gitStatus || []).map(s => `${s.status} ${s.file}`),
    actions: marcusLiveActions.slice(-20),
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
  return evt;
}

function isWebsiteLikeProject(project) {
  const text = `${project?.name || ''} ${project?.type || ''} ${project?.agentBrief || ''}`.toLowerCase();
  return /\b(website|web site|site|homepage|landing page|wordpress|webflow|shopify|seo|build|rebuild|revision)\b/.test(text);
}

function buildMarcusLiveProjectFocus(store, desktopData, nowMs = Date.now()) {
  const s = store && typeof store === 'object' ? store : EMPTY_STORE;
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const activeProject = findProjectForDesktopContext(s, desktopData);
  const activeProjectId = String(activeProject?.id || '').trim();
  const cutoffMs = nowMs - (14 * MS_PER_DAY);
  const today = new Date(nowMs).toISOString().slice(0, 10);
  const focus = [];
  const staleWebsite = [];

  for (const project of projects) {
    if (isClosedProjectStatus(project?.status)) continue;
    const linkedTasks = getLinkedProjectTasks(s, project).filter((task) => !isClosedTaskStatus(task?.status));
    const linkedInbox = getLinkedProjectInboxItems(s, project).filter((item) => {
      const status = String(item?.status || '').trim().toLowerCase();
      return status === 'new' || status === 'triaged' || !status;
    });
    const lastActivityMs = computeProjectLastActivityMs(s, project, linkedTasks, linkedInbox);
    const createdMs = parseTrackerTime(project?.createdAt);
    const dueDate = normalizeTrackerDueDate(project?.dueDate);
    const isDesktop = activeProjectId && String(project?.id || '') === activeProjectId;
    const fresh = lastActivityMs >= cutoffMs || createdMs >= cutoffMs;
    const urgent = linkedTasks.some((task) => {
      const due = normalizeTrackerDueDate(task?.dueDate);
      return Number(task?.priority) === 1 || (due && due <= today);
    });
    const openComms = linkedInbox.length > 0;
    const websiteLike = isWebsiteLikeProject(project);
    const current = isDesktop || fresh || openComms || urgent;
    const ageDays = lastActivityMs > 0 ? Math.max(0, Math.floor((nowMs - lastActivityMs) / MS_PER_DAY)) : null;

    const row = {
      id: String(project?.id || ''),
      name: String(project?.name || '').trim(),
      type: String(project?.type || '').trim(),
      status: String(project?.status || '').trim(),
      dueDate,
      workspacePath: String(project?.workspacePath || '').trim(),
      repoUrl: String(project?.repoUrl || '').trim(),
      lastActivityAt: lastActivityMs ? new Date(lastActivityMs).toISOString() : '',
      ageDays,
      openTaskCount: linkedTasks.length,
      pendingCommCount: linkedInbox.length,
      reason: isDesktop ? 'Active desktop workspace' : openComms ? 'Pending communication' : urgent ? 'Urgent task' : fresh ? 'Recent activity' : 'Stale',
      websiteLike,
    };

    if (current) focus.push(row);
    else if (websiteLike) staleWebsite.push(row);
  }

  focus.sort((a, b) => {
    const ar = a.reason === 'Active desktop workspace' ? 0 : a.pendingCommCount ? 1 : a.openTaskCount ? 2 : 3;
    const br = b.reason === 'Active desktop workspace' ? 0 : b.pendingCommCount ? 1 : b.openTaskCount ? 2 : 3;
    if (ar !== br) return ar - br;
    return String(b.lastActivityAt || '').localeCompare(String(a.lastActivityAt || ''));
  });
  staleWebsite.sort((a, b) => Number(b.ageDays || 9999) - Number(a.ageDays || 9999));

  return { focus: focus.slice(0, 12), staleWebsite: staleWebsite.slice(0, 40), activeProjectId };
}

function buildMarcusLivePendingCommunications(store, settings, limit = 24) {
  const s = store && typeof store === 'object' ? store : EMPTY_STORE;
  const visible = getVisibleInboxItemsFromSettings(s.inboxItems, settings);
  const projects = Array.isArray(s.projects) ? s.projects : [];
  const projectById = new Map(projects.map((p) => [String(p?.id || ''), p]));
  const items = visible
    .filter((item) => {
      const status = String(item?.status || '').trim().toLowerCase();
      if (status === 'done' || status === 'archived' || status === 'dismissed') return false;
      const source = String(item?.source || item?.channel || '').trim().toLowerCase();
      const text = String(item?.text || item?.body || item?.subject || '').trim();
      return text && (!source || ['email', 'slack', 'sms', 'quo', 'openphone', 'fireflies', 'other', 'inbox'].includes(source));
    })
    .sort((a, b) => String(b?.updatedAt || b?.createdAt || '').localeCompare(String(a?.updatedAt || a?.createdAt || '')))
    .slice(0, limit)
    .map((item) => {
      const projectId = String(item?.projectId || '').trim();
      const project = projectById.get(projectId) || null;
      const text = String(item?.text || item?.body || '').replace(/\s+/g, ' ').trim();
      const from = String(item?.from || item?.sender || item?.contactName || item?.clientName || item?.projectName || project?.clientName || project?.name || '').trim();
      return {
        id: String(item?.id || ''),
        person: from || 'Unknown',
        source: String(item?.source || item?.channel || 'inbox').trim(),
        status: String(item?.status || 'New').trim(),
        description: previewTextServer(text, 150),
        projectId,
        projectName: String(item?.projectName || project?.name || '').trim(),
        contactId: String(item?.contactId || '').trim(),
        createdAt: String(item?.createdAt || '').trim(),
        updatedAt: String(item?.updatedAt || '').trim(),
        actionHint: projectId ? 'Linked to project' : 'Needs account/project link',
      };
    });
  return items;
}

async function sendMarcusLiveDashboardSnapshot(req, res) {
  try {
    const businessKey = getBusinessKeyFromContext();
    const [store, settings, projectActivity] = await Promise.all([
      readStore(),
      readSettings(),
      projectEvidenceService.getActivity(businessKey).catch(() => null),
    ]);
    const nowMs = Date.now();
    const dc = desktopRelayCache?.data || desktopContextCache?.data || null;
    const health = dc?.systemHealth || null;
    const projects = buildMarcusLiveProjectFocus(store, dc, nowMs);
    const pendingCommunications = buildMarcusLivePendingCommunications(store, settings, 30);
    const evidenceSnapshots = Array.isArray(projectActivity?.snapshots) ? projectActivity.snapshots : [];
    const evidenceFocus = projectActivity?.currentFocus?.currentFocusProject || null;
    const evidenceFocusRows = evidenceSnapshots.filter((item) => !['stale', 'dormant', 'abandoned_candidate', 'unknown'].includes(item.state)).slice(0, 12).map((item) => ({
      id: item.projectRegistryId,
      name: item.projectName,
      status: item.state,
      reason: item.projectRegistryId === evidenceFocus?.projectRegistryId ? projectActivity.currentFocus.reason : item.reasons?.[0],
      activityScore: item.activityScore,
      focusScore: item.focusScore,
      confidence: item.confidence,
      commitCount7d: item.commitCount7d,
      codexJobs7d: item.codexJobs7d,
      desktopActiveMinutes7d: item.desktopActiveMinutes7d,
      deployments30d: item.deployments30d,
      risks: item.risks,
    }));
    res.json({
      ok: true,
      generatedAt: new Date(nowMs).toISOString(),
      desktop: dc ? {
        windowTitle: dc.windowTitle || '',
        processName: dc.processName || '',
        idleSeconds: dc.idleSeconds || 0,
        source: dc.source || '',
        workspace: dc.workspace ? {
          folderName: dc.workspace.folderName || '',
          workspacePath: dc.workspace.workspacePath || '',
          gitBranch: dc.workspace.gitBranch || '',
          activeFile: dc.workspace.activeFile || '',
          gitStatus: dc.workspace.gitStatus || [],
        } : null,
      } : null,
      systemHealth: health ? {
        cpuPercent: health.cpuPercent,
        memoryPercent: health.memoryPercent,
        memoryUsedGB: health.memoryUsedGB,
        memoryTotalGB: health.memoryTotalGB,
        disks: health.disks || [],
        topProcesses: health.topProcesses || [],
        topMemProcesses: health.topMemProcesses || [],
        defender: health.defender || {},
        recentThreats: health.recentThreats || [],
        unusualListeners: health.unusualListeners || [],
        uptimeHours: health.uptimeHours,
        collectedAt: health.collectedAt || '',
      } : null,
      currentFocus: evidenceFocusRows.length ? evidenceFocusRows : projects.focus,
      evidenceFocus: projectActivity?.currentFocus || null,
      projectActivity,
      staleProjects: projectActivity?.stale || [],
      projectBottlenecks: projectActivity?.bottlenecks || [],
      staleWebsiteProjects: projects.staleWebsite,
      pendingCommunications,
      counts: {
        currentFocus: evidenceFocusRows.length || projects.focus.length,
        staleProjects: projectActivity?.stale?.length || 0,
        projectBottlenecks: projectActivity?.bottlenecks?.length || 0,
        staleWebsiteProjects: projects.staleWebsite.length,
        pendingCommunications: pendingCommunications.length,
      },
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || 'Failed to build Marcus Live snapshot' });
  }
}

app.get('/api/marcus/live/dashboard', sendMarcusLiveDashboardSnapshot);

async function queueMarcusLivePerformanceAction(req, res) {
  const mode = String(req.body?.mode || '').trim().toLowerCase();
  const allowed = new Set(['balanced', 'performance', 'power-saver', 'optimize']);
  if (!allowed.has(mode)) return res.status(400).json({ ok: false, error: 'Invalid performance mode' });
  let action;
  try {
    action = await queueDesktopAction({
      type: 'set-performance-profile',
      payload: { mode },
      requestedBy: 'marcus-live',
    });
  } catch (error) {
    return res.status(503).json({ ok: false, error: 'The desktop action queue is unavailable.', code: error?.code || 'DESKTOP_ACTION_QUEUE_FAILED' });
  }
  rememberMarcusLiveAction({
    action: 'performance',
    label: `Marcus Live performance: ${mode}`,
    kind: 'system',
    target: mode,
  });
  res.status(202).json({ ok: true, queued: true, actionId: action.id, mode });
}

app.post('/api/marcus/live/performance', queueMarcusLivePerformanceAction);

app.post('/api/marcus/live/action', (req, res) => {
  const entry = rememberMarcusLiveAction(req.body || {});
  pushLiveEvent({ type: 'action', ...entry });
  res.json({ ok: true, action: entry });
});

app.get('/api/marcus/live/voice/status', (req, res) => {
  res.json({
    ok: true,
    provider: ELEVENLABS_API_KEY && ELEVENLABS_VOICE_ID ? 'elevenlabs' : 'browser',
    elevenLabsConfigured: Boolean(ELEVENLABS_API_KEY && ELEVENLABS_VOICE_ID),
    model: ELEVENLABS_API_KEY && ELEVENLABS_VOICE_ID ? ELEVENLABS_MODEL_ID : '',
    realtime: {
      provider: 'openai_realtime',
      model: MARCUS_REALTIME_MODEL,
      voice: MARCUS_REALTIME_VOICE,
    },
  });
});

app.post('/api/marcus/realtime/client-secret', async (req, res) => {
  try {
    const settings = await readSettings();
    const openai = getOpenAiSecrets(settings);
    if (!openai.apiKey) {
      return res.status(400).json({ ok: false, error: 'OpenAI is not configured for Marcus realtime voice.' });
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 20_000);
    let upstream;
    let data;
    try {
      const safetyIdentifier = crypto
        .createHash('sha256')
        .update(`marcus:${getBusinessKeyFromContext()}:owner`)
        .digest('hex');
      upstream = await fetch('https://api.openai.com/v1/realtime/client_secrets', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${openai.apiKey}`,
          'Content-Type': 'application/json',
          'OpenAI-Safety-Identifier': safetyIdentifier,
        },
        body: JSON.stringify(buildMarcusRealtimeClientSecretRequest({
          model: MARCUS_REALTIME_MODEL,
          voice: MARCUS_REALTIME_VOICE,
        })),
        signal: controller.signal,
      });
      data = await upstream.json().catch(() => ({}));
    } finally {
      clearTimeout(timer);
    }

    if (!upstream?.ok || !data?.value) {
      const message = data?.error?.message || data?.message || `OpenAI realtime session setup failed (${upstream?.status || 'unknown'}).`;
      return res.status(502).json({ ok: false, error: message });
    }

    res.setHeader('Cache-Control', 'no-store');
    res.json({
      ok: true,
      value: data.value,
      expiresAt: data.expires_at || data.expiresAt || null,
      session: {
        provider: 'openai_realtime',
        model: MARCUS_REALTIME_MODEL,
        voice: MARCUS_REALTIME_VOICE,
      },
    });
  } catch (err) {
    const timedOut = err?.name === 'AbortError';
    res.status(timedOut ? 504 : 502).json({
      ok: false,
      error: timedOut ? 'OpenAI realtime session setup timed out.' : (err?.message || 'OpenAI realtime session setup failed.'),
    });
  }
});

app.post('/api/marcus/realtime/telemetry', async (req, res) => {
  try {
    const events = Array.isArray(req.body?.events) ? req.body.events : [req.body?.event || req.body];
    const result = await realtimeTelemetryStore.append(getBusinessKeyFromContext(), events);
    res.status(202).json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || 'Marcus realtime telemetry could not be recorded.' });
  }
});

app.get('/api/marcus/realtime/acceptance', async (req, res) => {
  try {
    const result = await realtimeTelemetryStore.acceptance(getBusinessKeyFromContext(), {
      sessionId: typeof req.query?.sessionId === 'string' ? req.query.sessionId : '',
      limit: req.query?.limit,
    });
    res.setHeader('Cache-Control', 'no-store');
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || 'Marcus realtime acceptance could not be read.' });
  }
});

app.post('/api/marcus/transcribe', express.raw({
  type: ['audio/*', 'video/webm', 'application/octet-stream'],
  limit: '25mb',
}), async (req, res) => {
  try {
    const audio = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
    if (!audio.length) return res.status(400).json({ ok: false, error: 'No audio received.' });
    if (audio.length > 25 * 1024 * 1024) return res.status(413).json({ ok: false, error: 'Audio is too large.' });

    const settings = await readSettings();
    const openai = getOpenAiSecrets(settings);
    if (!openai.apiKey) return res.status(400).json({ ok: false, error: 'OpenAI API key is not configured.' });

    const contentType = String(req.headers['content-type'] || 'audio/webm').split(';')[0].trim().toLowerCase() || 'audio/webm';
    const extension = contentType.includes('mp4') ? 'mp4'
      : contentType.includes('mpeg') || contentType.includes('mp3') ? 'mp3'
        : contentType.includes('wav') ? 'wav'
          : contentType.includes('ogg') ? 'ogg'
            : 'webm';
    const model = String(process.env.OPENAI_TRANSCRIPTION_MODEL || '').trim() || 'whisper-1';
    const form = new FormData();
    form.append('model', model);
    form.append('language', 'en');
    form.append('file', new Blob([audio], { type: contentType }), `marcus.${extension}`);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 45_000);
    let upstream;
    let data;
    try {
      upstream = await fetch('https://api.openai.com/v1/audio/transcriptions', {
        method: 'POST',
        headers: { Authorization: `Bearer ${openai.apiKey}` },
        body: form,
        signal: controller.signal,
      });
      data = await upstream.json().catch(() => ({}));
    } finally {
      clearTimeout(timer);
    }
    if (!upstream?.ok) {
      const msg = data?.error?.message || data?.message || `OpenAI transcription failed (${upstream?.status || 'unknown'}).`;
      return res.status(upstream?.status || 502).json({ ok: false, error: msg });
    }
    const text = String(data?.text || '').trim();
    res.json({ ok: true, text, model });
  } catch (err) {
    const aborted = err?.name === 'AbortError';
    res.status(aborted ? 504 : 500).json({ ok: false, error: aborted ? 'Transcription timed out.' : (err?.message || 'Transcription failed.') });
  }
});

app.post('/api/marcus/live/voice/speak', async (req, res) => {
  const text = typeof req.body?.text === 'string'
    ? req.body.text.replace(/\s+/g, ' ').trim().slice(0, 900)
    : '';
  if (!text) return res.status(400).json({ error: 'Empty text' });
  if (!ELEVENLABS_API_KEY || !ELEVENLABS_VOICE_ID) {
    return res.status(501).json({ error: 'ElevenLabs voice is not configured.' });
  }

  try {
    const url = new URL(`https://api.elevenlabs.io/v1/text-to-speech/${encodeURIComponent(ELEVENLABS_VOICE_ID)}`);
    if (ELEVENLABS_OUTPUT_FORMAT) url.searchParams.set('output_format', ELEVENLABS_OUTPUT_FORMAT);
    const upstream = await fetch(url, {
      method: 'POST',
      headers: {
        'Accept': 'audio/mpeg',
        'Content-Type': 'application/json',
        'xi-api-key': ELEVENLABS_API_KEY,
      },
      body: JSON.stringify({
        text,
        model_id: ELEVENLABS_MODEL_ID,
        voice_settings: {
          stability: 0.46,
          similarity_boost: 0.82,
          style: 0.28,
          use_speaker_boost: true,
        },
      }),
    });
    if (!upstream.ok) {
      const errText = await upstream.text().catch(() => '');
      return res.status(502).json({ error: `ElevenLabs failed (${upstream.status})`, detail: errText.slice(0, 500) });
    }
    const audio = Buffer.from(await upstream.arrayBuffer());
    res.setHeader('Content-Type', upstream.headers.get('content-type') || 'audio/mpeg');
    res.setHeader('Cache-Control', 'no-store');
    res.send(audio);
  } catch (err) {
    res.status(502).json({ error: String(err?.message || err) });
  }
});

app.get('/api/marcus/active-brief', async (req, res) => {
  try {
    const [brief, controls, sessionState] = await Promise.all([
      buildMarcusActiveBrief(),
      readMarcusOperationalControls(),
      readMarcusSessionState(),
    ]);
    const controlledBrief = applyOperationalControlsToBrief(brief, controls);
    res.json(applyMarcusSessionContextToBrief(controlledBrief, sessionState));
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/api/marcus/session-state', async (req, res) => {
  try {
    res.json(await readMarcusSessionState());
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/session/check-in', async (req, res) => {
  try {
    const [brief, controls, existing] = await Promise.all([
      buildMarcusActiveBrief(),
      readMarcusOperationalControls(),
      readMarcusSessionState(),
    ]);
    const controlledBrief = applyOperationalControlsToBrief(brief, controls);
    const sessionContext = buildSessionContextForBrief(controlledBrief, existing);
    const now = nowIso();
    const saved = await writeMarcusSessionState({
      ...existing,
      lastCheckInAt: now,
      lastOpenedAt: now,
      lastBriefHash: sessionContext.currentBriefHash,
      checkInCount: Number(existing.checkInCount || 0) + 1,
    });
    res.json({
      ok: true,
      sessionState: saved,
      previousSessionContext: sessionContext,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

function normalizeMarcusCommandIntent(message) {
  const text = String(message || '').trim().toLowerCase();
  if (!text) return 'empty';
  if (/\b(memory correction|correct memory|forget memory|archive memory|archive this memory|important memory|mark memory important|mark outdated memory)\b/.test(text)) return 'memory_correction';
  if (/\b(keep this active|keep .* active|reactivate|pin project|project is done|project.*done|mark project complete|this project is complete|archive this|archive project|move .* history|known history)\b/.test(text)) return 'project_correction';
  if (/\b(that is wrong|that's wrong|incorrect|not true|forget this|pin this|mark important|this is important|mark outdated|this is outdated|do not bother me|don't bother me|keep this active|reactivate this)\b/.test(text)) return 'memory_correction';
  if (parseFocusCommand(message)) return 'focus_control';
  if (parseProactiveModeCommand(text)) return 'proactive_mode';
  if (parseCodexGoalCommand(message)) return 'codex_goal';
  if (parseProjectDraftCommand(message)) return 'project_create';
  const wantsNewAction =
    (/\b(create|add|make|turn this into)\b/.test(text) && /\b(action|reminder|follow[- ]?up|reply|draft)\b/.test(text)) ||
    /\bremind me\b/.test(text) ||
    /\bdraft\s+(?:a|the)?\s*(?:reply|response)\b/.test(text) ||
    (/\bfollow[- ]?up\b/.test(text) && /\b(action|task|todo|to-do|create|add|make)\b/.test(text));
  if (wantsNewAction) return 'action_create';
  if (/\bwhat\s+did\s+i\s+say\s+about\s+.+\s+last\s+time\b/.test(text)) return 'entity_context';
  if (/\b(what changed|changed since|since last|check-?in|last time)\b/.test(text)) return 'briefing';
  if (/\b(brief|briefing|session brief|brief me)\b/.test(text)) return 'briefing';
  if (/\b(what matters|today|right now|attention|priority|priorities)\b/.test(text)) return 'what_matters';
  if (/\b(decision|decisions|decide|needs deciding|need to decide|what should i decide|approval point|approval points)\b/.test(text)) return 'decisions';
  if (/\b(can wait|what can wait|what should i ignore|ignore for now|deprioriti[sz]ed|not important|what can i ignore)\b/.test(text)) return 'ignore_queue';
  if (/\b(forgetting|forget|missing|missed|blind spot|blindspot)\b/.test(text)) return 'what_forgetting';
  if (/\b(waiting on me|waiting on mark|need me|needs me|my blocker|i'?m blocking)\b/.test(text)) return 'waiting_on_mark';
  if (/\b(blocked projects?|blocked work|stuck projects?|stuck work|find blockers|show blockers)\b/.test(text)) return 'blocked_projects';
  if (/\b(stale clients?|stale contacts?|silent clients?|which clients are stale|old clients)\b/.test(text)) return 'stale_clients';
  if (/\b(compare|versus| vs |difference between)\b/.test(text)) return 'compare_context';
  if (/\b(show sources?|show evidence|sources only|source list|where did you get)\b/.test(text)) return 'show_sources';
  if (/\b(communication|communications|follow-ups?|follow ups?|draftable|reply|replies|unusual silence|missed opportunities|waiting on others)\b/.test(text)) return 'communication_intelligence';
  if (/\b(stale|dormant|known history|old projects|archive|historical)\b/.test(text)) return 'stale_work';
  if (/\b(confidence|why|ranked|ranking|score|source|evidence)\b/.test(text)) return 'explain_confidence';
  if (/\b(system|systems|health|integration|credential|api|website|automation)\b/.test(text)) return 'system_health';
  if (/\b(memory|remember|knows|source|stale assumption|outdated)\b/.test(text)) return 'memory';
  if (/\b(summary|session)\b/.test(text)) return 'briefing';
  if (/\b(approve|approved|complete|completed|done|dismiss|dismissed)\b/.test(text) && /\b(action|draft|approval)\b/.test(text)) return 'action_transition';
  if (/\b(action|actions|approval|draft|execute|next move)\b/.test(text)) return 'actions';
  return 'general_operational';
}

function parseProactiveModeCommand(message) {
  const text = String(message || '').trim().toLowerCase();
  if (!text) return '';
  const hasModeCue = /\b(mode|go|switch|set|make)\b/.test(text);
  if (/\b(current focus|focus lane|focus lanes|focus on|working on)\b/.test(text) && !/\bfocus mode\b/.test(text)) return '';
  if (/\b(do not disturb|don't disturb|heads down)\b/.test(text) || (hasModeCue && /\bquiet\b/.test(text))) return 'quiet';
  if (hasModeCue && /\b(normal|standard|default)\b/.test(text)) return 'normal';
  if (hasModeCue && /\b(aggressive|high signal|show everything|tell me everything)\b/.test(text)) return 'aggressive';
  if (/\bfocus mode\b/.test(text) || (hasModeCue && /\bfocus\b/.test(text) && /\bmode\b/.test(text))) return 'focus';
  if (hasModeCue && /\b(away|offline|out of office)\b/.test(text)) return 'away';
  return '';
}

function parseFocusCommand(message) {
  const raw = String(message || '').replace(/\s+/g, ' ').trim().slice(0, 1000);
  const text = raw.toLowerCase();
  if (!raw) return null;
  if (/\b(clear|remove|unset)\b/.test(text) && /\b(current focus|focus lane|focus lanes|pinned focus)\b/.test(text)) {
    return { action: 'clear' };
  }
  if (!/\b(set|pin|make|current focus|focus on|working on|focus lane)\b/.test(text)) return null;
  const match = raw.match(/\b(?:set|pin|make)\s+(?:my\s+)?(?:current\s+)?focus(?:\s+lane)?(?:\s+to|:)?\s+(.+)$/i)
    || raw.match(/\b(?:focus on|working on)\s+(.+)$/i);
  const title = String(match?.[1] || '').replace(/\.$/, '').trim();
  if (!title || title.length < 2) return null;
  return {
    action: 'set',
    title: title.slice(0, 180),
    summary: `Pinned current focus from command: ${title.slice(0, 220)}`,
  };
}

function parseProjectDraftCommand(message) {
  const raw = String(message || '').replace(/\s+/g, ' ').trim().slice(0, 1000);
  const text = raw.toLowerCase();
  if (!raw) return null;
  if (/\b(archive|complete|done|history|historical|keep active|reactivate)\b/.test(text)) return null;
  if (!/\b(project|initiative|workstream)\b/.test(text)) return null;
  const match = raw.match(/\b(?:turn this into|make this|convert this into)\s+(?:a\s+)?(?:project|initiative|workstream)(?::|\s+)?(.*)$/i)
    || raw.match(/\b(?:create|add|draft|start)\s+(?:a\s+)?(?:new\s+)?(?:project|initiative|workstream)(?:\s+called|\s+named|\s+for|:)?\s*(.*)$/i);
  if (!match) return null;
  const name = String(match[1] || '').replace(/[.!?]+$/g, '').trim();
  return {
    action: 'create_project_draft',
    projectName: (name || 'New project from command').slice(0, 180),
    summary: raw,
    sourceText: raw,
  };
}

function parseCodexGoalCommand(message) {
  const text = String(message || '').trim().toLowerCase();
  if (!text || !/\bcodex\b/.test(text)) return null;
  if (/\b(prompt|goal|handoff|hand off|send|queue|start|spin up|work on|build|implement|fix|refactor|audit|review|test|ship)\b/.test(text)) {
    return { sourceText: String(message || '').trim() };
  }
  return null;
}

function buildCodexGoalDraftFromCommand(message, brief = {}) {
  const raw = String(message || '').replace(/\s+/g, ' ').trim().slice(0, 1600);
  const focus = brief?.currentFocus || brief?.activeProject || {};
  const project = focus && typeof focus === 'object' ? focus : {};
  const topAttention = Array.isArray(brief?.controlledAttention?.topPriorities)
    ? brief.controlledAttention.topPriorities
    : (Array.isArray(brief?.topPriorities) ? brief.topPriorities : []);
  const decisions = Array.isArray(brief?.decisionQueue) ? brief.decisionQueue : [];
  const systems = Array.isArray(brief?.systemHealth?.items) ? brief.systemHealth.items : [];
  const projectName = String(project?.title || project?.name || '').trim();
  const workspacePath = String(project?.workspacePath || project?.path || '').trim();
  const businessName = String(project?.businessName || project?.businessKey || '').trim();
  const cleanObjective = raw
    .replace(/^(please\s+)?(create|make|queue|draft|send|prompt)\s+(a\s+)?/i, '')
    .replace(/\b(codex|goal|handoff|hand off|prompt)\b/ig, '')
    .replace(/\s+/g, ' ')
    .replace(/^[\s:.-]+/, '')
    .trim();
  const objective = cleanObjective || raw || 'Work from the MARCUS project plan and implement the next approved coding step.';
  const contextLines = [
    projectName ? `Project/focus: ${projectName}` : '',
    businessName ? `Business/context: ${businessName}` : '',
    workspacePath ? `Workspace path: ${workspacePath}` : '',
    project?.summary || project?.detail ? `Focus detail: ${String(project.summary || project.detail).replace(/\s+/g, ' ').trim().slice(0, 500)}` : '',
    ...topAttention.slice(0, 4).map((item, idx) => `Attention ${idx + 1}: ${String(item?.title || item?.summary || '').replace(/\s+/g, ' ').trim().slice(0, 220)}${item?.recommendedAction ? `; suggested: ${String(item.recommendedAction).slice(0, 180)}` : ''}`),
    ...decisions.slice(0, 3).map((item, idx) => `Decision ${idx + 1}: ${String(item?.title || item?.question || item?.summary || '').replace(/\s+/g, ' ').trim().slice(0, 220)}`),
    ...systems.filter((item) => String(item?.status || '').toLowerCase() !== 'ok').slice(0, 3).map((item, idx) => `System ${idx + 1}: ${String(item?.title || item?.summary || '').replace(/\s+/g, ' ').trim().slice(0, 220)}`),
  ].filter(Boolean);

  const prompt = [
    '# Goal for Codex',
    '',
    '## Objective',
    objective,
    '',
    '## MARCUS Context',
    contextLines.length ? contextLines.map((line) => `- ${line}`).join('\n') : '- No specific project context was available. Inspect the current repository before making assumptions.',
    '',
    '## Operating Rules',
    '- Read the current codebase first and preserve existing data/functionality.',
    '- Keep the change scoped to this objective unless you find a concrete dependency.',
    '- Treat external side effects such as deploy, publish, send, bill, delete, DNS, or merge as approval-gated.',
    '- Prefer focused implementation plus verification over broad refactors.',
    '',
    '## Expected Output',
    '- Implement the requested change or explain the blocker with evidence.',
    '- Run relevant syntax/tests/runtime checks.',
    '- Summarize changed files, behavior, and residual risks.',
  ].join('\n');

  return {
    title: `Codex goal: ${objective}`.slice(0, 180),
    summary: `Approval-gated Codex handoff drafted from MARCUS planning${projectName ? ` for ${projectName}` : ''}.`,
    body: prompt.slice(0, 6000),
    type: 'codex_goal',
    lifecycle: 'draft_action',
    requiresApproval: true,
    changedBy: 'command',
    payload: {
      codexPrompt: prompt.slice(0, 6000),
      sourceText: raw,
      projectName,
      workspacePath,
      businessName,
    },
  };
}

function buildManualActionDraftFromCommand(message) {
  const raw = String(message || '').replace(/\s+/g, ' ').trim().slice(0, 1000);
  let cleaned = raw
    .replace(/^(please\s+)?(create|add|make)\s+(a\s+)?/i, '')
    .replace(/^turn this into\s+(an?\s+)?/i, '')
    .replace(/^remind me\s+(to|if|when)?\s*/i, '')
    .replace(/^draft\s+(a|the)?\s*(reply|response)(\s+but\s+don'?t\s+send)?[.:]?\s*/i, '')
    .trim();
  const isReminder = /\b(remind|reminder)\b/i.test(raw);
  const isFollowUp = /\b(follow[- ]?up)\b/i.test(raw);
  const isDraft = /\b(draft|reply|response)\b/i.test(raw);
  const type = isReminder ? 'reminder' : isFollowUp ? 'follow_up' : isDraft ? 'draft_reply' : 'manual_action';
  const titlePrefix = isReminder ? 'Reminder' : isFollowUp ? 'Follow-up' : isDraft ? 'Draft reply' : 'Action';
  if (isFollowUp && /^follow[- ]?up\.?$/i.test(cleaned)) cleaned = 'Review the most important follow-up from current context';
  if (isDraft && !cleaned) cleaned = 'Prepare reply draft for the current communication context';
  const titleText = cleaned || raw || 'Review manual action';
  return {
    title: `${titlePrefix}: ${titleText}`.slice(0, 180),
    summary: raw || titleText,
    body: raw,
    type,
    lifecycle: isDraft ? 'draft_action' : 'suggested_action',
    requiresApproval: true,
    changedBy: 'command',
  };
}

function commandLineForItem(item, idx) {
  const title = String(item?.title || item?.name || 'Untitled').trim();
  const rawDetail = String(item?.detail || item?.summary || item?.reason || '').replace(/\s+/g, ' ').trim();
  const detail = rawDetail.toLowerCase() === title.toLowerCase() ? '' : rawDetail;
  const reason = Array.isArray(item?.reasons) && item.reasons.length ? ` Reason: ${item.reasons[0]}.` : '';
  const compressed = Number(item?.duplicateCount || 0) > 1 ? ` Compressed ${Number(item.duplicateCount)} related signals.` : '';
  const score = Number.isFinite(Number(item?.score)) ? ` Score ${Math.round(Number(item.score))}.` : '';
  return `${idx + 1}. ${title}${detail ? ` - ${detail.slice(0, 180)}` : ''}${reason}${compressed}${score}`;
}

function commandEvidenceForItem(item) {
  if (!item || typeof item !== 'object') return null;
  const reasons = Array.isArray(item.reasons) ? item.reasons : Array.isArray(item.scoreReasons) ? item.scoreReasons : [];
  const related = Array.isArray(item.relatedEntities) ? item.relatedEntities : Array.isArray(item.relationships) ? item.relationships : [];
  return {
    id: String(item.id || item.target || item.name || item.title || '').trim(),
    title: String(item.title || item.name || item.summary || 'Evidence').trim().slice(0, 160),
    source: String(item.source || item.channel || item.group || item.type || '').trim().slice(0, 80),
    confidence: Number.isFinite(Number(item.confidence)) ? Math.round(Number(item.confidence) * 100) : null,
    score: Number.isFinite(Number(item.score)) ? Math.round(Number(item.score)) : null,
    why: String(reasons[0] || item.reason || item.recommendedAction || item.summary || '').trim().slice(0, 220),
    relatedEntities: related.slice(0, 6),
  };
}

function buildCommandEvidence(cards, brief) {
  const evidence = (Array.isArray(cards) ? cards : []).map(commandEvidenceForItem).filter(Boolean).slice(0, 8);
  const sourceCounts = {};
  for (const item of evidence) {
    const source = item.source || 'unknown';
    sourceCounts[source] = Number(sourceCounts[source] || 0) + 1;
  }
  return {
    items: evidence,
    sourceCounts,
    policy: brief?.attentionPolicy || null,
    lowSignalSuppressedCount: Number(brief?.lowSignalSuppressedCount || 0),
  };
}

function briefOperationalCollections(brief) {
  const memoryRecords = Array.isArray(brief?.memoryPulse?.records) ? brief.memoryPulse.records : [];
  const systemItems = Array.isArray(brief?.systemHealth?.items) ? brief.systemHealth.items : [];
  const worldEntities = flattenWorldEntities(brief?.worldModel);
  return [
    ...(Array.isArray(brief?.controlledAttention?.attentionQueue) ? brief.controlledAttention.attentionQueue : []),
    ...(Array.isArray(brief?.topPriorities) ? brief.topPriorities : []),
    ...(Array.isArray(brief?.urgentInterrupts) ? brief.urgentInterrupts : []),
    ...(Array.isArray(brief?.operationalSignals) ? brief.operationalSignals : []),
    ...(Array.isArray(brief?.decisionQueue) ? brief.decisionQueue : []),
    ...(Array.isArray(brief?.activeActionQueue) ? brief.activeActionQueue : []),
    ...(Array.isArray(brief?.actionQueue) ? brief.actionQueue : []),
    ...(Array.isArray(brief?.ignoreQueue) ? brief.ignoreQueue : []),
    ...memoryRecords,
    ...systemItems,
    ...worldEntities,
  ];
}

function findBriefOperationalItem(brief, itemIdOrQuery) {
  const query = String(itemIdOrQuery || '').trim();
  if (!query) return null;
  const q = query.toLowerCase();
  const rows = briefOperationalCollections(brief);
  const exact = rows.find((item) => {
    const ids = [
      item?.id,
      item?.sourceSignalId,
      item?.sourceActionId,
      item?.target,
      item?.targetId,
      item?.name,
      item?.title,
    ].map((value) => String(value || '').trim());
    return ids.some((id) => id && id === query);
  });
  if (exact) return exact;
  return rows.find((item) => {
    const text = `${item?.id || ''} ${item?.name || ''} ${item?.title || ''} ${item?.summary || ''}`.toLowerCase();
    return text.includes(q);
  }) || null;
}

function inspectBriefOperationalItem(brief, itemIdOrQuery) {
  const item = findBriefOperationalItem(brief, itemIdOrQuery);
  if (!item) return null;
  const itemId = String(item.id || item.sourceSignalId || item.name || item.title || '').trim();
  const entityContext = itemId ? inspectWorldEntity(brief, itemId) : null;
  const evidence = buildCommandEvidence([item], brief);
  const reasons = Array.isArray(item.reasons) ? item.reasons : Array.isArray(item.scoreReasons) ? item.scoreReasons : [];
  const sourceRefs = Array.isArray(item.sourceRefs) ? item.sourceRefs : [];
  const relatedEntities = Array.isArray(item.relatedEntities) ? item.relatedEntities : [];
  const controls = {
    canSnooze: Boolean(item.sourceSignalId || (item.id && !String(item.id).startsWith('decision:'))),
    canDismiss: Boolean(item.sourceSignalId || item.sourceActionId || item.id),
    canConvertToAction: Boolean(item.sourceSignalId || (item.id && !String(item.id).startsWith('decision:'))),
    canApprove: Boolean(item.sourceActionId || (item.id && (item.approvalRequired || item.requiresApproval || String(item.lifecycle || '').includes('draft')))),
    canCorrectMemory: String(item.type || item.group || '').toLowerCase().includes('memory') || String(item.id || '').startsWith('memory:'),
  };
  return {
    ok: true,
    item,
    explanation: {
      title: String(item.title || item.name || 'Operational item').trim(),
      summary: String(item.detail || item.summary || item.recommendedAction || item.nextAction || '').trim(),
      why: String(reasons[0] || item.reason || item.recommendedAction || item.summary || 'This item is present in the current ActiveBrief.').trim(),
      source: String(item.source || item.channel || item.group || item.type || 'unknown').trim(),
      score: Number.isFinite(Number(item.score)) ? Math.round(Number(item.score)) : null,
      confidence: Number.isFinite(Number(item.confidence)) ? Math.round(Number(item.confidence) * 100) : null,
      bucket: String(item.bucket || item.status || '').trim(),
      recommendedAction: String(item.recommendedAction || item.nextAction || item.suggestedButtonLabel || '').trim(),
    },
    evidence,
    sourceRefs,
    relatedEntities,
    relatedContext: entityContext ? {
      relationships: entityContext.relationships || [],
      relatedEntities: entityContext.relatedEntities || [],
      relatedSignals: entityContext.relatedSignals || [],
      relatedDecisions: entityContext.relatedDecisions || [],
      relatedActions: entityContext.relatedActions || [],
      relatedSystems: entityContext.relatedSystems || [],
      relatedMemory: entityContext.relatedMemory || [],
      counts: entityContext.counts || {},
    } : null,
    controls,
    generatedAt: nowIso(),
  };
}

function flattenWorldEntities(worldModel) {
  const wm = worldModel && typeof worldModel === 'object' ? worldModel : {};
  const entities = wm.entities && typeof wm.entities === 'object' ? wm.entities : {};
  const rows = [];
  for (const [group, list] of Object.entries(entities)) {
    if (!Array.isArray(list)) continue;
    for (const entity of list) {
      if (!entity || typeof entity !== 'object') continue;
      rows.push({
        ...entity,
        group,
        id: String(entity.id || entity.name || '').trim(),
        name: String(entity.name || entity.title || entity.id || '').trim(),
      });
    }
  }
  return rows.filter((entity) => entity.id || entity.name);
}

function scoreEntityMatch(entity, query) {
  const q = String(query || '').trim().toLowerCase();
  if (!q) return 0;
  const name = String(entity?.name || '').toLowerCase();
  const id = String(entity?.id || '').toLowerCase();
  const type = String(entity?.type || entity?.group || '').toLowerCase();
  const source = String(entity?.source || '').toLowerCase();
  let score = 0;
  if (name === q || id === q) score += 100;
  if (name.includes(q)) score += 60;
  if (id.includes(q)) score += 45;
  if (type.includes(q)) score += 20;
  if (source.includes(q)) score += 10;
  for (const part of q.split(/\s+/g).filter(Boolean)) {
    if (name.includes(part)) score += 12;
    if (id.includes(part)) score += 8;
  }
  return score;
}

function searchWorldEntities(brief, query, { limit = 10 } = {}) {
  const rows = flattenWorldEntities(brief?.worldModel);
  return rows
    .map((entity) => ({ ...entity, matchScore: scoreEntityMatch(entity, query) }))
    .filter((entity) => entity.matchScore > 0)
    .sort((a, b) => b.matchScore - a.matchScore || String(a.name).localeCompare(String(b.name)))
    .slice(0, Math.max(1, Math.min(50, Number(limit) || 10)));
}

function entityGroupMatches(entity, groups = []) {
  const set = new Set(groups.map((group) => String(group || '').toLowerCase()));
  const group = String(entity?.group || '').toLowerCase();
  const type = String(entity?.type || '').toLowerCase();
  return set.has(group) || set.has(type);
}

function uniqueRelatedRows(rows = []) {
  const out = [];
  const seen = new Set();
  for (const row of Array.isArray(rows) ? rows : []) {
    const id = String(row?.id || row?.name || row?.title || '').trim();
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(row);
  }
  return out;
}

function inspectWorldEntity(brief, entityIdOrQuery) {
  const rows = flattenWorldEntities(brief?.worldModel);
  const query = String(entityIdOrQuery || '').trim();
  if (!query) return null;
  const entity = rows.find((row) => row.id === query) || searchWorldEntities(brief, query, { limit: 1 })[0] || null;
  if (!entity) return null;

  const relationships = Array.isArray(brief?.worldModel?.relationships) ? brief.worldModel.relationships : [];
  const relatedEdges = relationships.filter((rel) => rel?.from === entity.id || rel?.to === entity.id);
  const relatedIds = new Set();
  for (const rel of relatedEdges) {
    if (rel?.from && rel.from !== entity.id) relatedIds.add(rel.from);
    if (rel?.to && rel.to !== entity.id) relatedIds.add(rel.to);
  }
  const relatedEntities = rows.filter((row) => relatedIds.has(row.id)).slice(0, 20);
  const haystackTerms = [entity.id, entity.name].filter(Boolean).map((x) => String(x).toLowerCase());
  const includesEntity = (item) => {
    const text = JSON.stringify(item || {}).toLowerCase();
    return haystackTerms.some((term) => term && text.includes(term));
  };
  const rowsByGroup = (groups) => relatedEntities.filter((row) => entityGroupMatches(row, groups));
  const relatedSignals = uniqueRelatedRows([
    ...rowsByGroup(['signals']),
    ...(Array.isArray(brief?.operationalSignals) ? brief.operationalSignals : []).filter(includesEntity),
  ]).slice(0, 10);
  const relatedDecisions = uniqueRelatedRows([
    ...rowsByGroup(['decisions', 'Decision']),
    ...(Array.isArray(brief?.decisionQueue) ? brief.decisionQueue : []).filter(includesEntity),
  ]).slice(0, 10);
  const relatedActions = uniqueRelatedRows([
    ...rowsByGroup(['actions', 'ActionDraft', 'Action']),
    ...(Array.isArray(brief?.actionQueue) ? brief.actionQueue : []).filter(includesEntity),
  ]).slice(0, 10);
  const relatedSystems = uniqueRelatedRows([
    ...rowsByGroup(['systems', 'System', 'SystemSignal', 'Tool', 'Website', 'Payment']),
    ...(Array.isArray(brief?.systemHealth?.items) ? brief.systemHealth.items : []).filter(includesEntity),
  ]).slice(0, 10);
  const relatedMemory = uniqueRelatedRows([
    ...rowsByGroup(['memory', 'Memory']),
    ...(Array.isArray(brief?.memoryPulse?.records) ? brief.memoryPulse.records : []).filter(includesEntity),
  ]).slice(0, 10);
  return {
    ok: true,
    entity,
    relationships: relatedEdges.slice(0, 30),
    relatedEntities,
    relatedSignals,
    relatedDecisions,
    relatedActions,
    relatedSystems,
    relatedMemory,
    counts: {
      relationships: relatedEdges.length,
      relatedEntities: relatedEntities.length,
      signals: relatedSignals.length,
      decisions: relatedDecisions.length,
      actions: relatedActions.length,
      systems: relatedSystems.length,
      memory: relatedMemory.length,
    },
  };
}

function extractEntityQuery(message) {
  const raw = String(message || '').trim();
  const patterns = [
    /\bwhat\s+did\s+i\s+say\s+about\s+(.+?)\s+last\s+time\??$/i,
    /\b(?:show|find|inspect|summarize|open)\s+(?:me\s+)?(?:everything\s+)?(?:related\s+to|about|for)\s+(.+)$/i,
    /\b(?:what changed with|what do you know about|everything related to)\s+(.+)$/i,
    /\b(?:related)\s+(.+)$/i,
  ];
  for (const re of patterns) {
    const m = raw.match(re);
    if (m?.[1]) return m[1].replace(/[?.!]+$/g, '').trim();
  }
  return '';
}

function parseCompareCommand(message) {
  const raw = String(message || '').replace(/\s+/g, ' ').trim().slice(0, 1000);
  if (!raw) return null;
  const patterns = [
    /\bcompare\s+(.+?)\s+(?:and|to|with)\s+(.+)$/i,
    /\bdifference between\s+(.+?)\s+and\s+(.+)$/i,
    /\b(.+?)\s+vs\.?\s+(.+)$/i,
  ];
  for (const re of patterns) {
    const match = raw.match(re);
    if (!match?.[1] || !match?.[2]) continue;
    const left = String(match[1] || '').replace(/[?.!]+$/g, '').trim();
    const right = String(match[2] || '').replace(/[?.!]+$/g, '').trim();
    if (left && right) return { left: left.slice(0, 160), right: right.slice(0, 160) };
  }
  return null;
}

function buildMarcusCommandResponse({ message, brief }) {
  const intent = normalizeMarcusCommandIntent(message);
  const top = Array.isArray(brief?.controlledAttention?.topPriorities) ? brief.controlledAttention.topPriorities : (Array.isArray(brief?.topPriorities) ? brief.topPriorities : []);
  const waiting = Array.isArray(brief?.waitingOnMark) ? brief.waitingOnMark : [];
  const actions = Array.isArray(brief?.activeActionQueue) && brief.activeActionQueue.length ? brief.activeActionQueue : (Array.isArray(brief?.actionQueue) && brief.actionQueue.length ? brief.actionQueue : (Array.isArray(brief?.preparedActions) ? brief.preparedActions : []));
  const decisions = Array.isArray(brief?.decisionQueue) ? brief.decisionQueue : [];
  const ignoreQueue = Array.isArray(brief?.ignoreQueue) ? brief.ignoreQueue : [];
  const stale = Array.isArray(brief?.worldModel?.knownHistory) ? brief.worldModel.knownHistory : (Array.isArray(brief?.stalledProjects) ? brief.stalledProjects : []);
  const projectActivity = Array.isArray(brief?.projectActivity) ? brief.projectActivity : [];
  const systems = Array.isArray(brief?.systemHealth?.items) ? brief.systemHealth.items : [];
  const comms = brief?.communicationIntelligence && typeof brief.communicationIntelligence === 'object' ? brief.communicationIntelligence : {};
  const memory = brief?.memoryPulse && typeof brief.memoryPulse === 'object' ? brief.memoryPulse : {};
  const session = brief?.sessionBriefing && typeof brief.sessionBriefing === 'object' ? brief.sessionBriefing : {};
  const sessionContext = brief?.sessionContext && typeof brief.sessionContext === 'object' ? brief.sessionContext : {};
  const confidence = Number.isFinite(Number(brief?.confidence)) ? Math.round(Number(brief.confidence) * 100) : null;

  let title = 'Operational answer';
  let lines = [];
  let suggestedActions = [];
  let cards = [];
  const entityQuery = extractEntityQuery(message);
  const compareQuery = parseCompareCommand(message);

  let responseIntent = intent;

  if (compareQuery) {
    responseIntent = 'compare_context';
    const left = inspectWorldEntity(brief, compareQuery.left);
    const right = inspectWorldEntity(brief, compareQuery.right);
    title = `Compare: ${compareQuery.left} vs ${compareQuery.right}`;
    lines = [
      left?.entity ? `${left.entity.name || left.entity.id}: ${left.counts?.signals || 0} signals, ${left.counts?.actions || 0} actions, ${left.counts?.relationships || 0} relationships.` : `${compareQuery.left}: no exact entity match.`,
      right?.entity ? `${right.entity.name || right.entity.id}: ${right.counts?.signals || 0} signals, ${right.counts?.actions || 0} actions, ${right.counts?.relationships || 0} relationships.` : `${compareQuery.right}: no exact entity match.`,
      left?.relatedSignals?.[0] ? `Left top signal: ${left.relatedSignals[0].title || left.relatedSignals[0].name}.` : '',
      right?.relatedSignals?.[0] ? `Right top signal: ${right.relatedSignals[0].title || right.relatedSignals[0].name}.` : '',
      'Use the cards to inspect the underlying sources before deciding priority.',
    ].filter(Boolean);
    cards = [
      ...(left ? [left.entity, ...left.relatedSignals, ...left.relatedActions] : searchWorldEntities(brief, compareQuery.left, { limit: 3 })),
      ...(right ? [right.entity, ...right.relatedSignals, ...right.relatedActions] : searchWorldEntities(brief, compareQuery.right, { limit: 3 })),
    ].filter(Boolean).slice(0, 10);
  } else if (entityQuery) {
    responseIntent = 'entity_context';
    const inspected = inspectWorldEntity(brief, entityQuery);
    title = inspected?.entity ? `Entity context: ${inspected.entity.name || inspected.entity.id}` : `Entity context: ${entityQuery}`;
    if (inspected?.entity) {
      const q = entityQuery.toLowerCase();
      const changedRows = Array.isArray(sessionContext.changedSinceLastCheckIn) ? sessionContext.changedSinceLastCheckIn : [];
      const relatedChanges = changedRows.filter((item) => JSON.stringify(item || {}).toLowerCase().includes(q)).slice(0, 4);
      lines = [
        `${inspected.entity.name || inspected.entity.id} is a ${inspected.entity.type || inspected.entity.group || 'known entity'} from ${inspected.entity.source || 'unknown source'} with ${Math.round(Number(inspected.entity.confidence || 0) * 100)}% confidence.`,
        relatedChanges.length ? `Changed since last check-in: ${relatedChanges.map((item) => item.title || item.name || item.summary).join('; ')}.` : '',
        inspected.relatedEntities.length ? `Related entities: ${inspected.relatedEntities.slice(0, 8).map((e) => e.name || e.id).join(', ')}.` : 'No direct related entities were found in the current world model.',
        inspected.relatedSignals.length ? `Related signals: ${inspected.relatedSignals.slice(0, 3).map((s) => s.title).join('; ')}.` : '',
        inspected.relatedDecisions.length ? `Related decisions: ${inspected.relatedDecisions.slice(0, 3).map((d) => d.question || d.title || d.name).join('; ')}.` : '',
        inspected.relatedActions.length ? `Related actions: ${inspected.relatedActions.slice(0, 3).map((a) => a.title).join('; ')}.` : '',
        inspected.relatedSystems.length ? `Related systems: ${inspected.relatedSystems.slice(0, 3).map((s) => s.title || s.name).join('; ')}.` : '',
        inspected.relatedMemory.length ? `Related memory: ${inspected.relatedMemory.slice(0, 3).map((m) => m.title).join('; ')}.` : '',
      ].filter(Boolean);
      cards = [
        ...inspected.relatedDecisions,
        ...inspected.relatedActions,
        ...inspected.relatedSignals,
        ...inspected.relatedSystems,
        ...inspected.relatedMemory,
        ...inspected.relatedEntities,
      ].slice(0, 10);
    } else {
      const matches = searchWorldEntities(brief, entityQuery, { limit: 6 });
      lines = matches.length
        ? [`I did not find an exact entity for "${entityQuery}", but these are close:`, ...matches.map((e, idx) => `${idx + 1}. ${e.name || e.id} (${e.type || e.group}, ${e.source || 'unknown source'}).`)]
        : [`I could not find "${entityQuery}" in the current world model. It may be absent, stale, or named differently.`];
      cards = matches;
    }
  } else if (intent === 'what_matters') {
    title = 'What matters right now';
    lines = top.length
      ? top.slice(0, 5).map(commandLineForItem)
      : ['Nothing crossed the active attention threshold. MARCUS is still monitoring for blockers, client risk, and approvals.'];
    suggestedActions = actions.slice(0, 3);
    cards = top.slice(0, 5);
  } else if (intent === 'what_forgetting') {
    title = 'What you may be forgetting';
    const staleLines = stale.slice(0, 3).map((item, idx) => `${idx + 1}. ${item.name || item.title} is not active attention anymore. Keep it in known history unless it should be pinned or reactivated.`);
    const waitingLines = waiting.slice(0, 3).map((item, idx) => `${idx + 1 + staleLines.length}. ${item.title} appears to be waiting on Mark.`);
    const uncertain = Array.isArray(memory.uncertain) ? memory.uncertain.slice(0, 2).map((item, idx) => `${idx + 1 + staleLines.length + waitingLines.length}. Low-confidence memory/signal: ${item.title || item.summary}.`) : [];
    lines = [...waitingLines, ...staleLines, ...uncertain];
    if (!lines.length) lines = ['No obvious forgotten item surfaced. The main risk is still stale or weakly linked memory, so use Memory review if something feels off.'];
    cards = [...waiting, ...stale].slice(0, 6);
  } else if (intent === 'waiting_on_mark') {
    title = 'Waiting on Mark';
    lines = waiting.length ? waiting.slice(0, 6).map(commandLineForItem) : ['No high-confidence item is currently marked as waiting on Mark.'];
    suggestedActions = actions.filter((a) => /reply|draft|prepare|open/i.test(`${a.title || ''} ${a.type || ''}`)).slice(0, 3);
    cards = waiting.slice(0, 6);
  } else if (intent === 'blocked_projects') {
    title = 'Blocked work';
    const blockedSignals = [
      ...top,
      ...(Array.isArray(brief?.operationalSignals) ? brief.operationalSignals : []),
    ].filter((item) => /\b(blocked|stuck|waiting|dependency|credentials|access|approval)\b/i.test(`${item?.title || ''} ${item?.summary || ''} ${item?.detail || ''} ${item?.status || ''} ${item?.recommendedAction || ''}`));
    const blockedProjects = projectActivity.filter((project) => /\b(blocked|stuck|waiting|review)\b/i.test(`${project?.activityStatus || ''} ${project?.status || ''} ${project?.reason || ''} ${project?.nextAction || ''}`));
    cards = [...blockedSignals, ...blockedProjects].slice(0, 8);
    lines = cards.length
      ? cards.slice(0, 6).map((item, idx) => `${idx + 1}. ${item.title || item.name}: ${item.recommendedAction || item.nextAction || item.summary || item.reason || 'Inspect the blocker.'}`)
      : ['No explicit blocked project or stuck work signal crossed the current threshold.'];
    suggestedActions = actions.filter((a) => /\b(block|credential|access|approval|follow)\b/i.test(`${a.title || ''} ${a.summary || ''} ${a.type || ''}`)).slice(0, 3);
  } else if (intent === 'stale_clients') {
    title = 'Stale clients and silent relationships';
    const clientEntities = flattenWorldEntities(brief?.worldModel).filter((entity) => entityGroupMatches(entity, ['clients', 'Client', 'people', 'Person']));
    const silence = Array.isArray(comms.unusualSilence) ? comms.unusualSilence : [];
    const followUps = Array.isArray(comms.followUpsDue) ? comms.followUpsDue : [];
    const staleProjectClients = projectActivity
      .filter((project) => ['parked', 'historical', 'archived', 'dormant'].includes(String(project?.activityStatus || project?.status || '').toLowerCase()) && (project?.clientName || project?.businessName))
      .slice(0, 6)
      .map((project) => ({
        id: project.id,
        title: project.clientName || project.businessName || project.name,
        summary: `${project.name || project.title || 'Work'} is ${project.activityStatus || project.status || 'not active'}.`,
        source: 'projectActivity',
        confidence: project.confidence || 0.68,
      }));
    cards = [...silence, ...followUps, ...staleProjectClients, ...clientEntities].slice(0, 10);
    lines = cards.length
      ? cards.slice(0, 6).map((item, idx) => `${idx + 1}. ${item.title || item.name}: ${item.reason || item.summary || item.status || 'Review relationship context.'}`)
      : ['No stale clients or unusual silence records are visible in the current brief.'];
    suggestedActions = followUps.slice(0, 3).map((item) => ({
      title: `Create follow-up: ${item.title || item.name}`,
      summary: item.summary || item.reason || 'Follow up with this relationship.',
      type: 'follow_up',
      requiresApproval: true,
    }));
  } else if (intent === 'decisions') {
    title = 'Decision queue';
    lines = decisions.length
      ? decisions.slice(0, 6).map((item, idx) => `${idx + 1}. ${item.question || 'Decision needed'} ${item.title ? `(${item.title})` : ''}${item.summary ? ` - ${String(item.summary).slice(0, 180)}` : ''}`)
      : ['No explicit decision points crossed the current signal threshold. MARCUS is still watching for approvals, blockers, and waiting-on-Mark items.'];
    suggestedActions = actions.filter((a) => a?.approvalRequired || a?.requiresApproval).slice(0, 3);
    cards = decisions.slice(0, 8);
  } else if (intent === 'ignore_queue') {
    title = 'What can wait';
    lines = ignoreQueue.length
      ? ignoreQueue.slice(0, 8).map((item, idx) => `${idx + 1}. ${item.title} - ${item.reason || item.summary || 'Can wait.'}${Number.isFinite(Number(item.score)) ? ` Score ${Math.round(Number(item.score))}.` : ''}`)
      : ['Nothing specific is marked as safe to ignore beyond the current suppressed low-signal set. Dormant projects remain searchable but should not compete for attention.'];
    cards = ignoreQueue.slice(0, 8);
  } else if (intent === 'communication_intelligence') {
    title = 'Communication intelligence';
    const counts = comms.counts || {};
    lines = [
      `${Number(counts.waitingOnMark || 0)} waiting on Mark.`,
      `${Number(counts.waitingOnOthers || 0)} waiting on someone else.`,
      `${Number(counts.draftableReplies || 0)} draftable repl${Number(counts.draftableReplies || 0) === 1 ? 'y' : 'ies'}.`,
      `${Number(counts.followUpsDue || 0)} follow-up${Number(counts.followUpsDue || 0) === 1 ? '' : 's'} due.`,
      `${Number(counts.unusualSilence || 0)} unusual silence item${Number(counts.unusualSilence || 0) === 1 ? '' : 's'}.`,
      `${Number(counts.highValueMissedOpportunities || 0)} possible missed opportunit${Number(counts.highValueMissedOpportunities || 0) === 1 ? 'y' : 'ies'}.`,
      ...(Array.isArray(comms.followUpsDue) ? comms.followUpsDue.slice(0, 3).map((item, idx) => `Follow-up ${idx + 1}: ${item.title} - ${String(item.summary || '').slice(0, 160)}`) : []),
      ...(Array.isArray(comms.highValueMissedOpportunities) ? comms.highValueMissedOpportunities.slice(0, 2).map((item, idx) => `Opportunity ${idx + 1}: ${item.title} - ${String(item.summary || '').slice(0, 160)}`) : []),
    ];
    cards = [
      ...(Array.isArray(comms.waitingOnMark) ? comms.waitingOnMark : []),
      ...(Array.isArray(comms.followUpsDue) ? comms.followUpsDue : []),
      ...(Array.isArray(comms.highValueMissedOpportunities) ? comms.highValueMissedOpportunities : []),
    ].slice(0, 8);
    suggestedActions = Array.isArray(comms.draftableReplies) ? comms.draftableReplies.slice(0, 3) : [];
  } else if (intent === 'stale_work') {
    title = 'Stale or historical work';
    lines = stale.length
      ? stale.slice(0, 8).map((item, idx) => `${idx + 1}. ${item.name || item.title} - ${item.activityStatus || item.status || 'history'}${item.lastActivityAt ? `, last activity ${item.lastActivityAt}` : ''}.`)
      : ['No dormant or historical projects were returned in this brief.'];
    cards = stale.slice(0, 8);
  } else if (intent === 'project_correction') {
    title = 'Work state correction';
    const active = projectActivity.filter((project) => ['active', 'waiting', 'warming'].includes(String(project?.activityStatus || project?.status || '').toLowerCase()));
    const history = projectActivity.filter((project) => ['parked', 'historical', 'archived', 'dormant', 'complete'].includes(String(project?.activityStatus || project?.status || '').toLowerCase()));
    const candidates = [...active, ...history].slice(0, 10);
    lines = [
      candidates.length
        ? 'Pick the exact work item below, then keep it active, move it to known history, or mark it complete/archive.'
        : 'No project activity records are available in this brief.',
      'MARCUS records this as a project-state overlay; the source project record is preserved until explicit execution is added.',
    ];
    suggestedActions = [
      { title: 'Keep selected work active', lifecycle: 'suggested_action', requiresApproval: false },
      { title: 'Move selected work to known history', lifecycle: 'suggested_action', requiresApproval: false },
      { title: 'Mark selected work complete/archive', lifecycle: 'suggested_action', requiresApproval: false },
    ];
    cards = candidates;
  } else if (intent === 'explain_confidence') {
    title = 'Why MARCUS ranked this way';
    lines = [
      `Brief confidence is ${confidence === null ? 'not available' : `${confidence}%`}.`,
      'Ranking weighs urgency, importance, deadline proximity, money/relationship risk, current focus, whether Mark is the blocker, and source confidence.',
      `${Number(brief?.lowSignalSuppressedCount || 0)} low-signal item${Number(brief?.lowSignalSuppressedCount || 0) === 1 ? '' : 's'} were suppressed.`,
      ...top.slice(0, 3).map((item, idx) => `${idx + 1}. ${item.title}: ${(Array.isArray(item.reasons) && item.reasons[0]) || 'ranked by attention score'}${Number.isFinite(Number(item.score)) ? `, score ${Math.round(Number(item.score))}` : ''}.`),
    ];
    cards = top.slice(0, 5);
  } else if (intent === 'show_sources') {
    title = 'Sources and evidence';
    cards = [
      ...top.slice(0, 5),
      ...systems.slice(0, 4),
      ...(Array.isArray(memory.records) ? memory.records.slice(0, 4) : []),
    ].slice(0, 10);
    const sourceEvidence = buildCommandEvidence(cards, brief);
    const sourceCounts = sourceEvidence.sourceCounts || {};
    lines = [
      Object.keys(sourceCounts).length ? `Current answer sources: ${Object.entries(sourceCounts).map(([source, count]) => `${source} ${count}`).join(', ')}.` : 'No source counts are available for the current cards.',
      `${Number(brief?.lowSignalSuppressedCount || 0)} low-signal item${Number(brief?.lowSignalSuppressedCount || 0) === 1 ? '' : 's'} are suppressed.`,
      'Use card Inspect/Why controls to see item-level evidence, confidence, and related context.',
    ];
  } else if (intent === 'system_health') {
    title = 'System health';
    lines = systems.length
      ? systems.map((item, idx) => `${idx + 1}. ${item.title}: ${item.status}. ${item.summary || ''} Recommended: ${item.recommendedAction || 'Monitor.'}`)
      : ['No system health records are available in this brief.'];
    cards = systems;
  } else if (intent === 'memory') {
    title = 'Memory pulse';
    const newFacts = Array.isArray(memory.newFacts) ? memory.newFacts.slice(0, 3).map((item) => `New: ${item.title || item.summary} (${item.source || 'unknown source'}).`) : [];
    const staleFacts = Array.isArray(memory.staleAssumptions) ? memory.staleAssumptions.slice(0, 3).map((item) => `Stale: ${item.title || item.summary}.`) : [];
    const uncertain = Array.isArray(memory.uncertain) ? memory.uncertain.slice(0, 3).map((item) => `Uncertain: ${item.title || item.summary}.`) : [];
    lines = [...newFacts, ...staleFacts, ...uncertain];
    if (!lines.length) lines = ['No memory pulse records are available.'];
    cards = Array.isArray(memory.records) ? memory.records.slice(0, 8) : [];
  } else if (intent === 'memory_correction') {
    title = 'Memory correction';
    const records = Array.isArray(memory.records) ? memory.records : [];
    const staleFacts = Array.isArray(memory.staleAssumptions) ? memory.staleAssumptions : [];
    const uncertainFacts = Array.isArray(memory.uncertain) ? memory.uncertain : [];
    const candidates = [...uncertainFacts, ...staleFacts, ...records].filter(Boolean);
    lines = [
      candidates.length
        ? 'Pick the exact memory or signal below, then mark it important, pin it, archive it, mark it outdated, forget it, or inspect sources before changing it.'
        : 'No memory records are available in this brief. Refresh Memory or inspect an entity first.',
      'MARCUS records corrections as a control overlay so the source history remains intact until a future execution system performs deeper edits.',
    ];
    suggestedActions = [
      { title: 'Pin the selected memory', lifecycle: 'suggested_action', requiresApproval: false },
      { title: 'Mark the selected memory important', lifecycle: 'suggested_action', requiresApproval: false },
      { title: 'Archive the selected memory', lifecycle: 'suggested_action', requiresApproval: false },
      { title: 'Mark the selected memory outdated', lifecycle: 'suggested_action', requiresApproval: false },
      { title: 'Forget the selected memory', lifecycle: 'suggested_action', requiresApproval: false },
    ];
    cards = candidates.slice(0, 8);
  } else if (intent === 'briefing') {
    title = 'Session briefing';
    const changed = Array.isArray(sessionContext.changedSinceLastCheckIn) ? sessionContext.changedSinceLastCheckIn : (Array.isArray(session.changedSinceLastTime) ? session.changedSinceLastTime : []);
    lines = [
      sessionContext.briefingLine ? `Changed: ${sessionContext.briefingLine}` : '',
      ...changed.slice(0, 4).map((item, idx) => `Change ${idx + 1}: ${item.title || item.name}${item.summary ? ` - ${String(item.summary).slice(0, 160)}` : ''}.`),
      ...(Array.isArray(session.needsAttention) ? session.needsAttention.slice(0, 3).map((item, idx) => `Attention ${idx + 1}: ${item.title || item.name}.`) : []),
      ...(Array.isArray(session.waitingOnMark) ? session.waitingOnMark.slice(0, 3).map((item, idx) => `Waiting on Mark ${idx + 1}: ${item.title || item.name}.`) : []),
      ...(Array.isArray(session.topActions) ? session.topActions.slice(0, 3).map((item, idx) => `Action ${idx + 1}: ${item.title || item.summary}.`) : []),
      ...(Array.isArray(session.canIgnore) ? session.canIgnore.slice(0, 2).map((item) => {
        if (item && typeof item === 'object') {
          return `Can ignore: ${item.title || item.summary || 'Can wait'}${item.reason ? ` - ${String(item.reason).slice(0, 160)}` : ''}.`;
        }
        return `Can ignore: ${item}.`;
      }) : []),
    ].filter(Boolean);
    if (!lines.length) lines = [brief?.narrativeSummary || 'No briefing details were returned.'];
    suggestedActions = Array.isArray(session.topActions) ? session.topActions.slice(0, 3) : [];
    cards = [...changed.slice(0, 4), ...top.slice(0, 3), ...actions.slice(0, 3)];
  } else if (intent === 'actions') {
    title = 'Action queue';
    lines = actions.length
      ? actions.slice(0, 6).map((item, idx) => {
        const lifecycle = item.lifecycle || item.type || 'suggested_action';
        const executionStatus = item.executionStatus || (lifecycle === 'approved_action' ? 'approved_pending_execution' : 'not_executable');
        return `${idx + 1}. ${item.title || item.summary} - ${lifecycle}; execution ${executionStatus}${item.requiresApproval || item.approvalRequired ? ' (approval required)' : ''}.`;
      })
      : ['No prepared action drafts are queued.'];
    cards = actions.slice(0, 6);
  } else if (intent === 'action_transition') {
    title = 'Action approval workflow';
    lines = [
      'Action lifecycle supports suggested_action, draft_action, approved_action, completed_action, and dismissed_action.',
      'Use the action controls in the Suggested Actions panel to approve, complete, or dismiss a specific action.',
      'External execution remains intentionally unimplemented here; approving records intent and keeps execution controlled.',
    ];
    cards = actions.slice(0, 6);
  } else {
    title = 'Operational readout';
    lines = [
      brief?.narrativeSummary || 'MARCUS has an active brief but no deterministic handler matched this command.',
      top[0] ? `Top signal: ${top[0].title}.` : '',
      waiting.length ? `${waiting.length} item${waiting.length === 1 ? '' : 's'} waiting on Mark.` : '',
    ].filter(Boolean);
    suggestedActions = actions.slice(0, 3);
    cards = top.slice(0, 5);
  }

  const evidence = buildCommandEvidence(cards, brief);
  const sourceSummary = Object.entries(evidence.sourceCounts)
    .slice(0, 4)
    .map(([source, count]) => `${source}: ${count}`)
    .join(', ');
  const sourceLine = sourceSummary ? `\n\nSources: ${sourceSummary}.` : '';
  const reply = `${title}\n\n${lines.join('\n')}${suggestedActions.length ? `\n\nSuggested next actions:\n${suggestedActions.map((a, idx) => `${idx + 1}. ${a.title || a.summary || a.suggestedButtonLabel || 'Review action'}`).join('\n')}` : ''}${sourceLine}`;
  return {
    ok: true,
    handled: intent !== 'empty',
    intent: responseIntent,
    title,
    reply,
    cards,
    suggestedActions,
    evidence,
    confidence: brief?.confidence ?? null,
    generatedAt: nowIso(),
  };
}

app.post('/api/marcus/command', async (req, res) => {
  try {
    const message = typeof req.body?.message === 'string' ? req.body.message.trim().slice(0, 2000) : '';
    if (!message) return res.status(400).json({ ok: false, error: 'Message required' });
    if (shouldCreateDurableOperationForRequest(message)) {
      const result = await createOrReuseDurableOperationForMessage(message, { source: 'marcus_command' });
      return res.json({
        ok: true,
        handled: true,
        intent: 'durable_operation',
        title: result.reused ? 'Durable operation resumed' : 'Durable operation created',
        reply: formatOperationStatusForMarcus(result.operation, result.resolution, { reused: result.reused }),
        cards: [],
        suggestedActions: [],
        operation: result.operation,
        generatedAt: nowIso(),
      });
    }
    const commandIntent = normalizeMarcusCommandIntent(message);
    if (commandIntent === 'project_create') {
      const projectIntent = parseProjectDraftCommand(message);
      const existingControls = await readMarcusOperationalControls();
      const { next, action } = createProjectDraftActionControl(existingControls, {
        projectName: projectIntent?.projectName,
        summary: projectIntent?.summary,
        sourceText: projectIntent?.sourceText,
        changedBy: 'command',
      });
      await writeMarcusOperationalControls(next);
      const [brief, sessionState] = await Promise.all([
        buildMarcusActiveBrief(),
        readMarcusSessionState(),
      ]);
      const enrichedBrief = applyMarcusSessionContextToBrief(applyOperationalControlsToBrief(brief, next), sessionState);
      const response = buildMarcusCommandResponse({ message: 'Show my action queue.', brief: enrichedBrief });
      return res.json({
        ...response,
        intent: 'project_create',
        title: 'Project draft queued',
        reply: `Project draft queued\n\n${action.title}\n${action.summary ? `\n${action.summary}` : ''}\n\nIt is queued as ${action.lifecycle}. No project record was created yet; execution remains approval-gated.`,
        cards: [action, ...(Array.isArray(response.cards) ? response.cards : [])].slice(0, 8),
        suggestedActions: [action],
        createdAction: action,
        evidence: buildCommandEvidence([action], enrichedBrief),
      });
    }
    if (commandIntent === 'focus_control') {
      const focusIntent = parseFocusCommand(message);
      const existingControls = await readMarcusOperationalControls();
      let next = normalizeMarcusOperationalControls(existingControls);
      let focus = null;
      if (focusIntent?.action === 'clear') {
        next.focus = {};
      } else {
        const created = createManualFocusControl(existingControls, {
          title: focusIntent?.title,
          summary: focusIntent?.summary,
          changedBy: 'command',
        });
        next = created.next;
        focus = created.focus;
      }
      await writeMarcusOperationalControls(next);
      const [brief, sessionState] = await Promise.all([
        buildMarcusActiveBrief(),
        readMarcusSessionState(),
      ]);
      const enrichedBrief = applyMarcusSessionContextToBrief(applyOperationalControlsToBrief(brief, next), sessionState);
      const currentFocus = enrichedBrief.currentFocus || focus;
      const card = currentFocus ? {
        ...currentFocus,
        id: currentFocus.id || focus?.id || 'focus:current',
        type: currentFocus.type || 'focus_control',
        title: currentFocus.title || currentFocus.name || 'Current focus',
        summary: currentFocus.summary || currentFocus.detail || 'Pinned current focus.',
        source: currentFocus.source || 'operational-controls',
        confidence: Number.isFinite(Number(currentFocus.confidence)) ? Number(currentFocus.confidence) : 1,
      } : {
        id: 'focus:cleared',
        type: 'focus_control',
        title: 'Current focus cleared',
        summary: 'MARCUS will infer focus from active work, desktop context, and ranked signals.',
        source: 'operational-controls',
        confidence: 1,
      };
      return res.json({
        ok: true,
        handled: true,
        intent: 'focus_control',
        title: focusIntent?.action === 'clear' ? 'Current focus cleared' : 'Current focus pinned',
        reply: focusIntent?.action === 'clear'
          ? 'Current focus cleared\n\nMARCUS will infer focus from active work, desktop context, and ranked signals.'
          : `Current focus pinned\n\n${card.title}\n${card.summary || ''}`.trim(),
        cards: [card],
        suggestedActions: enrichedBrief.activeActionQueue || [],
        evidence: buildCommandEvidence([card], enrichedBrief),
        focusPolicy: enrichedBrief.focusPolicy || null,
        currentFocus: enrichedBrief.currentFocus || null,
        generatedAt: nowIso(),
      });
    }
    if (commandIntent === 'proactive_mode') {
      const mode = parseProactiveModeCommand(message);
      const existingControls = await readMarcusOperationalControls();
      const next = normalizeMarcusOperationalControls({
        ...existingControls,
        proactiveMode: mode || existingControls.proactiveMode,
      });
      await writeMarcusOperationalControls(next);
      const [brief, sessionState] = await Promise.all([
        buildMarcusActiveBrief(),
        readMarcusSessionState(),
      ]);
      const enrichedBrief = applyMarcusSessionContextToBrief(applyOperationalControlsToBrief(brief, next), sessionState);
      const policy = enrichedBrief.attentionPolicy || { mode: next.proactiveMode };
      const card = {
        id: `proactive-mode:${policy.mode || next.proactiveMode}`,
        type: 'proactive_mode',
        title: `Proactive mode: ${policy.mode || next.proactiveMode}`,
        summary: policy.description || 'Attention policy updated.',
        source: 'operational-controls',
        confidence: 1,
        visibleAttentionCount: policy.visibleAttentionCount,
        suppressedByModeCount: policy.suppressedByModeCount,
        suppressedByControlsCount: policy.suppressedByControlsCount,
      };
      return res.json({
        ok: true,
        handled: true,
        intent: 'proactive_mode',
        title: 'Proactive mode updated',
        reply: `Proactive mode updated\n\nMARCUS is now in ${policy.mode || next.proactiveMode} mode.\n${policy.description || ''}`.trim(),
        cards: [card],
        suggestedActions: enrichedBrief.activeActionQueue || [],
        evidence: buildCommandEvidence([card], enrichedBrief),
        attentionPolicy: policy,
        generatedAt: nowIso(),
      });
    }
    if (commandIntent === 'codex_goal') {
      const existingControls = await readMarcusOperationalControls();
      const [brief, sessionState] = await Promise.all([
        buildMarcusActiveBrief(),
        readMarcusSessionState(),
      ]);
      const preBrief = applyMarcusSessionContextToBrief(applyOperationalControlsToBrief(brief, existingControls), sessionState);
      const draft = buildCodexGoalDraftFromCommand(message, preBrief);
      const { next, action } = createManualActionControl(existingControls, draft);
      await writeMarcusOperationalControls(next);
      const enrichedBrief = applyMarcusSessionContextToBrief(applyOperationalControlsToBrief(brief, next), sessionState);
      const response = buildMarcusCommandResponse({ message: 'Show my action queue.', brief: enrichedBrief });
      return res.json({
        ...response,
        intent: 'codex_goal',
        title: 'Codex goal drafted',
        reply: `Codex goal drafted\n\n${action.title}\n\n${action.body || action.summary}\n\nIt is queued as ${action.lifecycle}. Mark should approve the handoff before Codex execution starts.`,
        cards: [action, ...(Array.isArray(response.cards) ? response.cards : [])].slice(0, 8),
        suggestedActions: [action],
        createdAction: action,
        evidence: buildCommandEvidence([action], enrichedBrief),
      });
    }
    if (commandIntent === 'action_create') {
      const existingControls = await readMarcusOperationalControls();
      const draft = buildManualActionDraftFromCommand(message);
      const { next, action } = createManualActionControl(existingControls, draft);
      await writeMarcusOperationalControls(next);
      const [brief, sessionState] = await Promise.all([
        buildMarcusActiveBrief(),
        readMarcusSessionState(),
      ]);
      const enrichedBrief = applyMarcusSessionContextToBrief(applyOperationalControlsToBrief(brief, next), sessionState);
      const response = buildMarcusCommandResponse({ message: 'Show my action queue.', brief: enrichedBrief });
      return res.json({
        ...response,
        intent: 'action_create',
        title: 'Action draft created',
        reply: `Action draft created\n\n${action.title}\n${action.summary ? `\n${action.summary}` : ''}\n\nIt is queued as ${action.lifecycle}. Execution remains approval-gated.`,
        cards: [action, ...(Array.isArray(response.cards) ? response.cards : [])].slice(0, 8),
        suggestedActions: [action],
        createdAction: action,
        evidence: buildCommandEvidence([action], enrichedBrief),
      });
    }
    const [brief, controls, sessionState] = await Promise.all([
      buildMarcusActiveBrief(),
      readMarcusOperationalControls(),
      readMarcusSessionState(),
    ]);
    const enrichedBrief = applyMarcusSessionContextToBrief(applyOperationalControlsToBrief(brief, controls), sessionState);
    res.json(buildMarcusCommandResponse({ message, brief: enrichedBrief }));
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/api/marcus/entities/search', async (req, res) => {
  try {
    const q = String(req.query?.q || '').trim().slice(0, 200);
    const limit = Math.max(1, Math.min(50, Number(req.query?.limit) || 10));
    const brief = await buildMarcusActiveBrief();
    res.json({ ok: true, query: q, items: searchWorldEntities(brief, q, { limit }) });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/api/marcus/entities/inspect', async (req, res) => {
  try {
    const id = String(req.query?.id || req.query?.q || '').trim().slice(0, 240);
    if (!id) return res.status(400).json({ ok: false, error: 'id or q is required' });
    const brief = await buildMarcusActiveBrief();
    const result = inspectWorldEntity(brief, id);
    if (!result) return res.status(404).json({ ok: false, error: 'Entity not found' });
    res.json(result);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/api/marcus/items/inspect', async (req, res) => {
  try {
    const id = String(req.query?.id || req.query?.q || '').trim().slice(0, 240);
    if (!id) return res.status(400).json({ ok: false, error: 'id or q is required' });
    const [brief, controls, sessionState] = await Promise.all([
      buildMarcusActiveBrief(),
      readMarcusOperationalControls(),
      readMarcusSessionState(),
    ]);
    const enrichedBrief = applyMarcusSessionContextToBrief(applyOperationalControlsToBrief(brief, controls), sessionState);
    const result = inspectBriefOperationalItem(enrichedBrief, id);
    if (!result) return res.status(404).json({ ok: false, error: 'Item not found' });
    res.json(result);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/api/marcus/operational-controls', async (req, res) => {
  try {
    res.json(await readMarcusOperationalControls());
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.put('/api/marcus/operational-controls/proactive-mode', async (req, res) => {
  try {
    const existing = await readMarcusOperationalControls();
    const next = await writeMarcusOperationalControls({
      ...existing,
      proactiveMode: normalizeProactiveModeServer(req.body?.mode),
    });
    res.json(next);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/operational-controls/:section/:id', async (req, res) => {
  try {
    const existing = await readMarcusOperationalControls();
    const next = mergeControlPatch(existing, req.params.section, req.params.id, req.body?.patch || req.body || {});
    const saved = await writeMarcusOperationalControls(next);
    res.json(saved);
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/actions/:id/transition', async (req, res) => {
  try {
    const existing = await readMarcusOperationalControls();
    const next = transitionActionControl(existing, req.params.id, req.body?.lifecycle, req.body?.patch || {});
    const saved = await writeMarcusOperationalControls(next);
    res.json(saved);
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err?.message || err) });
  }
});

app.delete('/api/marcus/operational-controls/:section/:id', async (req, res) => {
  try {
    const section = String(req.params.section || '').trim();
    const id = String(req.params.id || '').trim();
    if (!['signals', 'memory', 'actions', 'projects', 'focus'].includes(section) || !id) throw new Error('Invalid control section or id');
    const existing = await readMarcusOperationalControls();
    const next = normalizeMarcusOperationalControls(existing);
    delete next[section][id];
    const saved = await writeMarcusOperationalControls(next);
    res.json(saved);
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/api/marcus/operator-health', async (req, res) => {
  try {
    res.json(await buildMarcusOperatorHealth());
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/api/marcus/acceptance', async (req, res) => {
  try {
    res.json(await buildMarcusAcceptanceReport({
      sessionId: typeof req.query?.sessionId === 'string' ? req.query.sessionId : '',
    }));
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/api/marcus/providers/config', async (req, res) => {
  try {
    res.json(getMarcusProviderConfiguration(await readSettings()));
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.put('/api/marcus/providers/config', async (req, res) => {
  try {
    res.json(await updateMarcusProviderConfiguration(req.body || {}));
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/providers/verify', async (req, res) => {
  const type = String(req.body?.type || '').trim().toLowerCase();
  if (!['text', 'email'].includes(type)) {
    return res.status(400).json({ ok: false, error: 'Provider type must be text or email.' });
  }
  try {
    const settings = await readSettings();
    if (type === 'text') {
      const config = getQuoOutboundConfig(settings);
      const sender = await resolveQuoSender(config);
      await persistResolvedQuoSender(sender);
      await persistMarcusProviderVerification('text', {
        phoneNumberId: sender.phoneNumberId,
        fromNumber: sender.from,
        userId: sender.userId,
      });
      return res.json({
        ok: true,
        type,
        provider: 'quo',
        verified: true,
        sent: false,
        sender: {
          phoneNumberId: sender.phoneNumberId,
          fromNumber: sender.from,
          userId: sender.userId,
        },
        configuration: getMarcusProviderConfiguration(await readSettings()).text,
      });
    }

    const email = getEmailConfig(settings);
    const verified = await withSmtpTransport(email, async (_transport, profile) => ({ profile: describeSmtpProfile(profile) }), { timeoutMs: 8_000 });
    await persistMarcusProviderVerification('email', {
      fromAddress: email.fromAddress,
      profile: verified.value.profile,
    });
    return res.json({
      ok: true,
      type,
      provider: 'smtp',
      verified: true,
      sent: false,
      profile: verified.value.profile,
      fromAddress: email.fromAddress,
      attempts: verified.attempts,
      configuration: getMarcusProviderConfiguration(await readSettings()).email,
    });
  } catch (err) {
    const message = String(err?.message || err);
    const status = /not configured|missing|invalid|configure|required/i.test(message) ? 400 : 502;
    return res.status(status).json({ ok: false, type, verified: false, sent: false, error: message });
  }
});

app.get('/api/marcus/external-actions', async (req, res) => {
  try {
    const settings = await readSettings();
    res.json({ ok: true, actions: normalizeExternalActionDrafts(settings.externalActionDrafts).slice(-100).reverse() });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/external-actions/draft', async (req, res) => {
  try {
    const draft = await createExternalActionDraft(req.body || {});
    res.status(201).json({ ok: true, action: draft });
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/external-actions/:id/approve', async (req, res) => {
  const id = String(req.params.id || '').trim();
  const message = String(req.body?.message || '').trim().slice(0, 1_000);
  if (!id) return res.status(400).json({ ok: false, error: 'Action id is required.' });
  try {
    const action = await approveExternalAction(id, message);
    res.json({ ok: true, action, note: 'Approval recorded. Sending remains a separate explicit provider action.' });
  } catch (err) {
    res.status(Number(err?.statusCode) || 400).json({ ok: false, approvalRequired: err?.approvalRequired === true, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/external-actions/:id/send', async (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'Action id is required.' });
  try {
    const result = await sendApprovedExternalAction(id);
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(Number(err?.statusCode) || 400).json({ ok: false, approvalRequired: err?.approvalRequired === true, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/external-actions/:id/reject', async (req, res) => {
  const id = String(req.params.id || '').trim();
  const message = String(req.body?.message || '').trim().slice(0, 1_000);
  if (!id) return res.status(400).json({ ok: false, error: 'Action id is required.' });
  writeLock = writeLock.catch(() => {}).then(async () => {
    const settings = await readSettings();
    const actions = normalizeExternalActionDrafts(settings.externalActionDrafts);
    const idx = actions.findIndex((item) => item.id === id);
    if (idx < 0) return res.status(404).json({ ok: false, error: 'External action draft not found.' });
    if (actions[idx].status !== 'pending_approval') {
      return res.status(409).json({ ok: false, error: `External action cannot be rejected from ${actions[idx].status}.` });
    }
    const now = nowIso();
    actions[idx] = {
      ...actions[idx],
      status: 'rejected',
      rejectedAt: now,
      rejectedBy: 'mark',
      rejectionMessage: message,
      updatedAt: now,
    };
    await writeSettings({ ...settings, externalActionDrafts: actions, updatedAt: now });
    res.json({ ok: true, action: actions[idx] });
  });
  await writeLock;
});

app.post('/api/marcus/project-operator', async (req, res) => {
  const message = typeof req.body?.message === 'string' ? req.body.message.trim().slice(0, 12_000) : '';
  const projectId = typeof req.body?.projectId === 'string' ? req.body.projectId.trim().slice(0, 160) : '';
  if (!message) return res.status(400).json({ ok: false, error: 'Message required' });
  try {
    const result = await projectOperatorService.prepareCodexOperation(getBusinessKeyFromContext(), {
      message,
      projectId,
      source: 'project_operator_api',
      autoStart: req.body?.autoStart !== false && !explicitlyDefersCodexStart(message),
    });
    res.status(result.status === 'needs_project' ? 200 : 201).json(result);
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post('/api/marcus/project-bootstrap', async (req, res) => {
  const message = typeof req.body?.message === 'string' ? req.body.message.trim().slice(0, 12_000) : '';
  const projectName = typeof req.body?.projectName === 'string' ? req.body.projectName.trim().slice(0, 120) : '';
  if (!message) return res.status(400).json({ ok: false, error: 'Message required' });
  try {
    const result = await prepareNewProjectBootstrap(message, { projectName, source: 'project_bootstrap_api' });
    res.status(result.status === 'project_bootstrap_started' ? 201 : 200).json(result);
  } catch (error) {
    res.status(400).json({ ok: false, error: String(error?.message || error) });
  }
});

// Chat from Marcus Live panel
app.post('/api/marcus/live/chat', async (req, res) => {
  const message = typeof req.body?.message === 'string' ? req.body.message.trim().slice(0, 2000) : '';
  if (!message) return res.status(400).json({ error: 'Empty message' });

  try {
    const conversation = await readMarcusLiveConversation();
    const approvalAuthorized = hasDurableAdminAuthentication(req);

    const sentExternalAction = await maybeApproveAndSendExternalActionFromMessage(message, { approvalAuthorized });
    if (sentExternalAction) {
      await recordMarcusLiveExchange(message, sentExternalAction.reply, { externalActionId: sentExternalAction.externalAction?.id || '' });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: sentExternalAction.reply, ts: Date.now() });
      return res.json(sentExternalAction);
    }

    const approvedOperation = await maybeApprovePendingOperationFromMessage(message, { approvalAuthorized });
    if (approvedOperation) {
      await recordMarcusLiveExchange(message, approvedOperation.reply, {
        operationId: approvedOperation.operation?.id || '',
        project: approvedOperation.operation ? {
          projectRegistryId: approvedOperation.operation.projectRegistryId,
          projectId: approvedOperation.operation.projectId,
          name: approvedOperation.operation.projectName,
        } : null,
      });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: approvedOperation.reply, ts: Date.now() });
      return res.json(approvedOperation);
    }

    const missionMemoryCommand = parseMissionMemoryCommand(message);
    if (missionMemoryCommand) {
      const memoryResult = await handleMissionMemoryCommand(getBusinessKeyFromContext(), missionMemoryCommand);
      await recordMarcusLiveExchange(message, memoryResult.reply, {
        missionMemoryId: memoryResult.memory?.id || '',
        missionMemoryStatus: memoryResult.status,
      });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: memoryResult.reply, ts: Date.now() });
      return res.status(memoryResult.ok ? 200 : 400).json(memoryResult);
    }

    if (isNewProjectBootstrapRequest(message)) {
      const result = await prepareNewProjectBootstrap(message, { source: 'marcus_live_project_bootstrap' });
      const project = result.project ? {
        projectRegistryId: result.project.id,
        projectId: result.project.projectId,
        name: result.project.canonicalName,
        repo: result.project.repo?.fullName || '',
      } : null;
      await recordMarcusLiveExchange(message, result.reply, {
        operationId: result.operation?.id || '',
        project,
      });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: result.reply, ts: Date.now() });
      return res.status(result.ok ? 200 : 400).json(result);
    }

    if (isProjectSwitchRequest(message)) {
      const switched = await projectOperatorService.resolveProjectContext(getBusinessKeyFromContext(), {
        message,
        currentProjectId: conversation.activeProject.projectRegistryId || conversation.activeProject.projectId,
      });
      const project = switched.project;
      if (!project) {
        const reply = 'I could not resolve that project with enough confidence. Use its exact project name or GitHub owner/repository.';
        await recordMarcusLiveExchange(message, reply);
        return res.json({ ok: false, status: 'needs_project', reply });
      }
      let desktopActionId = '';
      if (project.workspacePath && project.workspaceTrust === 'approved') {
        const action = await queueDesktopAction({
          type: 'open-vscode', payload: { path: project.workspacePath }, requestedBy: 'marcus-project-switch',
        });
        desktopActionId = action.id;
      }
      const projectRef = { projectRegistryId: project.id, projectId: project.projectId, name: project.name, repo: project.repo };
      const reply = `Active project: ${project.name}${project.repo ? ` (${project.repo})` : ''}.${desktopActionId ? ' I also queued its verified workspace to open on your PC.' : project.workspacePath ? ' Its workspace authorization is still being attested.' : ''}`;
      await recordMarcusLiveExchange(message, reply, { project: projectRef });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: reply, ts: Date.now() });
      return res.json({ ok: true, status: 'project_switched', project, desktopActionId, reply });
    }

    if (projectOperatorService.shouldHandleStatus(message)) {
      const result = await projectOperatorService.readProjectStatus(getBusinessKeyFromContext(), {
        message,
        currentProjectId: conversation.activeProject.projectRegistryId || conversation.activeProject.projectId,
      });
      await recordMarcusLiveExchange(message, result.reply, { project: result.project || null });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: result.reply, ts: Date.now() });
      return res.json(result);
    }

    const declaresProjectContext = isProjectContextDeclaration(message);
    const handlesProjectRequest = projectOperatorService.shouldHandle(message);
    const resolvedRequestContext = (declaresProjectContext || handlesProjectRequest)
      ? await projectOperatorService.resolveProjectContext(getBusinessKeyFromContext(), {
        message,
        currentProjectId: conversation.activeProject.projectRegistryId || conversation.activeProject.projectId,
      })
      : null;
    const requestProject = resolvedRequestContext?.project || conversation.activeProject;
    const createsDurableRequest = !isExternalCommunicationRequest(message) && shouldCreateDurableOperationForRequest(message);
    const retainedRequirements = (declaresProjectContext || handlesProjectRequest || createsDurableRequest)
      ? await collectMarcusLiveProjectRequirements(getBusinessKeyFromContext(), conversation, message, requestProject, { limit: 8 })
      : [];
    const projectRequest = buildMarcusLiveProjectRequest(conversation, message, requestProject, retainedRequirements);

    if (declaresProjectContext && !handlesProjectRequest) {
      const project = resolvedRequestContext?.project || null;
      const executionDeferred = explicitlyDefersProjectAudit(message) || explicitlyDefersCodexStart(message);
      const replyRequirements = retainedRequirements.slice(0, 3);
      const reply = project
        ? [
            `Active project: ${project.name}${project.repo ? ` (${project.repo})` : ''}.`,
            replyRequirements.length ? `Retained requirements:\n${replyRequirements.map((item) => `- ${item}`).join('\n')}` : 'I retained the current project context.',
            executionDeferred ? 'I did not audit the repository or start Codex.' : 'I will carry these requirements into a later repository audit and Codex prompt.',
          ].join('\n')
        : 'I could not verify that GitHub project yet. Give me the exact owner/repository name so I can inspect the right code instead of guessing.';
      await recordMarcusLiveExchange(message, reply, { project, requirements: retainedRequirements });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: reply, ts: Date.now() });
      return res.json({ ok: Boolean(project), status: project ? 'project_context_set' : 'needs_project', project, reply });
    }

    if (handlesProjectRequest) {
      const result = await projectOperatorService.prepareCodexOperation(getBusinessKeyFromContext(), {
        message: projectRequest,
        resolutionRequest: declaresProjectContext ? message : projectRequest,
        projectId: requestProject.projectId,
        projectRegistryId: requestProject.projectRegistryId,
        source: 'marcus_live_project_operator',
        autoStart: !explicitlyDefersCodexStart(message),
      });
      await recordMarcusLiveExchange(message, result.reply, {
        operationId: result.operation?.id || '',
        project: result.project || (result.operation ? {
          projectRegistryId: result.operation.projectRegistryId,
          projectId: result.operation.projectId,
          name: result.operation.projectName,
        } : null),
        requirements: retainedRequirements,
      });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: result.reply, ts: Date.now() });
      return res.json(result);
    }

    // Marcus Live is a second chat surface, so durable work requests must enter the
    // same operation engine instead of being answered as if an AI chat executed them.
    if (createsDurableRequest) {
      const result = await createOrReuseDurableOperationForMessage(projectRequest, {
        projectId: requestProject.projectId,
        projectName: requestProject.name,
        source: 'marcus_live',
      });
      const reply = formatOperationStatusForMarcus(result.operation, result.resolution, { reused: result.reused });
      await recordMarcusLiveExchange(message, reply, {
        operationId: result.operation?.id || '',
        project: result.operation ? {
          projectRegistryId: result.operation.projectRegistryId,
          projectId: result.operation.projectId,
          name: result.operation.projectName,
        } : null,
        requirements: retainedRequirements,
      });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: reply, ts: Date.now() });
      return res.json({ ok: true, reply, operation: result.operation });
    }

    // Build context from current workspace
    const ws = desktopRelayCache?.data?.workspace;
    const contextParts = [];
    if (conversation.activeProject.name || conversation.activeProject.repo) {
      contextParts.push(`ACTIVE CONVERSATION PROJECT: ${conversation.activeProject.name || 'unnamed'}${conversation.activeProject.repo ? ` (${conversation.activeProject.repo})` : ''}`);
    }
    try {
      const memories = await missionMemoryStore.relevant(getBusinessKeyFromContext(), message, { limit: 12 });
      const formattedMemory = formatMissionMemoryForPrompt(memories);
      if (formattedMemory) contextParts.push(`DURABLE MISSION MEMORY:\n${formattedMemory}`);
    } catch {}
    try {
      const registeredProjects = await operationsEngine.listProjectRegistry(getBusinessKeyFromContext());
      if (registeredProjects.length) {
        contextParts.push(`REGISTERED PROJECTS:\n${registeredProjects.slice(0, 30).map((project) => `- ${project.canonicalName}${project.repo?.fullName ? `: ${project.repo.fullName}` : ''}${project.aliases?.length ? `; aliases ${project.aliases.slice(0, 6).join(', ')}` : ''}`).join('\n')}`);
      }
    } catch {}
    const recentCodexWorkspaces = Array.isArray(desktopRelayCache?.data?.codexWorkspaces)
      ? desktopRelayCache.data.codexWorkspaces
      : [];
    if (recentCodexWorkspaces.length) {
      contextParts.push(`RECENT CODEX WORKSPACES (metadata only):\n${recentCodexWorkspaces.slice(0, 12).map((item) => {
        const branch = item.gitBranch ? `; branch ${item.gitBranch}` : '';
        const changes = Number(item.gitStatusCount || 0) ? `; ${Number(item.gitStatusCount)} local change(s)` : '';
        const repo = item.gitRemote ? `; ${item.gitRemote}` : '';
        return `- ${item.projectName || item.folderName}: ${item.workspacePath}${branch}${changes}${repo}; last active ${item.modifiedAt || 'unknown'}`;
      }).join('\n')}`);
    }
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
    if (marcusLiveActions.length) {
      contextParts.push(`\nRECENT HUD ACTIONS TAKEN BY MARK:\n${marcusLiveActions.slice(-12).map(a => `- ${new Date(a.ts).toISOString()} ${a.action.toUpperCase()} ${a.kind || 'item'}: ${a.label || a.itemId}${a.target ? ` (${a.target})` : ''}`).join('\n')}`);
    }
    try {
      const brief = await buildMarcusActiveBrief();
      if (brief?.narrativeSummary) {
        contextParts.push(`ACTIVE BRIEF:\n${brief.narrativeSummary}`);
      }
      if (Array.isArray(brief?.topPriorities) && brief.topPriorities.length) {
        contextParts.push(`TOP OPERATIONAL PRIORITIES:\n${brief.topPriorities.slice(0, 6).map((p) => `- ${p.title}${p.detail ? `: ${previewTextServer(p.detail, 180)}` : ''}`).join('\n')}`);
      }
      if (Array.isArray(brief?.preparedActions) && brief.preparedActions.length) {
        contextParts.push(`PREPARED ACTIONS WAITING FOR APPROVAL:\n${brief.preparedActions.slice(0, 4).map((a) => `- ${a.title}: ${previewTextServer(a.body || a.summary || '', 220)}${a.requiresApproval ? ' (approval required)' : ''}`).join('\n')}`);
      }
      if (brief?.activeProject?.name) {
        contextParts.push(`ACTIVE PROJECT DETECTED: ${brief.activeProject.name} (${brief.activeProject.businessName || brief.activeProject.businessKey || 'workspace'})`);
      }
      if (Array.isArray(brief?.projects) && brief.projects.length) {
        contextParts.push(`CURRENT/RECENT PROJECTS TO PAY ATTENTION TO:\n${brief.projects.slice(0, 5).map((p) => {
          const bits = [p.reason, p.openTasks ? `${p.openTasks} open task(s)` : '', p.inboxCount ? `${p.inboxCount} recent conversation(s)` : ''].filter(Boolean).join('; ');
          return `- ${p.name}${bits ? `: ${bits}` : ''}`;
        }).join('\n')}`);
      }
      if (Array.isArray(brief?.conversations) && brief.conversations.length) {
        contextParts.push(`PENDING CONVERSATIONS / MESSAGES:\n${brief.conversations.slice(0, 8).map((c) => {
          const project = c.projectName ? ` [${c.projectName}]` : '';
          const action = c.needsAction ? 'actionable' : 'watch';
          return `- ${c.who || c.source || 'Contact'}${project}: ${action}; ${c.preview || ''}`;
        }).join('\n')}`);
      }
      if (Array.isArray(brief?.messageDrafts) && brief.messageDrafts.length) {
        contextParts.push(`AVAILABLE DRAFT/HANDOFF:\n${brief.messageDrafts.slice(0, 2).map((d) => `- To ${d.to || 'Team'}${d.projectName ? ` for ${d.projectName}` : ''}: ${previewTextServer(d.body || d.reason || '', 220)}`).join('\n')}`);
      }
      if (brief?.stats) {
        contextParts.push(`WORKLOAD COUNTS: openTasks=${brief.stats.openTasks || 0}, dueToday=${brief.stats.dueTodayTasks || 0}, overdue=${brief.stats.overdueTasks || 0}, inboxActionable=${brief.stats.inboxActionable || 0}`);
      }
    } catch {}

    const systemPrompt = `You are WARREN / M.A.R.C.U.S., Mark's personal operations assistant and proactive technical partner. Mark may call you Warren or Marcus; answer naturally to either. You are watching the current work surface, recent projects, messages, tasks, and desktop context so Mark should not have to keep re-explaining what he is doing.

Your personality: Direct, sharp, calm under pressure, and deeply situationally aware. You feel like a capable right hand: concise readouts, clear risks, decisive next moves. You remember that Mark works across websites, client communications, admin, strategy, and non-website projects.

RULES:
- Talk to Mark like a lifelong coworker/friend who is smart but does not want raw technical noise.
- Default to plain English: what Mark is working on, what needs attention, why it matters, and the next useful move.
- Prefer current desktop context and activity from the last 14 days. Treat older website projects as archived/background unless they are active on the desktop, have recent communication, have urgent current tasks, or Mark explicitly reactivates them.
- Track conversations as operational signals: identify the person, linked account/project, likely needed response/action, and whether Mark should send, delegate, or ignore.
- If context is missing, say what you can infer and ask for the smallest missing detail; do not pretend certainty.
- Do NOT lead with full file paths, API routes, stack traces, or function names unless Mark asks for technical detail.
- If a dev should handle it, offer to package a clean prompt for the dev.
- Keep responses concise - 2-4 sentences usually. Think chat message, not essay.
- If you spot something great, say so. If something concerns you, say so directly.
- When asked about the code, use the actual file contents you can see.
- If Mark asks for a readout, lead with what matters now, then the next best move.
- Avoid robotic phrasing like "I have identified" or "it is recommended."
- Preserve the recent conversation. Resolve short follow-ups such as "Reggie", "that repo", or "do it" from prior turns and the active conversation project instead of restarting clarification.
- When Mark asks to draft, email, text, reply, or send an external message, call draft_external_message. The first call only creates an approval-gated draft and must never claim the message was sent.

CURRENT WORKSPACE CONTEXT:
${contextParts.join('\n')}`;

    const recentConversation = recentMarcusLiveMessages(conversation);
    const externalCommunicationRequest = isExternalCommunicationRequest(message);
    const result = await aiChatCompletion({
      routeKey: 'marcusChat',
      messages: [
        { role: 'system', content: systemPrompt },
        ...recentConversation.map((item) => ({ role: item.role, content: item.content })),
        { role: 'user', content: message },
      ],
      tools: [getExternalMessageDraftToolDefinition()],
      tool_choice: externalCommunicationRequest
        ? { type: 'function', function: { name: 'draft_external_message' } }
        : 'auto',
      timeoutMs: 25_000,
    });

    if (!result.ok) {
      return res.json({ ok: false, error: result.error || 'AI call failed' });
    }

    const draftCall = (Array.isArray(result.message?.tool_calls) ? result.message.tool_calls : [])
      .find((call) => call?.function?.name === 'draft_external_message');
    if (draftCall) {
      let args = {};
      try { args = JSON.parse(draftCall.function.arguments || '{}'); }
      catch { return res.json({ ok: false, error: 'Marcus could not parse the external message draft.' }); }
      const draft = await createExternalActionDraft(args);
      const reply = `I drafted the ${draft.type} to ${draft.to}${draft.subject ? ` with subject "${draft.subject}"` : ''}. Nothing has been sent. Say "approve and send ${draft.id}" after you review it.`;
      await recordMarcusLiveExchange(message, reply, { externalActionId: draft.id, project: conversation.activeProject });
      pushLiveEvent({ type: 'chat', from: 'marcus', text: reply, ts: Date.now() });
      return res.json({ ok: true, reply, externalAction: draft, approvalRequired: true });
    }

    const reply = result.message?.content || '';
    await recordMarcusLiveExchange(message, reply, { project: conversation.activeProject });
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
    if (marcusLiveActions.length) {
      contextParts.push(`\nRECENT HUD ACTIONS TAKEN BY MARK:\n${marcusLiveActions.slice(-12).map(a => `- ${new Date(a.ts).toISOString()} ${a.action.toUpperCase()} ${a.kind || 'item'}: ${a.label || a.itemId}${a.target ? ` (${a.target})` : ''}`).join('\n')}`);
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

    const systemPrompt = `You are M.A.R.C.U.S., Mark's personal technical assistant and proactive pair programming partner observing your operator's workspace in real-time.

Your job: Analyze the code they're actively working on and share observations WITHOUT being asked. Think of yourself as a sharp, situationally aware right hand who spots risks, openings, and the next best move.

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
- Write for Mark first, not for the codebase. Say the project/work area, the plain-English issue, the consequence, and the next move.
- Do NOT lead with full file paths, API routes, line patterns, or function names. Keep technical specifics implicit unless the issue cannot be understood without one short reference.
- Prefer wording like: "There's an issue in Marcus Live. If we ignore it, the admin screen may fail after deploy. Want me to prep a dev prompt?"
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
- Keep it conversational and direct, with light coworker banter when natural. Lead with signal. No fluff, no robotic phrasing.`;

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
  const evt = buildMarcusLiveContextEvent();
  if (evt) pushLiveEvent(evt);
}

// Proactive analysis timer - keep live context responsive as Mark switches windows.
setInterval(() => {
  pushLiveContext();
  runProactiveAnalysis();
}, 5_000);

app.post('/api/projects/:id/template', async (req, res) => {
  const projectId = req.params.id;
  const baseRevision = Number(req.body?.baseRevision);

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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
    writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

    if (effectiveThreadId !== 'operator_bio' && shouldCreateDurableOperationForRequest(message)) {
      const created = await createOrReuseDurableOperationForMessage(message, {
        projectId: effectiveProjectId || '',
        projectName: effectiveProject?.name || '',
        source: 'marcus_chat',
      });
      return { content: formatOperationStatusForMarcus(created.operation, created.resolution, { reused: created.reused }) };
    }

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
      "- Project activity, focus, staleness, and bottleneck claims must come from ProjectActivitySnapshot evidence. Never use Airtable status alone as proof of real activity.\n" +
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

    try {
      const activity = await projectEvidenceService.getActivity(getBusinessKeyFromContext());
      const focus = activity?.currentFocus;
      const active = (Array.isArray(activity?.snapshots) ? activity.snapshots : []).slice(0, 8).map((item) => ({
        projectRegistryId: item.projectRegistryId,
        projectName: item.projectName,
        state: item.state,
        activityScore: item.activityScore,
        confidence: item.confidence,
        evidenceCount: item.evidenceCount,
        reasons: item.reasons?.slice(0, 3),
        risks: item.risks?.map((risk) => ({ code: risk.code, summary: risk.summary })).slice(0, 5),
      }));
      context += `PROJECT EVIDENCE ACTIVITY (deterministic; cite these records for activity claims):\n${JSON.stringify({ currentFocus: focus, projects: active })}\n\n`;
    } catch {
      context += 'PROJECT EVIDENCE ACTIVITY: unavailable; do not infer activity from Airtable alone.\n\n';
    }

    try {
      const missionMemories = await missionMemoryStore.relevant(getBusinessKeyFromContext(), message, { limit: 12 });
      const formattedMissionMemory = formatMissionMemoryForPrompt(missionMemories);
      if (formattedMissionMemory) context += `DURABLE MISSION MEMORY (business-scoped; explicit operator instructions):\n${formattedMissionMemory}\n\n`;
    } catch {
      context += 'DURABLE MISSION MEMORY: unavailable; do not invent remembered instructions.\n\n';
    }

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
      const liveProjectShape = buildMarcusLiveProjectFocus(store, desktopRelayCache?.data || desktopContextCache?.data || null, Date.now());
      const currentProjectIds = new Set((Array.isArray(liveProjectShape.focus) ? liveProjectShape.focus : []).map((p) => String(p?.id || '')).filter(Boolean));
      const staleWebsiteIds = new Set((Array.isArray(liveProjectShape.staleWebsite) ? liveProjectShape.staleWebsite : []).map((p) => String(p?.id || '')).filter(Boolean));
      const projectsOverview = (store.projects || [])
        .filter((p) => !isClosedProjectStatus(p?.status))
        .filter((p) => currentProjectIds.has(String(p?.id || '')) || !staleWebsiteIds.has(String(p?.id || '')))
        .map((p) => ({
          id: p.id,
          name: p.name,
          type: p.type,
          status: p.status,
          dueDate: p.dueDate,
          owner: p.owner,
          workspacePath: p.workspacePath,
          repoUrl: p.repoUrl,
          docsUrl: p.docsUrl,
        }));
      context += `ALL PROJECTS (JSON): ${JSON.stringify(projectsOverview).slice(0, 24000)}\n\n`;
      if (staleWebsiteIds.size) {
        context += `STALE WEBSITE PROJECTS EXCLUDED FROM CURRENT CONTEXT: ${Array.from(staleWebsiteIds).slice(0, 80).join(', ')}\n\n`;
      }
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
        },
        {
          type: "function",
          function: {
            name: "open_project_in_vscode",
            description: "Open a registered project workspace in VS Code by project id/name or direct workspace path. Use when Mark asks to open, work on, or pull up a project locally.",
            parameters: {
              type: "object",
              properties: {
                projectId: { type: "string", description: "Preferred when known" },
                projectName: { type: "string", description: "Case-insensitive project name match if id is not known" },
                workspacePath: { type: "string", description: "Absolute local folder path if the project is not registered yet" }
              }
            }
          }
        },
        {
          type: "function",
          function: {
            name: "prepare_project_publish",
            description: "Queue a local desktop-agent review of a project repo before publishing. Use before commit/push/deploy work to inspect branch, changes, remotes, scripts, and risk.",
            parameters: {
              type: "object",
              properties: {
                projectId: { type: "string", description: "Preferred when known" },
                projectName: { type: "string", description: "Case-insensitive project name match if id is not known" },
                workspacePath: { type: "string", description: "Absolute local folder path if not registered" }
              }
            }
          }
        },
        {
          type: "function",
          function: {
            name: "publish_project_changes",
            description: "Queue an explicitly approved local git publish action: optionally run npm scripts, commit current changes, and push the branch. Only use after Mark has explicitly approved publishing/pushing.",
            parameters: {
              type: "object",
              properties: {
                projectId: { type: "string", description: "Preferred when known" },
                projectName: { type: "string", description: "Case-insensitive project name match if id is not known" },
                workspacePath: { type: "string", description: "Absolute local folder path if not registered" },
                commitMessage: { type: "string", description: "Commit message for the approved changes" },
                buildScript: { type: "string", description: "Optional npm script to run before committing, usually build" },
                testScript: { type: "string", description: "Optional npm script to run before committing, usually test" },
                push: { type: "boolean", description: "Whether to push the current branch after committing. Default true." }
              },
              required: ["commitMessage"]
            }
          }
        },
        {
          type: "function",
          function: {
            name: "run_project_script",
            description: "Queue a safe npm script from package.json on a local project through the desktop agent. Use for preview/build/test/lint style scripts, not arbitrary shell commands.",
            parameters: {
              type: "object",
              properties: {
                projectId: { type: "string", description: "Preferred when known" },
                projectName: { type: "string", description: "Case-insensitive project name match if id is not known" },
                workspacePath: { type: "string", description: "Absolute local folder path if not registered" },
                scriptName: { type: "string", description: "npm script name from package.json, for example build, test, lint" }
              },
              required: ["scriptName"]
            }
          }
        },
        {
          type: "function",
          function: {
            name: "clone_github_project",
            description: "Queue a local git clone for a GitHub repo, then optionally open it in VS Code. Use when Mark asks to pull up a repo that is not yet registered locally.",
            parameters: {
              type: "object",
              properties: {
                repoUrl: { type: "string", description: "GitHub HTTPS or SSH clone URL" },
                parentPath: { type: "string", description: "Parent folder to clone into. Optional." },
                folderName: { type: "string", description: "Optional destination folder name." },
                openInVsCode: { type: "boolean", description: "Open after cloning. Default true." }
              },
              required: ["repoUrl"]
            }
          }
        },
        {
          type: "function",
          function: {
            name: "get_desktop_action_results",
            description: "Read recent desktop-agent action results, including queued publish/open actions. Use when Mark asks whether a local action finished or failed.",
            parameters: {
              type: "object",
              properties: {}
            }
          }
        }
      );
    }

    tools.push(
      {
        type: 'function',
        function: {
          name: 'github_list_repos',
          description: 'List GitHub repositories available to the hosted Marcus cloud credentials. Read-only.',
          parameters: {
            type: 'object',
            properties: {
              owner: { type: 'string', description: 'Optional GitHub owner/user/org. Defaults to GITHUB_OWNER or authenticated user repos.' },
              limit: { type: 'number', description: 'Max repos, 1-100. Default 30.' },
            },
          },
        },
      },
      {
        type: 'function',
        function: {
          name: 'github_get_repo_file',
          description: 'Read a file or directory listing from a GitHub repo through hosted cloud credentials. Read-only.',
          parameters: {
            type: 'object',
            properties: {
              owner: { type: 'string' },
              repo: { type: 'string' },
              path: { type: 'string', description: 'File path or directory path in repo.' },
              ref: { type: 'string', description: 'Optional branch, tag, or SHA.' },
            },
            required: ['owner', 'repo', 'path'],
          },
        },
      },
      {
        type: 'function',
        function: {
          name: 'github_get_pull_request',
          description: 'Inspect one GitHub pull request, its exact head SHA, merge state, check runs, and commit statuses before preparing a merge. Read-only.',
          parameters: {
            type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, pullNumber: { type: 'integer' } },
            required: ['owner', 'repo', 'pullNumber'],
          },
        },
      },
      {
        type: 'function',
        function: {
          name: 'cloudflare_list_zones',
          description: 'List Cloudflare zones available to hosted Marcus credentials. Read-only.',
          parameters: { type: 'object', properties: {} },
        },
      },
      {
        type: 'function',
        function: {
          name: 'cloudflare_list_dns_records',
          description: 'List DNS records for a Cloudflare zone. Read-only.',
          parameters: {
            type: 'object',
            properties: {
              zoneId: { type: 'string', description: 'Cloudflare zone id. Defaults to CLOUDFLARE_DEFAULT_ZONE_ID.' },
            },
          },
        },
      },
      {
        type: 'function',
        function: {
          name: 'cloudflare_list_workers',
          description: 'List Worker scripts in Marcus\'s configured Cloudflare account. Read-only.',
          parameters: { type: 'object', properties: {} },
        },
      },
      {
        type: 'function',
        function: {
          name: 'cloudflare_list_worker_versions',
          description: 'List deployable versions for one Worker script in Marcus\'s configured Cloudflare account. Read-only.',
          parameters: { type: 'object', properties: { scriptName: { type: 'string' } }, required: ['scriptName'] },
        },
      },
      {
        type: 'function',
        function: {
          name: 'cloudflare_list_worker_deployments',
          description: 'List current/recent deployments and version percentages for one Worker script before preparing a production promotion. Read-only.',
          parameters: { type: 'object', properties: { scriptName: { type: 'string' } }, required: ['scriptName'] },
        },
      },
      {
        type: 'function',
        function: {
          name: 'render_list_services',
          description: 'List Render services available to hosted Marcus credentials. Read-only.',
          parameters: {
            type: 'object',
            properties: {
              limit: { type: 'number', description: 'Max services, 1-100. Default 50.' },
            },
          },
        },
      },
    );

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

      tools.push(...getMarcusOperationToolDefinitions());
      tools.push(...getMarcusProjectActivityToolDefinitions());

      const messages = [
        {
          role: 'system',
          content:
            systemPrompt +
            (effectiveThreadId === 'operator_bio'
              ? "\n\nIMPORTANT: Use set_operator_bio to persist changes to the bio."
              : "\n\nIMPORTANT: When Mark asks for an internal/admin action, use tools instead of only describing the action. Use create_operation for multi-step code/project work, work spanning systems, work that must survive interruption, approval-gated work, or work requiring later verification. Do not create operations for trivial questions. Use the operation lifecycle tools to report the resolved project, risk, approvals, and what is actually running versus waiting. Use project activity tools for focus, staleness, momentum, and bottleneck questions, and cite their evidence instead of Airtable status. For project updates use create_project / update_project / create_tasks. For local project work use open_project_in_vscode when a workspace path is available. If Mark names a GitHub repo that is not local and the desktop agent is available, use clone_github_project; if Mark is remote or the desktop is offline, use the GitHub cloud tools to inspect repos/files. Before a GitHub merge, inspect the exact pull request with github_get_pull_request, then use prepare_github_merge with its exact head SHA. Before a Cloudflare Worker deployment, inspect workers, versions, and current deployments, then use prepare_cloudflare_worker_deployment with the exact target version and current deployment ID. Use prepare_cloudflare_dns_change for exact project-bound DNS mutations. These preparation tools create durable approval-gated operations and never perform the mutation immediately. For build/test/lint/preview requests, use run_project_script with a package.json script name. For publish requests, use prepare_project_publish first unless Mark has already explicitly approved committing/pushing. publish_project_changes is server-guarded and requires explicit approval in Mark's message. For high-impact external actions like publish/deploy/merge/send/billing/delete/DNS changes, prepare the action and ask for explicit approval before executing. Never claim Codex or another provider is running unless an operation runner actually started that provider path. When Mark asks whether a queued local action finished, use get_desktop_action_results."),
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
      if (isMarcusProjectActivityTool(toolName)) {
        return executeMarcusProjectActivityTool({
          name: toolName,
          args,
          service: projectEvidenceService,
          businessKey: getBusinessKeyFromContext(),
        });
      }
      if (isMarcusOperationTool(toolName)) {
        return executeMarcusOperationTool({
          name: toolName,
          args,
          engine: operationsEngine,
          businessKey: getBusinessKeyFromContext(),
          requestMessage: message,
        });
      }
      const resolveProjectWorkspace = () => {
        const projectIdArg = typeof args?.projectId === 'string' ? args.projectId.trim() : '';
        const projectNameArg = typeof args?.projectName === 'string' ? args.projectName.trim() : '';
        let workspacePath = typeof args?.workspacePath === 'string' ? args.workspacePath.trim() : '';
        let project = null;

        const projects = Array.isArray(store?.projects) ? store.projects : [];
        if (projectIdArg) {
          project = projects.find((p) => String(p?.id || '') === projectIdArg) || null;
        }
        if (!project && projectNameArg) {
          const key = projectNameArg.toLowerCase();
          project = projects.find((p) => String(p?.name || '').trim().toLowerCase() === key) || null;
        }
        if (!project && effectiveProject) {
          project = effectiveProject;
        }
        if (!workspacePath && project) {
          workspacePath = typeof project.workspacePath === 'string' ? project.workspacePath.trim() : '';
        }
        return { project, workspacePath };
      };

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
      if (toolName === 'open_project_in_vscode') {
        const { project, workspacePath } = resolveProjectWorkspace();
        if (!workspacePath) {
          return {
            ok: false,
            error: project
              ? `No workspacePath is saved for ${project.name || 'that project'}.`
              : 'Project not found and no workspacePath was provided.',
          };
        }

        if (process.platform === 'win32') {
          const result = await new Promise((resolve) => {
            launchVsCodeNative(workspacePath, (error) => {
              resolve(error ? { ok: false, error: error.message } : { ok: true, mode: 'native' });
            });
          });
          return {
            ...result,
            projectId: project?.id || '',
            projectName: project?.name || '',
            workspacePath,
          };
        }

        const action = await queueDesktopAction({
          type: 'open-vscode',
          payload: { path: workspacePath },
          requestedBy: 'marcus-chat',
        });
        return {
          ok: true,
          queued: true,
          mode: 'desktop-agent',
          actionId: action.id,
          projectId: project?.id || '',
          projectName: project?.name || '',
          workspacePath,
        };
      }
      if (toolName === 'prepare_project_publish') {
        const { project, workspacePath } = resolveProjectWorkspace();
        if (!workspacePath) {
          return {
            ok: false,
            error: project
              ? `No workspacePath is saved for ${project.name || 'that project'}.`
              : 'Project not found and no workspacePath was provided.',
          };
        }
        const action = await queueDesktopAction({
          type: 'prepare-publish',
          payload: { path: workspacePath },
          requestedBy: 'marcus-chat',
        });
        return {
          ok: true,
          queued: true,
          actionId: action.id,
          projectId: project?.id || '',
          projectName: project?.name || '',
          workspacePath,
        };
      }
      if (toolName === 'publish_project_changes') {
        const requestedPush = args?.push !== false;
        const authorization = scopeAuthorizedPublishActions(message, { commit: true, push: requestedPush });
        if (!authorization.ok) {
          return {
            ok: false,
            approvalRequired: true,
            error: `Explicit action-specific approval is required for: ${authorization.unauthorizedActions.join(', ')}. A local commit does not authorize push or deploy, and negated instructions always win.`,
          };
        }
        const { project, workspacePath } = resolveProjectWorkspace();
        const commitMessage = typeof args?.commitMessage === 'string' ? args.commitMessage.trim() : '';
        if (!commitMessage) return { ok: false, error: 'commitMessage is required.' };
        if (!workspacePath) {
          return {
            ok: false,
            error: project
              ? `No workspacePath is saved for ${project.name || 'that project'}.`
              : 'Project not found and no workspacePath was provided.',
          };
        }
        const action = await queueDesktopAction({
          type: 'publish-project-changes',
          payload: {
            path: workspacePath,
            commitMessage,
            buildScript: typeof args?.buildScript === 'string' ? args.buildScript.trim() : '',
            testScript: typeof args?.testScript === 'string' ? args.testScript.trim() : '',
            commit: true,
            push: requestedPush,
            authorizedActions: authorization.authorizedActions,
          },
          requestedBy: 'marcus-chat',
        });
        return {
          ok: true,
          queued: true,
          actionId: action.id,
          projectId: project?.id || '',
          projectName: project?.name || '',
          workspacePath,
        };
      }
      if (toolName === 'run_project_script') {
        const { project, workspacePath } = resolveProjectWorkspace();
        const scriptName = typeof args?.scriptName === 'string' ? args.scriptName.trim() : '';
        if (!scriptName) return { ok: false, error: 'scriptName is required.' };
        if (!new Set(['install', 'dev', 'build', 'test', 'lint', 'typecheck']).has(scriptName)) return { ok: false, error: 'scriptName is not allowlisted.' };
        if (!workspacePath) {
          return {
            ok: false,
            error: project
              ? `No workspacePath is saved for ${project.name || 'that project'}.`
              : 'Project not found and no workspacePath was provided.',
          };
        }
        const action = await queueDesktopAction({
          type: 'run-project-script',
          payload: { path: workspacePath, scriptName },
          requestedBy: 'marcus-chat',
        });
        return {
          ok: true,
          queued: true,
          actionId: action.id,
          projectId: project?.id || '',
          projectName: project?.name || '',
          workspacePath,
          scriptName,
        };
      }
      if (toolName === 'clone_github_project') {
        const repoUrl = typeof args?.repoUrl === 'string' ? args.repoUrl.trim() : '';
        if (!/^((https:\/\/github\.com\/[^/\s]+\/[^/\s]+?(\.git)?)|(git@github\.com:[^/\s]+\/[^/\s]+?(\.git)?))$/i.test(repoUrl)) {
          return { ok: false, error: 'A GitHub HTTPS or SSH clone URL is required.' };
        }
        const parentPath = typeof args?.parentPath === 'string' ? args.parentPath.trim() : '';
        const folderName = typeof args?.folderName === 'string' ? args.folderName.trim() : '';
        const action = await queueDesktopAction({
          type: 'clone-github-project',
          payload: {
            repoUrl,
            parentPath,
            folderName,
            openInVsCode: args?.openInVsCode !== false,
          },
          requestedBy: 'marcus-chat',
        });
        return { ok: true, queued: true, actionId: action.id, repoUrl };
      }
      if (toolName === 'get_desktop_action_results') {
        pruneDesktopActionResults();
        return { ok: true, results: desktopActionResults.slice(-20) };
      }
      if (toolName === 'github_list_repos') {
        const cfg = getGitHubCloudConfig(await readSettings());
        const owner = typeof args?.owner === 'string' && args.owner.trim() ? args.owner.trim() : cfg.owner;
        const limit = Math.max(1, Math.min(100, Number(args?.limit) || 30));
        const pathPart = owner
          ? `/users/${encodeURIComponent(owner)}/repos?per_page=${limit}&sort=updated`
          : `/user/repos?per_page=${limit}&sort=updated&affiliation=owner,collaborator,organization_member`;
        const repos = await githubApi(pathPart);
        return {
          ok: true,
          repos: (Array.isArray(repos) ? repos : []).map((repo) => ({
            name: repo.name,
            fullName: repo.full_name,
            private: Boolean(repo.private),
            defaultBranch: repo.default_branch,
            htmlUrl: repo.html_url,
            updatedAt: repo.updated_at,
            pushedAt: repo.pushed_at,
          })),
        };
      }
      if (toolName === 'github_get_repo_file') {
        const owner = typeof args?.owner === 'string' ? args.owner.trim() : '';
        const repo = typeof args?.repo === 'string' ? args.repo.trim() : '';
        const filePath = typeof args?.path === 'string' ? args.path.trim() : '';
        const ref = typeof args?.ref === 'string' ? args.ref.trim() : '';
        if (!owner || !repo || !filePath) return { ok: false, error: 'owner, repo, and path are required.' };
        const qs = ref ? `?ref=${encodeURIComponent(ref)}` : '';
        const data = await githubApi(`/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/contents/${filePath.split('/').map(encodeURIComponent).join('/')}${qs}`);
        if (Array.isArray(data)) {
          return { ok: true, type: 'dir', entries: data.map((item) => ({ name: item.name, path: item.path, type: item.type, size: item.size })).slice(0, 100) };
        }
        const encoded = String(data?.content || '').replace(/\s+/g, '');
        const content = encoded ? Buffer.from(encoded, 'base64').toString('utf8') : '';
        return { ok: true, type: data?.type || 'file', name: data?.name || '', path: data?.path || filePath, size: data?.size || 0, content: content.slice(0, 40_000) };
      }
      if (toolName === 'github_get_pull_request') {
        return { ok: true, ...await inspectGitHubPullRequest(args?.owner, args?.repo, args?.pullNumber) };
      }
      if (toolName === 'cloudflare_list_zones') {
        const data = await cloudflareApi('/zones?per_page=50');
        return {
          ok: true,
          zones: (Array.isArray(data?.result) ? data.result : []).map((zone) => ({
            id: zone.id,
            name: zone.name,
            status: zone.status,
            paused: Boolean(zone.paused),
            type: zone.type,
          })),
        };
      }
      if (toolName === 'cloudflare_list_dns_records') {
        const cfg = getCloudflareConfig(await readSettings());
        const zoneId = typeof args?.zoneId === 'string' && args.zoneId.trim() ? args.zoneId.trim() : cfg.defaultZoneId;
        if (!zoneId) return { ok: false, error: 'zoneId is required or CLOUDFLARE_DEFAULT_ZONE_ID must be configured.' };
        const data = await cloudflareApi(`/zones/${encodeURIComponent(zoneId)}/dns_records?per_page=100`);
        return {
          ok: true,
          zoneId,
          records: (Array.isArray(data?.result) ? data.result : []).map((record) => ({
            id: record.id,
            type: record.type,
            name: record.name,
            content: record.content,
            proxied: Boolean(record.proxied),
            ttl: record.ttl,
            modifiedOn: record.modified_on,
          })),
        };
      }
      if (toolName === 'cloudflare_list_workers') {
        return { ok: true, ...await cloudflareWorkerInspection('workers') };
      }
      if (toolName === 'cloudflare_list_worker_versions' || toolName === 'cloudflare_list_worker_deployments') {
        const kind = toolName === 'cloudflare_list_worker_versions' ? 'versions' : 'deployments';
        return { ok: true, ...await cloudflareWorkerInspection(kind, args?.scriptName) };
      }
      if (toolName === 'render_list_services') {
        const limit = Math.max(1, Math.min(100, Number(args?.limit) || 50));
        const data = await renderApi(`/services?limit=${limit}`);
        const rows = Array.isArray(data) ? data : Array.isArray(data?.services) ? data.services : [];
        return {
          ok: true,
          services: rows.map((row) => {
            const service = row.service || row;
            return {
              id: service.id,
              name: service.name,
              type: service.type,
              repo: service.repo,
              branch: service.branch,
              updatedAt: service.updatedAt || service.updated_at,
            };
          }),
        };
      }
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

  writeLock = writeLock.catch(() => {}).then(async () => {
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

      const missionMemoryCommand = parseMissionMemoryCommand(message);
      if (missionMemoryCommand) {
        const memoryResult = await handleMissionMemoryCommand(getBusinessKeyFromContext(), missionMemoryCommand, 'main_chat_explicit_command');
        res.status(memoryResult.ok ? 200 : 400).json({ reply: memoryResult.reply, missionMemory: memoryResult });
        return;
      }

      if (isNewProjectBootstrapRequest(message)) {
        const result = await prepareNewProjectBootstrap(message, { source: 'main_chat_project_bootstrap' });
        res.status(result.ok ? 200 : 400).json({ reply: result.reply, projectBootstrap: result });
        return;
      }

      if (projectOperatorService.shouldHandle(message) && /\b(codex|audit|repo|repository|fix|build|implement|get .* working|start .* session)\b/i.test(message)) {
        const result = await projectOperatorService.prepareCodexOperation(getBusinessKeyFromContext(), {
          message,
          projectId: projectId || '',
          source: 'main_chat_project_operator',
          autoStart: !explicitlyDefersCodexStart(message),
        });
        const reply = String(result.reply || '').trim();
        if (result.operation?.projectId) {
          store.projectChats = store.projectChats || {};
          const existing = store.projectChats[result.operation.projectId];
          const chatHistory = Array.isArray(existing)
            ? existing
            : (existing && typeof existing === 'object' && Array.isArray(existing.messages))
                ? existing.messages
                : [];
          const ts = nowIso();
          chatHistory.push({ role: 'user', content: message, timestamp: ts });
          chatHistory.push({ role: 'ai', content: reply, timestamp: ts, mode: 'project_operator', operationId: result.operation.id });
          store.projectChats[result.operation.projectId] = { messages: chatHistory, updatedAt: ts };
          store.revision++;
          store.updatedAt = ts;
          await writeStore(store);
        }
        res.json({ reply, projectOperator: result });
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

function startProjectEvidenceScheduler() {
  let running = false;
  const tick = async () => {
    if (running) return;
    running = true;
    try {
      const settings = await readSettings();
      const cfg = getBusinessConfigFromSettings(settings);
      const sources = ['operations', 'airtable'];
      if (getGitHubCloudConfig(settings).configured) sources.push('github');
      if (getRenderCloudConfig(settings).configured) sources.push('render');
      if (getCloudflareConfig(settings).configured) sources.push('cloudflare');
      for (const business of cfg.businesses || []) {
        const businessKey = normalizeBusinessKey(business?.key || '') || DEFAULT_BUSINESS_KEY;
        await withBusinessKey(businessKey, () => projectEvidenceService.refresh(businessKey, { sources }));
      }
    } catch (error) {
      console.error('Project evidence refresh failed:', error?.message || error);
    } finally {
      running = false;
    }
  };
  const first = setTimeout(() => { void tick(); }, 5_000);
  const interval = setInterval(() => { void tick(); }, 10 * 60_000);
  if (typeof first.unref === 'function') first.unref();
  if (typeof interval.unref === 'function') interval.unref();
}

function startDurableOperationMonitor() {
  return startOperationMonitor({
    engine: operationsEngine,
    listBusinessKeys: async () => {
      await refreshBusinessCacheFromSettings();
      return (Array.isArray(cachedBusinesses) ? cachedBusinesses : [])
        .map((business) => normalizeBusinessKey(business?.key || '') || DEFAULT_BUSINESS_KEY);
    },
    initialDelayMs: Math.max(1_000, Number(process.env.MARCUS_OPERATION_MONITOR_INITIAL_DELAY_MS) || 3_000),
    intervalMs: Math.max(5_000, Number(process.env.MARCUS_OPERATION_MONITOR_INTERVAL_MS) || 15_000),
    maxOperationsPerBusiness: 20,
    onError: (error, context = {}) => {
      console.error(`Durable operation monitor ${context.phase || 'pass'} failed${context.operationId ? ` for ${context.operationId}` : ''}:`, error?.message || error);
    },
  });
}

const httpServer = app.listen(PORT, SERVER_HOST, async () => {
  await refreshBusinessCacheFromSettings();
  const businesses = Array.isArray(cachedBusinesses) ? cachedBusinesses : [{ key: DEFAULT_BUSINESS_KEY }];
  for (const biz of businesses) {
    const bKey = normalizeBusinessKey(biz?.key || '') || DEFAULT_BUSINESS_KEY;
    await missionMemoryStore.ensureDefaults(bKey).catch((error) => {
      console.error(`Mission memory startup initialization failed for ${bKey}:`, error?.message || error);
    });
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
  try {
    const operationStartup = await operationsEngine.initializeBusinesses(businesses.map((business) => business?.key || DEFAULT_BUSINESS_KEY));
    const recoveredCount = operationStartup.reduce((total, item) => total + (Array.isArray(item.recovered) ? item.recovered.length : 0), 0);
    if (recoveredCount) console.warn(`M.A.R.C.U.S. reconciled ${recoveredCount} interrupted durable operation(s) without assuming completion.`);
  } catch (error) {
    console.error('Durable operations startup recovery failed; operation files were left intact:', error);
  }
  for (const business of businesses) {
    const businessKey = normalizeBusinessKey(business?.key || '') || DEFAULT_BUSINESS_KEY;
    await withBusinessKey(businessKey, () => projectEvidenceService.refresh(businessKey, { sources: ['operations', 'airtable'] })).catch((error) => {
      console.error(`Project evidence startup reconciliation failed for ${businessKey}:`, error?.message || error);
    });
  }
  await backupCriticalFiles({ force: true }).catch(() => {
    // ignore startup backup errors
  });
  if (String(process.env.MARCUS_STARTUP_CHECK || '').trim().toLowerCase() === 'true') {
    console.log('M.A.R.C.U.S. startup validation completed.');
    httpServer.close(() => process.exit(0));
    return;
  }
  startBackupScheduler();
  startGa4Scheduler();
  startAirtableRequestsAutoSyncScheduler();
  startDurableOperationMonitor();
  startProjectEvidenceScheduler();
  startMarcusBriefScheduler();
  // eslint-disable-next-line no-console
  console.log(`M.A.R.C.U.S. running on http://${SERVER_HOST}:${PORT}`);
});






