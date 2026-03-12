/* =========================================
    M.A.R.C.U.S. — Modular Autonomous Routing & Coordination Utility System
    ========================================= */

/* --- State Management --- */
const state = {
    revision: 1,
    updatedAt: "",

    currentView: "godview",
    currentProjectId: null,
    currentClientName: null,
    settingsPane: "",
    projectsSettingsLimit: 200,

    godViewData: null,
    godViewLoading: false,

    projects: [],
    clients: [],
    tasks: [],
    inboxItems: [],
    inboxConvertContactById: {},
    inboxMarcusRecommendationsById: {},
    inboxMarcusRecommendationsById: {},
    inboxAutomationDigest: { items: [], loading: false, loadedAt: 0, error: '' },
    inboxDigestSelectionsById: {},

    projectScratchpads: {},
    projectNoteEntries: {},
    projectCommunications: {},

    projectRightTabById: {},
    bulkProjectDeleteSelectedById: {},
    dashboardCalls: { loading: false, fetchedAt: 0, error: '', events: [] },
    dashboardGhl: { loading: false, fetchedAt: 0, error: '', snapshot: null },
    dashboardAiPreviews: { loading: false, fetchedAt: 0, error: '', ai: false, tasks: {}, inbox: {} },

    chatHistory: [],
    globalChatHistory: [],
    operatorBioChatHistory: [],
    isChatOpen: true,

    chatThreadId: 'default',

    settings: {
        openaiModel: "gpt-4o-mini",
    },
    openAiModelsCatalog: { items: [], loading: false, error: '', fetchedAt: 0, source: 'fallback' },

    businesses: [],
    activeBusinessKey: 'personal',
    uiPrefs: {
        weekStartsOnMonday: false,
    },

    auth: {
        required: false,
        authenticated: true,
        lastCheckedAt: 0,
        lastError: '',
    },

    // Background sync UX
    backgroundDirty: false,
    lastInteractionAt: 0,

    // View search
    projectsSearch: '',
    revisionsSearch: '',

    // Focus timer state (Pomodoro)
    focusTimer: {
        running: false,
        remaining: 25 * 60,   // seconds
        duration: 25 * 60,
        intervalId: null,
    },

    // Mock team if API fails, but we'll try to fetch
    team: [
        { id: "u1", name: "Mark", role: "admin", avatar: "M" },
        { id: "u2", name: "Sarah", role: "designer", avatar: "S" },
        { id: "u3", name: "David", role: "developer", avatar: "D" },
        { id: "ai", name: "M.A.R.C.U.S.", role: "ai", avatar: "AI" },
    ],
};

let pollIntervalId = null;

function preserveMarcusDrawerDuringRerender() {
    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;
    // If a view rerender is about to clear a parent via `.innerHTML = ''`,
    // move the drawer back to <body> first so it doesn't get destroyed.
    try {
        if (drawer.parentElement && drawer.parentElement !== document.body) {
            document.body.appendChild(drawer);
        }
    } catch {
        // ignore
    }
}

function stopPolling() {
    if (pollIntervalId) {
        try { clearInterval(pollIntervalId); } catch { /* ignore */ }
        pollIntervalId = null;
    }
}

function startPolling() {
    stopPolling();
    const seconds = Math.max(10, Number(state.uiPrefs.autoRefreshSeconds) || 30);
    pollIntervalId = setInterval(() => {
        fetchState({ background: true });
        if (state.currentView === 'godview') refreshGodView();
    }, seconds * 1000);
}

async function refreshAuthStatus() {
    try {
        const token = getStoredAdminToken();
        const headers = new Headers();
        if (token) headers.set('Authorization', `Bearer ${token}`);
        const r = await fetch('/api/auth/status', { headers });
        const s = await r.json().catch(() => ({}));
        state.auth.required = !!s.authRequired;
        state.auth.authenticated = (s && typeof s === 'object' && 'authenticated' in s) ? !!s.authenticated : !state.auth.required;
        state.auth.lastCheckedAt = Date.now();
        if (state.auth.required && !state.auth.authenticated) {
            stopPolling();
        }
        return s;
    } catch (e) {
        state.auth.lastCheckedAt = Date.now();
        return null;
    }
}

const THEME_STORAGE_KEY = 'opsTheme';
const LAYOUT_STORAGE_KEY = 'opsLayout';
const ADMIN_TOKEN_STORAGE_KEY = 'opsAdminToken';

const BUSINESS_KEY_STORAGE_KEY = 'opsBusinessKey';

const MARCUS_OPEN_STORAGE_KEY = 'opsMarcusOpen';
const MARCUS_DETACHED_STORAGE_KEY = 'opsMarcusDetached';
const MARCUS_PANEL_STORAGE_KEY = 'opsMarcusPanel';
const MARCUS_THREAD_STORAGE_KEY = 'opsMarcusThread';
const MARCUS_SYNC_CHANNEL = 'opsMarcusSync';
const MARCUS_SYNC_STORAGE_KEY = 'opsMarcusSyncEvent';
const MARCUS_VOICE_IN_STORAGE_KEY = 'opsMarcusVoiceIn';
const MARCUS_VOICE_OUT_STORAGE_KEY = 'opsMarcusVoiceOut';
const MARCUS_FOCUS_NUDGE_LAST_TS_KEY = 'opsMarcusFocusNudgeLastTs';

const MARCUS_PANEL_MIN_WIDTH = 320;
const MARCUS_PANEL_MIN_HEIGHT = 420;
const MARCUS_TYPING_ID = 'marcus-typing-indicator';

const MARCUS_THINKING_LINES = [
    'SCANNING',
    'SYNTHESIZING',
    'MODELING',
    'ROUTING',
    'EVALUATING',
];
const MARCUS_RESPONDING_LINES = [
    'DRAFTING',
    'COMPILING',
    'TRANSMITTING',
    'CONFIRMING',
];

const IS_MARCUS_POPOUT = (() => {
    try {
        return new URLSearchParams(window.location.search).get('marcusPopout') === '1';
    } catch {
        return false;
    }
})();

const MARCUS_INSTANCE_ID = (() => {
    try {
        const k = 'opsMarcusInstanceId';
        const existing = String(sessionStorage.getItem(k) || '').trim();
        if (existing) return existing;
        const id = `marcus_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;
        sessionStorage.setItem(k, id);
        return id;
    } catch {
        return `marcus_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;
    }
})();

let marcusSyncChannel = null;
let marcusDockRestore = null;

function setStoredMarcusOpen(open) {
    try {
        localStorage.setItem(MARCUS_OPEN_STORAGE_KEY, open ? '1' : '0');
    } catch {
        // ignore
    }
}

function setStoredMarcusVoiceIn(enabled) {
    try {
        localStorage.setItem(MARCUS_VOICE_IN_STORAGE_KEY, enabled ? '1' : '0');
    } catch {
        // ignore
    }
}

function getStoredMarcusVoiceIn() {
    try {
        const raw = String(localStorage.getItem(MARCUS_VOICE_IN_STORAGE_KEY) || '').trim().toLowerCase();
        return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
    } catch {
        return false;
    }
}

function setStoredMarcusVoiceOut(enabled) {
    try {
        localStorage.setItem(MARCUS_VOICE_OUT_STORAGE_KEY, enabled ? '1' : '0');
    } catch {
        // ignore
    }
}

function getStoredMarcusVoiceOut() {
    try {
        const raw = String(localStorage.getItem(MARCUS_VOICE_OUT_STORAGE_KEY) || '').trim().toLowerCase();
        return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
    } catch {
        return false;
    }
}

function stripForSpeech(input) {
    const s = String(input || '');
    return s
        .replace(/```[\s\S]*?```/g, ' ')
        .replace(/`[^`]*`/g, ' ')
        .replace(/\[(.*?)\]\((.*?)\)/g, '$1')
        .replace(/[#*_>]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function pulseMarcusAmbient(mode = 'active', durationMs = 1400) {
    try {
        const bars = document.querySelectorAll('.marcus-ambient');
        const avatars = document.querySelectorAll('.marcus-dashboard-avatar');
        if ((!bars || !bars.length) && (!avatars || !avatars.length)) return;
        bars.forEach((bar) => {
            bar.classList.remove('marcus-busy', 'marcus-responding');
            if (mode === 'busy') bar.classList.add('marcus-busy');
            else if (mode === 'responding') bar.classList.add('marcus-responding');
            bar.classList.add('marcus-live');

            const activeMs = Number.isFinite(Number(durationMs)) ? Math.max(250, Math.min(5000, Number(durationMs))) : 1400;
            window.setTimeout(() => {
                bar.classList.remove('marcus-live', 'marcus-busy', 'marcus-responding');
            }, activeMs);
        });
        avatars.forEach((avatar) => {
            avatar.classList.remove('idle', 'busy', 'responding');
            if (mode === 'busy') avatar.classList.add('busy');
            else if (mode === 'responding') avatar.classList.add('responding');
            else avatar.classList.add('idle');

            const activeMs = Number.isFinite(Number(durationMs)) ? Math.max(250, Math.min(5000, Number(durationMs))) : 1400;
            window.setTimeout(() => {
                avatar.classList.remove('busy', 'responding');
                avatar.classList.add('idle');
            }, activeMs);
        });
    } catch {
        // ignore
    }
}

function speakMarcus(text) {
    pulseMarcusAmbient('responding', 1800);
    if (!state.marcusVoiceOut) return;
    try {
        const spoken = stripForSpeech(text);
        if (!spoken) return;
        if (!('speechSynthesis' in window)) return;
        window.speechSynthesis.cancel();
        const u = new SpeechSynthesisUtterance(spoken.slice(0, 1200));
        u.rate = 1;
        u.pitch = 1;
        u.volume = 1;
        window.speechSynthesis.speak(u);
    } catch {
        // ignore
    }
}

function getStoredMarcusOpen() {
    try {
        const raw = String(localStorage.getItem(MARCUS_OPEN_STORAGE_KEY) || '').trim().toLowerCase();
        if (!raw) return true;
        return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'open';
    } catch {
        return true;
    }
}

function setStoredMarcusThread(threadId) {
    try {
        const t = safeText(threadId).trim() || 'default';
        localStorage.setItem(MARCUS_THREAD_STORAGE_KEY, t);
    } catch {
        // ignore
    }
}

function getStoredMarcusThread() {
    try {
        const raw = String(localStorage.getItem(MARCUS_THREAD_STORAGE_KEY) || '').trim();
        return raw || 'default';
    } catch {
        return 'default';
    }
}

function getStoredMarcusFocusNudgeLastTs() {
    try {
        const raw = String(localStorage.getItem(MARCUS_FOCUS_NUDGE_LAST_TS_KEY) || '').trim();
        const n = Number(raw);
        return Number.isFinite(n) ? n : 0;
    } catch {
        return 0;
    }
}

function setStoredMarcusFocusNudgeLastTs(ts) {
    try {
        const n = Number(ts) || 0;
        localStorage.setItem(MARCUS_FOCUS_NUDGE_LAST_TS_KEY, String(n));
    } catch {
        // ignore
    }
}

function setStoredBusinessKey(key) {
    try {
        localStorage.setItem(BUSINESS_KEY_STORAGE_KEY, safeText(key).trim());
    } catch {
        // ignore
    }
}

function getStoredBusinessKey() {
    try {
        const raw = String(localStorage.getItem(BUSINESS_KEY_STORAGE_KEY) || '').trim();
        return raw;
    } catch {
        return '';
    }
}

function normalizeBusinessKey(input) {
    const raw = safeText(input).trim().toLowerCase();
    if (!raw) return '';
    return raw.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 64);
}

function businessAbbrev(name, key) {
    const n = safeText(name).trim();
    if (n) {
        const parts = n.split(/\s+/g).filter(Boolean);
        if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
        if (parts.length === 1 && parts[0].length >= 2) return parts[0].slice(0, 2).toUpperCase();
        if (parts.length === 1 && parts[0].length === 1) return parts[0][0].toUpperCase();
    }
    const k = safeText(key).trim();
    return (k ? k.slice(0, 2) : 'B').toUpperCase();
}

function applyBusinessConfig(cfg) {
    const businesses = Array.isArray(cfg?.businesses) ? cfg.businesses : [];
    state.businesses = businesses
        .map((b) => ({
            key: normalizeBusinessKey(b?.key || ''),
            name: safeText(b?.name || '').trim(),
            phoneNumbers: Array.isArray(b?.phoneNumbers) ? b.phoneNumbers.map((x) => safeText(x).trim()).filter(Boolean) : [],
        }))
        .filter((b) => b.key && b.name);

    const serverActive = normalizeBusinessKey(cfg?.activeBusinessKey || cfg?.activeBusiness || '');
    const stored = normalizeBusinessKey(getStoredBusinessKey());
    const keys = new Set(state.businesses.map((b) => b.key));
    const next = (stored && keys.has(stored)) ? stored : (serverActive && keys.has(serverActive)) ? serverActive : keys.has('personal') ? 'personal' : (state.businesses[0]?.key || 'personal');

    state.activeBusinessKey = next;
    setStoredBusinessKey(next);
}

async function fetchBusinesses() {
    const data = await apiJson('/api/businesses');
    applyBusinessConfig(data);
    return data;
}

async function setActiveBusinessKey(key, { persistServer = true } = {}) {
    const next = normalizeBusinessKey(key);
    if (!next) return;
    if (next === normalizeBusinessKey(state.activeBusinessKey)) return;

    state.activeBusinessKey = next;
    setStoredBusinessKey(next);
    if (persistServer) {
        try {
            await apiJson('/api/businesses/active', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ key: next })
            });
        } catch {
            // ignore server persistence failures
        }
    }

    state.currentProjectId = null;
    state.currentView = 'dashboard';
    showLoading();
    await Promise.all([
        fetchState(),
        fetchSettings(),
        loadChatHistory(),
    ]);
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

function getStoredMarcusDetached() {
    try {
        const raw = String(localStorage.getItem(MARCUS_DETACHED_STORAGE_KEY) || '').trim().toLowerCase();
        return raw === '1' || raw === 'true' || raw === 'yes';
    } catch {
        return false;
    }
}

function setStoredMarcusDetached(detached) {
    try {
        localStorage.setItem(MARCUS_DETACHED_STORAGE_KEY, detached ? '1' : '0');
    } catch {
        // ignore
    }
}

function syncMarcusDetachedIndicator() {
    const el = document.getElementById('marcus-detached-indicator');
    if (!el) return;
    el.classList.toggle('hidden', !getStoredMarcusDetached());
}

function applyMarcusOpenState(open) {
    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;
    const isOpen = Boolean(open);
    drawer.classList.toggle('hidden', !isOpen);
    drawer.classList.toggle('flex', isOpen);
    state.isChatOpen = isOpen;
}

function makeMarcusSyncEvent(type, payload = {}) {
    return {
        type: String(type || '').trim(),
        payload: (payload && typeof payload === 'object') ? payload : {},
        source: MARCUS_INSTANCE_ID,
        ts: Date.now(),
    };
}

function publishMarcusSync(type, payload = {}) {
    const ev = makeMarcusSyncEvent(type, payload);
    if (marcusSyncChannel) {
        try { marcusSyncChannel.postMessage(ev); } catch {}
    }
    try {
        localStorage.setItem(MARCUS_SYNC_STORAGE_KEY, JSON.stringify(ev));
    } catch {}
}

function sameChatEntry(a, b) {
    return safeText(a?.role) === safeText(b?.role) && safeText(a?.content) === safeText(b?.content);
}

async function applyMarcusRemoteContext(payload) {
    const p = (payload && typeof payload === 'object') ? payload : {};
    const nextProjectId = safeText(p.currentProjectId || '');
    const nextView = safeText(p.currentView || 'dashboard') || 'dashboard';

    if (safeText(state.currentProjectId) === nextProjectId && safeText(state.currentView) === nextView) return;

    state.currentProjectId = nextProjectId || null;
    state.currentView = nextView;
    await loadChatHistory();
    renderChat();
}

function applyMarcusRemoteChat(payload) {
    const p = (payload && typeof payload === 'object') ? payload : {};
    const entry = p.entry && typeof p.entry === 'object' ? p.entry : null;
    if (!entry) return;
    const targetProjectId = safeText(p.projectId || '');
    const targetThreadId = safeText(p.threadId || 'default') || 'default';
    const localProjectId = safeText(state.currentProjectId || '');

    if (targetProjectId !== localProjectId) return;

    const normalizedEntry = { role: normalizeRole(entry.role), content: safeText(entry.content) };
    if (!normalizedEntry.content) return;

    const target = targetProjectId
        ? (Array.isArray(state.chatHistory) ? state.chatHistory : [])
        : (targetThreadId === 'operator_bio'
            ? (Array.isArray(state.operatorBioChatHistory) ? state.operatorBioChatHistory : [])
            : (Array.isArray(state.globalChatHistory) ? state.globalChatHistory : []));
    const last = target[target.length - 1];
    if (last && sameChatEntry(last, normalizedEntry)) return;

    if (targetProjectId) {
        state.chatHistory = [...target, normalizedEntry];
    } else {
        if (targetThreadId === 'operator_bio') {
            state.operatorBioChatHistory = [...target, normalizedEntry];
            if ((state.chatThreadId || 'default') === 'operator_bio') state.chatHistory = state.operatorBioChatHistory;
        } else {
            state.globalChatHistory = [...target, normalizedEntry];
            if (!state.currentProjectId && (state.chatThreadId || 'default') !== 'operator_bio') state.chatHistory = state.globalChatHistory;
        }
    }
    renderChat();
}

async function handleMarcusSyncEvent(ev) {
    const e = (ev && typeof ev === 'object') ? ev : null;
    if (!e || safeText(e.source) === MARCUS_INSTANCE_ID) return;
    const type = safeText(e.type);

    if (type === 'context') {
        await applyMarcusRemoteContext(e.payload);
        return;
    }
    if (type === 'chat-entry') {
        applyMarcusRemoteChat(e.payload);
        return;
    }
    if (type === 'request-sync') {
        publishMarcusSync('sync-state', {
            currentProjectId: safeText(state.currentProjectId || ''),
            currentView: safeText(state.currentView || 'dashboard'),
            globalChatHistory: Array.isArray(state.globalChatHistory) ? state.globalChatHistory.slice(-30) : [],
        });
        return;
    }
    if (type === 'sync-state') {
        const payload = e.payload && typeof e.payload === 'object' ? e.payload : {};
        if (!safeText(state.currentProjectId) && safeText(payload.currentProjectId)) {
            await applyMarcusRemoteContext(payload);
        }
        if (!state.globalChatHistory.length && Array.isArray(payload.globalChatHistory) && payload.globalChatHistory.length) {
            state.globalChatHistory = payload.globalChatHistory
                .map((x) => ({ role: normalizeRole(x?.role), content: safeText(x?.content) }))
                .filter((x) => x.content);
            if (!state.currentProjectId) state.chatHistory = state.globalChatHistory;
            renderChat();
        }
        return;
    }
    if (type === 'popout-closed') {
        if (!IS_MARCUS_POPOUT) {
            setStoredMarcusDetached(false);
            syncMarcusDetachedIndicator();
            applyMarcusOpenState(true);
            setStoredMarcusOpen(true);
        }
        return;
    }
}

function initMarcusSync() {
    try {
        if (typeof BroadcastChannel === 'function') {
            marcusSyncChannel = new BroadcastChannel(MARCUS_SYNC_CHANNEL);
            marcusSyncChannel.onmessage = (msg) => {
                handleMarcusSyncEvent(msg?.data).catch(() => {});
            };
        }
    } catch {
        marcusSyncChannel = null;
    }

    window.addEventListener('storage', (evt) => {
        if (evt.key !== MARCUS_SYNC_STORAGE_KEY || !evt.newValue) return;
        try {
            const parsed = JSON.parse(evt.newValue);
            handleMarcusSyncEvent(parsed).catch(() => {});
        } catch {}
    });

    if (IS_MARCUS_POPOUT) {
        window.addEventListener('beforeunload', () => {
            publishMarcusSync('popout-closed', {});
        });
    }

    publishMarcusSync('request-sync', {});
}

function broadcastMarcusContext() {
    publishMarcusSync('context', {
        currentProjectId: safeText(state.currentProjectId || ''),
        currentView: safeText(state.currentView || 'dashboard'),
    });
}

function getDefaultMarcusPanelLayout() {
    const width = Math.min(420, Math.max(MARCUS_PANEL_MIN_WIDTH, Math.floor(window.innerWidth * 0.33)));
    const height = Math.min(640, Math.max(MARCUS_PANEL_MIN_HEIGHT, Math.floor(window.innerHeight * 0.58)));
    return {
        x: Math.max(8, window.innerWidth - width - 24),
        y: Math.max(8, window.innerHeight - height - 24),
        width,
        height,
    };
}

function clampMarcusPanelLayout(layout) {
    const l = (layout && typeof layout === 'object') ? layout : {};
    const width = Math.min(window.innerWidth - 8, Math.max(MARCUS_PANEL_MIN_WIDTH, Number(l.width) || MARCUS_PANEL_MIN_WIDTH));
    const height = Math.min(window.innerHeight - 8, Math.max(MARCUS_PANEL_MIN_HEIGHT, Number(l.height) || MARCUS_PANEL_MIN_HEIGHT));
    const maxX = Math.max(0, window.innerWidth - width - 8);
    const maxY = Math.max(0, window.innerHeight - height - 8);
    const x = Math.min(maxX, Math.max(0, Number(l.x) || 0));
    const y = Math.min(maxY, Math.max(0, Number(l.y) || 0));
    return { x, y, width, height };
}

function getStoredMarcusPanelLayout() {
    try {
        const raw = String(localStorage.getItem(MARCUS_PANEL_STORAGE_KEY) || '').trim();
        if (!raw) return getDefaultMarcusPanelLayout();
        const parsed = JSON.parse(raw);
        return clampMarcusPanelLayout(parsed);
    } catch {
        return getDefaultMarcusPanelLayout();
    }
}

function setStoredMarcusPanelLayout(layout) {
    try {
        localStorage.setItem(MARCUS_PANEL_STORAGE_KEY, JSON.stringify(clampMarcusPanelLayout(layout)));
    } catch {
        // ignore
    }
}

function applyMarcusPanelLayout(layout) {
    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;
    if (drawer.dataset?.marcusDocked === '1') return;
    const next = clampMarcusPanelLayout(layout);
    drawer.style.left = `${next.x}px`;
    drawer.style.top = `${next.y}px`;
    drawer.style.width = `${next.width}px`;
    drawer.style.height = `${next.height}px`;
    drawer.style.right = 'auto';
    drawer.style.bottom = 'auto';
}

function dockMarcusToDashboardSlot(slotEl) {
    const slot = slotEl && typeof slotEl === 'object' ? slotEl : null;
    const drawer = document.getElementById('neural-drawer');
    if (!slot || !drawer) return;
    if (drawer.dataset?.marcusDocked === '1' && drawer.parentElement === slot) return;

    const parent = drawer.parentElement;
    if (!parent) return;

    // Only capture restore info when docking from a floating state.
    if (drawer.dataset?.marcusDocked !== '1') {
        marcusDockRestore = {
            parent,
            nextSibling: drawer.nextSibling,
            className: drawer.className,
            style: drawer.getAttribute('style') || '',
        };
    }

    drawer.dataset.marcusDocked = '1';
    drawer.className = drawer.className
        .replace(/\bfixed\b/g, '')
        .replace(/\bright-\S+\b/g, '')
        .replace(/\bbottom-\S+\b/g, '');
    drawer.classList.add('relative', 'w-full', 'h-full');
    drawer.style.left = 'auto';
    drawer.style.top = 'auto';
    drawer.style.right = 'auto';
    drawer.style.bottom = 'auto';

    const resizeHandle = document.getElementById('marcus-resize-handle');
    if (resizeHandle) resizeHandle.classList.add('hidden');

    const dragHandle = document.getElementById('marcus-drag-handle');
    if (dragHandle) dragHandle.classList.remove('cursor-move');

    slot.appendChild(drawer);
}

function ensurePersistentMarcusLayout() {
    const main = document.getElementById('main-port');
    if (!main) return null;

    let viewPort = document.getElementById('view-port');
    let marcusPort = document.getElementById('marcus-port');

    const needsRebuild = !viewPort || !marcusPort || viewPort.parentElement !== main || marcusPort.parentElement !== main;
    if (needsRebuild) {
        main.innerHTML = '';
        // Use inline styles for the grid so Tailwind CDN doesn't need to JIT-compile arbitrary values.
        main.className = 'flex-1 min-h-0 overflow-hidden';
        main.style.display = 'grid';
        main.style.gridTemplateColumns = '1fr 22rem';
        main.style.gridTemplateRows = '1fr';
        main.style.height = '100%';

        viewPort = document.createElement('div');
        viewPort.id = 'view-port';
        viewPort.className = 'min-h-0 overflow-y-auto';

        marcusPort = document.createElement('div');
        marcusPort.id = 'marcus-port';
        marcusPort.className = 'min-h-0 overflow-hidden border-l border-ops-border';

        main.appendChild(viewPort);
        main.appendChild(marcusPort);
    }

    return { main, viewPort, marcusPort };
}

function dockMarcusToPersistentSlot() {
    const ports = ensurePersistentMarcusLayout();
    if (!ports) return;

    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;

    const slot = ports.marcusPort;
    if (!slot) return;

    // Always force docked & open.
    setStoredMarcusDetached(false);
    syncMarcusDetachedIndicator();
    applyMarcusOpenState(true);
    setStoredMarcusOpen(true);

    // Strip all floating / absolute positioning and fill the right column completely.
    drawer.dataset.marcusDocked = '1';
    drawer.className = 'flex flex-col overflow-hidden';
    // Force drawer to fill the marcus-port slot exactly.
    drawer.style.cssText = 'position:relative; width:100%; height:100%; min-width:0; min-height:0; border:none; border-radius:0; box-shadow:none;';

    const resizeHandle = document.getElementById('marcus-resize-handle');
    if (resizeHandle) resizeHandle.classList.add('hidden');
    const dragHandle = document.getElementById('marcus-drag-handle');
    if (dragHandle) dragHandle.classList.remove('cursor-move');
    const popoutToggle = document.getElementById('marcus-popout-toggle');
    if (popoutToggle) popoutToggle.classList.add('hidden');

    if (drawer.parentElement !== slot) {
        slot.innerHTML = '';
        slot.appendChild(drawer);
    }
}

function undockMarcusFromDashboard() {
    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;
    if (drawer.dataset?.marcusDocked !== '1') return;
    if (!marcusDockRestore || !marcusDockRestore.parent) return;

    drawer.dataset.marcusDocked = '0';

    const resizeHandle = document.getElementById('marcus-resize-handle');
    if (resizeHandle) resizeHandle.classList.remove('hidden');

    const dragHandle = document.getElementById('marcus-drag-handle');
    if (dragHandle) dragHandle.classList.add('cursor-move');

    if (marcusDockRestore.nextSibling && marcusDockRestore.nextSibling.parentNode === marcusDockRestore.parent) {
        marcusDockRestore.parent.insertBefore(drawer, marcusDockRestore.nextSibling);
    } else {
        marcusDockRestore.parent.appendChild(drawer);
    }

    drawer.className = marcusDockRestore.className;
    drawer.setAttribute('style', marcusDockRestore.style);
    marcusDockRestore = null;

    const next = clampMarcusPanelLayout(getStoredMarcusPanelLayout());
    applyMarcusPanelLayout(next);
    setStoredMarcusPanelLayout(next);
}

function normalizeModelLabel(model) {
    const raw = String(model || '').trim();
    if (!raw) return '';
    const lower = raw.toLowerCase();
    if (lower === 'gpt-4o-mini') return 'GPT-4o mini';
    if (lower === 'gpt-4o') return 'GPT-4o';
    if (lower === 'gpt-4.1-mini') return 'GPT-4.1 mini';
    if (lower === 'gpt-4.1') return 'GPT-4.1';
    return raw;
}

function syncMarcusModelUi() {
    const model = String(state.settings?.openaiModel || '').trim();
    const badge = document.getElementById('ai-model-badge');
    const select = document.getElementById('marcus-model-select');
    if (badge) badge.innerText = normalizeModelLabel(model || 'AI');
    if (select) {
        const options = Array.from(select.options).map((o) => String(o.value || '').trim());
        if (model && !options.includes(model)) {
            const custom = document.createElement('option');
            custom.value = model;
            custom.textContent = model;
            select.appendChild(custom);
        }
        select.value = model || 'gpt-4o-mini';
    }
}

function getStoredAdminToken() {
    try {
        const t = String(localStorage.getItem(ADMIN_TOKEN_STORAGE_KEY) || '').trim();
        return t;
    } catch {
        return '';
    }
}

function setStoredAdminToken(token) {
    try {
        const t = safeText(token).trim();
        if (!t) {
            localStorage.removeItem(ADMIN_TOKEN_STORAGE_KEY);
            return;
        }
        localStorage.setItem(ADMIN_TOKEN_STORAGE_KEY, t);
    } catch {
        // ignore
    }
}

function getStoredTheme() {
    try {
        const t = String(localStorage.getItem(THEME_STORAGE_KEY) || '').trim().toLowerCase();
        return (t === 'light' || t === 'dark') ? t : '';
    } catch {
        return '';
    }
}

function setStoredTheme(theme) {
    try {
        localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
        // ignore
    }
}

function isPortraitCompactMode() {
    try {
        return window.innerHeight > window.innerWidth && window.innerWidth <= 1100;
    } catch {
        return false;
    }
}

function setMarcusPresence(mode = 'idle') {
    const panel = document.getElementById('neural-drawer');
    const orb = document.getElementById('marcus-orb');
    const statusText = document.getElementById('marcus-state');
    const normalized = String(mode || '').toLowerCase();
    const busy = normalized === 'busy';
    const responding = normalized === 'responding';

    if (panel) {
        panel.classList.remove('marcus-thinking', 'marcus-responding');
        if (busy) panel.classList.add('marcus-thinking');
        if (responding) panel.classList.add('marcus-responding');
    }

    if (orb) {
        orb.classList.remove('idle', 'busy', 'responding');
        if (busy) orb.classList.add('busy');
        else if (responding) orb.classList.add('responding');
        else orb.classList.add('idle');
    }

    if (statusText) {
        if (busy) {
            const line = MARCUS_THINKING_LINES[Math.floor(Math.random() * MARCUS_THINKING_LINES.length)];
            statusText.textContent = `M.A.R.C.U.S. THINKING • ${line}`;
        } else if (responding) {
            const line = MARCUS_RESPONDING_LINES[Math.floor(Math.random() * MARCUS_RESPONDING_LINES.length)];
            statusText.textContent = `M.A.R.C.U.S. RESPONDING • ${line}`;
        } else {
            statusText.textContent = 'M.A.R.C.U.S. IDLE • READY FOR ORDERS';
        }
    }
}

function removeMarcusTypingIndicator() {
    const stream = document.getElementById('chat-stream');
    if (!stream) return;
    const existing = stream.querySelector(`#${MARCUS_TYPING_ID}`);
    if (existing) existing.remove();
}

function showMarcusTypingIndicator() {
    const stream = document.getElementById('chat-stream');
    if (!stream) return;
    removeMarcusTypingIndicator();

    const line = MARCUS_THINKING_LINES[Math.floor(Math.random() * MARCUS_THINKING_LINES.length)];
    const wrap = document.createElement('div');
    wrap.id = MARCUS_TYPING_ID;
    wrap.className = 'flex flex-col gap-1 mb-4 animate-fade-in';
    wrap.innerHTML = `
        <span class="text-[10px] uppercase font-bold tracking-wider text-blue-400">M.A.R.C.U.S.</span>
        <div class="p-2 rounded text-xs bg-zinc-800/60 text-zinc-200 border-l-2 border-blue-500 max-w-[90%] break-words shadow-sm">
            <div class="flex items-center gap-2">
                <span>${escapeHtml(line)}</span>
                <span class="inline-flex items-center gap-1">
                    <span class="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse"></span>
                    <span class="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse [animation-delay:120ms]"></span>
                    <span class="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse [animation-delay:240ms]"></span>
                </span>
            </div>
        </div>
    `;

    stream.appendChild(wrap);
    stream.scrollTop = stream.scrollHeight;
}

function initializeMarcusWidget() {
    const drawer = document.getElementById('neural-drawer');
    const dragHandle = document.getElementById('marcus-drag-handle');
    const resizeHandle = document.getElementById('marcus-resize-handle');
    const popoutToggle = document.getElementById('marcus-popout-toggle');
    if (!drawer) return;

    if (IS_MARCUS_POPOUT) {
        document.body.classList.add('marcus-popout-mode');
        setStoredMarcusDetached(true);
    }
    syncMarcusDetachedIndicator();

    applyMarcusPanelLayout(getStoredMarcusPanelLayout());
    applyMarcusOpenState(getStoredMarcusOpen());
    syncMarcusModelUi();
    setMarcusPresence('idle');

    state.marcusVoiceIn = getStoredMarcusVoiceIn();
    state.marcusVoiceOut = getStoredMarcusVoiceOut();
    state.marcusVoiceListening = false;
    syncMarcusVoiceUi();

    if (IS_MARCUS_POPOUT) {
        applyMarcusOpenState(true);
    }

    if (popoutToggle) {
        const icon = popoutToggle.querySelector('i');
        if (IS_MARCUS_POPOUT) {
            popoutToggle.title = 'Return to app window';
            if (icon) icon.className = 'fa-solid fa-down-left-and-up-right-to-center';
        }
        popoutToggle.addEventListener('click', () => {
            const baseUrl = `${window.location.origin}${window.location.pathname}`;
            if (IS_MARCUS_POPOUT) {
                setStoredMarcusDetached(false);
                const main = window.open(baseUrl, '_blank');
                if (main) main.focus();
                window.close();
                return;
            }

            const rect = drawer.getBoundingClientRect();
            const width = Math.max(380, Math.floor(rect.width));
            const height = Math.max(420, Math.floor(rect.height));
            const left = Math.max(0, Math.floor(window.screenX + rect.left));
            const top = Math.max(0, Math.floor(window.screenY + rect.top));
            const target = `${baseUrl}?marcusPopout=1`;
            const features = `popup=yes,width=${width},height=${height},left=${left},top=${top},resizable=yes,scrollbars=no`;
            const w = window.open(target, `marcus-popout-${Date.now()}`, features);
            if (w) {
                w.focus();
                setStoredMarcusDetached(true);
                syncMarcusDetachedIndicator();
                applyMarcusOpenState(false);
                setStoredMarcusOpen(false);
            }
        });
    }

    if (IS_MARCUS_POPOUT) return;

    let drag = null;
    let resize = null;

    if (dragHandle) {
        dragHandle.addEventListener('pointerdown', (e) => {
            if (drawer.dataset?.marcusDocked === '1') return;
            if (e.target && e.target.closest('button,select,input,textarea')) return;
            const rect = drawer.getBoundingClientRect();
            drag = {
                pointerId: e.pointerId,
                startX: e.clientX,
                startY: e.clientY,
                originX: rect.left,
                originY: rect.top,
            };
            dragHandle.setPointerCapture?.(e.pointerId);
            e.preventDefault();
        });

        dragHandle.addEventListener('pointermove', (e) => {
            if (!drag || drag.pointerId !== e.pointerId) return;
            const next = {
                ...getStoredMarcusPanelLayout(),
                x: drag.originX + (e.clientX - drag.startX),
                y: drag.originY + (e.clientY - drag.startY),
                width: drawer.getBoundingClientRect().width,
                height: drawer.getBoundingClientRect().height,
            };
            applyMarcusPanelLayout(next);
        });

        const stopDrag = (e) => {
            if (!drag || drag.pointerId !== e.pointerId) return;
            const rect = drawer.getBoundingClientRect();
            setStoredMarcusPanelLayout({
                x: rect.left,
                y: rect.top,
                width: rect.width,
                height: rect.height,
            });
            dragHandle.releasePointerCapture?.(e.pointerId);
            drag = null;
        };

        dragHandle.addEventListener('pointerup', stopDrag);
        dragHandle.addEventListener('pointercancel', stopDrag);
    }

    if (resizeHandle) {
        resizeHandle.addEventListener('pointerdown', (e) => {
            if (drawer.dataset?.marcusDocked === '1') return;
            const rect = drawer.getBoundingClientRect();
            resize = {
                pointerId: e.pointerId,
                startX: e.clientX,
                startY: e.clientY,
                originW: rect.width,
                originH: rect.height,
                x: rect.left,
                y: rect.top,
            };
            resizeHandle.setPointerCapture?.(e.pointerId);
            e.preventDefault();
        });

        resizeHandle.addEventListener('pointermove', (e) => {
            if (!resize || resize.pointerId !== e.pointerId) return;
            const next = {
                x: resize.x,
                y: resize.y,
                width: resize.originW + (e.clientX - resize.startX),
                height: resize.originH + (e.clientY - resize.startY),
            };
            applyMarcusPanelLayout(next);
        });

        const stopResize = (e) => {
            if (!resize || resize.pointerId !== e.pointerId) return;
            const rect = drawer.getBoundingClientRect();
            setStoredMarcusPanelLayout({
                x: rect.left,
                y: rect.top,
                width: rect.width,
                height: rect.height,
            });
            resizeHandle.releasePointerCapture?.(e.pointerId);
            resize = null;
        };

        resizeHandle.addEventListener('pointerup', stopResize);
        resizeHandle.addEventListener('pointercancel', stopResize);
    }

    window.addEventListener('resize', () => {
        if (drawer.dataset?.marcusDocked === '1') return;
        const next = clampMarcusPanelLayout(getStoredMarcusPanelLayout());
        applyMarcusPanelLayout(next);
        setStoredMarcusPanelLayout(next);
    });
}


function getStoredLayout() {
    try { return localStorage.getItem('opsLayout') || 'standard'; } catch { return 'standard'; }
}
function setStoredLayout(l) {
    try { localStorage.setItem('opsLayout', l); } catch {}
}
function applyLayout(l) {
    state.layoutMode = l || 'standard';
    try { document.documentElement.setAttribute('data-layout', state.layoutMode); } catch {}
    
    const btn = document.getElementById('toggle-layout');
    const icon = btn ? btn.querySelector('i') : null;
    if (icon) {
        icon.className = `fa-solid ${state.layoutMode === 'landscape' ? 'fa-table-list' : 'fa-table-columns'}`;
    }
}

function applyTheme(theme) {
    const t = (theme === 'light' || theme === 'dark') ? theme : 'dark';
    state.theme = t;
    try {
        document.documentElement.dataset.theme = t;
    } catch {
        // ignore
    }

    const btn = document.getElementById('toggle-theme');
    const icon = btn ? btn.querySelector('i') : null;
    if (icon) {
        icon.className = `fa-solid ${t === 'light' ? 'fa-sun' : 'fa-moon'}`;
    }
}

function applyUiPreferencesFromSettings(settings) {
    const s = (settings && typeof settings === 'object') ? settings : {};
    const titleScaleRaw = safeText(s.uiTitleScale).trim().toLowerCase();
    const densityRaw = safeText(s.uiDensity).trim().toLowerCase();

    const titleScale = ['sm', 'md', 'lg', 'xl'].includes(titleScaleRaw) ? titleScaleRaw : 'md';
    const density = ['compact', 'comfortable'].includes(densityRaw) ? densityRaw : 'comfortable';

    const titleMap = {
        // Inter is loaded at 400/500/600/700 — stay within real weights.
        sm: { page: '1.25rem', pageWeight: '700', section: '0.8125rem', sectionWeight: '600' },
        md: { page: '1.4rem', pageWeight: '700', section: '0.875rem', sectionWeight: '600' },
        lg: { page: '1.55rem', pageWeight: '700', section: '0.95rem', sectionWeight: '600' },
        xl: { page: '1.75rem', pageWeight: '700', section: '1.0rem', sectionWeight: '600' },
    };

    const densityMap = {
        comfortable: { pad: '16px', gap: '12px', headPy: '10px', previewPb: '10px' },
        compact: { pad: '12px', gap: '8px', headPy: '8px', previewPb: '8px' },
    };

    try {
        const root = document.documentElement;
        const style = root.style;
        const t = titleMap[titleScale] || titleMap.md;
        const d = densityMap[density] || densityMap.comfortable;

        style.setProperty('--ops-page-title-size', t.page);
        style.setProperty('--ops-page-title-weight', t.pageWeight);
        style.setProperty('--ops-section-title-size', t.section);
        style.setProperty('--ops-section-title-weight', t.sectionWeight);

        style.setProperty('--ops-dash-pad', d.pad);
        style.setProperty('--ops-dash-gap', d.gap);
        style.setProperty('--ops-dash-card-head-py', d.headPy);
        style.setProperty('--ops-dash-card-preview-pb', d.previewPb);

        root.dataset.uiTitleScale = titleScale;
        root.dataset.uiDensity = density;
    } catch {
        // ignore
    }
}

function getPageElementsPreferences(settings) {
    const s = (settings && typeof settings === 'object') ? settings : {};
    const pe = (s.pageElements && typeof s.pageElements === 'object') ? s.pageElements : {};
    const dashRaw = (pe.dashboard && typeof pe.dashboard === 'object') ? pe.dashboard : {};
    const godRaw = (pe.godview && typeof pe.godview === 'object') ? pe.godview : {};

    const boolDefaultTrue = (v) => (v === false ? false : true);

    return {
        dashboard: {
            missionControl: boolDefaultTrue(dashRaw.missionControl),
            newProjectIntake: boolDefaultTrue(dashRaw.newProjectIntake),
            commsRadar: boolDefaultTrue(dashRaw.commsRadar),
            deliveryBoard: boolDefaultTrue(dashRaw.deliveryBoard),
        },
        godview: {
            businessesRadar: boolDefaultTrue(godRaw.businessesRadar),
            marcusBrief: boolDefaultTrue(godRaw.marcusBrief),
            upcoming: boolDefaultTrue(godRaw.upcoming),
            teamComms: boolDefaultTrue(godRaw.teamComms),
            globalFocus: boolDefaultTrue(godRaw.globalFocus),
        },
    };
}

function isEditableTarget(el) {
    if (!el || typeof el !== 'object') return false;
    const tag = String(el.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea' || tag === 'select') return true;
    return !!el.isContentEditable;
}

function openCommandPalette(prefill = '') {
    state.commandPaletteOpen = true;
    state.commandPaletteQuery = safeText(prefill).trim();
    state.commandPaletteSelection = 0;
    renderCommandPaletteOverlay();
}

function closeCommandPalette() {
    state.commandPaletteOpen = false;
    state.commandPaletteQuery = '';
    state.commandPaletteSelection = 0;
    renderCommandPaletteOverlay();
}

function getCommandPaletteItems() {
    const projects = Array.isArray(state.projects) ? state.projects.slice(0, 12) : [];
    const currentProject = projects.find((p) => safeText(p?.id) === safeText(state.currentProjectId));
    const actions = [
        {
            id: 'go-dashboard',
            label: 'Go to Dashboard',
            hint: 'Navigation',
            keywords: 'dashboard home overview',
            run: async () => { await openDashboard(); },
        },
        {
            id: 'go-inbox',
            label: 'Go to Inbox',
            hint: 'Navigation',
            keywords: 'inbox messages radar',
            run: async () => { await openInbox(); },
        },
        {
            id: 'new-project',
            label: 'Create New Project',
            hint: 'Action',
            keywords: 'new project intake create',
            run: async () => { await createNewProjectPrompt(); },
        },
        {

        // M.A.R.C.U.S. voice
        marcusVoiceIn: false,
        marcusVoiceOut: false,
        marcusVoiceListening: false,
            id: 'new-inbox-item',
            label: 'Capture Inbox Item',
            hint: 'Action',
            keywords: 'capture inbox message add note',
            run: async () => {
                const text = safeText(window.prompt('Capture to inbox:') || '').trim();
                if (!text) return;
                await createInboxItem(text);
                renderMain();
            },
        },
        {
            id: 'refresh-calls',
            label: 'Refresh Calls Feed',
            hint: 'Action',
            keywords: 'google calls refresh calendar',
            run: async () => { await refreshDashboardCalls({ force: true }); },
        },
        {
            id: 'refresh-ghl',
            label: 'Refresh Mini GHL',
            hint: 'Action',
            keywords: 'ghl highlevel leadconnector dashboard refresh',
            run: async () => { await refreshDashboardGhl({ force: true }); },
        },
    ];

    if (currentProject) {
        actions.push({
            id: 'new-task-current-project',
            label: `New Task in ${safeText(currentProject.name) || 'Current Project'}`,
            hint: 'Task',
            keywords: 'task create current project',
            run: async () => { await promptNewTask(currentProject); },
        });
    }

    for (const p of projects) {
        const pid = safeText(p?.id);
        const pname = safeText(p?.name);
        if (!pid || !pname) continue;
        actions.push({
            id: `open-project-${pid}`,
            label: `Open Project: ${pname}`,
            hint: 'Project',
            keywords: `open project ${pname.toLowerCase()}`,
            run: async () => { await openProject(pid); },
        });
    }

    return actions;
}

function renderCommandPaletteOverlay() {
    const existing = document.getElementById('command-palette-overlay');
    if (existing) existing.remove();
    if (!state.commandPaletteOpen) return;

    const query = safeText(state.commandPaletteQuery).trim().toLowerCase();
    const all = getCommandPaletteItems();
    const filtered = all.filter((item) => {
        if (!query) return true;
        const hay = `${safeText(item.label).toLowerCase()} ${safeText(item.hint).toLowerCase()} ${safeText(item.keywords).toLowerCase()}`;
        return hay.includes(query);
    }).slice(0, 12);

    if (state.commandPaletteSelection >= filtered.length) {
        state.commandPaletteSelection = Math.max(0, filtered.length - 1);
    }

    const overlay = document.createElement('div');
    overlay.id = 'command-palette-overlay';
    overlay.className = 'fixed inset-0 z-[70] bg-black/50 backdrop-blur-[1px] flex items-start justify-center pt-24 px-4';
    overlay.innerHTML = `
        <div class="w-full max-w-2xl border border-zinc-800 rounded-xl bg-zinc-950 shadow-2xl overflow-hidden">
            <div class="px-4 py-3 border-b border-zinc-800 bg-zinc-900/50">
                <input id="command-palette-input" type="text" value="${escapeHtml(state.commandPaletteQuery)}" placeholder="Type a command… (Dashboard, Inbox, Project, Task)" class="w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40" />
                <div class="text-[10px] text-zinc-500 font-mono mt-1">Enter: run • ↑/↓: navigate • Esc: close • /: open</div>
            </div>
            <div class="max-h-[55vh] overflow-y-auto p-2">
                ${filtered.length ? filtered.map((item, idx) => `
                    <button data-cmd-run="${escapeHtml(item.id)}" class="w-full text-left px-3 py-2 rounded-lg border ${idx === state.commandPaletteSelection ? 'border-blue-600/40 bg-blue-600/10' : 'border-transparent hover:border-zinc-800 hover:bg-zinc-900/40'} transition-colors">
                        <div class="flex items-center justify-between gap-3">
                            <div class="text-sm text-zinc-100 truncate">${escapeHtml(item.label)}</div>
                            <div class="text-[10px] font-mono text-zinc-500 uppercase tracking-widest">${escapeHtml(item.hint)}</div>
                        </div>
                    </button>
                `).join('') : `<div class="px-3 py-8 text-center text-sm text-zinc-500">No commands match your search.</div>`}
            </div>
        </div>
    `;

    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) closeCommandPalette();
    });

    document.body.appendChild(overlay);

    const input = overlay.querySelector('#command-palette-input');
    if (input) {
        input.focus();
        input.setSelectionRange(input.value.length, input.value.length);
        input.addEventListener('input', () => {
            state.commandPaletteQuery = safeText(input.value);
            state.commandPaletteSelection = 0;
            renderCommandPaletteOverlay();
        });
        input.addEventListener('keydown', async (e) => {
            if (e.key === 'Escape') {
                e.preventDefault();
                closeCommandPalette();
                return;
            }
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                state.commandPaletteSelection = Math.min(state.commandPaletteSelection + 1, Math.max(0, filtered.length - 1));
                renderCommandPaletteOverlay();
                return;
            }
            if (e.key === 'ArrowUp') {
                e.preventDefault();
                state.commandPaletteSelection = Math.max(state.commandPaletteSelection - 1, 0);
                renderCommandPaletteOverlay();
                return;
            }
            if (e.key === 'Enter') {
                e.preventDefault();
                const item = filtered[state.commandPaletteSelection] || filtered[0];
                if (!item) return;
                closeCommandPalette();
                await item.run();
            }
        });
    }

    overlay.querySelectorAll('button[data-cmd-run]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const id = safeText(btn.getAttribute('data-cmd-run'));
            const item = all.find((x) => safeText(x.id) === id);
            closeCommandPalette();
            if (item) await item.run();
        });
    });
}

function toggleTheme() {
    const next = state.theme === 'light' ? 'dark' : 'light';
    setStoredTheme(next);
    applyTheme(next);
}

function isUserEditingNow() {
    try {
        const el = document.activeElement;
        if (!el) return false;
        if (el.isContentEditable) return true;
        const tag = String(el.tagName || '').toLowerCase();
        if (tag === 'textarea') return true;
        if (tag === 'select') return true;
        if (tag === 'input') {
            const type = String(el.getAttribute('type') || 'text').toLowerCase();
            // Text-ish inputs only; don't block refresh for checkboxes/buttons.
            return !['button', 'submit', 'checkbox', 'radio', 'range', 'color', 'file', 'reset'].includes(type);
        }
        return false;
    } catch {
        return false;
    }
}

function markUiInteraction() {
    state.lastInteractionAt = Date.now();
}

function isUserIdle(ms = 2500) {
    const last = Number(state.lastInteractionAt || 0);
    if (!last) return true;
    return (Date.now() - last) > ms;
}

function setPageMeta(text) {
    const el = document.getElementById('page-meta');
    if (!el) return;
    const t = safeText(text).trim();
    if (!t) {
        el.textContent = '';
        el.classList.add('hidden');
        return;
    }
    el.textContent = t;
    el.classList.remove('hidden');
}

function snapshotViewUiState() {
    const ports = ensurePersistentMarcusLayout();
    const viewPort = ports?.viewPort;
    if (!viewPort) return null;

    const snap = {
        view: safeText(state.currentView),
        scrollTop: null,
        expandedCards: [],
        scrollSelector: '',
    };

    if (state.currentView === 'dashboard' || state.currentView === 'godview') {
        const scrollEl = viewPort.querySelector('.dash-stagger');
        if (scrollEl) {
            snap.scrollSelector = '.dash-stagger';
            snap.scrollTop = scrollEl.scrollTop;
        }
        snap.expandedCards = Array.from(viewPort.querySelectorAll('.dash-card.expanded[data-card-id]'))
            .map((el) => safeText(el?.dataset?.cardId))
            .filter(Boolean);
        return snap;
    }

    // Best-effort: preserve the first scrollable region inside the view port.
    const scrollEl = viewPort.querySelector('.overflow-y-auto') || viewPort;
    snap.scrollSelector = scrollEl === viewPort ? '' : '.overflow-y-auto';
    snap.scrollTop = scrollEl ? scrollEl.scrollTop : null;
    return snap;
}

function restoreViewUiState(snap) {
    if (!snap || snap.view !== safeText(state.currentView)) return;
    const ports = ensurePersistentMarcusLayout();
    const viewPort = ports?.viewPort;
    if (!viewPort) return;

    if (snap.view === 'dashboard') {
        try {
            for (const id of Array.isArray(snap.expandedCards) ? snap.expandedCards : []) {
                const card = viewPort.querySelector(`.dash-card[data-card-id="${CSS.escape(id)}"]`);
                if (card) card.classList.add('expanded');
            }
        } catch {
            // ignore selector errors
        }
        const scrollEl = viewPort.querySelector('.dash-stagger');
        if (scrollEl && Number.isFinite(Number(snap.scrollTop))) {
            scrollEl.scrollTop = Number(snap.scrollTop);
        }
        return;
    }

    const scrollEl = (snap.scrollSelector ? viewPort.querySelector(snap.scrollSelector) : null) || viewPort.querySelector('.overflow-y-auto') || viewPort;
    if (scrollEl && Number.isFinite(Number(snap.scrollTop))) {
        scrollEl.scrollTop = Number(snap.scrollTop);
    }
}

function rerenderMainPreservingUi() {
    const snap = snapshotViewUiState();
    renderNav();
    renderMain();
    requestAnimationFrame(() => restoreViewUiState(snap));
}

function flushDeferredRerenderIfSafe() {
    if (!state.deferRerender) return;
    if (Date.now() < Number(state.rerenderPauseUntil || 0)) return;
    if (isUserEditingNow()) return;
    state.deferRerender = false;
    rerenderMainPreservingUi();
}

let marcusSpeechRecognition = null;

function getSpeechRecognitionCtor() {
    try {
        return window.SpeechRecognition || window.webkitSpeechRecognition || null;
    } catch {
        return null;
    }
}

function syncMarcusVoiceUi() {
    const mic = document.getElementById('cmd-mic');
    const speak = document.getElementById('cmd-speak');
    if (mic) {
        mic.classList.toggle('text-blue-400', !!state.marcusVoiceIn);
        mic.classList.toggle('text-emerald-300', !!state.marcusVoiceListening);
        mic.title = state.marcusVoiceListening ? 'Listening… (click to stop)' : 'Voice input';
    }
    if (speak) {
        speak.classList.toggle('text-blue-400', !!state.marcusVoiceOut);
        speak.title = state.marcusVoiceOut ? 'Voice output on' : 'Voice output off';
    }
}

function stopMarcusListening() {
    state.marcusVoiceListening = false;
    try {
        marcusSpeechRecognition?.stop?.();
    } catch {
        // ignore
    }
    syncMarcusVoiceUi();
}

function startMarcusListening() {
    const Ctor = getSpeechRecognitionCtor();
    if (!Ctor) {
        alert('Voice input is not supported in this browser. Try Chrome/Edge on desktop.');
        state.marcusVoiceIn = false;
        setStoredMarcusVoiceIn(false);
        syncMarcusVoiceUi();
        return;
    }

    if (!marcusSpeechRecognition) {
        marcusSpeechRecognition = new Ctor();
        marcusSpeechRecognition.lang = 'en-US';
        marcusSpeechRecognition.interimResults = true;
        marcusSpeechRecognition.continuous = false;
    }

    const input = document.getElementById('cmd-input');
    let finalTranscript = '';

    state.marcusVoiceListening = true;
    syncMarcusVoiceUi();

    marcusSpeechRecognition.onresult = (e) => {
        try {
            let interim = '';
            for (let i = e.resultIndex; i < e.results.length; i++) {
                const r = e.results[i];
                const t = r && r[0] && r[0].transcript ? String(r[0].transcript) : '';
                if (r.isFinal) finalTranscript += t;
                else interim += t;
            }
            const combined = (finalTranscript + interim).trim();
            if (input && combined) input.value = combined;
        } catch {
            // ignore
        }
    };

    marcusSpeechRecognition.onerror = (e) => {
        const code = safeText(e?.error).trim().toLowerCase();
        state.marcusVoiceListening = false;
        syncMarcusVoiceUi();
        if (code === 'not-allowed' || code === 'service-not-allowed') {
            alert('Microphone permission is blocked. Allow mic access in your browser/site settings and try again.');
        } else if (code === 'no-speech') {
            // no-op; silent so quick retry feels natural
        }
    };

    marcusSpeechRecognition.onend = () => {
        state.marcusVoiceListening = false;
        syncMarcusVoiceUi();
        const final = String(finalTranscript || '').trim();
        if (final) {
            try {
                if (input) input.value = final;
                handleChatSubmit();
            } catch {
                // ignore
            }
        }
    };

    try {
        marcusSpeechRecognition.start();
        if (input) input.focus?.();
    } catch {
        state.marcusVoiceListening = false;
        syncMarcusVoiceUi();
    }
}

function ensureAiTeamMember() {
    const list = Array.isArray(state.team) ? state.team : [];
    const hasAi = list.some((m) => safeText(m?.id) === 'ai');
    if (hasAi) return;
    state.team = [...list, { id: 'ai', name: 'Marcus', role: 'ai', avatar: 'AI' }];
}

function normalizeCsvList(text) {
    return safeText(text)
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
        .slice(0, 32);
}

function getHumanTeamMembers() {
    const list = Array.isArray(state.team) ? state.team : [];
    return list.filter((m) => safeText(m?.id) && safeText(m?.id) !== 'ai' && safeText(m?.name));
}

function isArchivedProject(project) {
    const s = safeText(project?.status).trim().toLowerCase();
    return s === 'done' || s === 'completed' || s === 'complete' || s === 'archived' || s === 'archive';
}

function isContactOnlyProject(project) {
    if (project && typeof project === 'object' && project.isContactRecord === true) return true;
    const brief = safeText(project?.agentBrief).toLowerCase();
    if (brief.includes('imported from airtable (clients)')) return true;
    return false;
}

function isRevisionRequestProject(project) {
    const p = (project && typeof project === 'object') ? project : {};
    if (safeText(p?.airtableSource).trim() === 'revision-requests') return true;
    if (safeText(p?.airtableRequestsKey).trim()) return true;
    const brief = safeText(p?.agentBrief).toLowerCase();
    if (brief.includes('imported from airtable (revision requests)')) return true;
    return false;
}

function getCurrentUserName() {
    const teamMembers = Array.isArray(state.team) ? state.team : [];
    const adminName = safeText(teamMembers.find((m) => safeText(m?.role).toLowerCase() === 'admin')?.name).trim();
    if (adminName) return adminName;
    const firstHuman = safeText(teamMembers.find((m) => safeText(m?.id) && safeText(m?.id) !== 'ai' && safeText(m?.name))?.name).trim();
    return firstHuman || 'Operator';
}

function getProjectOwnerName(project) {
    const owner = safeText(project?.owner).trim();
    return owner;
}

function shouldShowProjectInMyProjects(project) {
    if (!isRevisionRequestProject(project)) return true;
    const owner = getProjectOwnerName(project);
    if (!owner) return false;
    return owner === getCurrentUserName();
}

function getActiveProjects() {
    const list = Array.isArray(state.projects) ? state.projects : [];
    return list.filter((p) => !isArchivedProject(p) && !isContactOnlyProject(p) && shouldShowProjectInMyProjects(p));
}

function getArchivedProjects() {
    const list = Array.isArray(state.projects) ? state.projects : [];
    return list.filter((p) => isArchivedProject(p) && !isContactOnlyProject(p) && shouldShowProjectInMyProjects(p));
}

function getAssignableOwnerNames() {
    const humanNames = getHumanTeamMembers()
        .map((m) => safeText(m?.name).trim())
        .filter(Boolean);
    const me = safeText(getCurrentUserName()).trim();
    const merged = me ? [me, ...humanNames] : humanNames;
    return Array.from(new Set(merged));
}

async function refreshSlackTeamPresence({ force = false } = {}) {
    if (!state.settings?.slackInstalled) {
        state.teamPresenceByMemberId = {};
        state.teamPresenceError = '';
        state.teamPresenceLoading = false;
        state.teamPresenceFetchedAt = Date.now();
        return;
    }

    const age = Date.now() - Number(state.teamPresenceFetchedAt || 0);
    if (!force && age < 60_000) return;
    if (state.teamPresenceLoading) return;

    state.teamPresenceLoading = true;
    try {
        const data = await apiJson('/api/integrations/slack/team-presence');
        const members = Array.isArray(data?.members) ? data.members : [];
        const map = {};
        for (const member of members) {
            const id = safeText(member?.memberId).trim();
            if (!id) continue;
            map[id] = member;
        }
        state.teamPresenceByMemberId = map;
        state.teamPresenceError = safeText(data?.error).trim();
        state.teamPresenceFetchedAt = Date.now();
    } catch (e) {
        state.teamPresenceError = e?.message || 'Failed to load Slack presence';
        state.teamPresenceFetchedAt = Date.now();
    } finally {
        state.teamPresenceLoading = false;
    }
}

function getOpenTaskCountByOwner() {
    const counts = {};
    const tasks = Array.isArray(state.tasks) ? state.tasks : [];
    for (const t of tasks) {
        if (isDoneTask(t)) continue;
        const o = safeText(t?.owner).trim();
        if (!o) continue;
        counts[o] = (counts[o] || 0) + 1;
    }
    return counts;
}

function getWipLimitForOwner(ownerName) {
    const name = safeText(ownerName).trim();
    const member = getHumanTeamMembers().find((m) => safeText(m?.name).trim() === name);
    const limit = Number(member?.wipLimit);
    if (!Number.isFinite(limit) || limit <= 0) return Infinity;
    return limit;
}

function computeSkillScore(task, member) {
    const title = safeText(task?.title).toLowerCase();
    const type = safeText(task?.type).toLowerCase();
    const skills = Array.isArray(member?.skills) ? member.skills : [];
    let score = 0;
    for (const s of skills) {
        const k = safeText(s).toLowerCase();
        if (!k) continue;
        if (type && k === type) score += 3;
        if (title && k.length >= 3 && title.includes(k)) score += 2;
    }
    return score;
}

function getTranscriptDraft(projectId) {
    const id = safeText(projectId);
    const map = (state.projectTranscriptDraftById && typeof state.projectTranscriptDraftById === 'object') ? state.projectTranscriptDraftById : {};
    const existing = map[id];
    if (existing && typeof existing === 'object') return existing;
    const created = { text: '', proposal: null, analyzing: false, applying: false, error: '' };
    state.projectTranscriptDraftById = { ...map, [id]: created };
    return created;
}

function setTranscriptDraft(projectId, patch) {
    const id = safeText(projectId);
    const map = (state.projectTranscriptDraftById && typeof state.projectTranscriptDraftById === 'object') ? state.projectTranscriptDraftById : {};
    const existing = getTranscriptDraft(id);
    const p = (patch && typeof patch === 'object') ? patch : {};
    state.projectTranscriptDraftById = { ...map, [id]: { ...existing, ...p } };
}

function getTodayNextActions() {
    const today = ymdToday();
    const tasks = (Array.isArray(state.tasks) ? state.tasks : []).filter((t) => !isDoneTask(t));
    const score = (t) => {
        const due = safeText(t?.dueDate).trim();
        const pr = Number(t?.priority) || 3;
        const status = safeText(t?.status).toLowerCase();
        const overdue = due && due < today;
        const dueToday = due && due === today;
        const nextish = status === 'next';
        return (
            (overdue ? 0 : 1000) +
            (dueToday ? 1 : 1000) +
            (nextish ? 2 : 1000) +
            (pr * 10) +
            (due ? 0 : 50)
        );
    };
    return tasks
        .slice()
        .sort((a, b) => score(a) - score(b))
        .slice(0, 10);
}

function formatTimeFromIso(iso) {
    const s = safeText(iso).trim();
    if (!s) return '';
    const d = new Date(s);
    if (Number.isNaN(d.getTime())) return '';
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

async function refreshDashboardCalls({ force = false } = {}) {
    const connected = !!state.settings?.googleConnected;
    if (!connected) return;
    if (state.dashboardCalls.loading) return;
    const ageMs = Date.now() - (Number(state.dashboardCalls.fetchedAt) || 0);
    if (!force && ageMs < 60_000) return;

    state.dashboardCalls = { ...state.dashboardCalls, loading: true, error: '' };
    try {
        const data = await apiJson('/api/integrations/google/upcoming?days=1&max=10');
        const events = Array.isArray(data?.events) ? data.events : [];
        state.dashboardCalls = { loading: false, fetchedAt: Date.now(), error: '', events };
    } catch (e) {
        state.dashboardCalls = { ...state.dashboardCalls, loading: false, fetchedAt: Date.now(), error: e?.message || 'Failed to load calls' };
    }
    renderMain();
}

async function refreshDashboardAiPreviews({ taskIds = [], inboxIds = [], force = false } = {}) {
    if (state.dashboardAiPreviews?.loading) return;

    const tIds = Array.isArray(taskIds) ? taskIds.map((v) => safeText(v).trim()).filter(Boolean).slice(0, 24) : [];
    const iIds = Array.isArray(inboxIds) ? inboxIds.map((v) => safeText(v).trim()).filter(Boolean).slice(0, 24) : [];
    if (!tIds.length && !iIds.length) return;

    const ageMs = Date.now() - (Number(state.dashboardAiPreviews?.fetchedAt) || 0);
    if (!force && ageMs < 60_000) return;

    state.dashboardAiPreviews = { ...(state.dashboardAiPreviews || {}), loading: true, error: '' };
    try {
        const data = await apiJson('/api/dashboard/ai-previews', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ taskIds: tIds, inboxIds: iIds }),
        });
        const tasks = (data?.tasks && typeof data.tasks === 'object') ? data.tasks : {};
        const inbox = (data?.inbox && typeof data.inbox === 'object') ? data.inbox : {};
        state.dashboardAiPreviews = {
            loading: false,
            fetchedAt: Date.now(),
            error: '',
            ai: Boolean(data?.ai),
            tasks,
            inbox,
        };
    } catch (e) {
        state.dashboardAiPreviews = { ...(state.dashboardAiPreviews || {}), loading: false, fetchedAt: Date.now(), error: e?.message || 'Failed to load AI previews' };
    }

    if (state.currentView === 'dashboard') renderMain();
}

async function refreshDashboardGhl({ force = false } = {}) {
    const configured = !!state.settings?.ghlConfigured;
    if (!configured) {
        state.dashboardGhl = { loading: false, fetchedAt: Date.now(), error: '', snapshot: null };
        return;
    }
    if (state.dashboardGhl.loading) return;
    const ageMs = Date.now() - (Number(state.dashboardGhl.fetchedAt) || 0);
    if (!force && ageMs < 60_000) return;

    state.dashboardGhl = { ...state.dashboardGhl, loading: true, error: '' };
    try {
        const data = await apiJson('/api/integrations/ghl/snapshot');
        state.dashboardGhl = { loading: false, fetchedAt: Date.now(), error: '', snapshot: data };
    } catch (e) {
        state.dashboardGhl = { ...state.dashboardGhl, loading: false, fetchedAt: Date.now(), error: e?.message || 'Failed to load GHL snapshot' };
    }
    renderMain();
}

function setNewProjectDraft(patch) {
    const next = { ...(state.newProjectDraft || {}) };
    const p = (patch && typeof patch === 'object') ? patch : {};
    for (const [k, v] of Object.entries(p)) next[k] = v;
    state.newProjectDraft = next;
}

function resetNewProjectDraft() {
    state.newProjectDraft = {
        name: '',
        type: 'Build',
        status: 'Active',
        dueDate: '',
        projectValue: '',
        repoUrl: '',
        docsUrl: '',
        stripeInvoiceUrl: '',
        workspacePath: '',
        agentBrief: '',
    };
}

async function createProjectFromDraft() {
    const d = state.newProjectDraft || {};
    let name = safeText(d.name).trim();
    if (!name) {
        // Fallback: read live DOM value (draft listeners may not have run yet).
        const nameEl = document.getElementById('np-name');
        const liveName = safeText(nameEl?.value).trim();
        if (liveName) {
            name = liveName;
            setNewProjectDraft({ name: liveName });
        }
    }
    if (!name) throw new Error('Project name is required');

    const project = {
        name,
        type: safeText(d.type).trim() || 'Other',
        status: safeText(d.status).trim() || 'Active',
        dueDate: safeText(d.dueDate).trim(),
        projectValue: safeText(d.projectValue).trim(),
        repoUrl: safeText(d.repoUrl).trim(),
        docsUrl: safeText(d.docsUrl).trim(),
        stripeInvoiceUrl: safeText(d.stripeInvoiceUrl).trim(),
        workspacePath: safeText(d.workspacePath).trim(),
        priority: safeText(d.priority).trim() || 'Medium',
        importance: safeText(d.importance).trim() || 'Medium',
        risk: safeText(d.risk).trim() || 'None',
        agentBrief: safeText(d.agentBrief).trim(),
        clientName: safeText(d.clientName).trim(),
        clientPhone: safeText(d.clientPhone).trim(),
    };

    const store = await apiJson('/api/projects', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, project })
    });

    applyStore(store);

    const created = (Array.isArray(store.projects) ? store.projects : []).find((p) => safeText(p?.name).trim() === name) || (store.projects && store.projects[0]);
    const projectId = safeText(created?.id);

    const brief = safeText(d.agentBrief).trim();
    if (projectId && brief) {
        await saveScratchpad(projectId, brief);
    }

    state.showNewProjectIntake = false;
    resetNewProjectDraft();
    renderNav();
    if (projectId) {
        await openProject(projectId);
        try {
            const nextStore = await apiJson(`/api/projects/${encodeURIComponent(projectId)}/auto-suggest-tasks`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ baseRevision: state.revision })
            });
            applyStore(nextStore);
            await loadChatHistory();
            renderChat();
            renderMain();
        } catch {
            // ignore auto-suggest failures
        }
    } else {
        renderMain();
    }
}

function setMainPortScrolling(enabled) {
    const el = document.getElementById('view-port') || document.getElementById('main-port');
    if (!el) return;
    el.classList.toggle('overflow-y-auto', !!enabled);
    el.classList.toggle('overflow-hidden', !enabled);
}

/* --- Initialization --- */

window.addEventListener("DOMContentLoaded", init);

function applyInitialDeepLinkFromUrl() {
    try {
        const params = new URLSearchParams(window.location.search);
        const view = safeText(params.get('view') || '').trim().toLowerCase();
        const pane = safeText(params.get('pane') || '').trim().toLowerCase();
        if (view === 'settings') {
            state.currentView = 'settings';
            state.settingsPane = pane;
            state.currentProjectId = null;
            state.currentClientName = null;
        }
    } catch {
        // ignore
    }
}

async function init() {
    const initErrors = [];
    const step = async (label, fn) => {
        try {
            await fn();
        } catch (e) {
            initErrors.push({ label, message: safeText(e?.message) || String(e) });
            console.error(`Init step failed: ${label}`, e);
        }
    };

    try {
        console.log("Initializing Marcus...");
        state.lastInteractionAt = Date.now();
        applyTheme(getStoredTheme() || 'dark');
        applyLayout(getStoredLayout() || 'standard');
        showLoading();

        // Auth status is a public endpoint; check early so we can avoid a broken/empty UI.
        await step('refreshAuthStatus', async () => { await refreshAuthStatus(); });

        // Businesses (workspace selection)
        await step('fetchBusinesses', async () => {
            await fetchBusinesses();
        });
        if (!safeText(state.activeBusinessKey).trim()) {
            const stored = normalizeBusinessKey(getStoredBusinessKey());
            state.activeBusinessKey = stored || 'personal';
        }
        
        // Initial Fetch (best-effort; these functions already swallow most errors)
        await step('fetchState', async () => { await fetchState(); });
        await step('fetchSettings', async () => { await fetchSettings(); });

        // Optional deep link routing (used for Settings panes opened in a new tab).
        await step('applyInitialDeepLinkFromUrl', async () => { applyInitialDeepLinkFromUrl(); });
        
        // Setup UI
        await step('setupEventListeners', async () => { setupEventListeners(); });
        await step('initializeMarcusWidget', async () => { initializeMarcusWidget(); });
        await step('initMarcusSync', async () => { initMarcusSync(); });
        await step('loadChatHistory', async () => { await loadChatHistory(); });
        await step('renderNav', async () => { renderNav(); });
        await step('renderMain', async () => { renderMain(); });
        await step('renderChat', async () => { renderChat(); });

        await step('ensureAiTeamMember', async () => { ensureAiTeamMember(); });
        
        // Polling (Auto-Refresh)
        await step('startPolling', async () => {
            if (!(state.auth.required && !state.auth.authenticated)) {
                startPolling();
            }
        });

        await step('startProactiveFocusNudges', async () => { startProactiveFocusNudges(); });
        
        console.log("System Online");
    } catch (e) {
        console.error("Critical Failure:", e);
        const details = initErrors.length
            ? `${safeText(e?.message || 'Unknown error')}\n\nStartup warnings:\n${initErrors.map((x) => `- ${x.label}: ${x.message}`).join('\n')}`
            : safeText(e?.message || 'Unknown error');
        showError(details);
    }
}

function isDoneTask(task) {
    const s = String(task?.status || "").trim().toLowerCase();
    return s === "done" || s === "completed";
}

function normalizeRole(role) {
    const r = String(role || "").toLowerCase();
    if (r === "assistant") return "ai";
    if (r === "system") return "ai";
    if (r === "ai") return "ai";
    return "user";
}

function applyStore(store) {
    if (!store || typeof store !== 'object') return;
    if (Number.isFinite(Number(store.revision))) state.revision = Number(store.revision);
    if (typeof store.updatedAt === 'string') state.updatedAt = store.updatedAt;
    if (Array.isArray(store.projects)) state.projects = store.projects;
    state.clients = Array.isArray(store.clients) ? store.clients : [];
    if (Array.isArray(store.tasks)) state.tasks = store.tasks;
    if (Array.isArray(store.inboxItems)) state.inboxItems = store.inboxItems;
    if (Array.isArray(store.team)) state.team = store.team;
    if (store.projectScratchpads && typeof store.projectScratchpads === 'object') state.projectScratchpads = store.projectScratchpads;
    if (store.projectNoteEntries && typeof store.projectNoteEntries === 'object') state.projectNoteEntries = store.projectNoteEntries;
    if (store.projectCommunications && typeof store.projectCommunications === 'object') state.projectCommunications = store.projectCommunications;

    ensureAiTeamMember();
}

function safeText(v) {
    return (typeof v === 'string') ? v : '';
}

function previewText(text, maxLen = 160) {
    const s = safeText(text).replace(/\s+/g, ' ').trim();
    if (!s) return '';
    if (s.length <= maxLen) return s;
    return `${s.slice(0, Math.max(0, maxLen - 1)).trimEnd()}…`;
}

async function promptDatePicker({ title, label, defaultValue }) {
    return new Promise((resolve) => {
        const existing = document.getElementById('ops-date-picker');
        if (existing) existing.remove();

        const overlay = document.createElement('div');
        overlay.id = 'ops-date-picker';
        overlay.className = 'fixed inset-0 z-[9999] bg-black/60 flex items-center justify-center p-4';
        overlay.innerHTML = `
            <div class="w-full max-w-sm bg-zinc-950/80 border border-zinc-800 rounded-lg p-4 shadow-xl">
                <div class="text-xs font-mono uppercase tracking-widest text-zinc-400">${escapeHtml(safeText(title) || 'Select Date')}</div>
                <div class="mt-3 text-xs text-zinc-500">${escapeHtml(safeText(label) || 'Due date')}</div>
                <input id="ops-date-input" type="date" class="mt-2 w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200 transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40" value="${escapeHtml(safeText(defaultValue))}">
                <div class="mt-4 flex justify-end gap-2">
                    <button id="ops-date-cancel" class="px-3 py-2 rounded border border-zinc-800 text-xs font-mono text-zinc-300 hover:bg-zinc-900/40 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Cancel</button>
                    <button id="ops-date-ok" class="px-3 py-2 rounded bg-blue-600/20 border border-blue-600/40 text-xs font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">OK</button>
                </div>
            </div>
        `;
        document.body.appendChild(overlay);

        const input = overlay.querySelector('#ops-date-input');
        const ok = overlay.querySelector('#ops-date-ok');
        const cancel = overlay.querySelector('#ops-date-cancel');

        const cleanup = (value) => {
            overlay.remove();
            resolve(value);
        };

        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) cleanup(null);
        });

        cancel.addEventListener('click', () => cleanup(null));
        ok.addEventListener('click', () => cleanup(safeText(input?.value).trim()));

        window.addEventListener(
            'keydown',
            (e) => {
                if (e.key === 'Escape') cleanup(null);
            },
            { once: true },
        );

        try {
            input?.focus();
            input?.showPicker?.();
        } catch {
            // ignore
        }
    });
}

function ymdToday() {
    try {
        const d = new Date();
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        return `${y}-${m}-${day}`;
    } catch {
        return '';
    }
}

async function apiFetch(url, options) {
    const opts = (options && typeof options === 'object') ? { ...options } : {};
    const headers = new Headers(opts.headers || {});
    const token = getStoredAdminToken();
    if (token) headers.set('Authorization', `Bearer ${token}`);

    const businessKey = normalizeBusinessKey(getStoredBusinessKey() || state.activeBusinessKey || '');
    if (businessKey) headers.set('X-Business-Key', businessKey);

    opts.headers = headers;

    let res = await fetch(url, opts);

    // If hosted with ADMIN_TOKEN enabled:
    // - if no token is stored, prompt once
    // - if a token is stored but rejected (401/403), prompt again (token likely rotated)
    if (res.status === 401 || res.status === 403) {
        if (token) setStoredAdminToken('');
        const entered = safeText(window.prompt('This server is protected. Paste ADMIN_TOKEN to continue:') || '').trim();
        if (entered) {
            try {
                await fetch('/api/auth/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ token: entered, remember: true })
                });
            } catch {
                // ignore; keep local token fallback below
            }
            setStoredAdminToken(entered);
            const retryHeaders = new Headers(opts.headers || {});
            retryHeaders.set('Authorization', `Bearer ${entered}`);
            res = await fetch(url, { ...opts, headers: retryHeaders });
        }
    }

    return res;
}

async function apiJson(url, options) {
    const res = await apiFetch(url, options);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data?.error || `Request failed (${res.status})`);
    return data;
}

const DEFAULT_OPENAI_MODELS = [
    'gpt-5',
    'gpt-5-mini',
    'gpt-5-nano',
    'gpt-4.1',
    'gpt-4.1-mini',
    'gpt-4o',
    'gpt-4o-mini',
];

function normalizeModelList(items) {
    const rows = Array.isArray(items) ? items : [];
    return Array.from(new Set(rows.map((x) => safeText(x).trim()).filter(Boolean)));
}

function buildOpenAiModelOptions(extra = []) {
    const catalog = state.openAiModelsCatalog && typeof state.openAiModelsCatalog === 'object'
        ? state.openAiModelsCatalog
        : { items: [] };
    const combined = normalizeModelList([
        ...(Array.isArray(catalog.items) ? catalog.items : []),
        ...DEFAULT_OPENAI_MODELS,
        ...(Array.isArray(extra) ? extra : []),
    ]);
    const order = new Map(DEFAULT_OPENAI_MODELS.map((id, idx) => [id, idx]));
    combined.sort((a, b) => {
        const ai = order.has(a) ? Number(order.get(a)) : Number.MAX_SAFE_INTEGER;
        const bi = order.has(b) ? Number(order.get(b)) : Number.MAX_SAFE_INTEGER;
        if (ai !== bi) return ai - bi;
        return a.localeCompare(b);
    });
    return combined;
}

async function fetchOpenAiModelsCatalog({ force = false } = {}) {
    const current = state.openAiModelsCatalog && typeof state.openAiModelsCatalog === 'object'
        ? state.openAiModelsCatalog
        : { items: [], loading: false, error: '', fetchedAt: 0, source: 'fallback' };
    if (current.loading) return;

    const now = Date.now();
    const fresh = Number(current.fetchedAt) > 0 && (now - Number(current.fetchedAt)) < (5 * 60 * 1000);
    if (!force && fresh && Array.isArray(current.items) && current.items.length) return;

    state.openAiModelsCatalog = { ...current, loading: true, error: '' };
    if (state.currentView === 'settings') rerenderMainPreservingUi();

    try {
        const query = force ? '?refresh=1' : '';
        const data = await apiJson(`/api/integrations/openai/models${query}`);
        const items = normalizeModelList(data?.models);
        state.openAiModelsCatalog = {
            items,
            loading: false,
            error: '',
            fetchedAt: Number(data?.fetchedAt) || Date.now(),
            source: safeText(data?.source).trim() || 'live',
        };
    } catch (e) {
        state.openAiModelsCatalog = {
            items: normalizeModelList(DEFAULT_OPENAI_MODELS),
            loading: false,
            error: safeText(e?.message).trim() || 'Model discovery unavailable',
            fetchedAt: Date.now(),
            source: 'fallback',
        };
    }

    if (state.currentView === 'settings') rerenderMainPreservingUi();
}

function isRevisionMismatchError(err) {
    const msg = String(err?.message || '').toLowerCase();
    return msg.includes('revision mismatch') || msg.includes('reload and try again');
}

async function withRevisionRetry(fn) {
    try {
        return await fn();
    } catch (e) {
        if (!isRevisionMismatchError(e)) throw e;
        await fetchState();
        return await fn();
    }
}

async function saveProjectPatch(projectId, patch) {
    const cleanPatch = (patch && typeof patch === 'object') ? patch : {};
    const store = await apiJson(`/api/projects/${encodeURIComponent(projectId)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, patch: cleanPatch })
    });
    applyStore(store);
}

async function setProjectArchived(projectId, archived) {
    const nextStatus = archived ? 'Archived' : 'Active';
    await withRevisionRetry(() => saveProjectPatch(projectId, { status: nextStatus }));
}

async function deleteProjectsByIdList(projectIds) {
    const ids = Array.isArray(projectIds) ? projectIds.map((v) => safeText(v).trim()).filter(Boolean) : [];
    if (!ids.length) return;
    const data = await withRevisionRetry(() => apiJson('/api/projects/bulk-delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, projectIds: ids })
    }));
    const nextStore = data?.store && typeof data.store === 'object' ? data.store : data;
    applyStore(nextStore);
    if (ids.includes(String(state.currentProjectId || ''))) {
        state.currentProjectId = null;
        state.currentView = 'dashboard';
    }
}

async function bulkUpdateProjectsByIdList(projectIds, patch) {
    const ids = Array.isArray(projectIds) ? projectIds.map((v) => safeText(v).trim()).filter(Boolean) : [];
    const cleanPatch = (patch && typeof patch === 'object') ? patch : {};
    if (!ids.length) return;
    const store = await withRevisionRetry(() => apiJson('/api/projects/bulk-update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, projectIds: ids, patch: cleanPatch })
    }));
    applyStore(store);
}

async function saveProjectLinks(projectId, { workspacePath, airtableUrl }) {
    await saveProjectPatch(projectId, {
        workspacePath: safeText(workspacePath).trim(),
        airtableUrl: safeText(airtableUrl).trim(),
    });
}

async function launchVsCodeFolder(path) {
    const p = safeText(path).trim();
    if (!p) throw new Error('Workspace path is empty. Add it first.');
    await apiJson('/api/launch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: p })
    });
}

async function pickFolderPath() {
    const data = await apiJson('/api/pick-folder', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
    });
    return safeText(data?.path).trim();
}

async function saveScratchpad(projectId, text) {
    const cleanText = safeText(text);
    const store = await withRevisionRetry(() => apiJson(`/api/projects/${encodeURIComponent(projectId)}/scratchpad`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, text: cleanText })
    }));
    applyStore(store);
}

async function addProjectNote(projectId, entry) {
    const safeEntry = (entry && typeof entry === 'object') ? { ...entry } : {};
    const store = await withRevisionRetry(() => apiJson(`/api/projects/${encodeURIComponent(projectId)}/notes`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, entry: safeEntry })
    }));
    applyStore(store);
}

async function addProjectCommunication(projectId, communication) {
    const safeCommunication = (communication && typeof communication === 'object') ? { ...communication } : {};
    const store = await withRevisionRetry(() => apiJson(`/api/projects/${encodeURIComponent(projectId)}/communications`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, communication: safeCommunication })
    }));
    // This endpoint returns {communications:[...]}, not the full store.
    // Refresh store after creating an entry.
    await fetchState();
    return store;
}

function setupEventListeners() {
    let gotoPrimed = false;
    let gotoTimer = 0;

    const primeGoto = () => {
        gotoPrimed = true;
        if (gotoTimer) clearTimeout(gotoTimer);
        gotoTimer = setTimeout(() => { gotoPrimed = false; }, 1100);
    };

    document.addEventListener('keydown', async (e) => {
        markUiInteraction();
        const target = e.target;
        const typing = isEditableTarget(target);

        if (!typing && e.key === '/') {
            e.preventDefault();
            openCommandPalette();
            return;
        }

        if (state.commandPaletteOpen && e.key === 'Escape') {
            e.preventDefault();
            closeCommandPalette();
            return;
        }

        if (typing || state.commandPaletteOpen) return;

        const k = String(e.key || '').toLowerCase();
        if (k === 'g') {
            primeGoto();
            return;
        }
        if (gotoPrimed && k === 'd') {
            gotoPrimed = false;
            e.preventDefault();
            await openDashboard();
            return;
        }
        if (gotoPrimed && k === 'i') {
            gotoPrimed = false;
            e.preventDefault();
            await openInbox();
            return;
        }
        if (k === 'n') {
            e.preventDefault();
            if (state.currentView === 'project') {
                const p = (Array.isArray(state.projects) ? state.projects : []).find((x) => safeText(x?.id) === safeText(state.currentProjectId));
                if (p) {
                    await promptNewTask(p);
                    return;
                }
            }
            await createNewProjectPrompt();
        }
    });

    // Consider pointer + scroll as interaction to avoid applying background updates mid-gesture.
    document.addEventListener('pointerdown', () => markUiInteraction(), { capture: true, passive: true });
    document.addEventListener('wheel', () => markUiInteraction(), { capture: true, passive: true });

    // Navigation
    const status = document.getElementById("server-status");
    if(status) status.addEventListener("click", () => alert("Server Online"));
    
    // Chat Toggle
    const toggle = document.getElementById("toggle-chat");
    if(toggle) toggle.addEventListener("click", toggleChat);

    // Theme Toggle
    const themeToggle = document.getElementById('toggle-theme');
    if (themeToggle) themeToggle.addEventListener('click', toggleTheme);

    const layoutToggle = document.getElementById('toggle-layout');
    if (layoutToggle) {
        layoutToggle.addEventListener('click', () => {
            const current = getStoredLayout();
            const next = current === 'standard' ? 'landscape' : 'standard';
            setStoredLayout(next);
            applyLayout(next);
            renderMain();
        });
    }

    
    // Chat Input
    const input = document.getElementById("cmd-input");
    const send = document.getElementById("cmd-send");
    const modelSelect = document.getElementById('marcus-model-select');
    const threadSelect = document.getElementById('marcus-thread-select');

    // Submit behavior (robust across browsers):
    // - Enter submits, Shift+Enter inserts newline
    // - Click send submits
    const onChatKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleChatSubmit();
        }
    };

    if (input) {
        input.addEventListener('keydown', onChatKeyDown);
    }

    if (send) {
        send.addEventListener('click', (e) => {
            e.preventDefault();
            handleChatSubmit();
        });
    }

    // Fallback delegation in case the drawer is re-created.
    document.addEventListener('keydown', (e) => {
        const t = e.target;
        if (t && t.id === 'cmd-input') onChatKeyDown(e);
    });
    document.addEventListener('click', (e) => {
        const btn = e.target?.closest?.('#cmd-send');
        if (!btn) return;
        e.preventDefault();
        handleChatSubmit();
    });

    document.addEventListener('click', (e) => {
        const btn = e.target?.closest?.('#cmd-mic');
        if (!btn) return;
        e.preventDefault();
        const next = !state.marcusVoiceIn;
        state.marcusVoiceIn = next;
        setStoredMarcusVoiceIn(next);
        if (next) startMarcusListening();
        else stopMarcusListening();
        syncMarcusVoiceUi();
    });

    document.addEventListener('click', (e) => {
        const btn = e.target?.closest?.('#cmd-speak');
        if (!btn) return;
        e.preventDefault();
        const next = !state.marcusVoiceOut;
        state.marcusVoiceOut = next;
        setStoredMarcusVoiceOut(next);
        if (!next) {
            try { window.speechSynthesis?.cancel?.(); } catch {}
        }
        syncMarcusVoiceUi();
    });

    if (modelSelect) {
        modelSelect.addEventListener('change', async () => {
            const model = safeText(modelSelect.value).trim();
            if (!model) return;
            try {
                await saveSettingsPatch({ openaiModel: model });
                syncMarcusModelUi();
            } catch (e) {
                alert(e?.message || 'Failed to save model');
            }
        });
    }

    if (threadSelect) {
        // Initialize from storage.
        const stored = getStoredMarcusThread();
        threadSelect.value = stored;
        state.chatThreadId = stored;

        threadSelect.addEventListener('change', async () => {
            const next = safeText(threadSelect.value).trim() || 'default';
            // Threads only apply to global chat.
            if (state.currentProjectId) {
                threadSelect.value = 'default';
                state.chatThreadId = 'default';
                setStoredMarcusThread('default');
                alert('Bio thread is only available when no project is selected.');
                await loadChatHistory();
                renderChat();
                return;
            }

            state.chatThreadId = next;
            setStoredMarcusThread(next);
            await loadChatHistory();
            renderChat();
        });
    }

    // Avoid auto-refresh re-rendering while typing.
    document.addEventListener(
        'focusout',
        () => {
            // focusout fires before the next element receives focus.
            setTimeout(() => flushDeferredRerenderIfSafe(), 0);
        },
        true,
    );

    // Send Summary Btn
    const sendSummaryBtn = document.getElementById("send-summary-btn");
    if (sendSummaryBtn) {
        sendSummaryBtn.addEventListener("click", async () => {
            const lastSent = store.settings?.lastAssignSummaryAt || 0;
            const now = Date.now();
            
            // Find newly assigned tasks 
            const newTasks = store.tasks.filter(t => 
                t.owner && 
                t.owner !== 'Unassigned' && 
                t.owner !== 'AI' &&
                t.status !== 'Done' &&
                new Date(t.updatedAt).getTime() > new Date(lastSent).getTime()
            );

            if (newTasks.length === 0) {
                alert('No new task assignments to send!');
                return;
            }

            // Group by owner
            const grouped = {};
            newTasks.forEach(t => {
                if (!grouped[t.owner]) grouped[t.owner] = [];
                grouped[t.owner].push(t);
            });

            let msg = "*Task Assignment Summary* 🚀\n_Here are the pending tasks delegated to the team:_\n\n";
            for (const [owner, tasks] of Object.entries(grouped)) {
                msg += `*${owner}*:\n`;
                tasks.forEach(t => {
                    const projectName = store.projects.find(p => p.id === t.projectId)?.name || 'Direct Task';
                    msg += `• [${projectName}] ${t.title}\n`;
                });
                msg += `\n`;
            }

            const icon = sendSummaryBtn.querySelector('i');
            if (icon) {
                icon.classList.remove('fa-paper-plane');
                icon.classList.add('fa-spinner', 'fa-spin');
            }
            sendSummaryBtn.disabled = true;

            try {
                const res = await apiFetch('/api/integrations/slack/send-summary', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ text: msg, channel: '@christian' })
                });

                if (!res.ok) {
                    const errData = await res.json().catch(()=>({}));
                    throw new Error(errData?.error || 'Failed to send summary via Slack');
                }

                await saveSettingsPatch({ lastAssignSummaryAt: now });
                alert('Summary sent to @christian successfully!');
            } catch (err) {
                alert('Error sending summary: ' + err.message);
            } finally {
                if (icon) {
                    icon.classList.remove('fa-spinner', 'fa-spin');
                    icon.classList.add('fa-paper-plane');
                }
                sendSummaryBtn.disabled = false;
            }
        });
    }

    // Sync Btn
    const sync = document.getElementById("sync-btn");
    if (sync) {
        sync.addEventListener("click", async () => {
            const icon = sync.querySelector('i');
            if (icon) icon.classList.add('fa-spin');
            sync.disabled = true;
            try {
                const statusRes = await apiFetch('/api/integrations/google/status');
                const status = statusRes.ok ? await statusRes.json() : { configured: false, connected: false };

                if (!status.configured) {
                    alert('Google is not configured yet. Open Settings → Google Calendar to add your Client ID.');
                    await openSettings();
                    return;
                }

                if (!status.connected) {
                    await openGoogleAuthWindow();
                    return;
                }

                const syncRes = await apiFetch('/api/integrations/sync', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
                const syncData = await syncRes.json().catch(() => ({}));
                if (!syncRes.ok) throw new Error(syncData?.error || 'Sync failed');

                const g = syncData?.results?.google;
                if (g && g.ok) {
                    const pushed = Number(g.pushed) || 0;
                    const pulled = Number(g.pulledUpdates) || 0;
                    if (pushed || pulled) alert(`Google Calendar synced. Pushed: ${pushed}, Pulled updates: ${pulled}.`);
                }
            } catch (e) {
                alert(e?.message || 'Sync failed.');
            } finally {
                sync.disabled = false;
                if (icon) icon.classList.remove('fa-spin');
                await fetchState();
            }
        });
    }
}

function showLoading() {
    const container = document.getElementById("view-port") || document.getElementById("main-port");
    if(container) container.innerHTML = `<div class="flex h-full items-center justify-center text-blue-500">
        <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mr-3"></div>
        <span class="font-mono text-xs tracking-widest">CONNECTING TO M.A.R.C.U.S....</span>
    </div>`;
}

function showError(msg) {
    const container = document.getElementById("view-port") || document.getElementById("main-port");
    if(container) container.innerHTML = `<div class="p-8 text-red-500 font-mono">
        <h1 class="text-xl font-bold mb-2">CRITICAL ERROR</h1>
        <pre>${msg}</pre>
    </div>`;
}

/* --- API --- */

async function refreshGodView() {
    if (state.godViewLoading) return;
    state.godViewLoading = true;
    try {
        const res = await apiFetch('/api/me/dashboard');
        if (res.ok) {
            state.godViewData = await res.json();
            
            const connected = !!state.settings?.googleConnected;
            if (connected) {
                await refreshDashboardCalls({ force: false }).catch(() => {});
            }
            
            if (state.currentView === 'godview') renderMain();
        }
    } catch(e) {
        console.error("Failed to refresh god view", e);
    } finally {
        state.godViewLoading = false;
    }
}

async function fetchState({ background = false } = {}) {
    try {
        const prevRevision = Number(state.revision) || 0;
        const prevUpdatedAt = safeText(state.updatedAt).trim();

        const res = await apiFetch("/api/tasks");
        if (!res.ok) {
            if (res.status === 401 || res.status === 403) {
                state.auth.lastError = 'Unauthorized (missing/invalid ADMIN_TOKEN).';
                await refreshAuthStatus();
                renderMain();
                return;
            }
            throw new Error(`Failed to load store (${res.status})`);
        }
        const store = await res.json();

        const nextRevision = Number(store?.revision);
        const nextUpdatedAt = safeText(store?.updatedAt).trim();
        const changed =
            (Number.isFinite(nextRevision) && nextRevision !== prevRevision) ||
            (!!nextUpdatedAt && nextUpdatedAt !== prevUpdatedAt);

        applyStore(store);
        
        state.lastSync = new Date();

        // If nothing changed, avoid re-rendering entirely (prevents the "refresh" feel).
        if (!changed) {
            if (state.backgroundDirty) setPageMeta('Updates ready');
            return;
        }

        // Background polling: sync store silently and only apply to UI when user is idle.
        // This keeps data fresh without constantly re-rendering the view.
        if (background) {
            state.backgroundDirty = true;
            setPageMeta('Updates ready');

            const paused = Date.now() < Number(state.rerenderPauseUntil || 0);
            if (!paused && !isUserEditingNow() && isUserIdle(2500)) {
                state.backgroundDirty = false;
                setPageMeta('');
                rerenderMainPreservingUi();
            }
            return;
        }

        // Foreground fetch: apply immediately.
        state.backgroundDirty = false;
        setPageMeta('');

        if (Date.now() < Number(state.rerenderPauseUntil || 0)) {
            state.deferRerender = true;
            return;
        }

        if (isUserEditingNow()) {
            state.deferRerender = true;
            return;
        }

        state.deferRerender = false;
        rerenderMainPreservingUi();
    } catch (e) {
        console.error("Fetch State Error:", e);
    }
}

async function fetchSettings() {
    try {
        const res = await apiFetch("/api/settings");
        if (res.status === 401 || res.status === 403) {
            state.auth.lastError = 'Unauthorized (missing/invalid ADMIN_TOKEN).';
            await refreshAuthStatus();
            updateSystemStatus(false);
            return;
        }
        if(res.ok) {
            state.settings = await res.json();
            state.aiAvailable = !!state.settings.aiEnabled;
            updateSystemStatus(true);

            applyUiPreferencesFromSettings(state.settings);

            // UI prefs (optional, stored in settings)
            if (Number.isFinite(Number(state.settings.autoRefreshSeconds))) {
                state.uiPrefs.autoRefreshSeconds = Math.max(10, Number(state.settings.autoRefreshSeconds));
            }
            if (typeof state.settings.weekStartsOnMonday === 'boolean') {
                state.uiPrefs.weekStartsOnMonday = state.settings.weekStartsOnMonday;
            }
            if (typeof state.settings.defaultShowCompleted === 'boolean') {
                state.uiPrefs.defaultShowCompleted = state.settings.defaultShowCompleted;
                state.showCompleted = state.uiPrefs.defaultShowCompleted;
            }

            syncMarcusModelUi();
        } else {
            updateSystemStatus(false);
        }
    } catch(e) {
        console.warn("Settings unreachable", e);
        updateSystemStatus(false);
    }
}

function updateSystemStatus(online) {
    const el = document.getElementById("server-status");
    if(el) {
        el.className = `w-2 h-2 rounded-full mx-auto ${online ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]' : 'bg-red-500'}`;
        el.title = online ? "System Online" : "System Offline";
    }
}

/* --- Rendering: Navigation --- */

function renderNav() {
    const nav = document.getElementById("primary-nav");
    if (!nav) return;

    // Preserve Create Button if it exists? No, rebuild.
    nav.innerHTML = "";

    nav.appendChild(createNavIcon("fa-satellite-dish", "God View", () => openGodView(), state.currentView === "godview"));

    const bizSepTop = document.createElement('div');
    bizSepTop.className = 'h-px w-8 bg-zinc-800 mx-auto my-2';
    nav.appendChild(bizSepTop);

    // Businesses (workspaces)
    const businesses = Array.isArray(state.businesses) && state.businesses.length ? state.businesses : [{ key: 'personal', name: 'Personal' }];
    for (const b of businesses) {
        const k = normalizeBusinessKey(b.key);
        const name = safeText(b.name).trim() || k;
        if (!k) continue;
        const active = normalizeBusinessKey(state.activeBusinessKey) === k;
        nav.appendChild(createNavIcon('', `Business: ${name}`, () => setActiveBusinessKey(k), active, businessAbbrev(name, k)));
    }

    const bizSep = document.createElement('div');
    bizSep.className = 'h-px w-8 bg-zinc-800 mx-auto my-2';
    nav.appendChild(bizSep);
    
    nav.appendChild(createNavIcon("fa-grip", "Dashboard", () => openDashboard(), state.currentView === "dashboard"));
    nav.appendChild(createNavIcon("fa-inbox", "Inbox", () => openInbox(), state.currentView === "inbox"));
    nav.appendChild(createNavIcon("fa-rotate", "Revisions", () => openRevisions(), state.currentView === "revisions"));
    nav.appendChild(createNavIcon("fa-address-book", "Clients", () => openClients(), state.currentView === "clients" || state.currentView === "client"));
    nav.appendChild(createNavIcon("fa-folder", "Projects", () => openProjects(), state.currentView === "projects" || state.currentView === "project"));
    nav.appendChild(createNavIcon("fa-calendar-days", "Calendar", () => openCalendar(), state.currentView === "calendar"));
    nav.appendChild(createNavIcon("fa-user-group", "Team", () => openTeam(), state.currentView === "team"));

    const sep = document.createElement("div");
    sep.className = "h-px w-8 bg-zinc-800 mx-auto my-2";
    nav.appendChild(sep);

    nav.appendChild(createNavIcon("fa-gear", "Settings", () => openSettings(), state.currentView === "settings"));
}

async function openGodView() {
    state.currentView = 'godview';
    state.currentProjectId = null;
    state.currentClientName = null;
    renderNav();
    renderMain();
    renderChat();
}

async function openDashboard() {
    state.currentView = 'dashboard';
    state.currentProjectId = null;
    state.currentClientName = null;
    // Presence dots on the dashboard rely on settings + presence being loaded.
    // Fetch is best-effort and should not block navigation.
    await fetchSettings().catch(() => {});
    const presencePromise = refreshSlackTeamPresence({ force: true }).catch(() => {});
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();

    presencePromise.then(() => {
        if (state.currentView === 'dashboard') rerenderMainPreservingUi();
    });
}

async function openInbox() {
    state.currentView = 'inbox';
    state.currentProjectId = null;
    state.currentClientName = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

async function openRevisions() {
    state.currentView = 'revisions';
    state.currentProjectId = null;
    state.currentClientName = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

async function openClients() {
    state.currentView = 'clients';
    state.currentProjectId = null;
    state.currentClientName = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

async function openClient(clientName) {
    state.currentView = 'client';
    state.currentProjectId = null;
    state.currentClientName = safeText(clientName).trim() || null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

async function openProjects() {
    state.currentView = 'projects';
    state.currentProjectId = null;
    state.currentClientName = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

async function openProject(projectId) {
    state.currentView = 'project';
    state.currentProjectId = projectId;
    state.currentClientName = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

async function openSettings() {
    state.currentView = 'settings';
    state.currentProjectId = null;
    state.currentClientName = null;
    state.settingsPane = '';
    await fetchSettings();
    await refreshSlackTeamPresence({ force: true });
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

async function openCalendar() {
    state.currentView = 'calendar';
    state.currentProjectId = null;
    state.currentClientName = null;
    await fetchSettings();
    await refreshDashboardCalls({ force: true }).catch(() => {});
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

async function openTeam() {
    state.currentView = 'team';
    state.currentProjectId = null;
    state.currentClientName = null;
    await fetchSettings();
    await refreshSlackTeamPresence({ force: true }).catch(() => {});
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMarcusContext();
}

function createNavIcon(iconClass, tooltip, onClick, active, textLabel) {
    const btn = document.createElement("button");
    btn.className = `w-10 h-10 rounded-xl mb-2 flex items-center justify-center transition-all relative group mx-auto ${active ? 'bg-blue-600 text-white shadow-lg shadow-blue-900/50' : 'text-zinc-400 hover:bg-zinc-800 hover:text-white'}`;
    btn.onclick = onClick;
    
    if (active) {
        // Active indicator dot
        const dot = document.createElement("div");
        dot.className = "absolute -left-3 top-1/2 -translate-y-1/2 w-1 h-3 bg-white rounded-r-full";
        btn.appendChild(dot);
    }

    if (iconClass) {
        btn.innerHTML += `<i class="fa-solid ${iconClass}"></i>`;
    } else { // Text Label Fallback
        btn.innerHTML += `<span class="text-xs font-bold leading-none">${textLabel}</span>`;
    }
    
    // Simple browser tooltip
    btn.title = tooltip;
    
    return btn;
}

/* --- Rendering: Main Views --- */

function renderMain() {
    const ports = ensurePersistentMarcusLayout();
    if (!ports) return;

    const container = ports.viewPort;
    if (!container) return;

    const side = ports.marcusPort;

    preserveMarcusDrawerDuringRerender();

    container.innerHTML = "";
    if (side) side.innerHTML = '';

    // If the server requires ADMIN_TOKEN and we're not authenticated,
    // show a clear gate instead of rendering empty/disabled views.
    if (state.auth.required && !state.auth.authenticated) {
        setMainPortScrolling(true);
        const wrap = document.createElement('div');
        wrap.className = 'p-8 max-w-3xl';
        wrap.innerHTML = `
            <div class="border border-ops-border rounded-xl bg-ops-surface/40 p-6">
                <div class="text-white font-semibold">Access Required</div>
                <div class="text-xs text-ops-light mt-1">This server is protected by an admin token. Paste it once in Settings → Access to unlock projects, inbox, and M.A.R.C.U.S..</div>
                <div class="mt-4 flex flex-wrap gap-2">
                    <button id="btn-open-access" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Open Settings</button>
                    <button id="btn-recheck-auth" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Re-check</button>
                </div>
                ${state.auth.lastError ? `<div class="mt-3 text-[11px] font-mono text-amber-300">${escapeHtml(state.auth.lastError)}</div>` : ''}
            </div>
        `;
        container.appendChild(wrap);

        wrap.querySelector('#btn-open-access')?.addEventListener('click', async () => {
            state.currentView = 'settings';
            renderNav();
            renderMain();
            // focus token input if present
            setTimeout(() => {
                try { document.getElementById('set-admin-token')?.focus?.(); } catch { /* ignore */ }
            }, 0);
        });
        wrap.querySelector('#btn-recheck-auth')?.addEventListener('click', async () => {
            await refreshAuthStatus();
            renderMain();
        });
        return;
    }

    // Reduce full-page scrolling: scroll inside panes for data-heavy views.
    if (state.currentView === 'project' || state.currentView === 'projects' || state.currentView === 'revisions' || state.currentView === 'dashboard' || state.currentView === 'godview' || state.currentView === 'inbox' || state.currentView === 'calendar' || state.currentView === 'team') {
        setMainPortScrolling(false);
    } else {
        setMainPortScrolling(true);
    }

    if (state.currentView === "godview") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderGodView(container);
    } else if (state.currentView === "dashboard") {
        // Restore the classic layout: Dashboard in the main pane, M.A.R.C.U.S. on the right.
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        try {
            renderDashboard(container, null);
        } catch (e) {
            console.error('Dashboard render failed', e);
            const msg = safeText(e?.message || 'Unknown dashboard error').trim();
            const stack = safeText(e?.stack).trim();
            const detail = `Dashboard render failed: ${msg}${stack ? `\n\n${stack}` : ''}`;
            showError(detail.slice(0, 4000));
        }
    } else if (state.currentView === "inbox") {
        // All other views keep M.A.R.C.U.S. docked on the right.
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        try {
            renderInbox(container);
        } catch (e) {
            console.error('Inbox render failed', e);
            const msg = safeText(e?.message || 'Unknown inbox error').trim();
            const stack = safeText(e?.stack).trim();
            const detail = `Inbox render failed: ${msg}${stack ? `\n\n${stack}` : ''}`;
            showError(detail.slice(0, 4000));
        }
    } else if (state.currentView === "revisions") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderRevisions(container);
    } else if (state.currentView === "clients") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderClients(container);
    } else if (state.currentView === "client") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderClientView(container);
    } else if (state.currentView === "projects") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderProjects(container);
    } else if (state.currentView === "project") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderProjectView(container);
    } else if (state.currentView === "calendar") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderCalendar(container);
    } else if (state.currentView === "team") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderTeam(container);
    } else if (state.currentView === "settings") {
        dockMarcusToPersistentSlot();
        container.className = 'min-h-0 overflow-y-auto';
        renderSettings(container);
    }

    renderCommandPaletteOverlay();
}

function normalizeClientLabel(name) {
    const n = safeText(name).trim();
    return n || 'Unnamed Client';
}

function buildClientsIndexFromProjects(projects) {
    const list = Array.isArray(projects) ? projects : [];
    const byKey = new Map();

    for (const p of list) {
        const name = normalizeClientLabel(p?.clientName);
        const key = name.toLowerCase();
        const phone = safeText(p?.clientPhone).trim();
        const existing = byKey.get(key) || { name, phone: '', projects: [] };
        if (!existing.phone && phone) existing.phone = phone;
        existing.projects.push(p);
        byKey.set(key, existing);
    }

    const out = Array.from(byKey.values());
    out.sort((a, b) => a.name.localeCompare(b.name));
    return out;
}

function buildClientsIndexFromStore({ projects, clients }) {
    const list = Array.isArray(projects) ? projects : [];
    const contacts = Array.isArray(clients) ? clients : [];
    const byKey = new Map();

    for (const c of contacts) {
        const name = normalizeClientLabel(c?.name);
        const key = name.toLowerCase();
        const phone = safeText(c?.phone).trim();
        byKey.set(key, { name, phone, projects: [] });
    }

    const fromProjects = buildClientsIndexFromProjects(list);
    for (const c of fromProjects) {
        const name = normalizeClientLabel(c?.name);
        const key = name.toLowerCase();
        const existing = byKey.get(key) || { name, phone: '', projects: [] };
        if (!existing.phone) existing.phone = safeText(c?.phone).trim();
        existing.projects = [...(Array.isArray(existing.projects) ? existing.projects : []), ...(Array.isArray(c?.projects) ? c.projects : [])];
        byKey.set(key, existing);
    }

    const out = Array.from(byKey.values());
    out.sort((a, b) => safeText(a?.name).localeCompare(safeText(b?.name)));
    return out;
}

function renderClients(container) {
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Clients';

    const projects = Array.isArray(state.projects) ? state.projects : [];
    const clients = buildClientsIndexFromStore({ projects, clients: state.clients });

    const wrap = document.createElement('div');
    wrap.className = 'h-full min-h-0 overflow-y-auto p-6';

    const card = document.createElement('div');
    card.className = 'border border-ops-border rounded-xl bg-ops-surface/30 p-4';
    card.innerHTML = `
        <div class="flex items-center justify-between gap-3">
            <div>
                <div class="text-xs font-semibold text-white">Clients</div>
                <div class="text-[10px] font-mono text-ops-light">Contacts + client names found on projects</div>
            </div>
            <button id="btn-client-new" class="px-3 py-2 rounded border border-ops-border text-[11px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">New Project</button>
        </div>
        <div class="mt-4">
            ${clients.length ? `<div class="space-y-2">${clients.map((c) => {
                const projCount = Array.isArray(c.projects) ? c.projects.length : 0;
                const activeCount = (Array.isArray(c.projects) ? c.projects : [])
                    .filter((p) => String(p?.status || '').trim().toLowerCase() !== 'archived')
                    .length;
                const phone = safeText(c.phone).trim();
                return `
                    <button data-client-open="${escapeHtml(c.name)}" class="w-full text-left border border-ops-border rounded-lg bg-ops-bg/40 px-3 py-2 hover:bg-ops-surface/60 transition-colors">
                        <div class="flex items-center justify-between gap-3">
                            <div class="min-w-0">
                                <div class="text-sm text-white truncate">${escapeHtml(c.name)}</div>
                                <div class="text-[10px] font-mono text-ops-light truncate">${escapeHtml(phone || '—')}</div>
                            </div>
                            <div class="shrink-0 text-right">
                                <div class="text-[11px] font-mono text-white">${projCount}</div>
                                <div class="text-[9px] font-mono text-ops-light/60">${activeCount} active</div>
                            </div>
                        </div>
                    </button>
                `;
            }).join('')}</div>` : `<div class="text-[11px] text-ops-light/80">No clients yet. Add a project with a client name.</div>`}
        </div>
    `;

    wrap.appendChild(card);
    container.appendChild(wrap);

    card.querySelector('#btn-client-new')?.addEventListener('click', () => createNewProjectPrompt());
    card.querySelectorAll('button[data-client-open]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const name = safeText(btn.getAttribute('data-client-open'));
            await openClient(name);
        });
    });
}

function renderClientView(container) {
    const clientName = normalizeClientLabel(state.currentClientName);
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = clientName;

    const projects = Array.isArray(state.projects) ? state.projects : [];
    const matching = projects
        .filter((p) => !isContactOnlyProject(p))
        .filter((p) => normalizeClientLabel(p?.clientName).toLowerCase() === clientName.toLowerCase())
        .slice();
    matching.sort((a, b) => safeText(a?.name).localeCompare(safeText(b?.name)));
    const phoneFromProjects = safeText((matching.find((p) => safeText(p?.clientPhone).trim()) || {})?.clientPhone).trim();
    const phoneFromContacts = safeText((Array.isArray(state.clients) ? state.clients : []).find((c) => normalizeClientLabel(c?.name).toLowerCase() === clientName.toLowerCase())?.phone).trim();
    const phone = phoneFromProjects || phoneFromContacts;

    const wrap = document.createElement('div');
    wrap.className = 'h-full min-h-0 overflow-y-auto p-6';

    const head = document.createElement('div');
    head.className = 'border border-ops-border rounded-xl bg-ops-surface/30 p-4';
    head.innerHTML = `
        <div class="flex items-start justify-between gap-3">
            <div class="min-w-0">
                <div class="text-xs font-semibold text-white">Client</div>
                <div class="mt-1 text-lg text-white font-semibold truncate">${escapeHtml(clientName)}</div>
                <div class="mt-1 text-[10px] font-mono text-ops-light truncate">${escapeHtml(phone || '—')}</div>
            </div>
            <div class="shrink-0 flex items-center gap-2">
                <button id="btn-client-back" class="px-3 py-2 rounded border border-ops-border text-[11px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Back</button>
                <button id="btn-client-new-project" class="px-3 py-2 rounded bg-blue-600/20 border border-blue-600/40 text-[11px] font-mono text-blue-200 hover:bg-blue-600/30 transition-colors">New Project</button>
            </div>
        </div>
    `;
    wrap.appendChild(head);

    const projCard = document.createElement('div');
    projCard.className = 'mt-4 border border-ops-border rounded-xl bg-ops-surface/30 p-4';
    projCard.innerHTML = `
        <div class="flex items-center justify-between gap-3">
            <div>
                <div class="text-xs font-semibold text-white">Projects</div>
                <div class="text-[10px] font-mono text-ops-light">Tracked under this client</div>
            </div>
            <div class="text-[11px] font-mono text-white">${matching.length}</div>
        </div>
        <div class="mt-4">
            ${matching.length ? `<div class="space-y-2">${matching.map((p) => {
                const pid = safeText(p?.id);
                const name = safeText(p?.name) || 'Untitled';
                const status = safeText(p?.status) || 'Active';
                const due = safeText(p?.dueDate);
                return `
                    <button data-client-proj-open="${escapeHtml(pid)}" class="w-full text-left border border-ops-border rounded-lg bg-ops-bg/40 px-3 py-2 hover:bg-ops-surface/60 transition-colors">
                        <div class="flex items-center justify-between gap-3">
                            <div class="min-w-0">
                                <div class="text-sm text-white truncate">${escapeHtml(name)}</div>
                                <div class="text-[10px] font-mono text-ops-light/70 truncate">${escapeHtml(status)}${due ? ` • ${escapeHtml(due)}` : ''}</div>
                            </div>
                            <i class="fa-solid fa-chevron-right text-ops-light/40"></i>
                        </div>
                    </button>
                `;
            }).join('')}</div>` : `<div class="text-[11px] text-ops-light/80">No projects yet for this client.</div>`}
        </div>
    `;
    wrap.appendChild(projCard);

    container.appendChild(wrap);

    head.querySelector('#btn-client-back')?.addEventListener('click', () => openClients());
    head.querySelector('#btn-client-new-project')?.addEventListener('click', () => {
        createNewProjectPrompt({ clientName, clientPhone: phone });
    });
    projCard.querySelectorAll('button[data-client-proj-open]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const pid = safeText(btn.getAttribute('data-client-proj-open'));
            if (pid) await openProject(pid);
        });
    });
}

function renderCalendar(container) {
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Calendar';

    const wrap = document.createElement('div');
    wrap.className = 'h-full min-h-0 overflow-y-auto p-6';

    const connected = !!state.settings?.googleConnected;
    const calls = Array.isArray(state.dashboardCalls?.events) ? state.dashboardCalls.events : [];
    const error = safeText(state.dashboardCalls?.error);
    const loading = !!state.dashboardCalls?.loading;
    if (connected) setTimeout(() => refreshDashboardCalls({ force: false }), 0);

    const card = document.createElement('div');
    card.className = 'border border-ops-border rounded-xl bg-ops-surface/30 p-4';

    const rows = !connected
        ? `<div class="text-[11px] text-ops-light/80">Connect Google Calendar in Settings to show events here.</div>
           <div class="mt-3"><button id="btn-calendar-settings" class="px-3 py-2 rounded border border-ops-border text-[11px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Open Settings</button></div>`
        : (error
            ? `<div class="text-[11px] text-amber-300">${escapeHtml(error)}</div>`
            : (loading && !calls.length)
                ? `<div class="text-[11px] text-ops-light/80">Loading events…</div>`
                : (calls.length
                    ? `<div class="space-y-2">${calls.slice(0, 24).map((ev) => {
                        const time = formatTimeFromIso(ev.start);
                        const title = safeText(ev.summary) || 'Untitled';
                        const link = safeText(ev.meetingLink);
                        return `
                            <div class="border border-ops-border rounded-lg bg-ops-bg/40 px-3 py-2">
                                <div class="flex items-center justify-between gap-3">
                                    <div class="min-w-0">
                                        <div class="text-xs text-white truncate">${escapeHtml(title)}</div>
                                        <div class="text-[10px] font-mono text-ops-light truncate">${escapeHtml(time || '')}</div>
                                    </div>
                                    ${link ? `<a class="shrink-0 text-[10px] font-mono text-ops-accent hover:underline" href="${escapeHtml(link)}" target="_blank" rel="noreferrer">Join</a>` : ''}
                                </div>
                            </div>
                        `;
                    }).join('')}</div>`
                    : `<div class="text-[11px] text-ops-light/80">No upcoming events.</div>`));

    card.innerHTML = `
        <div class="flex items-center justify-between gap-3">
            <div>
                <div class="text-xs font-semibold text-white">Upcoming Events</div>
                <div class="text-[10px] font-mono text-ops-light">Google Calendar feed</div>
            </div>
            <button id="btn-calendar-refresh" class="px-3 py-2 rounded border border-ops-border text-[11px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Refresh</button>
        </div>
        <div class="mt-4">${rows}</div>
    `;

    wrap.appendChild(card);
    container.appendChild(wrap);

    const btnRefresh = card.querySelector('#btn-calendar-refresh');
    if (btnRefresh) {
        btnRefresh.onclick = async () => {
            await refreshDashboardCalls({ force: true });
            renderMain();
        };
    }
    const btnSettings = card.querySelector('#btn-calendar-settings');
    if (btnSettings) {
        btnSettings.onclick = async () => {
            await openSettings();
        };
    }
}

function renderTeam(container) {
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Team';

    const wrap = document.createElement('div');
    wrap.className = 'h-full min-h-0 overflow-y-auto p-6';

    const card = document.createElement('div');
    card.className = 'border border-ops-border rounded-xl bg-ops-surface/30 p-4';

    const humans = getHumanTeamMembers();
    const presenceMap = (state.teamPresenceByMemberId && typeof state.teamPresenceByMemberId === 'object') ? state.teamPresenceByMemberId : {};
    const slackInstalled = !!state.settings?.slackInstalled;

    const presenceFor = (m) => {
        const id = safeText(m?.id).trim();
        return id ? (presenceMap[id] || null) : null;
    };

    const rows = humans.length
        ? `<div class="space-y-2">${humans.map((m) => {
            const name = safeText(m?.name) || 'Member';
            const role = safeText(m?.role);
            const avatar = safeText(m?.avatar) || name.slice(0, 1).toUpperCase();
            const p = presenceFor(m);
            const online = p && Object.prototype.hasOwnProperty.call(p, 'online') ? p.online : null;
            const rawPresence = safeText(p?.presence).toLowerCase();
            const statusText = !slackInstalled
                ? 'not connected'
                : (online === true ? 'online' : (online === false ? 'offline' : (rawPresence || 'unknown')));
            const statusClass = !slackInstalled
                ? 'bg-zinc-600'
                : (online === true || rawPresence === 'active' || rawPresence === 'online')
                    ? 'bg-emerald-400'
                    : (rawPresence === 'away')
                        ? 'bg-amber-300'
                        : (online === null ? 'bg-amber-300' : 'bg-zinc-600');
            return `
                <div class="border border-ops-border rounded-lg bg-ops-bg/40 px-3 py-2">
                    <div class="flex items-center justify-between gap-3">
                        <div class="flex items-center gap-3 min-w-0">
                            <div class="w-8 h-8 rounded-lg border border-ops-border bg-ops-surface/60 flex items-center justify-center text-xs font-mono text-white">${escapeHtml(avatar)}</div>
                            <div class="min-w-0">
                                <div class="text-sm text-white truncate">${escapeHtml(name)}</div>
                                <div class="text-[10px] font-mono text-ops-light truncate">${escapeHtml(role || '—')}</div>
                            </div>
                        </div>
                        <div class="shrink-0 flex items-center gap-2">
                            <span class="inline-block w-2 h-2 rounded-full ${statusClass}"></span>
                            <span class="text-[10px] font-mono text-ops-light uppercase tracking-widest">${escapeHtml(statusText)}</span>
                        </div>
                    </div>
                </div>
            `;
        }).join('')}</div>`
        : `<div class="text-[11px] text-ops-light/80">No team members yet.</div>`;

    card.innerHTML = `
        <div class="flex items-center justify-between gap-3">
            <div>
                <div class="text-xs font-semibold text-white">Team</div>
                <div class="text-[10px] font-mono text-ops-light">Slack presence ${slackInstalled ? 'enabled' : 'not connected'}</div>
            </div>
            <div class="flex items-center gap-2">
                <button id="btn-team-refresh" class="px-3 py-2 rounded border border-ops-border text-[11px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Refresh</button>
                <button id="btn-team-settings" class="px-3 py-2 rounded border border-ops-border text-[11px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Settings</button>
            </div>
        </div>
        ${state.teamPresenceError ? `<div class="mt-3 text-[11px] text-amber-300">${escapeHtml(state.teamPresenceError)}</div>` : ''}
        <div class="mt-4">${rows}</div>
    `;

    wrap.appendChild(card);
    container.appendChild(wrap);

    const btnRefresh = card.querySelector('#btn-team-refresh');
    if (btnRefresh) {
        btnRefresh.onclick = async () => {
            await refreshSlackTeamPresence({ force: true });
            renderMain();
        };
    }
    const btnSettings = card.querySelector('#btn-team-settings');
    if (btnSettings) {
        btnSettings.onclick = async () => {
            await openSettings();
        };
    }
}

function getInboxItems() {
    const list = getDisplayInboxItems();
    if (state.inboxShowArchived) return list;
    return list.filter((x) => String(x?.status || '').toLowerCase() !== 'archived');
}

function inboxStatusBadge(status) {
    const s = String(status || '').trim();
    const key = s.toLowerCase();
    const map = {
        new: 'bg-blue-600/20 border-blue-600/40 text-blue-200',
        triaged: 'bg-amber-600/20 border-amber-600/40 text-amber-200',
        done: 'bg-emerald-600/20 border-emerald-600/40 text-emerald-200',
        archived: 'bg-zinc-900/30 border-zinc-800 text-zinc-300',
    };
    const cls = map[key] || map.new;
    return `<span class="px-2 py-0.5 rounded border text-[10px] font-mono ${cls}">${escapeHtml(s || 'New')}</span>`;
}

function inboxSourceBadge(source) {
    const s = String(source || '').trim();
    if (!s) return '';
    return `<span class="px-2 py-0.5 rounded border border-zinc-800 bg-zinc-950/40 text-[10px] font-mono text-zinc-300">${escapeHtml(s)}</span>`;
}

function normalizeInboxSourceKey(source) {
    const s = String(source || '').trim().toLowerCase();
    if (!s) return 'other';
    if (s.includes('fireflies')) return 'fireflies';
    if (s.includes('crm') || s.includes('hubspot') || s.includes('salesforce') || s.includes('pipedrive')) return 'crm';
    if (s.includes('ga4') || s.includes('google analytics') || s.includes('analytics')) return 'ga4';
    if (s.includes('email') || s.includes('gmail') || s.includes('outlook')) return 'email';
    if (s.includes('sms') || s.includes('quo') || s.includes('twilio') || s.includes('text')) return 'sms';
    if (s.includes('call') || s.includes('voice')) return 'call';
    if (s.includes('slack')) return 'slack';
    return 'other';
}

function inboxSourceMeta(sourceKey) {
    const key = safeText(sourceKey).trim().toLowerCase();
    const map = {
        fireflies: { label: 'Fireflies', icon: 'fa-microphone-lines', tone: 'text-violet-300' },
        crm: { label: 'CRM', icon: 'fa-address-card', tone: 'text-emerald-300' },
        ga4: { label: 'GA4', icon: 'fa-chart-line', tone: 'text-blue-300' },
        email: { label: 'Email', icon: 'fa-envelope', tone: 'text-blue-300' },
        sms: { label: 'SMS', icon: 'fa-comment-sms', tone: 'text-emerald-300' },
        call: { label: 'Calls', icon: 'fa-phone', tone: 'text-amber-300' },
        slack: { label: 'Slack', icon: 'fa-slack', tone: 'text-indigo-300' },
        other: { label: 'Other', icon: 'fa-inbox', tone: 'text-zinc-300' },
    };
    return map[key] || map.other;
}

function inboxBusinessLabel(item) {
    const explicit = safeText(item?.businessLabel).trim();
    if (explicit) return explicit;
    const key = safeText(item?.businessKey).trim();
    if (key === 'unmapped-legacy') return 'Unmapped/Legacy';

    const text = safeText(item?.text).toLowerCase();
    const marker = text.match(/•\s*([^:\n]+)\s*:/);
    if (marker && marker[1]) return safeText(marker[1]).trim();
    return 'Unmapped/Legacy';
}

function groupInboxItemsByBusiness(items) {
    const list = Array.isArray(items) ? items : [];
    const map = new Map();
    for (const item of list) {
        const label = inboxBusinessLabel(item);
        if (!map.has(label)) map.set(label, []);
        map.get(label).push(item);
    }
    return Array.from(map.entries())
        .map(([label, grouped]) => ({ label, items: grouped }))
        .sort((a, b) => b.items.length - a.items.length || a.label.localeCompare(b.label));
}

function normalizeSmsAckFilterLevelClient(levelRaw) {
    const level = safeText(levelRaw).trim().toLowerCase();
    if (level === 'off' || level === 'low' || level === 'medium' || level === 'high') return level;
    return 'medium';
}

function normalizeAckSignalTextClient(text) {
    return safeText(text)
        .toLowerCase()
        .replace(/[\u2018\u2019]/g, "'")
        .replace(/[^a-z0-9'\s]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function isLowSignalAcknowledgementTextClient(text, levelRaw = 'medium') {
    const level = normalizeSmsAckFilterLevelClient(levelRaw);
    if (level === 'off') return false;

    const raw = safeText(text).trim();
    if (!raw) return false;

    const emojiOnly = raw
        .replace(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\uFE0F\u200D\s]/gu, '')
        .trim();
    if (!emojiOnly) return true;

    const normalized = normalizeAckSignalTextClient(raw);
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

    if (/^(ok|okay|yep|yup|yeah|yes|got it|copy|roger|understood|noted)(\s+(thanks|thank you|thx|ty|appreciate it))?$/.test(normalized)) return true;
    if (/^(thanks|thank you|thx|ty|appreciate it)(\s+(man|bro|dude|sir|maam|m'am))?$/.test(normalized)) return true;
    if (level === 'high' && /^(sounds good|all good|we re good|cool|great|awesome|perfect|done)(\s+(thanks|thank you|thx|ty))?$/.test(normalized)) return true;

    return false;
}

function shouldHideInboxItemByNoiseFilter(item) {
    const source = normalizeInboxSourceKey(item?.source);
    const sourceRaw = safeText(item?.source).trim().toLowerCase();
    if (sourceRaw === 'marcus') return true;

    const body = safeText(item?.text) || safeText(item?.content) || safeText(item?.body) || safeText(item?.message);

    const hasActionCue = (() => {
        const s = safeText(body).toLowerCase();
        if (!s) return false;
        if (s.includes('?')) return true;
        return /\b(need|needs|please|can you|could you|follow up|send|call|schedule|review|fix|update|quote|invoice|confirm|ship|deploy|publish|prepare|asap|urgent|tomorrow|today|deadline|due|assign|delegate)\b/.test(s);
    })();

    if (sourceRaw.includes('system') || sourceRaw.includes('notification') || sourceRaw.includes('alert')) {
        if (!hasActionCue) return true;
    }

    const compact = safeText(body).replace(/\s+/g, ' ').trim();
    const normalized = normalizeAckSignalTextClient(compact);
    const genericShortNoise = compact.length <= 28 && (
        /^(message|email|sms|text)\s+(sent|received|delivered|read)$/i.test(normalized)
        || /^(got it|ok|okay|yep|yup|yes|no)(\s+(thanks|thank you|thx|ty))?$/i.test(normalized)
        || ['received', 'delivered', 'seen', 'read', 'noted', 'copy that', 'message sent', 'sent', 'done thanks', 'ok thanks', 'thanks', 'thank you'].includes(normalized)
    );
    if (genericShortNoise && !hasActionCue) return true;

    if (source !== 'sms') return false;

    const level = normalizeSmsAckFilterLevelClient(state.settings?.smsAckFilterLevel);
    if (level === 'off') return false;
    return isLowSignalAcknowledgementTextClient(body, level);
}

function getDisplayInboxItems() {
    const list = Array.isArray(state.inboxItems) ? state.inboxItems : [];
    return list.filter((item) => !shouldHideInboxItemByNoiseFilter(item));
}

async function createInboxItem(text) {
    const cleanText = safeText(text).trim();
    if (!cleanText) throw new Error('Inbox text is required');
    const data = await withRevisionRetry(() => apiJson('/api/inbox', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, item: { text: cleanText } }),
    }));
    if (data?.store) applyStore(data.store);
}

async function patchInboxItem(inboxId, patch) {
    const id = safeText(inboxId).trim();
    if (!id) throw new Error('Missing inbox id');
    const cleanPatch = (patch && typeof patch === 'object') ? patch : {};
    const data = await withRevisionRetry(() => apiJson(`/api/inbox/${encodeURIComponent(id)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, patch: cleanPatch }),
    }));
    if (data?.store) applyStore(data.store);
}

async function convertInboxItem(inboxId, kind, payload) {
    const id = safeText(inboxId).trim();
    if (!id) throw new Error('Missing inbox id');
    const data = await withRevisionRetry(() => apiJson(`/api/inbox/${encodeURIComponent(id)}/convert`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, kind, payload: (payload && typeof payload === 'object') ? payload : {} }),
    }));
    if (data?.store) applyStore(data.store);
    return data;
}

async function linkInboxItemToProject(inboxId, projectId) {
    const id = safeText(inboxId).trim();
    const pid = safeText(projectId).trim();
    if (!id) throw new Error('Missing inbox id');
    if (!pid) throw new Error('Select a project first');
    const data = await withRevisionRetry(() => apiJson(`/api/inbox/${encodeURIComponent(id)}/link-project`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, projectId: pid }),
    }));
    if (data?.store) applyStore(data.store);
    return data;
}

async function linkInboxItemToContact(inboxId, contactId) {
    const id = safeText(inboxId).trim();
    const cid = safeText(contactId).trim();
    if (!id) throw new Error('Missing inbox id');
    if (!cid) throw new Error('Select a contact first');
    const data = await withRevisionRetry(() => apiJson(`/api/inbox/${encodeURIComponent(id)}/link-contact`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, contactId: cid }),
    }));
    if (data?.store) applyStore(data.store);
    return data;
}

async function createProjectFromInboxItem(inboxId, project) {
    const id = safeText(inboxId).trim();
    if (!id) throw new Error('Missing inbox id');
    const payload = (project && typeof project === 'object') ? project : {};
    const data = await withRevisionRetry(() => apiJson(`/api/inbox/${encodeURIComponent(id)}/create-project`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision, project: payload }),
    }));
    if (data?.store) applyStore(data.store);
    return data;
}

function renderInbox(container) {
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Inbox';

    const all = getDisplayInboxItems();
    const visible = getInboxItems();
    const newCount = all.filter((x) => String(x?.status || '').toLowerCase() === 'new').length;

    const digestState = state.inboxAutomationDigest && typeof state.inboxAutomationDigest === 'object'
        ? state.inboxAutomationDigest
        : { items: [], loading: false, loadedAt: 0, error: '' };
    const digestStale = !Number(digestState.loadedAt) || (Date.now() - Number(digestState.loadedAt) > 20000);
    if (!digestState.loading && digestStale) {
        state.inboxAutomationDigest = { ...digestState, loading: true, error: '' };
        fetchMarcusAutomationDigest()
            .then(() => {
                if (state.currentView === 'inbox') renderMain();
            })
            .catch((err) => {
                state.inboxAutomationDigest = {
                    ...(state.inboxAutomationDigest || {}),
                    loading: false,
                    loadedAt: Date.now(),
                    error: err?.message || 'Failed to load Marcus digest',
                };
                if (state.currentView === 'inbox') renderMain();
            });
    }

    const digestItems = Array.isArray((state.inboxAutomationDigest || {}).items) ? state.inboxAutomationDigest.items : [];
    const digestError = safeText((state.inboxAutomationDigest || {}).error);
    const digestLoading = Boolean((state.inboxAutomationDigest || {}).loading);
    const digestHtml = `
        <div class="mt-4 rounded-xl border border-emerald-700/30 bg-emerald-950/15 p-3">
            <div class="flex items-center justify-between gap-3">
                <div>
                    <div class="text-[11px] font-mono uppercase tracking-wide text-emerald-200">Marcus Daily Digest</div>
                    <div class="text-xs text-zinc-400">Accept or reject each part (project and task list).</div>
                </div>
                <div class="text-[11px] font-mono ${digestItems.length ? 'text-emerald-200' : 'text-zinc-500'}">${digestItems.length} pending</div>
            </div>
            ${digestLoading ? '<div class="mt-2 text-[11px] text-zinc-500">Loading digest…</div>' : ''}
            ${digestError ? `<div class="mt-2 text-[11px] text-red-300">${escapeHtml(digestError)}</div>` : ''}
            ${!digestLoading && !digestItems.length ? '<div class="mt-2 text-[11px] text-zinc-500">No pending recommendations.</div>' : ''}
            ${digestItems.slice(0, 8).map((entry) => {
                const did = safeText(entry?.id).trim();
                const iid = safeText(entry?.itemId).trim();
                const tasks = Array.isArray(entry?.tasks) ? entry.tasks : [];
                const sel = state.inboxDigestSelectionsById?.[did] || {};
                const checks = Array.isArray(sel.taskChecks) ? sel.taskChecks : tasks.map(() => true);
                return `
                    <div class="mt-3 rounded-lg border border-zinc-800 bg-zinc-900/35 p-3">
                        <div class="text-[11px] text-zinc-400 font-mono">Inbox ${escapeHtml(iid)} • ${escapeHtml(safeText(entry?.projectName) || 'Unlinked')}</div>
                        <div class="mt-1 text-xs text-zinc-300">${escapeHtml(safeText(entry?.signalPreview) || 'No preview')}</div>
                        <div class="mt-2 flex flex-wrap gap-3 text-[11px] text-zinc-300">
                            <label class="inline-flex items-center gap-2">
                                <input type="checkbox" data-digest-check-project="${escapeHtml(did)}" ${sel.acceptProjectLink ? 'checked' : ''} ${safeText(entry?.projectId).trim() ? '' : 'disabled'} class="accent-emerald-500" />
                                Link project
                            </label>
                        </div>
                        <div class="mt-2 space-y-1">
                            ${tasks.map((task, taskIdx) => `
                                <label class="flex items-center gap-2 text-[11px] text-zinc-200">
                                    <input type="checkbox" data-digest-check-task="${escapeHtml(did)}" data-digest-task-index="${taskIdx}" ${(checks[taskIdx] ?? true) ? 'checked' : ''} class="accent-emerald-500" />
                                    <span>${escapeHtml(safeText(task?.title) || 'Task')} <span class="text-zinc-500">(P${escapeHtml(String([1,2,3].includes(Number(task?.priority)) ? Number(task.priority) : 2))})</span></span>
                                </label>
                            `).join('')}
                        </div>
                        <div class="mt-3 flex flex-wrap gap-2">
                            <button data-digest-apply="${escapeHtml(did)}" class="px-2 py-1 rounded border border-emerald-600/40 bg-emerald-600/20 text-[10px] font-mono text-emerald-100 hover:bg-emerald-600/30">Apply Selected</button>
                            <button data-digest-reject="${escapeHtml(did)}" class="px-2 py-1 rounded border border-red-600/40 bg-red-600/15 text-[10px] font-mono text-red-100 hover:bg-red-600/25">Reject</button>
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
    `;

    const wrap = document.createElement('div');
    wrap.className = 'h-full flex flex-col min-h-0';

    const header = document.createElement('div');
    header.className = 'shrink-0 p-6 border-b border-zinc-800 bg-zinc-900/30';
    header.innerHTML = `
        <div class="flex items-start justify-between gap-4">
            <div>
                <h2 class="text-2xl text-white font-light leading-tight">Global Inbox</h2>
                <div class="text-xs text-zinc-500 mt-1">${newCount} new • ${visible.length} shown • rev ${state.revision}</div>
            </div>
            <div class="flex items-center gap-2">
                <button id="btn-inbox-marcus-filter" class="px-3 py-1.5 rounded border border-amber-600/40 bg-amber-600/15 text-[11px] font-mono text-amber-200 hover:bg-amber-600/25 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Run Marcus Filter</button>
                <button id="btn-inbox-marcus-triage" class="px-3 py-1.5 rounded border border-blue-600/40 bg-blue-600/15 text-[11px] font-mono text-blue-200 hover:bg-blue-600/25 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Run Marcus Triage</button>
                <button id="btn-inbox-marcus-auto" class="px-3 py-1.5 rounded border border-emerald-600/40 bg-emerald-600/15 text-[11px] font-mono text-emerald-200 hover:bg-emerald-600/25 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Run Marcus Auto</button>
                <button id="btn-inbox-marcus-coach" class="px-3 py-1.5 rounded border border-purple-600/40 bg-purple-600/15 text-[11px] font-mono text-purple-200 hover:bg-purple-600/25 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Marcus Coach</button>
                <label class="flex items-center gap-2 text-xs text-zinc-400 select-none">
                    <input id="inbox-show-archived" type="checkbox" class="accent-blue-500" ${state.inboxShowArchived ? 'checked' : ''} />
                    Show archived
                </label>
            </div>
        </div>

        <div class="mt-4 grid grid-cols-1 md:grid-cols-6 gap-3">
            <div class="md:col-span-5">
                <textarea id="inbox-draft" rows="2" class="w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40" placeholder="Capture anything…">${escapeHtml(state.inboxDraftText || '')}</textarea>
            </div>
            <div class="md:col-span-1 flex items-stretch">
                <button id="btn-inbox-add" class="w-full px-3 py-2 rounded-lg bg-blue-600/20 border border-blue-600/40 text-sm font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Add</button>
            </div>
        </div>
        ${digestHtml}
    `;

    const list = document.createElement('div');
    list.className = 'flex-1 min-h-0 overflow-y-auto p-6 space-y-3';

    if (!visible.length) {
        const empty = document.createElement('div');
        empty.className = 'border border-zinc-800 rounded-xl bg-zinc-900/30 p-6 text-sm text-zinc-400';
        empty.innerHTML = '<div class="text-white font-semibold">Inbox is empty</div><div class="text-xs text-zinc-500 mt-1">Capture tasks, notes, and comms here first — then convert.</div>';
        list.appendChild(empty);
    } else {
        const businessGroups = groupInboxItemsByBusiness(visible);
        for (const group of businessGroups) {
            const section = document.createElement('details');
            section.className = 'border border-zinc-800 rounded-xl bg-zinc-900/20';
            section.open = businessGroups.length <= 2 || group.label !== 'Unmapped/Legacy';
            section.innerHTML = `
                <summary class="cursor-pointer list-none px-4 py-3 flex items-center justify-between gap-3">
                    <div class="min-w-0">
                        <div class="text-zinc-300 text-xxs font-mono uppercase tracking-widest">Business</div>
                        <div class="text-sm text-white truncate">${escapeHtml(group.label)}</div>
                    </div>
                    <div class="text-[11px] text-zinc-500 font-mono">${group.items.length} item${group.items.length === 1 ? '' : 's'}</div>
                </summary>
                <div class="px-4 pb-4 space-y-3 border-t border-zinc-800/60"></div>
            `;
            list.appendChild(section);
            const sectionBody = section.querySelector('div');
            if (!sectionBody) continue;

            for (const item of group.items) {
                const id = safeText(item?.id);
                const status = safeText(item?.status) || 'New';
                const createdAt = safeText(item?.createdAt);
                const updatedAt = safeText(item?.updatedAt);
                const recommendation = getMarcusInboxRecommendation(id);
                const projectId = safeText((state.inboxConvertProjectById && state.inboxConvertProjectById[id]) || item?.projectId);
                const allContacts = Array.isArray(state.clients) ? state.clients : [];
                const contactByName = allContacts.find((c) => safeText(c?.name).trim().toLowerCase() === safeText(item?.contactName).trim().toLowerCase());
                const contactId = safeText((state.inboxConvertContactById && state.inboxConvertContactById[id]) || item?.contactId || contactByName?.id);
                const projectSearch = safeText(state.inboxProjectSearchById?.[id]).trim().toLowerCase();
                const allProjects = Array.isArray(state.projects) ? state.projects : [];
                const filteredProjects = allProjects.filter((p) => {
                    if (!projectSearch) return true;
                    const n = safeText(p?.name).toLowerCase();
                    const c = safeText(p?.clientName).toLowerCase();
                    return n.includes(projectSearch) || c.includes(projectSearch);
                });
                const isAssigned = projectId && projectId !== id;
                const isUnassignedNew = !isAssigned && String(status || '').trim().toLowerCase() === 'new';

                const recWho = safeText(recommendation?.who?.name).trim();
                const recProjectName = safeText(recommendation?.project?.projectName).trim();
                const recProjectId = safeText(recommendation?.project?.projectId).trim();
                const recTasks = Array.isArray(recommendation?.tasks) ? recommendation.tasks : [];
                const senderLabel = safeText(item?.contactName || item?.fromName || item?.sender || item?.fromNumber).trim();
                const msgCount = Number(item?.messageCount || 1);
                const recPanel = recommendation
                    ? `
                    <div class="mt-3 rounded-lg border border-blue-600/30 bg-blue-950/20 p-3 space-y-2">
                        <div class="text-[11px] font-mono uppercase tracking-wide text-blue-200">Marcus Recommendation</div>
                        <div class="text-[12px] text-zinc-200">
                            <span class="text-zinc-400">Who:</span> ${escapeHtml(recWho || 'Unknown')}
                            ${recommendation?.who?.confidence != null ? `<span class="text-zinc-500"> (${escapeHtml(formatMarcusConfidence(recommendation?.who?.confidence))})</span>` : ''}
                        </div>
                        <div class="text-[12px] text-zinc-200">
                            <span class="text-zinc-400">Project:</span> ${escapeHtml(recProjectName || 'Create new project')}
                            ${recommendation?.project?.confidence != null ? `<span class="text-zinc-500"> (${escapeHtml(formatMarcusConfidence(recommendation?.project?.confidence))})</span>` : ''}
                        </div>
                        <div class="text-[12px] text-zinc-300">
                            <span class="text-zinc-400">Task ideas:</span>
                            ${recTasks.length
                                ? `<ul class="mt-1 space-y-1">${recTasks.slice(0, 3).map((t) => `<li>- ${escapeHtml(safeText(t?.title) || 'Follow up')}</li>`).join('')}</ul>`
                                : '<span> none</span>'}
                        </div>
                        <div class="flex flex-wrap gap-2 pt-1">
                            ${recProjectId ? `<button data-marcus-apply-link="${escapeHtml(id)}" class="px-2 py-1 rounded border border-blue-600/40 bg-blue-600/20 text-[10px] font-mono text-blue-100 hover:bg-blue-600/30">Apply Link</button>` : ''}
                            ${recTasks.length ? `<button data-marcus-create-task="${escapeHtml(id)}" class="px-2 py-1 rounded border border-emerald-600/40 bg-emerald-600/20 text-[10px] font-mono text-emerald-100 hover:bg-emerald-600/30">Create Top Task</button>` : ''}
                            ${recTasks.length > 1 ? `<button data-marcus-create-all-tasks="${escapeHtml(id)}" class="px-2 py-1 rounded border border-emerald-600/40 bg-emerald-600/10 text-[10px] font-mono text-emerald-200 hover:bg-emerald-600/20">Create All (${recTasks.length})</button>` : ''}
                        </div>
                    </div>`
                    : '';

                const card = document.createElement('div');
                card.className = `border ${isUnassignedNew ? 'border-red-500/30 bg-red-500/5' : 'border-zinc-800 bg-zinc-900/30'} rounded-xl p-4`;
                card.innerHTML = `
                    <div class="flex items-start justify-between gap-4">
                        <div class="min-w-0">
                            <div class="flex items-center gap-2 flex-wrap">
                                ${inboxStatusBadge(status)}
                                ${inboxSourceBadge(item?.source)}
                                <span class="px-2 py-0.5 rounded border border-zinc-800 bg-zinc-950/40 text-[10px] font-mono text-zinc-300">${escapeHtml(inboxBusinessLabel(item))}</span>
                                                                    ${senderLabel ? `<span class="px-2 py-0.5 rounded border border-zinc-500/30 bg-zinc-800/40 text-[10px] font-mono text-zinc-300">From: ${escapeHtml(senderLabel)}</span>` : ""}
                                                                    ${msgCount > 1 ? `<span class="px-2 py-0.5 rounded border border-blue-600/30 bg-blue-600/10 text-[10px] font-mono text-blue-200">${msgCount} msgs</span>` : ''}
                                <div class="text-[11px] text-zinc-500 font-mono">${escapeHtml(createdAt ? formatTimeFromIso(createdAt) : '')}${updatedAt && updatedAt !== createdAt ? ` • upd ${escapeHtml(formatTimeFromIso(updatedAt))}` : ''}</div>
                            </div>
                            <div class="mt-2 text-sm text-zinc-200 whitespace-pre-wrap break-words">${escapeHtml(safeText(item?.text))}</div>
                            ${recPanel}
                            <div class="mt-3 flex flex-wrap items-center gap-2">
                                <input data-inbox-project-search="${escapeHtml(id)}" value="${escapeHtml(state.inboxProjectSearchById?.[id] || '')}" placeholder="Find project/client..." class="bg-zinc-900 border border-zinc-700 rounded px-2 py-1 text-xs text-zinc-100 placeholder-zinc-500 min-w-[180px]" />
                                <select data-inbox-project="${escapeHtml(id)}" style="color-scheme: dark;" class="bg-zinc-900 border border-zinc-700 rounded px-2 py-1 text-xs text-zinc-100 min-w-[220px]">
                                    <option value="">Project (optional)</option>
                                    ${filteredProjects.map((p) => {
                                        const pid = safeText(p?.id);
                                        const pname = safeText(p?.name) || 'Project';
                                        const client = safeText(p?.clientName).trim();
                                        const label = client ? `${pname} — ${client}` : pname;
                                        return `<option value="${escapeHtml(pid)}" ${pid === projectId ? 'selected' : ''}>${escapeHtml(label)}</option>`;
                                    }).join('')}
                                </select>
                                ${projectSearch && !filteredProjects.length ? '<span class="text-[10px] font-mono text-zinc-500">No matches</span>' : ''}
                                <button data-inbox-link-project="${escapeHtml(id)}" class="px-2 py-1 rounded border border-blue-600/40 bg-blue-600/20 text-[11px] font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Link</button>
                                <select data-inbox-contact="${escapeHtml(id)}" style="color-scheme: dark;" class="bg-zinc-900 border border-zinc-700 rounded px-2 py-1 text-xs text-zinc-100 min-w-[220px]">
                                    <option value="">Contact (optional)</option>
                                    ${allContacts.map((c) => {
                                        const cid = safeText(c?.id);
                                        const cname = safeText(c?.name) || 'Contact';
                                        const cphone = safeText(c?.phone).trim();
                                        const label = cphone ? `${cname} — ${cphone}` : cname;
                                        return `<option value="${escapeHtml(cid)}" ${cid === contactId ? 'selected' : ''}>${escapeHtml(label)}</option>`;
                                    }).join('')}
                                </select>
                                <button data-inbox-link-contact="${escapeHtml(id)}" class="px-2 py-1 rounded border border-purple-600/40 bg-purple-600/20 text-[11px] font-mono text-purple-200 hover:bg-purple-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Link Contact</button>
                                <button data-inbox-create-project="${escapeHtml(id)}" class="px-2 py-1 rounded border border-emerald-600/40 bg-emerald-600/20 text-[11px] font-mono text-emerald-200 hover:bg-emerald-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">New Project</button>
                                <button data-inbox-edit="${escapeHtml(id)}" class="px-2 py-1 rounded border border-zinc-800 text-[11px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Edit</button>
                            </div>
                        </div>
                        <div class="shrink-0 flex flex-col gap-2">
                            <button data-inbox-marcus-recommend="${escapeHtml(id)}" class="px-3 py-1.5 rounded border border-blue-600/40 bg-blue-600/15 text-[11px] font-mono text-blue-200 hover:bg-blue-600/25 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Marcus Recommend</button>
                            <button data-inbox-triage="${escapeHtml(id)}" class="px-3 py-1.5 rounded border border-zinc-800 text-[11px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Triage</button>
                            <button data-inbox-done="${escapeHtml(id)}" class="px-3 py-1.5 rounded bg-emerald-600/20 border border-emerald-600/40 text-[11px] font-mono text-emerald-200 hover:bg-emerald-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Done</button>
                            ${isAssigned
                                ? `<button data-inbox-archive="${escapeHtml(id)}" class="px-3 py-1.5 rounded bg-zinc-900/30 border border-zinc-800 text-[11px] font-mono text-zinc-200 hover:bg-zinc-800/40 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Archive</button>`
                                : `<button data-inbox-nohome="${escapeHtml(id)}" class="px-3 py-1.5 rounded bg-red-600/15 border border-red-600/30 text-[11px] font-mono text-red-200 hover:bg-red-600/25 transition-colors transition-transform duration-150 ease-out active:translate-y-px">No Home</button>`}
                        </div>
                    </div>
                    <div class="mt-4 flex flex-wrap gap-2">
                        <button data-inbox-to-task="${escapeHtml(id)}" class="px-3 py-1.5 rounded bg-blue-600/20 border border-blue-600/40 text-[11px] font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">→ Task</button>
                        <button data-inbox-to-note="${escapeHtml(id)}" class="px-3 py-1.5 rounded bg-amber-600/20 border border-amber-600/40 text-[11px] font-mono text-amber-200 hover:bg-amber-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">→ Note</button>
                        <button data-inbox-to-comm="${escapeHtml(id)}" class="px-3 py-1.5 rounded bg-indigo-600/20 border border-indigo-600/40 text-[11px] font-mono text-indigo-200 hover:bg-indigo-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">→ Comm</button>
                    </div>
                `;
                sectionBody.appendChild(card);
            }
        }
    }

    wrap.appendChild(header);
    wrap.appendChild(list);
    container.appendChild(wrap);

    const showArchived = header.querySelector('#inbox-show-archived');
    if (showArchived) {
        showArchived.onchange = (e) => {
            state.inboxShowArchived = !!e.target.checked;
            renderMain();
        };
    }

    const draft = header.querySelector('#inbox-draft');
    if (draft) {
        draft.oninput = (e) => {
            state.inboxDraftText = String(e.target.value || '');
        };
    }

    const addBtn = header.querySelector('#btn-inbox-add');
    if (addBtn) {
        addBtn.onclick = async () => {
            addBtn.disabled = true;
            try {
                await createInboxItem(state.inboxDraftText);
                state.inboxDraftText = '';
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to add inbox item');
            } finally {
                addBtn.disabled = false;
            }
        };
    }

    const filterBtn = header.querySelector('#btn-inbox-marcus-filter');
    if (filterBtn) {
        filterBtn.onclick = async () => {
            filterBtn.disabled = true;
            const prev = filterBtn.textContent;
            filterBtn.textContent = 'Filtering…';
            try {
                const result = await runMarcusInboxFilter();
                const archived = Number(result?.archived || 0);
                const matched = Number(result?.matched || 0);
                const collapsedThreads = Number(result?.collapsedThreads || 0);
                const mergedMessages = Number(result?.mergedMessages || 0);
                alert(`Marcus filter complete. Archived: ${archived}. Matched: ${matched}. Thread groups collapsed: ${collapsedThreads}. Messages merged: ${mergedMessages}.`);
                renderMain();
            } catch (e) {
                alert(e?.message || 'Marcus filter failed');
            } finally {
                filterBtn.disabled = false;
                filterBtn.textContent = prev;
            }
        };
    }

    const triageBtn = header.querySelector('#btn-inbox-marcus-triage');
    if (triageBtn) {
        triageBtn.onclick = async () => {
            triageBtn.disabled = true;
            const prev = triageBtn.textContent;
            triageBtn.textContent = 'Triaging...';
            try {
                const result = await runMarcusInboxTriage({ onlyNew: true, includeArchived: false, limit: 120 });
                alert(`Marcus triage complete. Recommendations: ${Number(result?.count || 0)}.`);
                renderMain();
            } catch (e) {
                alert(e?.message || 'Marcus triage failed');
            } finally {
                triageBtn.disabled = false;
                triageBtn.textContent = prev;
            }
        };
    }

    const autoBtn = header.querySelector('#btn-inbox-marcus-auto');
    if (autoBtn) {
        autoBtn.onclick = async () => {
            autoBtn.disabled = true;
            const prev = autoBtn.textContent;
            autoBtn.textContent = 'Running...';
            try {
                const result = await runMarcusInboxAutomation();
                const scanned = Number(result?.scanned || 0);
                const proposed = Number(result?.proposed || 0);
                const applied = Number(result?.applied || 0);
                const pending = Number(result?.digestPending || 0);
                const mode = safeText(result?.approvalMode).trim() || 'dailyDigest';
                alert(`Marcus automation complete. Mode: ${mode}. Scanned: ${scanned}. Proposed: ${proposed}. Auto-applied: ${applied}. Digest pending: ${pending}.`);
                await fetchMarcusAutomationDigest().catch(() => {});
                renderMain();
            } catch (e) {
                alert(e?.message || 'Marcus automation run failed');
            } finally {
                autoBtn.disabled = false;
                autoBtn.textContent = prev;
            }
        };
    }

    const coachBtn = header.querySelector('#btn-inbox-marcus-coach');
    if (coachBtn) {
        coachBtn.onclick = async () => {
            coachBtn.disabled = true;
            const prev = coachBtn.textContent;
            coachBtn.textContent = 'Coaching...';
            try {
                const result = await coachNextInboxStep();
                if (result?.applied) {
                    const msg = `Marcus coach applied. Linked: ${result.linked ? 'yes' : 'no'}. Tasks created: ${Number(result.tasksCreated || 0)}.`;
                    speakMarcus(msg);
                    alert(msg);
                }
                renderMain();
            } catch (e) {
                alert(e?.message || 'Marcus coach failed');
            } finally {
                coachBtn.disabled = false;
                coachBtn.textContent = prev;
            }
        };
    }

    container.querySelectorAll('input[data-digest-check-project]').forEach((inp) => {
        inp.addEventListener('change', () => {
            const digestId = safeText(inp.getAttribute('data-digest-check-project')).trim();
            if (!digestId) return;
            const prev = state.inboxDigestSelectionsById?.[digestId] || { acceptProjectLink: false, taskChecks: [] };
            state.inboxDigestSelectionsById = {
                ...(state.inboxDigestSelectionsById || {}),
                [digestId]: { ...prev, acceptProjectLink: !!inp.checked },
            };
        });
    });

    container.querySelectorAll('input[data-digest-check-task]').forEach((inp) => {
        inp.addEventListener('change', () => {
            const digestId = safeText(inp.getAttribute('data-digest-check-task')).trim();
            const idx = Number(inp.getAttribute('data-digest-task-index'));
            if (!digestId || !Number.isInteger(idx) || idx < 0) return;
            const entry = (Array.isArray(state.inboxAutomationDigest?.items) ? state.inboxAutomationDigest.items : []).find((x) => safeText(x?.id).trim() === digestId);
            const len = Array.isArray(entry?.tasks) ? entry.tasks.length : 0;
            const prev = state.inboxDigestSelectionsById?.[digestId] || { acceptProjectLink: false, taskChecks: Array.from({ length: len }, () => true) };
            const checks = Array.isArray(prev.taskChecks) ? [...prev.taskChecks] : Array.from({ length: len }, () => true);
            checks[idx] = !!inp.checked;
            state.inboxDigestSelectionsById = {
                ...(state.inboxDigestSelectionsById || {}),
                [digestId]: { ...prev, taskChecks: checks },
            };
        });
    });

    container.querySelectorAll('button[data-digest-apply]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const digestId = safeText(btn.getAttribute('data-digest-apply')).trim();
            if (!digestId) return;
            const sel = state.inboxDigestSelectionsById?.[digestId] || {};
            const taskChecks = Array.isArray(sel.taskChecks) ? sel.taskChecks : [];
            const acceptTaskIndexes = taskChecks
                .map((checked, idx) => checked ? idx : -1)
                .filter((idx) => idx >= 0);
            btn.disabled = true;
            try {
                const result = await decideMarcusAutomationDigest(digestId, {
                    acceptProjectLink: !!sel.acceptProjectLink,
                    acceptTaskIndexes,
                    reject: false,
                });
                alert(`Applied selection. Created ${Number(result?.createdTasks || 0)} tasks.`);
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to apply digest recommendation');
            } finally {
                btn.disabled = false;
            }
        });
    });

    container.querySelectorAll('button[data-digest-reject]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const digestId = safeText(btn.getAttribute('data-digest-reject')).trim();
            if (!digestId) return;
            btn.disabled = true;
            try {
                await decideMarcusAutomationDigest(digestId, { reject: true });
                alert('Digest recommendation rejected.');
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to reject digest recommendation');
            } finally {
                btn.disabled = false;
            }
        });
    });

    // Wire row actions
    const refreshProjectSelectForInput = (inp) => {
        const inboxId = safeText(inp?.getAttribute('data-inbox-project-search')).trim();
        if (!inboxId) return;

        const card = inp.closest('.rounded-xl');
        const select = card?.querySelector(`select[data-inbox-project="${inboxId}"]`);
        if (!select) return;

        const selectedId = safeText(state.inboxConvertProjectById?.[inboxId] || select.value).trim();
        const query = safeText(inp.value).trim().toLowerCase();
        const allProjects = Array.isArray(state.projects) ? state.projects : [];
        const filtered = allProjects.filter((p) => {
            if (!query) return true;
            const name = safeText(p?.name).toLowerCase();
            const client = safeText(p?.clientName).toLowerCase();
            return name.includes(query) || client.includes(query);
        });

        const options = [`<option value="">Project (optional)</option>`];
        for (const p of filtered) {
            const pid = safeText(p?.id);
            const pname = safeText(p?.name) || 'Project';
            const client = safeText(p?.clientName).trim();
            const label = client ? `${pname} — ${client}` : pname;
            options.push(`<option value="${escapeHtml(pid)}" ${pid === selectedId ? 'selected' : ''}>${escapeHtml(label)}</option>`);
        }
        if (!filtered.length) {
            options.push('<option value="" disabled>No matches</option>');
        }
        select.innerHTML = options.join('');
        if (selectedId && filtered.some((p) => safeText(p?.id) === selectedId)) {
            select.value = selectedId;
        }
    };

    container.querySelectorAll('input[data-inbox-project-search]').forEach((inp) => {
        inp.addEventListener('input', (e) => {
            const inboxId = safeText(inp.getAttribute('data-inbox-project-search')).trim();
            if (!inboxId) return;
            const value = safeText(e.target.value);
            state.inboxProjectSearchById = {
                ...(state.inboxProjectSearchById || {}),
                [inboxId]: value,
            };
            refreshProjectSelectForInput(inp);
        });

        inp.addEventListener('keydown', (e) => {
            if (e.key !== 'Enter') return;
            e.preventDefault();
            const inboxId = safeText(inp.getAttribute('data-inbox-project-search')).trim();
            if (!inboxId) return;

            const card = inp.closest('.rounded-xl');
            const select = card?.querySelector(`select[data-inbox-project="${inboxId}"]`);
            if (!select) return;

            const firstMatch = Array.from(select.options || []).find((opt) => safeText(opt?.value).trim());
            if (!firstMatch) return;

            const projectId = safeText(firstMatch.value).trim();
            if (!projectId) return;

            select.value = projectId;
            state.inboxConvertProjectById = {
                ...(state.inboxConvertProjectById || {}),
                [inboxId]: projectId,
            };

            const linkBtn = card?.querySelector(`button[data-inbox-link-project="${inboxId}"]`);
            if (linkBtn && typeof linkBtn.focus === 'function') linkBtn.focus();
        });
    });

    container.querySelectorAll('select[data-inbox-project]').forEach((sel) => {
        sel.addEventListener('change', (e) => {
            const inboxId = safeText(sel.getAttribute('data-inbox-project')).trim();
            if (!inboxId) return;
            const projectId = safeText(e.target.value).trim();
            state.inboxConvertProjectById = { ...(state.inboxConvertProjectById || {}), [inboxId]: projectId };
        });
    });

    container.querySelectorAll('select[data-inbox-contact]').forEach((sel) => {
        sel.addEventListener('change', (e) => {
            const inboxId = safeText(sel.getAttribute('data-inbox-contact')).trim();
            if (!inboxId) return;
            const contactId = safeText(e.target.value).trim();
            state.inboxConvertContactById = { ...(state.inboxConvertContactById || {}), [inboxId]: contactId };
        });
    });

    container.querySelectorAll('button[data-inbox-edit]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-inbox-edit')).trim();
            const item = getDisplayInboxItems().find((x) => safeText(x?.id) === inboxId);
            if (!inboxId || !item) return;
            const next = prompt('Edit inbox item:', safeText(item?.text));
            if (next === null) return;
            try {
                await patchInboxItem(inboxId, { text: next });
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to update inbox item');
            }
        });
    });

    container.querySelectorAll('button[data-inbox-marcus-recommend]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-inbox-marcus-recommend')).trim();
            if (!inboxId) return;
            btn.disabled = true;
            const prev = btn.textContent;
            btn.textContent = 'Thinking...';
            try {
                await runMarcusInboxTriage({ onlyNew: false, includeArchived: false, limit: 200 });
                renderMain();
            } catch (e) {
                alert(e?.message || 'Marcus recommendation failed');
            } finally {
                btn.disabled = false;
                btn.textContent = prev;
            }
        });
    });

    container.querySelectorAll('button[data-marcus-apply-link]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-marcus-apply-link')).trim();
            if (!inboxId) return;
            const rec = getMarcusInboxRecommendation(inboxId);
            const projectId = safeText(rec?.project?.projectId).trim();
            if (!projectId) {
                alert('No project recommendation to apply');
                return;
            }
            btn.disabled = true;
            try {
                await linkInboxItemToProject(inboxId, projectId);
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to apply project link');
            } finally {
                btn.disabled = false;
            }
        });
    });

    container.querySelectorAll('button[data-marcus-create-task]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-marcus-create-task')).trim();
            if (!inboxId) return;
            const rec = getMarcusInboxRecommendation(inboxId);
            btn.disabled = true;
            try {
                const result = await createTaskFromMarcusRecommendation(inboxId, rec);
                if (Number(result?.created || 0) > 0) {
                    alert(`Created ${Number(result.created)} task from Marcus recommendation.`);
                }
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to create task from recommendation');
            } finally {
                btn.disabled = false;
            }
        });
    });

    container.querySelectorAll('button[data-marcus-create-all-tasks]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-marcus-create-all-tasks')).trim();
            if (!inboxId) return;
            const rec = getMarcusInboxRecommendation(inboxId);
            btn.disabled = true;
            try {
                const result = await createAllTasksFromMarcusRecommendation(inboxId, rec);
                alert(`Created ${Number(result?.created || 0)} tasks from Marcus recommendation.`);
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to create tasks from recommendation');
            } finally {
                btn.disabled = false;
            }
        });
    });

    container.querySelectorAll('button[data-inbox-link-project]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-inbox-link-project')).trim();
            if (!inboxId) return;
            const projectId = safeText(state.inboxConvertProjectById?.[inboxId]).trim();
            btn.disabled = true;
            try {
                await linkInboxItemToProject(inboxId, projectId);
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to link inbox item');
            } finally {
                btn.disabled = false;
            }
        });
    });

    container.querySelectorAll('button[data-inbox-link-contact]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-inbox-link-contact')).trim();
            if (!inboxId) return;
            const contactId = safeText(state.inboxConvertContactById?.[inboxId]).trim();
            btn.disabled = true;
            try {
                await linkInboxItemToContact(inboxId, contactId);
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to link inbox item to contact');
            } finally {
                btn.disabled = false;
            }
        });
    });

    container.querySelectorAll('button[data-inbox-create-project]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-inbox-create-project')).trim();
            const item = getDisplayInboxItems().find((x) => safeText(x?.id) === inboxId);
            if (!inboxId || !item) return;

            const defaultName = safeText(item?.projectName).trim() || `Inbox Project ${new Date().toISOString().slice(0, 10)}`;
            const name = prompt('New project name:', defaultName);
            if (name === null) return;
            const cleanName = safeText(name).trim();
            if (!cleanName) {
                alert('Project name is required');
                return;
            }

            btn.disabled = true;
            try {
                const created = await createProjectFromInboxItem(inboxId, { name: cleanName, type: 'Other', status: 'Active' });
                renderMain();
                const projectId = safeText(created?.project?.id).trim();
                if (projectId) {
                    await openProject(projectId);
                }
            } catch (e) {
                alert(e?.message || 'Failed to create project from inbox item');
            } finally {
                btn.disabled = false;
            }
        });
    });

    const wireStatus = (attr, nextStatus) => {
        container.querySelectorAll(`button[${attr}]`).forEach((btn) => {
            btn.addEventListener('click', async () => {
                const inboxId = safeText(btn.getAttribute(attr)).trim();
                if (!inboxId) return;
                btn.disabled = true;
                try {
                    await patchInboxItem(inboxId, { status: nextStatus });
                    renderMain();
                } catch (e) {
                    alert(e?.message || 'Failed to update inbox item');
                } finally {
                    btn.disabled = false;
                }
            });
        });
    };

    wireStatus('data-inbox-triage', 'Triaged');
    wireStatus('data-inbox-done', 'Done');
    wireStatus('data-inbox-archive', 'Archived');
    wireStatus('data-inbox-nohome', 'Archived');

    const wireConvert = (attr, kind) => {
        container.querySelectorAll(`button[${attr}]`).forEach((btn) => {
            btn.addEventListener('click', async () => {
                const inboxId = safeText(btn.getAttribute(attr)).trim();
                if (!inboxId) return;
                btn.disabled = true;
                try {
                    const projectId = safeText(state.inboxConvertProjectById?.[inboxId]).trim();
                    const payload = {};
                    if (projectId) payload.projectId = projectId;
                    await convertInboxItem(inboxId, kind, payload);
                    renderMain();
                } catch (e) {
                    alert(e?.message || 'Failed to convert inbox item');
                } finally {
                    btn.disabled = false;
                }
            });
        });
    };

    wireConvert('data-inbox-to-task', 'task');
    wireConvert('data-inbox-to-note', 'note');
    wireConvert('data-inbox-to-comm', 'comm');
}

function maskHint(hint) {
    const s = String(hint || '').trim();
    return s || 'Not set';
}

function generateSecret() {
    const bytes = new Uint8Array(24);
    crypto.getRandomValues(bytes);
    return Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function saveSettingsPatch(patch) {
    const res = await apiFetch('/api/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch)
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data?.error || 'Failed to save settings');
    await fetchSettings();
    return data;
}

async function runMarcusInboxFilter() {
    pulseMarcusAmbient('busy', 1200);
    const res = await apiFetch('/api/inbox/marcus-filter', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ baseRevision: state.revision }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || 'M.A.R.C.U.S. filter failed');
    if (data?.store && typeof data.store === 'object') applyStore(data.store);
    await fetchState({ background: false });
    return data;
}

function formatMarcusConfidence(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '';
    return `${Math.round(Math.max(0, Math.min(1, n)) * 100)}%`;
}

function getMarcusInboxRecommendation(inboxId) {
    const id = safeText(inboxId).trim();
    if (!id) return null;
    const map = state.inboxMarcusRecommendationsById && typeof state.inboxMarcusRecommendationsById === 'object'
        ? state.inboxMarcusRecommendationsById
        : (state.inboxMarcusRecommendationsById && typeof state.inboxMarcusRecommendationsById === 'object'
            ? state.inboxMarcusRecommendationsById
            : {});
    const rec = map[id];
    return rec && typeof rec === 'object' ? rec : null;
}

async function runMarcusInboxTriage(options = {}) {
    pulseMarcusAmbient('busy', 1200);
    const onlyNew = options.onlyNew !== false;
    const includeArchived = options.includeArchived === true;
    const limit = Number.isFinite(Number(options.limit)) ? Math.max(1, Math.min(200, Math.floor(Number(options.limit)))) : 80;
    const qs = new URLSearchParams();
    qs.set('onlyNew', onlyNew ? '1' : '0');
    qs.set('includeArchived', includeArchived ? '1' : '0');
    qs.set('limit', String(limit));

    const res = await apiFetch(`/api/inbox/marcus-triage?${qs.toString()}`);
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || 'M.A.R.C.U.S. triage failed');

    const list = Array.isArray(data?.recommendations) ? data.recommendations : [];
    const next = { ...(state.inboxMarcusRecommendationsById || state.inboxMarcusRecommendationsById || {}) };
    for (const rec of list) {
        const itemId = safeText(rec?.itemId).trim();
        if (!itemId) continue;
        next[itemId] = rec;
    }
    state.inboxMarcusRecommendationsById = next;
    state.inboxMarcusRecommendationsById = next;
    return { ...data, recommendations: list };
}

async function runMarcusInboxTriage(options = {}) {
    return runMarcusInboxTriage(options);
}

async function runMarcusInboxAutomation(options = {}) {
    pulseMarcusAmbient('busy', 1400);
    const mode = safeText(options.approvalMode).trim();
    const payload = {};
    if (mode) payload.approvalMode = mode;
    const res = await apiFetch('/api/inbox/automation/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || 'Marcus automation run failed');
    if (data?.store && typeof data.store === 'object') applyStore(data.store);
    await fetchState({ background: false });
    return data;
}

async function coachNextInboxStep() {
    const visible = getInboxItems();
    const candidates = visible.filter((x) => String(x?.status || '').trim().toLowerCase() === 'new');
    if (!candidates.length) throw new Error('No new inbox items to coach right now');

    const target = candidates.find((x) => !safeText(x?.projectId).trim()) || candidates[0];
    const inboxId = safeText(target?.id).trim();
    if (!inboxId) throw new Error('Missing inbox item for coaching');

    await runMarcusInboxTriage({ onlyNew: false, includeArchived: false, limit: 200 });
    const rec = getMarcusInboxRecommendation(inboxId);
    if (!rec) throw new Error('No recommendation available for next inbox item');

    const projectName = safeText(rec?.project?.projectName).trim() || 'no project recommendation';
    const topTask = safeText(rec?.tasks?.[0]?.title).trim() || '';
    const lines = [
        'Marcus Coach ready.',
        `Inbox: ${previewText(safeText(target?.text), 100) || inboxId}`,
        `Project suggestion: ${projectName}`,
        `Top task: ${topTask || 'none'}`,
        '',
        'Apply this now? (link project + create top task)',
    ];
    const ok = window.confirm(lines.join('\n'));
    if (!ok) return { applied: false, inboxId };

    let linked = false;
    let tasksCreated = 0;
    const recProjectId = safeText(rec?.project?.projectId).trim();
    if (recProjectId && !safeText(target?.projectId).trim()) {
        await linkInboxItemToProject(inboxId, recProjectId);
        linked = true;
    }
    if (topTask) {
        const result = await createTaskFromMarcusRecommendation(inboxId, rec);
        tasksCreated = Number(result?.created || 0);
    }
    if (!linked && !tasksCreated) {
        await patchInboxItem(inboxId, { status: 'Triaged' });
    }

    await fetchMarcusAutomationDigest().catch(() => {});
    await fetchState({ background: false });
    return { applied: true, inboxId, linked, tasksCreated };
}

async function fetchMarcusAutomationDigest() {
    const res = await apiFetch('/api/inbox/automation/digest');
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || 'Failed to load Marcus digest');
    const items = Array.isArray(data?.items) ? data.items : [];
    state.inboxAutomationDigest = {
        items,
        loading: false,
        loadedAt: Date.now(),
        error: '',
    };
    const nextSel = { ...(state.inboxDigestSelectionsById || {}) };
    for (const item of items) {
        const id = safeText(item?.id).trim();
        if (!id || nextSel[id]) continue;
        const tasks = Array.isArray(item?.tasks) ? item.tasks : [];
        nextSel[id] = {
            acceptProjectLink: !!safeText(item?.projectId).trim(),
            taskChecks: tasks.map(() => true),
        };
    }
    state.inboxDigestSelectionsById = nextSel;
    return { ...data, items };
}

async function decideMarcusAutomationDigest(digestId, decision) {
    const id = safeText(digestId).trim();
    if (!id) throw new Error('Missing digest id');
    const payload = (decision && typeof decision === 'object') ? decision : {};
    const res = await apiFetch(`/api/inbox/automation/digest/${encodeURIComponent(id)}/decision`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || 'Failed to decide digest recommendation');
    if (data?.store && typeof data.store === 'object') applyStore(data.store);
    await fetchMarcusAutomationDigest();
    await fetchState({ background: false });
    return data;
}

async function createTaskFromMarcusRecommendation(inboxId, recommendation) {
    const id = safeText(inboxId).trim();
    if (!id) throw new Error('Missing inbox id');
    const rec = recommendation && typeof recommendation === 'object' ? recommendation : {};
    const topTask = Array.isArray(rec.tasks) && rec.tasks.length ? rec.tasks[0] : null;
    if (!topTask) throw new Error('No suggested task available');
    const payload = {
        title: safeText(topTask.title).trim() || 'Inbox follow-up',
        owner: '',
        priority: [1, 2, 3].includes(Number(topTask.priority)) ? Number(topTask.priority) : 2,
    };
    const pid = safeText(rec?.project?.projectId).trim();
    if (pid) payload.projectId = pid;
    await convertInboxItem(id, 'task', payload);
    return { created: 1 };
}

async function createAllTasksFromMarcusRecommendation(inboxId, recommendation) {
    const id = safeText(inboxId).trim();
    if (!id) throw new Error('Missing inbox id');
    const rec = recommendation && typeof recommendation === 'object' ? recommendation : {};
    const tasks = Array.isArray(rec.tasks) ? rec.tasks.filter((t) => safeText(t?.title).trim()) : [];
    if (!tasks.length) throw new Error('No suggested tasks available');

    const pid = safeText(rec?.project?.projectId).trim();
    let created = 0;

    for (const task of tasks) {
        const payload = {
            title: safeText(task?.title).trim() || 'Inbox follow-up',
            owner: '',
            priority: [1, 2, 3].includes(Number(task?.priority)) ? Number(task.priority) : 2,
        };
        if (pid) payload.projectId = pid;
        await convertInboxItem(id, 'task', payload);
        created += 1;
    }

    return { created };
}

async function openGoogleAuthWindow() {
    const r = await apiFetch('/api/integrations/google/auth-url');
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.url) {
        throw new Error(data?.error || 'Failed to start Google OAuth');
    }

    // Open popup directly to the auth URL (avoids getting stuck on about:blank).
    const w = window.open(data.url, '_blank', 'noopener,noreferrer');
    if (!w) {
        // Popup likely blocked — fall back to same-tab navigation.
        window.location.href = data.url;
        return;
    }

    alert('Google Calendar connect opened in a new tab. Finish it, then return here and refresh status (or click “Show next 7 days”).');
}

async function openSlackAuthWindow() {
    const r = await apiFetch('/api/integrations/slack/auth-url');
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.url) {
        throw new Error(data?.error || 'Failed to start Slack OAuth');
    }

    const w = window.open(data.url, '_blank', 'noopener,noreferrer');
    if (!w) {
        window.location.href = data.url;
        return;
    }

    alert('Slack connect opened in a new tab. Finish it, then return here and refresh Settings.');
}

function renderSettings(container) {
    const titleEl = document.getElementById("page-title");
    if(titleEl) titleEl.innerText = "Settings";

    const wrap = document.createElement('div');
    wrap.className = 'p-6 max-w-7xl mx-auto';

    const section = (title, subtitle) => {
        const el = document.createElement('div');
        el.className = 'border border-ops-border rounded-xl bg-ops-surface/40 p-6 mb-6';
        el.innerHTML = `
            <div class="flex items-start justify-between gap-4">
                <div>
                    <div class="text-white text-base font-semibold tracking-tight">${title}</div>
                    <div class="text-xs text-ops-light mt-1">${subtitle || ''}</div>
                </div>
            </div>
            <div class="mt-4" data-slot="body"></div>
        `;
        return el;
    };

    const settingsPane = safeText(state.settingsPane).trim().toLowerCase();

    const buildSettingsDeepLink = (pane) => {
        try {
            const u = new URL(window.location.href);
            u.searchParams.set('view', 'settings');
            if (pane) u.searchParams.set('pane', String(pane));
            else u.searchParams.delete('pane');
            u.hash = '';
            return `${u.pathname}?${u.searchParams.toString()}`;
        } catch {
            return pane ? `/?view=settings&pane=${encodeURIComponent(String(pane))}` : '/?view=settings';
        }
    };

    const projectsSettingsHref = buildSettingsDeepLink('projects');
    const pagesSettingsHref = buildSettingsDeepLink('pages');
    const fullSettingsHref = buildSettingsDeepLink('');

    // Tab strip (uses deep-link panes).
    {
        const active = settingsPane || '';
        const tab = (label, href, isActive, extraAttrs = '') => {
            const base = 'px-3 py-2 rounded border text-xs font-mono transition-colors';
            const cls = isActive
                ? `${base} bg-blue-600/20 border-blue-600/40 text-blue-200`
                : `${base} bg-ops-bg border-ops-border text-ops-light hover:text-white`;
            return `<a href="${escapeHtml(href)}" ${extraAttrs} class="${cls}">${escapeHtml(label)}</a>`;
        };

        const tabs = document.createElement('div');
        tabs.className = 'mb-6 flex flex-wrap gap-2';
        tabs.innerHTML = `
            ${tab('General', fullSettingsHref, !active)}
            ${tab('Pages', pagesSettingsHref, active === 'pages')}
            ${tab('Projects', projectsSettingsHref, active === 'projects', 'target="_blank" rel="noopener noreferrer"')}
        `;
        wrap.appendChild(tabs);
    }

    // Dedicated heavy pane: Projects (bulk delete). Keeping it isolated prevents Settings from OOM'ing
    // when there are lots of projects.
    if (settingsPane === 'projects') {
        if (titleEl) titleEl.innerText = 'Settings • Projects';

        const projectsSection = section('Projects', 'Bulk select projects and remove them. This also removes their tasks, notes, chat, scratchpad, and communications.');
        const projectsBody = projectsSection.querySelector('[data-slot="body"]');
        const projects = Array.isArray(state.projects) ? state.projects : [];
        const selectedMap = (state.bulkProjectDeleteSelectedById && typeof state.bulkProjectDeleteSelectedById === 'object') ? state.bulkProjectDeleteSelectedById : {};

        const renderProjectsBody = () => {
            const selectedIds = Object.keys(selectedMap).filter((id) => selectedMap[id]);
            const limit = Math.max(50, Number(state.projectsSettingsLimit) || 200);
            const shown = projects.slice(0, limit);
            const remaining = Math.max(0, projects.length - shown.length);

            projectsBody.innerHTML = `
                <div class="flex items-center justify-between gap-3 flex-wrap">
                    <div class="text-xs text-ops-light">Projects (<span id="projects-total">${projects.length}</span>) • Showing (<span id="projects-showing">${shown.length}</span>) • Selected (<span id="projects-selected">${selectedIds.length}</span>)</div>
                    <div class="flex gap-2 flex-wrap">
                        <a href="${escapeHtml(fullSettingsHref)}" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Back to Settings</a>
                        ${remaining > 0 ? `<button id="btn-projects-more" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Load ${Math.min(200, remaining)} more</button>` : ''}
                        <button id="btn-projects-clear" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white" ${selectedIds.length ? '' : 'disabled'}>Clear selection</button>
                        <button id="btn-projects-delete" class="px-3 py-2 rounded bg-red-600 text-white text-xs hover:bg-red-500" ${selectedIds.length ? '' : 'disabled'}>Delete selected</button>
                    </div>
                </div>
                <div class="mt-3 space-y-2" id="projects-list"></div>
                <div class="text-[11px] text-ops-light mt-3">Tip: This delete is permanent (it removes the project and its related data).</div>
            `;

            const listEl = projectsBody.querySelector('#projects-list');
            if (listEl) {
                if (!shown.length) {
                    listEl.innerHTML = `<div class="text-xs text-ops-light italic">No projects yet.</div>`;
                } else {
                    // Build rows without creating a single massive HTML string.
                    const frag = document.createDocumentFragment();
                    for (const p of shown) {
                        const id = safeText(p?.id);
                        const nm = safeText(p?.name);
                        const ty = safeText(p?.type) || 'Other';
                        const status = safeText(p?.status) || (isArchivedProject(p) ? 'Archived' : 'Active');
                        const due = safeText(p?.dueDate);
                        const checked = selectedMap[id] ? 'checked' : '';

                        const label = document.createElement('label');
                        label.className = 'flex items-start gap-3 border border-ops-border rounded-lg bg-ops-bg/30 p-3 cursor-pointer';
                        label.innerHTML = `
                            <input type="checkbox" data-proj-sel="${escapeHtml(id)}" class="mt-0.5" ${checked} />
                            <div class="min-w-0">
                                <div class="text-white text-sm font-semibold truncate">${escapeHtml(nm || '(Unnamed)')}</div>
                                <div class="text-[11px] text-ops-light mt-0.5">${escapeHtml(ty)} • ${escapeHtml(status)}${due ? ` • Due ${escapeHtml(due)}` : ''}</div>
                            </div>
                        `;
                        frag.appendChild(label);
                    }
                    listEl.appendChild(frag);
                }
            }

            const updateProjectBulkDeleteUi = () => {
                const selected = Object.keys(state.bulkProjectDeleteSelectedById || {}).filter((id) => state.bulkProjectDeleteSelectedById[id]);
                const selectedCount = selected.length;
                const selectedEl = projectsBody.querySelector('#projects-selected');
                if (selectedEl) selectedEl.textContent = String(selectedCount);
                const btnClear = projectsBody.querySelector('#btn-projects-clear');
                const btnDel = projectsBody.querySelector('#btn-projects-delete');
                if (btnClear) btnClear.disabled = selectedCount === 0;
                if (btnDel) btnDel.disabled = selectedCount === 0;
            };

            projectsBody.querySelectorAll('input[type="checkbox"][data-proj-sel]').forEach((cb) => {
                cb.addEventListener('change', () => {
                    const id = safeText(cb.getAttribute('data-proj-sel'));
                    state.bulkProjectDeleteSelectedById[id] = Boolean(cb.checked);
                    updateProjectBulkDeleteUi();
                });
            });

            const btnMore = projectsBody.querySelector('#btn-projects-more');
            if (btnMore) {
                btnMore.onclick = () => {
                    const next = Math.min(projects.length, (Math.max(50, Number(state.projectsSettingsLimit) || 200) + 200));
                    state.projectsSettingsLimit = next;
                    renderSettings(container);
                };
            }

            const btnClearProjects = projectsBody.querySelector('#btn-projects-clear');
            if (btnClearProjects) {
                btnClearProjects.onclick = () => {
                    state.bulkProjectDeleteSelectedById = {};
                    projectsBody.querySelectorAll('input[type="checkbox"][data-proj-sel]').forEach((cb) => {
                        cb.checked = false;
                    });
                    updateProjectBulkDeleteUi();
                };
            }

            const btnDeleteProjects = projectsBody.querySelector('#btn-projects-delete');
            if (btnDeleteProjects) {
                btnDeleteProjects.onclick = async () => {
                    const ids = Object.keys(state.bulkProjectDeleteSelectedById || {}).filter((id) => state.bulkProjectDeleteSelectedById[id]);
                    if (!ids.length) return;
                    if (!confirm(`Delete ${ids.length} project(s)? This will also remove their tasks, notes, chat, scratchpad, and communications.`)) return;
                    const prevLabel = btnDeleteProjects.textContent;
                    btnDeleteProjects.disabled = true;
                    btnDeleteProjects.textContent = 'Deleting…';
                    try {
                        const data = await withRevisionRetry(() => apiJson('/api/projects/bulk-delete', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ baseRevision: state.revision, projectIds: ids })
                        }));

                        const nextStore = data?.store && typeof data.store === 'object' ? data.store : data;
                        applyStore(nextStore);

                        if (ids.includes(String(state.currentProjectId || ''))) {
                            state.currentProjectId = null;
                            state.currentView = 'dashboard';
                        }

                        state.bulkProjectDeleteSelectedById = {};
                        renderNav();
                        renderSettings(container);
                    } catch (e) {
                        alert(e?.message || 'Failed to delete projects');
                    } finally {
                        btnDeleteProjects.disabled = false;
                        btnDeleteProjects.textContent = prevLabel;
                    }
                };
            }
        };

        renderProjectsBody();
        wrap.appendChild(projectsSection);
        container.appendChild(wrap);
        return;
    }

    // Dedicated pane: Pages (choose which blocks appear on pages)
    if (settingsPane === 'pages') {
        if (titleEl) titleEl.innerText = 'Settings • Pages';

        const pagesSection = section('Pages', 'Choose which blocks appear on Dashboard and God View.');
        const pagesBody = pagesSection.querySelector('[data-slot="body"]');

        const defaults = {
            dashboard: { missionControl: true, newProjectIntake: true, commsRadar: true, deliveryBoard: true },
            godview: { businessesRadar: true, marcusBrief: true, upcoming: true, teamComms: true, globalFocus: true },
        };

        const prefs = getPageElementsPreferences(state.settings);

        const ck = (id, label, checked, hint) => `
            <label class="flex items-start gap-3 border border-ops-border rounded-lg bg-ops-bg/30 p-3 cursor-pointer">
                <input id="${escapeHtml(id)}" type="checkbox" class="mt-1" ${checked ? 'checked' : ''} />
                <div class="min-w-0">
                    <div class="text-white text-sm font-semibold">${escapeHtml(label)}</div>
                    <div class="text-[11px] text-ops-light mt-0.5">${escapeHtml(hint || '')}</div>
                </div>
            </label>
        `;

        pagesBody.innerHTML = `
            <div class="flex items-center justify-between gap-3 flex-wrap">
                <div class="text-xs text-ops-light">These settings affect what you see (and what Marcus can reference visually) — they don’t delete data.</div>
                <a href="${escapeHtml(fullSettingsHref)}" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Back to Settings</a>
            </div>

            <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
                <div>
                    <div class="text-white text-sm font-semibold">Dashboard</div>
                    <div class="text-[11px] text-ops-light mt-1">Toggle major blocks to keep the dashboard focused.</div>
                    <div class="mt-3 space-y-2">
                        ${ck('pe-dash-mission', 'Mission Control', prefs.dashboard.missionControl, 'Greeting, stats, Marcus bar, and alerts.')}
                        ${ck('pe-dash-intake', 'New Project Intake', prefs.dashboard.newProjectIntake, 'Quick-add bar (new item + new project).')}
                        ${ck('pe-dash-comms', 'Comms Radar', prefs.dashboard.commsRadar, 'Inbox Radar + comms feed (Activity/Slack/Inbox/Team).')}
                        ${ck('pe-dash-delivery', 'Delivery Board', prefs.dashboard.deliveryBoard, 'Calendar + due date panels + focus + future projects.')}
                    </div>
                </div>
                <div>
                    <div class="text-white text-sm font-semibold">God View</div>
                    <div class="text-[11px] text-ops-light mt-1">Global cross-business overview sections.</div>
                    <div class="mt-3 space-y-2">
                        ${ck('pe-god-radar', 'Businesses Radar', prefs.godview.businessesRadar, 'The business cards radar grid.')}
                        ${ck('pe-god-brief', 'Marcus Brief', prefs.godview.marcusBrief, 'Shows latest scheduled brief items.')}
                        ${ck('pe-god-upcoming', 'Upcoming', prefs.godview.upcoming, 'Google Calendar upcoming panel.')}
                        ${ck('pe-god-team', 'Team Comms', prefs.godview.teamComms, 'Slack/team section.')}
                        ${ck('pe-god-focus', 'Global Focus', prefs.godview.globalFocus, 'Urgent projects list.')}
                    </div>
                </div>
            </div>

            <div class="flex gap-2 mt-4 flex-wrap">
                <button id="btn-pages-save" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save</button>
                <button id="btn-pages-reset" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Reset defaults</button>
            </div>
        `;

        const getBool = (id, fallback) => {
            const el = pagesBody.querySelector(`#${CSS.escape(id)}`);
            if (!el) return !!fallback;
            return !!el.checked;
        };

        const btnSave = pagesBody.querySelector('#btn-pages-save');
        if (btnSave) {
            btnSave.onclick = async () => {
                btnSave.disabled = true;
                try {
                    const next = {
                        dashboard: {
                            missionControl: getBool('pe-dash-mission', defaults.dashboard.missionControl),
                            newProjectIntake: getBool('pe-dash-intake', defaults.dashboard.newProjectIntake),
                            commsRadar: getBool('pe-dash-comms', defaults.dashboard.commsRadar),
                            deliveryBoard: getBool('pe-dash-delivery', defaults.dashboard.deliveryBoard),
                        },
                        godview: {
                            businessesRadar: getBool('pe-god-radar', defaults.godview.businessesRadar),
                            marcusBrief: getBool('pe-god-brief', defaults.godview.marcusBrief),
                            upcoming: getBool('pe-god-upcoming', defaults.godview.upcoming),
                            teamComms: getBool('pe-god-team', defaults.godview.teamComms),
                            globalFocus: getBool('pe-god-focus', defaults.godview.globalFocus),
                        },
                    };

                    state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {};
                    state.settings.pageElements = next;
                    await saveSettingsPatch({ pageElements: next });
                    renderSettings(container);
                } catch (e) {
                    alert(e?.message || 'Failed to save page settings');
                } finally {
                    btnSave.disabled = false;
                }
            };
        }

        const btnReset = pagesBody.querySelector('#btn-pages-reset');
        if (btnReset) {
            btnReset.onclick = async () => {
                btnReset.disabled = true;
                try {
                    state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {};
                    state.settings.pageElements = defaults;
                    await saveSettingsPatch({ pageElements: defaults });
                    renderSettings(container);
                } catch (e) {
                    alert(e?.message || 'Failed to reset page settings');
                } finally {
                    btnReset.disabled = false;
                }
            };
        }

        wrap.appendChild(pagesSection);
        container.appendChild(wrap);
        return;
    }

    // Access (admin token)
    const access = section('Access', 'If this server is protected (ADMIN_TOKEN), paste it once here. Stored locally in this browser.');
    const accessBody = access.querySelector('[data-slot="body"]');
    const storedAdminToken = getStoredAdminToken();
    const tokenHint = storedAdminToken ? `••••${escapeHtml(storedAdminToken.slice(-4))}` : 'None';
    accessBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">Admin Token</label>
                <input id="set-admin-token" type="password" autocomplete="off" placeholder="Paste ADMIN_TOKEN..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Current: ${tokenHint}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Server Auth Status</label>
                <div id="auth-status-line" class="mt-1 text-[11px] font-mono text-ops-light border border-ops-border rounded px-3 py-2 bg-ops-bg/40">Status: checking…</div>
                <div class="text-[11px] text-ops-light mt-1">If prompts are blocked, use this panel instead.</div>
            </div>
        </div>
        <div class="flex gap-2 mt-4">
            <button id="btn-save-auth" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Token</button>
            <button id="btn-clear-auth" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Clear Token</button>
        </div>
    `;
    wrap.appendChild(access);

    // UI
    const uiTuneSection = section('UI', 'Adjust title size and density for faster scanning.');
    const uiTuneBody = uiTuneSection.querySelector('[data-slot="body"]');
    const currentTitleScale = safeText(state.settings?.uiTitleScale).trim().toLowerCase() || 'md';
    const currentDensity = safeText(state.settings?.uiDensity).trim().toLowerCase() || 'comfortable';
    uiTuneBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">Title size</label>
                <select id="ui-title-scale" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm">
                    <option value="sm" ${currentTitleScale === 'sm' ? 'selected' : ''}>Small</option>
                    <option value="md" ${currentTitleScale === 'md' ? 'selected' : ''}>Medium (default)</option>
                    <option value="lg" ${currentTitleScale === 'lg' ? 'selected' : ''}>Large</option>
                    <option value="xl" ${currentTitleScale === 'xl' ? 'selected' : ''}>Extra large</option>
                </select>
                <div class="text-[11px] text-ops-light mt-1">Affects page title + dashboard section headers.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Density</label>
                <select id="ui-density" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm">
                    <option value="comfortable" ${currentDensity === 'comfortable' ? 'selected' : ''}>Comfortable (default)</option>
                    <option value="compact" ${currentDensity === 'compact' ? 'selected' : ''}>Compact</option>
                </select>
                <div class="text-[11px] text-ops-light mt-1">Controls dashboard padding + spacing.</div>
            </div>
        </div>
        <div class="flex gap-2 mt-4 flex-wrap">
            <button id="btn-ui-save" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save UI</button>
            <button id="btn-ui-reset" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Reset defaults</button>
        </div>
    `;
    wrap.appendChild(uiTuneSection);

    // Wire UI controls
    const uiTitleSel = uiTuneBody.querySelector('#ui-title-scale');
    const uiDensitySel = uiTuneBody.querySelector('#ui-density');
    const btnUiSave = uiTuneBody.querySelector('#btn-ui-save');
    const btnUiReset = uiTuneBody.querySelector('#btn-ui-reset');

    const previewUi = () => {
        const titleScale = safeText(uiTitleSel?.value).trim().toLowerCase();
        const density = safeText(uiDensitySel?.value).trim().toLowerCase();
        applyUiPreferencesFromSettings({ ...(state.settings || {}), uiTitleScale: titleScale, uiDensity: density });
    };

    if (uiTitleSel) uiTitleSel.addEventListener('change', previewUi);
    if (uiDensitySel) uiDensitySel.addEventListener('change', previewUi);

    if (btnUiSave) {
        btnUiSave.onclick = async () => {
            btnUiSave.disabled = true;
            const titleScale = safeText(uiTitleSel?.value).trim().toLowerCase() || 'md';
            const density = safeText(uiDensitySel?.value).trim().toLowerCase() || 'comfortable';
            try {
                state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {};
                state.settings.uiTitleScale = titleScale;
                state.settings.uiDensity = density;
                applyUiPreferencesFromSettings(state.settings);
                await saveSettingsPatch({ uiTitleScale: titleScale, uiDensity: density });
                renderSettings(container);
            } catch (e) {
                alert(e?.message || 'Failed to save UI settings');
            } finally {
                btnUiSave.disabled = false;
            }
        };
    }

    if (btnUiReset) {
        btnUiReset.onclick = async () => {
            const defaults = { uiTitleScale: 'md', uiDensity: 'comfortable' };
            if (uiTitleSel) uiTitleSel.value = defaults.uiTitleScale;
            if (uiDensitySel) uiDensitySel.value = defaults.uiDensity;
            previewUi();
            try {
                await saveSettingsPatch(defaults);
                renderSettings(container);
            } catch (e) {
                alert(e?.message || 'Failed to reset UI settings');
            }
        };
    }

    // Businesses
    const bizSection = section('Businesses', 'Each business is a separate workspace (projects, tasks, inbox).');
    const bizBody = bizSection.querySelector('[data-slot="body"]');
    const businesses = Array.isArray(state.businesses) && state.businesses.length ? state.businesses : [{ key: 'personal', name: 'Personal' }];
    const activeKey = normalizeBusinessKey(state.activeBusinessKey) || 'personal';

    bizBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">Active business</label>
                <select id="biz-active" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm">
                    ${businesses.map((b) => {
                        const k = normalizeBusinessKey(b.key);
                        const nm = escapeHtml(safeText(b.name).trim() || k);
                        const sel = k === activeKey ? 'selected' : '';
                        return `<option value="${escapeHtml(k)}" ${sel}>${nm}</option>`;
                    }).join('')}
                </select>
                <div class="text-[11px] text-ops-light mt-1">Switching changes the whole workspace.</div>
                <div class="flex gap-2 mt-3 flex-wrap">
                    <button id="btn-biz-switch" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Switch</button>
                    <button id="btn-biz-add" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Add business</button>
                    <button id="btn-biz-delete" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-red-300">Delete</button>
                    <button id="btn-biz-save-routing" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Save phone routing</button>
                </div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Businesses</label>
                <div class="mt-1 space-y-2">
                    ${businesses.map((b) => {
                        const k = normalizeBusinessKey(b.key);
                        const nm = escapeHtml(safeText(b.name).trim() || k);
                        const phones = Array.isArray(b.phoneNumbers) ? b.phoneNumbers : [];
                        const badge = k === activeKey ? '<span class="ml-2 text-[10px] px-2 py-0.5 rounded bg-blue-600/20 border border-blue-600/40 text-blue-200">Active</span>' : '';
                        return `
                            <div class="border border-ops-border rounded-lg bg-ops-bg/30 p-3">
                                <div class="flex items-center justify-between gap-2">
                                    <div class="min-w-0">
                                        <div class="text-white text-sm font-semibold truncate">${nm}${badge}</div>
                                        <div class="text-[11px] text-ops-light mt-0.5 font-mono">${escapeHtml(k)}</div>
                                        <div class="text-[11px] text-ops-light mt-2">Phone numbers (inbound routing)</div>
                                        <textarea rows="2" data-biz-phones="${escapeHtml(k)}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="+15551234567\n+15557654321">${escapeHtml(phones.join('\n'))}</textarea>
                                        <div class="text-[11px] text-ops-light/70 mt-1">One per line (or comma-separated).</div>
                                    </div>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>
        </div>
    `;
    wrap.appendChild(bizSection);

    // Airtable (per-business)
    const at = section('Airtable (this business)', 'Sync Clients into Contacts, and Revision Requests into Projects for this business workspace.');
    const atBody = at.querySelector('[data-slot="body"]');
    atBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">Airtable Personal Access Token (PAT)</label>
                <input id="set-airtable-pat" type="password" autocomplete="off" placeholder="pat..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Stored server-side for the active business only.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Airtable link (paste)</label>
                <input id="set-airtable-link" type="text" autocomplete="off" placeholder="https://airtable.com/app.../tbl.../viw..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">We’ll extract Base/Table/View IDs automatically.</div>
            </div>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
            <div>
                <label class="text-xs text-ops-light">Base ID</label>
                <input id="set-airtable-base" type="text" autocomplete="off" placeholder="app..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm font-mono" />
            </div>
            <div>
                <label class="text-xs text-ops-light">Clients table ID</label>
                <input id="set-airtable-table" type="text" autocomplete="off" placeholder="tbl..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm font-mono" />
            </div>
            <div>
                <label class="text-xs text-ops-light">Clients view ID (optional)</label>
                <input id="set-airtable-view" type="text" autocomplete="off" placeholder="viw..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm font-mono" />
            </div>
        </div>

        <div class="mt-5 pt-5 border-t border-ops-border">
            <div class="text-white text-sm font-semibold">Revision requests → Projects</div>
            <div class="text-[11px] text-ops-light mt-1">Paste a link to your Revision Requests table/view. Sync will create/update Revision projects (deduped) for this business.</div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mt-3">
                <div>
                    <label class="text-xs text-ops-light">Revision requests link (paste)</label>
                    <input id="set-airtable-req-link" type="text" autocomplete="off" placeholder="https://airtable.com/app.../tbl.../viw..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                    <div class="text-[11px] text-ops-light mt-1">We’ll extract Table/View IDs automatically.</div>
                </div>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label class="text-xs text-ops-light">Requests table ID</label>
                        <input id="set-airtable-req-table" type="text" autocomplete="off" placeholder="tbl..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm font-mono" />
                    </div>
                    <div>
                        <label class="text-xs text-ops-light">Requests view ID (optional)</label>
                        <input id="set-airtable-req-view" type="text" autocomplete="off" placeholder="viw..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm font-mono" />
                    </div>
                </div>
            </div>

            <div class="flex flex-wrap gap-2 mt-3">
                <button id="btn-test-airtable-requests" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Test (preview requests)</button>
                <button id="btn-sync-airtable-requests" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Sync requests → projects</button>
            </div>
        </div>

        <div class="text-[11px] text-ops-light mt-2" id="airtable-status-line">Status: checking...</div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-airtable" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Airtable</button>
            <button id="btn-test-airtable" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Test (preview clients)</button>
            <button id="btn-sync-airtable" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Sync clients → contacts</button>
        </div>
        <div id="airtable-output" class="mt-3 hidden bg-ops-bg border border-ops-border rounded px-3 py-2 text-xs text-ops-light font-mono whitespace-pre-wrap"></div>
    `;
    wrap.appendChild(at);

    const bizSelect = bizBody.querySelector('#biz-active');
    const btnBizSwitch = bizBody.querySelector('#btn-biz-switch');
    const btnBizAdd = bizBody.querySelector('#btn-biz-add');
    const btnBizDelete = bizBody.querySelector('#btn-biz-delete');
    const btnBizSaveRouting = bizBody.querySelector('#btn-biz-save-routing');

    if (btnBizSwitch && bizSelect) {
        btnBizSwitch.onclick = async () => {
            const key = normalizeBusinessKey(bizSelect.value);
            await setActiveBusinessKey(key);
        };
    }

    if (btnBizAdd) {
        btnBizAdd.onclick = async () => {
            const name = safeText(window.prompt('Business name (e.g., Scoop Doggy Logs):') || '').trim();
            if (!name) return;
            const key = normalizeBusinessKey(name);
            if (!key) {
                alert('Could not generate a key from that name.');
                return;
            }
            if (businesses.some((b) => normalizeBusinessKey(b.key) === key)) {
                alert('That business already exists.');
                return;
            }
            const nextBusinesses = [...businesses, { key, name, phoneNumbers: [] }];
            try {
                const resp = await apiJson('/api/businesses', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ businesses: nextBusinesses, activeBusinessKey: state.activeBusinessKey })
                });
                applyBusinessConfig(resp);
                renderNav();
                renderSettings(container);
            } catch (e) {
                alert(e?.message || 'Failed to add business');
            }
        };
    }

    if (btnBizSaveRouting) {
        btnBizSaveRouting.onclick = async () => {
            const inputs = Array.from(bizBody.querySelectorAll('textarea[data-biz-phones]'));
            const nextBusinesses = businesses.map((b) => {
                const k = normalizeBusinessKey(b.key);
                const ta = inputs.find((el) => normalizeBusinessKey(el.getAttribute('data-biz-phones')) === k);
                const raw = safeText(ta?.value || '').trim();
                const phoneNumbers = raw ? raw.split(/[\n,;]+/g).map((s) => safeText(s).trim()).filter(Boolean) : [];
                return { ...b, key: k, phoneNumbers };
            });
            btnBizSaveRouting.disabled = true;
            try {
                const resp = await apiJson('/api/businesses', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ businesses: nextBusinesses, activeBusinessKey: state.activeBusinessKey })
                });
                applyBusinessConfig(resp);
                renderNav();
                renderSettings(container);
            } catch (e) {
                alert(e?.message || 'Failed to save phone routing');
            } finally {
                btnBizSaveRouting.disabled = false;
            }
        };
    }

    if (btnBizDelete && bizSelect) {
        btnBizDelete.onclick = async () => {
            const key = normalizeBusinessKey(bizSelect.value);
            if (!key) return;
            if (key === 'personal') {
                alert('Personal cannot be deleted.');
                return;
            }
            const b = businesses.find((x) => normalizeBusinessKey(x.key) === key);
            const label = safeText(b?.name).trim() || key;
            if (!confirm(`Delete business “${label}”? This does not delete the data file automatically.`)) return;
            const nextBusinesses = businesses.filter((x) => normalizeBusinessKey(x.key) !== key);
            const nextActive = (normalizeBusinessKey(state.activeBusinessKey) === key) ? 'personal' : state.activeBusinessKey;
            try {
                const resp = await apiJson('/api/businesses', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ businesses: nextBusinesses, activeBusinessKey: nextActive })
                });
                applyBusinessConfig(resp);
                renderNav();
                renderSettings(container);
            } catch (e) {
                alert(e?.message || 'Failed to delete business');
            }
        };
    }

    // AI
    const ai = section('AI', 'Configure the API key/model used by Marcus.');
    const aiBody = ai.querySelector('[data-slot="body"]');
    const currentOpenAiModel = String(state.settings.openaiModel || '').trim() || 'gpt-4o-mini';
    const routeDefs = [
        { key: 'marcusChat', label: 'Marcus Chat' },
        { key: 'operatorBio', label: 'Operator Bio' },
        { key: 'projectAssistant', label: 'Project Assistant' },
        { key: 'dashboardPreview', label: 'Dashboard Preview' },
    ];
    const routeOpenAiModels = routeDefs
        .map((r) => {
            const entry = (state.settings?.aiRoutes && typeof state.settings.aiRoutes === 'object' && state.settings.aiRoutes[r.key] && typeof state.settings.aiRoutes[r.key] === 'object')
                ? state.settings.aiRoutes[r.key]
                : {};
            const provider = String(entry.provider || 'openai').trim().toLowerCase() || 'openai';
            return provider === 'openai' ? String(entry.model || '').trim() : '';
        })
        .filter(Boolean);

    const openAiModelOptions = buildOpenAiModelOptions([currentOpenAiModel, ...routeOpenAiModels]);
    const modelOptionHtml = openAiModelOptions
        .map((m) => `<option value="${escapeHtml(m)}" ${m === currentOpenAiModel ? 'selected' : ''}>${escapeHtml(m)}</option>`)
        .join('');

    const catalogState = state.openAiModelsCatalog && typeof state.openAiModelsCatalog === 'object'
        ? state.openAiModelsCatalog
        : { items: [], loading: false, error: '', fetchedAt: 0, source: 'fallback' };
    const fetchedTime = Number(catalogState.fetchedAt) > 0
        ? new Date(Number(catalogState.fetchedAt)).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
        : '';
    const modelCatalogStatus = catalogState.loading
        ? 'Model catalog: refreshing…'
        : catalogState.error
            ? `Model catalog: ${catalogState.error}`
            : `Model catalog: ${openAiModelOptions.length} models (${catalogState.source || 'fallback'}${fetchedTime ? ` • ${fetchedTime}` : ''})`;
    const routeRowsHtml = routeDefs.map((r) => {
        const entry = (state.settings?.aiRoutes && typeof state.settings.aiRoutes === 'object' && state.settings.aiRoutes[r.key] && typeof state.settings.aiRoutes[r.key] === 'object')
            ? state.settings.aiRoutes[r.key]
            : {};
        const provider = String(entry.provider || 'openai').trim().toLowerCase() || 'openai';
        const model = provider === 'openai' ? String(entry.model || '').trim() : '';
        const options = [`<option value="" ${model ? '' : 'selected'}>Use global (${escapeHtml(currentOpenAiModel)})</option>`]
            .concat(openAiModelOptions.map((m) => `<option value="${escapeHtml(m)}" ${m === model ? 'selected' : ''}>${escapeHtml(m)}</option>`))
            .join('');
        return `
            <div>
                <label class="text-xs text-ops-light">${escapeHtml(r.label)} model</label>
                <select id="set-openai-route-model-${escapeHtml(r.key)}" ${provider !== 'openai' ? 'disabled' : ''} class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm ${provider !== 'openai' ? 'opacity-60' : ''}">
                    ${options}
                </select>
                <div class="text-[10px] text-ops-light mt-1">Provider: ${escapeHtml(provider)}</div>
            </div>
        `;
    }).join('');
    aiBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">OpenAI API Key (stored locally)</label>
                <input id="set-openai-key" type="password" autocomplete="off" placeholder="sk-..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Current: ${maskHint(state.settings.openaiKeyHint)}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">OpenAI Model</label>
                <select id="set-openai-model-select" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm">
                    ${modelOptionHtml}
                </select>
                <input id="set-openai-model" type="text" placeholder="Custom model ID" value="${escapeHtml(currentOpenAiModel)}" class="mt-2 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="mt-2 flex items-center justify-between gap-2">
                    <div class="text-[11px] text-ops-light">${escapeHtml(modelCatalogStatus)}</div>
                    <button id="btn-refresh-openai-models" class="px-2.5 py-1 rounded bg-ops-bg border border-ops-border text-ops-light text-[11px] hover:text-white" ${catalogState.loading ? 'disabled' : ''}>Refresh list</button>
                </div>
                <label class="mt-2 inline-flex items-center gap-2 text-[11px] text-ops-light">
                    <input id="set-openai-apply-routes" type="checkbox" class="accent-blue-500" checked />
                    Apply model to all OpenAI routes
                </label>
                <div class="text-[11px] text-ops-light mt-1">Effective model: ${escapeHtml(currentOpenAiModel)} • AI Enabled: ${state.settings.aiEnabled ? 'Yes' : 'No'}</div>
            </div>
        </div>
        <div class="mt-4 border border-ops-border rounded p-3 bg-ops-bg/30">
            <div class="text-xs text-ops-light mb-2">Per-route OpenAI models</div>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                ${routeRowsHtml}
            </div>
        </div>
        <div class="flex gap-2 mt-4">
            <button id="btn-save-ai" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save AI</button>
            <button id="btn-clear-ai" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Clear stored key</button>
        </div>
    `;
    wrap.appendChild(ai);

    const needsModelCatalogRefresh = !catalogState.loading && (
        !Number(catalogState.fetchedAt) ||
        (Date.now() - Number(catalogState.fetchedAt)) > (15 * 60 * 1000)
    );
    if (needsModelCatalogRefresh) {
        fetchOpenAiModelsCatalog({ force: false }).catch(() => {});
    }

    // Marcus agent settings
    const agent = section('Marcus', 'Configure what Marcus knows, how it helps, and what it watches for.');
    const agentBody = agent.querySelector('[data-slot="body"]');
    const assistantOperatingDoctrine = typeof state.settings.assistantOperatingDoctrine === 'string'
        ? state.settings.assistantOperatingDoctrine
        : (typeof state.settings.operatorHelpPrompt === 'string' ? state.settings.operatorHelpPrompt : '');
    const personalityLayer = typeof state.settings.personalityLayer === 'string' ? state.settings.personalityLayer : '';
    const attentionRadar = typeof state.settings.attentionRadar === 'string' ? state.settings.attentionRadar : '';
    const strategicForecasting = typeof state.settings.strategicForecasting === 'string' ? state.settings.strategicForecasting : '';
    const executionAuthority = typeof state.settings.executionAuthority === 'string' ? state.settings.executionAuthority : '';
    const knowledgeArchive = typeof state.settings.knowledgeArchive === 'string' ? state.settings.knowledgeArchive : '';
    const dailyReportingStructure = typeof state.settings.dailyReportingStructure === 'string' ? state.settings.dailyReportingStructure : '';
    agentBody.innerHTML = `
        <div class="grid grid-cols-1 gap-4">
            <div>
                <label class="text-xs text-ops-light">Operator Bio (who you are)</label>
                <textarea id="set-operator-bio" rows="8" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: I am Mark. Roles: owner/operator, PM, closer. Needs: daily agenda + inbox triage + project next actions. Constraints: ...">${escapeHtml(String(state.settings.operatorBio || ''))}</textarea>
                <div class="text-[11px] text-ops-light mt-1">Included in every Marcus context. You can refine it in the Bio chat thread.</div>
            </div>

            <div>
                <label class="text-xs text-ops-light">Assistant Operating Doctrine (how to help you)</label>
                <textarea id="set-assistant-operating-doctrine" rows="7" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: If I sound unclear, ask up to 3 clarifying questions. When I ask for updates, summarize in bullets with next actions. Prefer default assumptions over long back-and-forth.">${escapeHtml(String(assistantOperatingDoctrine || ''))}</textarea>
            </div>

            <div>
                <label class="text-xs text-ops-light">Personality Layer (how M.A.R.C.U.S. behaves)</label>
                <textarea id="set-personality-layer" rows="6" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: Direct, calm, slightly sarcastic. Push back when I drift. End every response with next actions.">${escapeHtml(String(personalityLayer || ''))}</textarea>
            </div>

            <div>
                <label class="text-xs text-ops-light">Attention Radar (what M.A.R.C.U.S. watches for)</label>
                <textarea id="set-attention-radar" rows="6" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: Missed deadlines, stalled projects, inbox buildup, repeated blockers, context switching, low follow-up cadence.">${escapeHtml(String(attentionRadar || ''))}</textarea>
            </div>

            <div>
                <label class="text-xs text-ops-light">Strategic Forecasting</label>
                <textarea id="set-strategic-forecasting" rows="6" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: Anticipate downstream client risk, identify likely bottlenecks, and recommend compounding moves that improve resilience.">${escapeHtml(String(strategicForecasting || ''))}</textarea>
            </div>

            <div>
                <label class="text-xs text-ops-light">Execution Authority</label>
                <textarea id="set-execution-authority" rows="6" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: Default to direct recommendations, draft operational plans, and state the missing fact only when it changes the decision.">${escapeHtml(String(executionAuthority || ''))}</textarea>
            </div>

            <div>
                <label class="text-xs text-ops-light">Knowledge Archive</label>
                <textarea id="set-knowledge-archive" rows="6" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: Reuse prior decisions, preserve continuity across business contexts, and prefer verified facts over loose inference.">${escapeHtml(String(knowledgeArchive || ''))}</textarea>
            </div>

            <div>
                <label class="text-xs text-ops-light">Daily Reporting Structure</label>
                <textarea id="set-daily-reporting-structure" rows="10" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Morning\n• priorities\n• deadlines\n\nMidday\n• blockers\n• new patterns\n\nEnd of Day\n• what moved\n• what stalled\n• system opportunities detected">${escapeHtml(String(dailyReportingStructure || ''))}</textarea>
                <div class="text-[11px] text-ops-light mt-1">Pattern detection works best with consistent summaries.</div>
            </div>
        </div>
        <div class="flex gap-2 mt-4">
            <button id="btn-save-agent" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Marcus Settings</button>
        </div>
    `;
    wrap.appendChild(agent);

    // Projects (bulk delete) — moved to its own deep-link pane to avoid OOM.
    const projectsSection = section('Projects', 'This list can get big. Open it in a new tab to manage bulk deletes without slowing Settings.');
    const projectsBody = projectsSection.querySelector('[data-slot="body"]');
    const projects = Array.isArray(state.projects) ? state.projects : [];
    projectsBody.innerHTML = `
        <div class="flex items-center justify-between gap-3 flex-wrap">
            <div class="text-xs text-ops-light">Projects (${projects.length})</div>
            <a href="${escapeHtml(projectsSettingsHref)}" target="_blank" rel="noopener noreferrer" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Open Projects settings in new tab</a>
        </div>
        <div class="text-[11px] text-ops-light mt-2">Tip: Keep this tab for general settings; use the projects tab for bulk delete.</div>
    `;
    wrap.appendChild(projectsSection);

    // Team
    const teamSection = section('Team', 'Add team members with job titles, skills/abilities, and workflow limits. Delegation uses these settings.');
    const teamBody = teamSection.querySelector('[data-slot="body"]');
    const humans = getHumanTeamMembers();

    const editingId = safeText(state.settings?.teamEditingId);
    const editing = editingId ? humans.find((m) => safeText(m.id) === editingId) : null;

    teamBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div class="space-y-2">
                <div class="text-xs text-ops-light">${editing ? `Editing: <span class=\"text-white\">${escapeHtml(safeText(editing.name))}</span>` : 'New member'}</div>
                <input id="team-name" type="text" placeholder="Name" value="${escapeHtml(safeText(editing?.name))}" class="w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <input id="team-title" type="text" placeholder="Job title" value="${escapeHtml(safeText(editing?.title))}" class="w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <input id="team-avatar" type="text" placeholder="Avatar (optional, e.g. MK)" value="${escapeHtml(safeText(editing?.avatar))}" class="w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <input id="team-slack-user" type="text" placeholder="Slack link (U123..., @username, or email)" value="${escapeHtml(safeText(editing?.slackUserId))}" class="w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <input id="team-wip" type="number" min="0" max="99" placeholder="WIP limit (0 = no limit)" value="${editing && Number.isFinite(Number(editing.wipLimit)) ? Number(editing.wipLimit) : 0}" class="w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <input id="team-skills" type="text" placeholder="Skills (comma separated)" value="${escapeHtml((Array.isArray(editing?.skills)?editing.skills:[]).join(', '))}" class="w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <input id="team-abilities" type="text" placeholder="Abilities (comma separated)" value="${escapeHtml((Array.isArray(editing?.abilities)?editing.abilities:[]).join(', '))}" class="w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="flex gap-2 pt-1">
                    <button id="btn-team-save" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">${editing ? 'Save changes' : 'Add member'}</button>
                    <button id="btn-team-cancel" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Cancel</button>
                </div>
                <div class="text-[11px] text-ops-light">Set Slack link to map presence by ID, @username, or email.</div>
                <div id="team-error" class="text-xs text-red-400 hidden"></div>
            </div>
            <div class="space-y-2">
                <div class="flex items-center justify-between">
                    <div class="text-xs text-ops-light">Members (${humans.length})</div>
                </div>
                ${state.teamPresenceLoading ? `<div class="text-[11px] text-ops-light">Loading Slack presence…</div>` : ''}
                ${state.teamPresenceError ? `<div class="text-[11px] text-amber-300">Slack presence: ${escapeHtml(state.teamPresenceError)}</div>` : ''}
                <div class="space-y-2">
                    ${humans.length ? humans.map((m) => {
                        const nm = safeText(m.name);
                        const tt = safeText(m.title);
                        const w = Number(m.wipLimit) || 0;
                        const sk = Array.isArray(m.skills) ? m.skills.slice(0, 8).join(', ') : '';
                        const ab = Array.isArray(m.abilities) ? m.abilities.slice(0, 8).join(', ') : '';
                        const p = (state.teamPresenceByMemberId && typeof state.teamPresenceByMemberId === 'object') ? state.teamPresenceByMemberId[safeText(m.id)] : null;
                        const online = p && Object.prototype.hasOwnProperty.call(p, 'online') ? p.online : null;
                        const slackLabel = safeText(p?.slackLabel) || safeText(m?.slackUserId);
                        const statusClass = online === true ? 'bg-emerald-500' : (online === false ? 'bg-zinc-500' : 'bg-amber-400');
                        const statusText = online === true ? 'Online' : (online === false ? 'Offline' : 'Unknown');
                        return `
                            <div class="border border-ops-border rounded-lg bg-ops-bg/30 p-3">
                                <div class="flex items-start justify-between gap-2">
                                    <div class="min-w-0">
                                        <div class="text-white text-sm font-semibold truncate">${escapeHtml(nm)}</div>
                                        <div class="text-[11px] text-ops-light mt-0.5">${escapeHtml(tt || '—')}${w ? ` • WIP ${w}` : ''}</div>
                                        <div class="text-[11px] text-ops-light mt-1 flex items-center gap-1.5"><span class="inline-block w-2 h-2 rounded-full ${statusClass}"></span><span>Slack ${statusText}${slackLabel ? ` • ${escapeHtml(slackLabel)}` : ''}</span></div>
                                        ${sk ? `<div class=\"text-[11px] text-ops-light mt-1\"><span class=\"font-mono\">skills</span>: ${escapeHtml(sk)}</div>` : ''}
                                        ${ab ? `<div class=\"text-[11px] text-ops-light mt-1\"><span class=\"font-mono\">abilities</span>: ${escapeHtml(ab)}</div>` : ''}
                                    </div>
                                    <div class="shrink-0 flex gap-2">
                                        <button data-team-edit="${escapeHtml(safeText(m.id))}" class="px-2.5 py-1.5 rounded bg-ops-bg border border-ops-border text-ops-light text-[11px] hover:text-white">Edit</button>
                                        <button data-team-del="${escapeHtml(safeText(m.id))}" class="px-2.5 py-1.5 rounded bg-ops-bg border border-ops-border text-ops-light text-[11px] hover:text-red-300">Delete</button>
                                    </div>
                                </div>
                            </div>
                        `;
                    }).join('') : `<div class="text-xs text-ops-light italic">No team members yet.</div>`}
                </div>
            </div>
        </div>
    `;

    const teamErr = teamBody.querySelector('#team-error');
    const setTeamError = (msg) => {
        if (!teamErr) return;
        const m = safeText(msg).trim();
        teamErr.textContent = m;
        teamErr.classList.toggle('hidden', !m);
    };

    const btnCancel = teamBody.querySelector('#btn-team-cancel');
    if (btnCancel) btnCancel.onclick = async () => {
        setTeamError('');
        await saveSettingsPatch({ teamEditingId: '' });
        renderSettings(container);
    };

    teamBody.querySelectorAll('button[data-team-edit]').forEach((b) => {
        b.addEventListener('click', async () => {
            const id = safeText(b.getAttribute('data-team-edit'));
            await saveSettingsPatch({ teamEditingId: id });
            renderSettings(container);
        });
    });

    teamBody.querySelectorAll('button[data-team-del]').forEach((b) => {
        b.addEventListener('click', async () => {
            const id = safeText(b.getAttribute('data-team-del'));
            const member = humans.find((m) => safeText(m.id) === id);
            if (!member) return;
            if (!confirm(`Delete team member: ${member.name}?`)) return;
            try {
                const data = await withRevisionRetry(() => apiJson(`/api/team/${encodeURIComponent(id)}`, {
                    method: 'DELETE',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ baseRevision: state.revision })
                }));
                if (data?.store) applyStore(data.store);
                await saveSettingsPatch({ teamEditingId: '' });
                renderNav();
                renderSettings(container);
            } catch (e) {
                alert(e?.message || 'Failed to delete');
            }
        });
    });

    const btnSaveTeam = teamBody.querySelector('#btn-team-save');
    if (btnSaveTeam) {
        btnSaveTeam.onclick = async () => {
            setTeamError('');
            btnSaveTeam.disabled = true;
            try {
                const name = safeText(teamBody.querySelector('#team-name')?.value).trim();
                const title = safeText(teamBody.querySelector('#team-title')?.value).trim();
                const avatar = safeText(teamBody.querySelector('#team-avatar')?.value).trim();
                const slackUserId = safeText(teamBody.querySelector('#team-slack-user')?.value).trim();
                const wipLimit = Number(teamBody.querySelector('#team-wip')?.value);
                const skills = normalizeCsvList(teamBody.querySelector('#team-skills')?.value);
                const abilities = normalizeCsvList(teamBody.querySelector('#team-abilities')?.value);
                if (!name) throw new Error('Name is required');

                if (editing) {
                    const data = await withRevisionRetry(() => apiJson(`/api/team/${encodeURIComponent(editing.id)}`, {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ baseRevision: state.revision, patch: { name, title, avatar, slackUserId, wipLimit, skills, abilities } })
                    }));
                    if (data?.store) applyStore(data.store);
                } else {
                    const data = await withRevisionRetry(() => apiJson('/api/team', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ baseRevision: state.revision, member: { name, title, avatar, slackUserId, wipLimit, skills, abilities } })
                    }));
                    if (data?.store) applyStore(data.store);
                }

                await saveSettingsPatch({ teamEditingId: '' });
                await refreshSlackTeamPresence({ force: true });
                renderNav();
                renderSettings(container);
            } catch (e) {
                const msg = String(e?.message || '').trim();
                if (/already exists/i.test(msg)) {
                    setTeamError('That name already exists. Use Edit on the existing member (or choose a unique name).');
                } else {
                    setTeamError(msg || 'Failed to save team member');
                }
            } finally {
                btnSaveTeam.disabled = false;
            }
        };
    }

    wrap.appendChild(teamSection);

    // Google
    const g = section('Google Calendar', 'Connect Google Calendar (read-only calls/meetings; due-date sync optional).');
    const gBody = g.querySelector('[data-slot="body"]');
    gBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">Client ID</label>
                <input id="set-google-client-id" type="text" autocomplete="off" value="${String(state.settings.googleClientId || '')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Looks like: <span class="font-mono">123...xyz.apps.googleusercontent.com</span></div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Client Secret (optional)</label>
                <input id="set-google-client-secret" type="password" autocomplete="off" placeholder="(optional; leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">If blank, connect uses PKCE (no secret required).</div>
            </div>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
            <div>
                <label class="text-xs text-ops-light">Read calendar (for calls/meetings)</label>
                <input id="set-google-read-calendar-id" type="text" autocomplete="off" placeholder="primary" value="${String(state.settings.googleReadCalendarId || '')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Leave blank for <span class="font-mono">primary</span>.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Upcoming events</label>
                <div class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">GET /api/integrations/google/upcoming</div>
                <div class="text-[11px] text-ops-light mt-1">Read-only (no writes).</div>
            </div>
        </div>
        <div class="text-[11px] text-ops-light mt-2" id="google-status-line">Status: checking...</div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-google" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Google</button>
            <button id="btn-connect-google" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Connect</button>
            <button id="btn-disconnect-google" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Disconnect</button>
            <button id="btn-sync-google" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Sync now</button>
            <button id="btn-upcoming-google" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Show next 7 days</button>
        </div>
        <div id="google-upcoming-output" class="mt-3 hidden bg-ops-bg border border-ops-border rounded px-3 py-2 text-xs text-ops-light font-mono whitespace-pre-wrap"></div>
    `;
    wrap.appendChild(g);

    // Email (IMAP / SMTP)
    const emailSection = section('Email (IMAP / SMTP)', 'Sync email into Inbox, send mail over SMTP, and ingest archived email into Qdrant.');
    const emailBody = emailSection.querySelector('[data-slot="body"]');
    emailBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">IMAP Host</label>
                <input id="set-imap-host" type="text" autocomplete="off" value="${escapeHtml(String(state.settings.imapHost || ''))}" placeholder="imap.gmail.com" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div>
                <label class="text-xs text-ops-light">IMAP Port</label>
                <input id="set-imap-port" type="number" min="1" max="65535" value="${Number(state.settings.imapPort) || 993}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div>
                <label class="text-xs text-ops-light">IMAP Username</label>
                <input id="set-imap-username" type="text" autocomplete="off" value="${escapeHtml(String(state.settings.imapUsername || ''))}" placeholder="you@example.com" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div>
                <label class="text-xs text-ops-light">IMAP Password</label>
                <input id="set-imap-password" type="password" autocomplete="off" placeholder="(leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <label class="mt-2 inline-flex items-center gap-2 text-[11px] text-ops-light">
                    <input id="set-imap-secure" type="checkbox" class="accent-blue-500" ${(state.settings.imapSecure !== false) ? 'checked' : ''} />
                    Use TLS / secure IMAP
                </label>
            </div>
            <div>
                <label class="text-xs text-ops-light">IMAP Sync Folders</label>
                <textarea id="set-imap-sync-folders" rows="3" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="INBOX">${escapeHtml((Array.isArray(state.settings.imapSyncFolders) ? state.settings.imapSyncFolders : ['INBOX']).join('\n'))}</textarea>
                <div class="text-[11px] text-ops-light mt-1">One folder per line for inbox sync.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Archive Folders</label>
                <textarea id="set-imap-archive-folders" rows="3" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Archive\nAll Mail">${escapeHtml((Array.isArray(state.settings.imapArchiveFolders) ? state.settings.imapArchiveFolders : ['Archive', 'All Mail']).join('\n'))}</textarea>
                <div class="text-[11px] text-ops-light mt-1">Used for archive knowledge ingestion.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">SMTP Host</label>
                <input id="set-smtp-host" type="text" autocomplete="off" value="${escapeHtml(String(state.settings.smtpHost || ''))}" placeholder="smtp.gmail.com" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div>
                <label class="text-xs text-ops-light">SMTP Port</label>
                <input id="set-smtp-port" type="number" min="1" max="65535" value="${Number(state.settings.smtpPort) || 465}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div>
                <label class="text-xs text-ops-light">SMTP Username</label>
                <input id="set-smtp-username" type="text" autocomplete="off" value="${escapeHtml(String(state.settings.smtpUsername || ''))}" placeholder="you@example.com" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div>
                <label class="text-xs text-ops-light">SMTP Password</label>
                <input id="set-smtp-password" type="password" autocomplete="off" placeholder="(leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <label class="mt-2 inline-flex items-center gap-2 text-[11px] text-ops-light">
                    <input id="set-smtp-secure" type="checkbox" class="accent-blue-500" ${(state.settings.smtpSecure !== false) ? 'checked' : ''} />
                    Use TLS / secure SMTP
                </label>
            </div>
            <div>
                <label class="text-xs text-ops-light">From Address</label>
                <input id="set-smtp-from-address" type="text" autocomplete="off" value="${escapeHtml(String(state.settings.smtpFromAddress || ''))}" placeholder="Marcus <you@example.com>" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div>
                <label class="text-xs text-ops-light">Test Recipient</label>
                <input id="set-email-test-recipient" type="text" autocomplete="off" placeholder="recipient@example.com" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
            <label class="inline-flex items-center gap-2 text-[11px] text-ops-light">
                <input id="set-email-sync-enabled" type="checkbox" class="accent-blue-500" ${state.settings.emailSyncEnabled !== false ? 'checked' : ''} />
                Enable email inbox sync
            </label>
            <label class="inline-flex items-center gap-2 text-[11px] text-ops-light">
                <input id="set-email-archive-knowledge-enabled" type="checkbox" class="accent-blue-500" ${state.settings.emailArchiveKnowledgeEnabled !== false ? 'checked' : ''} />
                Enable archive knowledge ingestion
            </label>
        </div>
        <div class="text-[11px] text-ops-light mt-2" id="email-status-line">Status: checking...</div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-email" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Email</button>
            <button id="btn-test-email" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Test Connections</button>
            <button id="btn-sync-email" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Sync Inbox</button>
            <button id="btn-send-test-email" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Send Test Email</button>
            <button id="btn-email-archive-qdrant" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Ingest IMAP Archives → Qdrant</button>
            <button id="btn-email-local-archive-qdrant" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Ingest Local Archived Inbox → Qdrant</button>
        </div>
        <div id="email-output" class="mt-3 hidden bg-ops-bg border border-ops-border rounded px-3 py-2 text-xs text-ops-light font-mono whitespace-pre-wrap"></div>
    `;
    wrap.appendChild(emailSection);

    // Fireflies
    const f = section('Fireflies', 'Ingest meeting summaries into project notes via a secure webhook.');
    const fBody = f.querySelector('[data-slot="body"]');
    fBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">Shared Secret</label>
                <input id="set-fireflies-secret" type="password" autocomplete="off" placeholder="(not shown once saved)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Configured: ${state.settings.firefliesConfigured ? 'Yes' : 'No'}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Webhook</label>
                <div class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">POST /api/integrations/fireflies/ingest</div>
                <div class="text-[11px] text-ops-light mt-1">Header: <span class="font-mono">x-fireflies-secret</span></div>
            </div>
        </div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-generate-fireflies" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Generate secret</button>
            <button id="btn-save-fireflies" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Fireflies</button>
        </div>
    `;
    wrap.appendChild(f);

    // CRM
    const crm = section('CRM', 'Ingest new leads/messages into Inbox via a secure webhook (shared secret).');
    const crmBody = crm.querySelector('[data-slot="body"]');
    crmBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">API Base URL (optional)</label>
                <input id="set-crm-api-base" type="text" autocomplete="off" placeholder="https://api.yourcrm.com" value="${String(state.settings.crmApiBaseUrl || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Used later for pull/sync. Webhook works without it.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">API Key (optional)</label>
                <input id="set-crm-api-key" type="password" autocomplete="off" placeholder="(leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Kept server-side (never shown again once saved).</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Webhook Secret</label>
                <input id="set-crm-webhook-secret" type="password" autocomplete="off" placeholder="(not shown once saved)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Configured: ${state.settings.crmConfigured ? 'Yes' : 'No'}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Webhook</label>
                <div class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">POST /api/integrations/crm/webhook</div>
                <div class="text-[11px] text-ops-light mt-1">Header: <span class="font-mono">X-CRM-Secret</span></div>
            </div>
        </div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-generate-crm" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Generate secret</button>
            <button id="btn-save-crm" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save CRM</button>
        </div>
    `;
    wrap.appendChild(crm);

    // GA4
    const ga4 = section('GA4 (Analytics)', 'Pull a daily GA4 summary into Inbox using your Google connection.');
    const ga4Body = ga4.querySelector('[data-slot="body"]');
    ga4Body.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">GA4 Property ID</label>
                <input id="set-ga4-property-id" type="text" autocomplete="off" placeholder="123456789" value="${String(state.settings.ga4PropertyId || '').replace(/"/g, '&quot;')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Find in GA4 Admin → Property Settings.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Google Connection</label>
                <div class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs">Uses the Google Connect flow above (Calendar section).</div>
                <div class="text-[11px] text-ops-light mt-1">If you connected before GA4 was added, click <span class="font-mono">Disconnect</span> then <span class="font-mono">Connect</span> to grant GA4 read access.</div>
            </div>
        </div>
        <div class="text-[11px] text-ops-light mt-2">Configured: ${state.settings.ga4Configured ? 'Yes' : 'No'} • Endpoint: <span class="font-mono">POST /api/integrations/ga4/pull-now</span></div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-ga4" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save GA4</button>
            <button id="btn-pull-ga4" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Pull now</button>
        </div>
        <div id="ga4-pull-output" class="mt-3 hidden bg-ops-bg border border-ops-border rounded px-3 py-2 text-xs text-ops-light font-mono whitespace-pre-wrap"></div>
    `;
    wrap.appendChild(ga4);

    // Slack
    const slackOrigin = (typeof window !== 'undefined' && window.location && window.location.origin) ? window.location.origin : '';
    const slackAbs = (p) => slackOrigin ? `${slackOrigin}${p}` : p;
    const slack = section('Slack', 'Ingest Slack messages into Inbox via Slack Events API (signature verified).');
    const slackBody = slack.querySelector('[data-slot="body"]');
    slackBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">Client ID (OAuth)</label>
                <input id="set-slack-client-id" type="text" autocomplete="off" value="${String(state.settings.slackClientId || '').replace(/"/g, '&quot;')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">OAuth configured: ${state.settings.slackOAuthConfigured ? 'Yes' : 'No'} • Installed: ${state.settings.slackInstalled ? 'Yes' : 'No'}</div>
                ${(state.settings.slackTeamName || state.settings.slackTeamId) ? `<div class="text-[11px] text-ops-light mt-1">Workspace: ${String(state.settings.slackTeamName || state.settings.slackTeamId || '')}</div>` : ''}
            </div>
            <div>
                <label class="text-xs text-ops-light">Client Secret (OAuth)</label>
                <input id="set-slack-client-secret" type="password" autocomplete="off" placeholder="(leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Redirect URI: <span class="font-mono">${escapeHtml(slackAbs('/api/integrations/slack/oauth/callback'))}</span></div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Bot Token (optional)</label>
                <input id="set-slack-bot-token" type="password" autocomplete="off" placeholder="xoxb-... (leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Needed for “Send test”. Prefer using <span class="font-mono">Connect</span> to install.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Signing Secret</label>
                <input id="set-slack-signing-secret" type="password" autocomplete="off" placeholder="(leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Configured: ${state.settings.slackConfigured ? 'Yes' : 'No'}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Webhook</label>
                <div class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">POST ${escapeHtml(slackAbs('/api/integrations/slack/events'))}</div>
                <div class="text-[11px] text-ops-light mt-1">Slack headers: <span class="font-mono">X-Slack-Signature</span>, <span class="font-mono">X-Slack-Request-Timestamp</span></div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Test target (optional)</label>
                <input id="set-slack-test-target" type="text" autocomplete="off" placeholder="#general or @you" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Uses your Slack bot token to post a quick test.</div>
            </div>
        </div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-slack" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Slack</button>
            <button id="btn-connect-slack" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Connect</button>
            <button id="btn-disconnect-slack" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Disconnect</button>
            <button id="btn-diag-slack" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Diagnostics</button>
            <button id="btn-test-slack" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Send test</button>
        </div>
        <div class="text-[11px] text-ops-light mt-2">Inbox destination: current business <span class="font-mono">${escapeHtml(String(state.activeBusinessKey || 'personal'))}</span> • Captures: <span class="font-mono">message</span> + <span class="font-mono">app_mention</span> (no bots/subtypes).</div>
        <div id="slack-test-output" class="mt-3 hidden bg-ops-bg border border-ops-border rounded px-3 py-2 text-xs text-ops-light font-mono whitespace-pre-wrap"></div>
    `;
    wrap.appendChild(slack);

    // Quo (SMS/Calls)
    const quo = section('Quo (SMS/Calls)', 'Ingest inbound SMS and missed calls into Inbox (Twilio-style signature verified).');
    const quoBody = quo.querySelector('[data-slot="body"]');
    const smsAckFilterLevel = safeText(state.settings?.smsAckFilterLevel).trim().toLowerCase() || 'medium';
    const quoMapRaw = (state.settings && typeof state.settings.phoneBusinessMap === 'object' && state.settings.phoneBusinessMap)
        ? state.settings.phoneBusinessMap
        : {};
    let quoMapEntries = Object.entries(quoMapRaw)
        .map(([phone, business]) => ({ phone: safeText(phone), business: safeText(business) }))
        .filter((row) => row.phone || row.business)
        .slice(0, 6);
    if (!quoMapEntries.length) {
        quoMapEntries = [
            { phone: '8886107667', business: 'Web Agency - PoopSites' },
            { phone: '5207797667', business: 'Scoop Doggy Logs Pet Waste Removal' },
            { phone: '5203162667', business: 'Personal Priority Line' },
        ];
    }
    while (quoMapEntries.length < 3) quoMapEntries.push({ phone: '', business: '' });
    quoBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">Auth Token</label>
                <input id="set-quo-auth-token" type="password" autocomplete="off" placeholder="(leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Configured: ${state.settings.quoConfigured ? 'Yes' : 'No'}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Webhooks</label>
                <div class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">POST /api/integrations/quo/sms</div>
                <div class="mt-2 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">POST /api/integrations/quo/calls</div>
                <div class="text-[11px] text-ops-light mt-1">Header: <span class="font-mono">X-Twilio-Signature</span></div>
            </div>
        </div>
        <div class="mt-4 border border-ops-border rounded-lg p-3 bg-ops-bg/20">
            <label class="text-xs text-white font-semibold">SMS acknowledgement noise filter</label>
            <div class="text-[11px] text-ops-light mt-1">Choose how aggressively Marcus suppresses non-actionable acknowledgements (ok/thanks/etc.) from Inbox/Radar.</div>
            <select id="set-sms-ack-filter-level" class="mt-2 w-full md:w-80 bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs">
                <option value="off" ${smsAckFilterLevel === 'off' ? 'selected' : ''}>Off (show everything)</option>
                <option value="low" ${smsAckFilterLevel === 'low' ? 'selected' : ''}>Low (only obvious filler)</option>
                <option value="medium" ${smsAckFilterLevel === 'medium' ? 'selected' : ''}>Medium (recommended)</option>
                <option value="high" ${smsAckFilterLevel === 'high' ? 'selected' : ''}>High (very strict)</option>
            </select>
        </div>
        <div class="mt-4 border border-ops-border rounded-lg p-3 bg-ops-bg/20">
            <div class="text-xs text-white font-semibold">Business Routing (by destination number)</div>
            <div class="text-[11px] text-ops-light mt-1">Map each business phone number (To) to a business name for SMS/call separation.</div>
            <div class="mt-3 space-y-2">
                ${quoMapEntries.map((row, idx) => `
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-2">
                        <input data-quo-map-phone="${idx}" value="${escapeHtml(row.phone)}" placeholder="+15025550111" class="bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" />
                        <input data-quo-map-business="${idx}" value="${escapeHtml(row.business)}" placeholder="Business label (e.g. Freedom Scoopers)" class="bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs" />
                    </div>
                `).join('')}
            </div>
        </div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-quo" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Quo</button>
        </div>
    `;
    wrap.appendChild(quo);

    // Mini GHL
    const ghl = section('Mini GHL', 'Connect GoHighLevel/LeadConnector and show a compact dashboard snapshot.');
    const ghlBody = ghl.querySelector('[data-slot="body"]');
    ghlBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">API Key</label>
                <input id="set-ghl-api-key" type="password" autocomplete="off" placeholder="(leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Configured: ${state.settings.ghlConfigured ? 'Yes' : 'No'}${state.settings.ghlLocationId ? ` • Location ${escapeHtml(String(state.settings.ghlLocationId))}` : ''}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Location ID</label>
                <input id="set-ghl-location-id" type="text" autocomplete="off" value="${escapeHtml(String(state.settings.ghlLocationId || ''))}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm font-mono" />
                <div class="text-[11px] text-ops-light mt-1">Used for opportunities, conversations, and upcoming appointments.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">API Base URL (optional)</label>
                <input id="set-ghl-api-base" type="text" autocomplete="off" value="${escapeHtml(String(state.settings.ghlApiBaseUrl || ''))}" placeholder="https://services.leadconnectorhq.com" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm font-mono" />
            </div>
            <div>
                <label class="text-xs text-ops-light">Snapshot endpoint</label>
                <div class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">GET /api/integrations/ghl/snapshot</div>
                <div class="text-[11px] text-ops-light mt-1">Reads and summarizes live GHL data for dashboard cards.</div>
            </div>
        </div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-ghl" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save GHL</button>
            <button id="btn-test-ghl" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Test Snapshot</button>
        </div>
        <div id="ghl-test-output" class="mt-3 hidden bg-ops-bg border border-ops-border rounded px-3 py-2 text-xs text-ops-light font-mono whitespace-pre-wrap"></div>
    `;
    wrap.appendChild(ghl);

    // MCP
    const m = section('MCP', 'Run MCP servers alongside this app (stdio) so Marcus can call MCP tools (Render-friendly).');
    const mBody = m.querySelector('[data-slot="body"]');
    const mcp = (state.settings && state.settings.mcp && typeof state.settings.mcp === 'object') ? state.settings.mcp : {};
    const mcpEnabled = !!state.settings.mcpEnabled;
    const mcpCommand = String(mcp.command || '');
    const mcpArgs = Array.isArray(mcp.args) ? mcp.args.map(String).join(' ') : String(mcp.args || '');
    const mcpCwd = String(mcp.cwd || '');
    const mcpServers = Array.isArray(state.settings.mcpServers) ? state.settings.mcpServers : [];
    const mcpServersEnabled = mcpServers.filter((s) => s && typeof s === 'object' && s.enabled).length;
    mBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div class="flex items-center gap-2 md:col-span-3">
                <input id="set-mcp-enabled" type="checkbox" class="accent-blue-500" ${mcpEnabled ? 'checked' : ''} />
                <label for="set-mcp-enabled" class="text-xs text-ops-light">Enable MCP</label>
                <div class="text-[11px] text-ops-light ml-3">Configured: ${state.settings.mcpConfigured ? 'Yes' : 'No'}${mcpServers.length ? ` • Servers: ${mcpServersEnabled}/${mcpServers.length}` : ''}</div>
            </div>
            <div class="md:col-span-1">
                <label class="text-xs text-ops-light">Command</label>
                <input id="set-mcp-command" type="text" autocomplete="off" placeholder="npx" value="${mcpCommand.replace(/"/g, '&quot;')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Example: <span class="font-mono">npx</span></div>
            </div>
            <div class="md:col-span-2">
                <label class="text-xs text-ops-light">Args (space-separated; quotes supported)</label>
                <input id="set-mcp-args" type="text" autocomplete="off" placeholder='-y @modelcontextprotocol/server-filesystem --root "C:\\"' value="${mcpArgs.replace(/"/g, '&quot;')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Tip: keep <span class="font-mono">command</span> and <span class="font-mono">args</span> separate.</div>
            </div>
            <div class="md:col-span-3">
                <label class="text-xs text-ops-light">Working directory (optional)</label>
                <input id="set-mcp-cwd" type="text" autocomplete="off" placeholder="C:\\path\\to\\workspace" value="${mcpCwd.replace(/"/g, '&quot;')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div class="md:col-span-3 text-[11px] text-ops-light">Multi-server (recommended): set <span class="font-mono">mcpServers</span> in Advanced Settings JSON, then call tools as <span class="font-mono">serverName.toolName</span> (example <span class="font-mono">crm.search</span>).</div>
        </div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-mcp" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save MCP</button>
            <button id="btn-test-mcp" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Test / List Tools</button>
        </div>
        <div id="mcp-tools-output" class="mt-3 hidden bg-ops-bg border border-ops-border rounded px-3 py-2 text-xs text-ops-light font-mono whitespace-pre-wrap"></div>
    `;
    wrap.appendChild(m);

    // UI prefs
    const ui = section('App Preferences', 'Quality-of-life toggles that affect your workspace UX.');
    const uiBody = ui.querySelector('[data-slot="body"]');
    uiBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
                <label class="text-xs text-ops-light">Auto-refresh (seconds)</label>
                <input id="set-auto-refresh" type="number" min="10" value="${Number(state.uiPrefs.autoRefreshSeconds) || 30}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
            </div>
            <div class="flex items-center gap-2 mt-6">
                <input id="set-week-monday" type="checkbox" class="accent-blue-500" ${state.uiPrefs.weekStartsOnMonday ? 'checked' : ''} />
                <label for="set-week-monday" class="text-xs text-ops-light">Week starts Monday</label>
            </div>
            <div class="flex items-center gap-2 mt-6">
                <input id="set-show-completed" type="checkbox" class="accent-blue-500" ${state.uiPrefs.defaultShowCompleted ? 'checked' : ''} />
                <label for="set-show-completed" class="text-xs text-ops-light">Show completed by default</label>
            </div>
        </div>
        <div class="flex gap-2 mt-4">
            <button id="btn-save-ui" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Preferences</button>
        </div>
    `;
    wrap.appendChild(ui);

    // Advanced
    const adv = section('Advanced', 'Edit additional non-secret settings as JSON (saved to your local settings file).');
    const advBody = adv.querySelector('[data-slot="body"]');
    const safeSettings = { ...state.settings };
    // Remove read-only fields from the editor by default.
    delete safeSettings.aiEnabled;
    delete safeSettings.openaiKeyHint;
    delete safeSettings.source;
    delete safeSettings.settingsUpdatedAt;
    delete safeSettings.googleConfigured;
    delete safeSettings.googleConnected;
    delete safeSettings.firefliesConfigured;
    delete safeSettings.slackConfigured;
    delete safeSettings.slackOAuthConfigured;
    delete safeSettings.slackInstalled;
    delete safeSettings.quoConfigured;
    delete safeSettings.ghlConfigured;
    delete safeSettings.ga4Configured;
    delete safeSettings.imapConfigured;
    delete safeSettings.smtpConfigured;
    delete safeSettings.emailSyncEnabled;
    delete safeSettings.emailArchiveKnowledgeEnabled;
    advBody.innerHTML = `
        <label class="text-xs text-ops-light">Settings JSON</label>
        <textarea id="set-advanced-json" rows="10" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">${JSON.stringify(safeSettings, null, 2)}</textarea>
        <div class="flex gap-2 mt-4">
            <button id="btn-save-advanced" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Advanced</button>
            <button id="btn-reset-advanced" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Reset</button>
        </div>
        <div class="text-[11px] text-ops-light mt-2">Secrets/tokens are not shown here. Use the sections above for AI/Google/Fireflies secrets.</div>
    `;
    wrap.appendChild(adv);

    container.appendChild(wrap);

    // Wire actions
    const btnSaveAuth = document.getElementById('btn-save-auth');
    const btnClearAuth = document.getElementById('btn-clear-auth');
    const btnSaveAi = document.getElementById('btn-save-ai');
    const btnClearAi = document.getElementById('btn-clear-ai');
    const btnRefreshOpenAiModels = document.getElementById('btn-refresh-openai-models');
    const openAiModelSelect = document.getElementById('set-openai-model-select');
    const openAiModelInput = document.getElementById('set-openai-model');
    const btnSaveGoogle = document.getElementById('btn-save-google');
    const btnConnectGoogle = document.getElementById('btn-connect-google');
    const btnDisconnectGoogle = document.getElementById('btn-disconnect-google');
    const btnSyncGoogle = document.getElementById('btn-sync-google');
    const btnUpcomingGoogle = document.getElementById('btn-upcoming-google');
    const googleStatusLine = document.getElementById('google-status-line');
    const googleUpcomingOutput = document.getElementById('google-upcoming-output');
    const btnSaveEmail = document.getElementById('btn-save-email');
    const btnTestEmail = document.getElementById('btn-test-email');
    const btnSyncEmail = document.getElementById('btn-sync-email');
    const btnSendTestEmail = document.getElementById('btn-send-test-email');
    const btnEmailArchiveQdrant = document.getElementById('btn-email-archive-qdrant');
    const btnEmailLocalArchiveQdrant = document.getElementById('btn-email-local-archive-qdrant');
    const emailStatusLine = document.getElementById('email-status-line');
    const emailOutput = document.getElementById('email-output');
    const btnGenFireflies = document.getElementById('btn-generate-fireflies');
    const btnSaveFireflies = document.getElementById('btn-save-fireflies');
    const btnGenCrm = document.getElementById('btn-generate-crm');
    const btnSaveCrm = document.getElementById('btn-save-crm');
    const btnSaveGa4 = document.getElementById('btn-save-ga4');
    const btnPullGa4 = document.getElementById('btn-pull-ga4');
    const ga4PullOutput = document.getElementById('ga4-pull-output');
    const btnSaveSlack = document.getElementById('btn-save-slack');
    const btnConnectSlack = document.getElementById('btn-connect-slack');
    const btnDisconnectSlack = document.getElementById('btn-disconnect-slack');
    const btnDiagSlack = document.getElementById('btn-diag-slack');
    const btnTestSlack = document.getElementById('btn-test-slack');
    const slackTestOutput = document.getElementById('slack-test-output');
    const btnSaveQuo = document.getElementById('btn-save-quo');
    const btnSaveGhl = document.getElementById('btn-save-ghl');
    const btnTestGhl = document.getElementById('btn-test-ghl');
    const ghlTestOutput = document.getElementById('ghl-test-output');
    const btnSaveAgent = document.getElementById('btn-save-agent');
    const btnSaveUi = document.getElementById('btn-save-ui');
    const btnSaveAdvanced = document.getElementById('btn-save-advanced');
    const btnResetAdvanced = document.getElementById('btn-reset-advanced');
    const btnSaveMcp = document.getElementById('btn-save-mcp');
    const btnTestMcp = document.getElementById('btn-test-mcp');
    const mcpToolsOutput = document.getElementById('mcp-tools-output');

    const btnSaveAirtable = document.getElementById('btn-save-airtable');
    const btnTestAirtable = document.getElementById('btn-test-airtable');
    const btnSyncAirtable = document.getElementById('btn-sync-airtable');
    const btnTestAirtableRequests = document.getElementById('btn-test-airtable-requests');
    const btnSyncAirtableRequests = document.getElementById('btn-sync-airtable-requests');
    const airtableStatusLine = document.getElementById('airtable-status-line');
    const airtableOutput = document.getElementById('airtable-output');

    if (openAiModelSelect && openAiModelInput) {
        openAiModelSelect.addEventListener('change', () => {
            const selected = String(openAiModelSelect.value || '').trim();
            if (selected) openAiModelInput.value = selected;
        });
    }

    if (btnRefreshOpenAiModels) {
        btnRefreshOpenAiModels.onclick = async () => {
            btnRefreshOpenAiModels.disabled = true;
            try {
                await fetchOpenAiModelsCatalog({ force: true });
            } catch {
                // handled inside fetchOpenAiModelsCatalog
            } finally {
                renderSettings(container);
            }
        };
    }

    const refreshAuthStatus = async () => {
        const line = document.getElementById('auth-status-line');
        if (!line) return;
        try {
            const token = getStoredAdminToken();
            const headers = new Headers();
            if (token) headers.set('Authorization', `Bearer ${token}`);
            const r = await fetch('/api/auth/status', { headers });
            const s = await r.json().catch(() => ({}));
            const required = !!s.authRequired;
            const authed = (s && typeof s === 'object' && 'authenticated' in s) ? !!s.authenticated : !required;
            line.textContent = `Status: ${required ? 'Required' : 'Not required'} / ${authed ? 'Authenticated' : 'Not authenticated'}`;
        } catch {
            line.textContent = 'Status: unavailable';
        }
    };

    refreshAuthStatus();

    if (btnSaveAuth) btnSaveAuth.onclick = async () => {
        try {
            const token = String(document.getElementById('set-admin-token')?.value || '').trim();
            if (!token) {
                alert('Paste a token first.');
                return;
            }

            // Verify with server (also sets HttpOnly cookie when auth is enabled)
            const r = await fetch('/api/auth/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ token, remember: true }),
            });
            const data = await r.json().catch(() => ({}));
            if (!r.ok || data?.ok === false) {
                throw new Error(data?.error || `Invalid token (${r.status})`);
            }

            setStoredAdminToken(token);
            await refreshAuthStatus();
            await Promise.all([fetchState(), fetchSettings()]);
            startPolling();
            alert('Admin token saved.');
            state.currentView = 'dashboard';
            renderNav();
            renderMain();
            renderChat();
        } catch (e) {
            alert(e?.message || 'Failed to save admin token');
        }
    };

    if (btnClearAuth) btnClearAuth.onclick = async () => {
        try {
            setStoredAdminToken('');
            try {
                await fetch('/api/auth/logout', { method: 'POST' });
            } catch {
                // ignore
            }
            alert('Admin token cleared.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to clear admin token');
        }
    };

    const refreshGoogleStatus = async () => {
        try {
            const r = await apiFetch('/api/integrations/google/status');
            const s = await r.json().catch(() => ({}));
            const configured = !!s.configured;
            const connected = !!s.connected;
            if (googleStatusLine) {
                const clientOk = (s && typeof s === 'object' && 'clientIdValid' in s) ? !!s.clientIdValid : configured;
                const confTxt = configured ? (clientOk ? 'Configured' : 'Client ID invalid') : 'Not configured';
                googleStatusLine.textContent = `Status: ${confTxt} / ${connected ? 'Connected' : 'Not connected'}`;
            }
        } catch {
            if (googleStatusLine) googleStatusLine.textContent = 'Status: unavailable';
        }
    };

    refreshGoogleStatus();

    const splitTextareaList = (value) => String(value || '')
        .split(/[\n,;]+/g)
        .map((s) => safeText(s).trim())
        .filter(Boolean);

    const setEmailOutput = (txt) => {
        if (!emailOutput) return;
        const msg = safeText(txt);
        emailOutput.textContent = msg;
        emailOutput.classList.toggle('hidden', !msg);
    };

    const refreshEmailStatus = async () => {
        try {
            const r = await apiFetch('/api/integrations/email/status');
            const s = await r.json().catch(() => ({}));
            if (!r.ok || s?.ok === false) throw new Error(s?.error || 'Failed to load email status');
            if (emailStatusLine) {
                emailStatusLine.textContent = `Status: IMAP ${s.imapConfigured ? 'Configured' : 'Not configured'} / SMTP ${s.smtpConfigured ? 'Configured' : 'Not configured'} / Sync ${s.emailSyncEnabled ? 'Enabled' : 'Disabled'} / Archive KB ${s.emailArchiveKnowledgeEnabled ? 'Enabled' : 'Disabled'}`;
            }
        } catch {
            if (emailStatusLine) emailStatusLine.textContent = 'Status: unavailable';
        }
    };

    refreshEmailStatus();

    const parseAirtableIdsFromUrl = (urlStr) => {
        const raw = safeText(urlStr).trim();
        if (!raw) return { baseId: '', tableId: '', viewId: '' };
        const baseId = (raw.match(/app[a-zA-Z0-9]+/g) || [])[0] || '';
        const tableId = (raw.match(/tbl[a-zA-Z0-9]+/g) || [])[0] || '';
        const viewId = (raw.match(/viw[a-zA-Z0-9]+/g) || [])[0] || '';
        return { baseId, tableId, viewId };
    };

    const setAirtableOutput = (txt) => {
        if (!airtableOutput) return;
        const t = safeText(txt);
        airtableOutput.textContent = t;
        airtableOutput.classList.toggle('hidden', !t);
    };

    const refreshAirtableStatus = async () => {
        try {
            const r = await apiFetch('/api/integrations/airtable/config');
            const s = await r.json().catch(() => ({}));
            if (!r.ok || s?.ok === false) throw new Error(s?.error || 'Failed to load Airtable status');

            const baseEl = document.getElementById('set-airtable-base');
            const tableEl = document.getElementById('set-airtable-table');
            const viewEl = document.getElementById('set-airtable-view');
            const reqTableEl = document.getElementById('set-airtable-req-table');
            const reqViewEl = document.getElementById('set-airtable-req-view');
            if (baseEl && !String(baseEl.value || '').trim()) baseEl.value = String(s.baseId || '');
            if (tableEl && !String(tableEl.value || '').trim()) tableEl.value = String(s.clientsTableId || '');
            if (viewEl && !String(viewEl.value || '').trim()) viewEl.value = String(s.clientsViewId || '');
            if (reqTableEl && !String(reqTableEl.value || '').trim()) reqTableEl.value = String(s.requestsTableId || '');
            if (reqViewEl && !String(reqViewEl.value || '').trim()) reqViewEl.value = String(s.requestsViewId || '');

            if (airtableStatusLine) {
                const configured = !!s.configured;
                const hint = safeText(s.tokenHint || '');
                airtableStatusLine.textContent = `Status: ${configured ? 'Configured' : 'Not configured'}${hint ? ` / Token ${hint}` : ''}`;
            }
        } catch {
            if (airtableStatusLine) airtableStatusLine.textContent = 'Status: unavailable';
        }
    };

    refreshAirtableStatus();

    const atLink = document.getElementById('set-airtable-link');
    if (atLink) {
        atLink.addEventListener('input', () => {
            const baseEl = document.getElementById('set-airtable-base');
            const tableEl = document.getElementById('set-airtable-table');
            const viewEl = document.getElementById('set-airtable-view');
            const { baseId, tableId, viewId } = parseAirtableIdsFromUrl(atLink.value);
            if (baseEl && baseId) baseEl.value = baseId;
            if (tableEl && tableId) tableEl.value = tableId;
            if (viewEl && viewId) viewEl.value = viewId;
        });
    }

    const atReqLink = document.getElementById('set-airtable-req-link');
    if (atReqLink) {
        atReqLink.addEventListener('input', () => {
            const baseEl = document.getElementById('set-airtable-base');
            const reqTableEl = document.getElementById('set-airtable-req-table');
            const reqViewEl = document.getElementById('set-airtable-req-view');
            const { baseId, tableId, viewId } = parseAirtableIdsFromUrl(atReqLink.value);
            if (baseEl && baseId) baseEl.value = baseId;
            if (reqTableEl && tableId) reqTableEl.value = tableId;
            if (reqViewEl && viewId) reqViewEl.value = viewId;
        });
    }

    if (btnSaveAirtable) btnSaveAirtable.onclick = async () => {
        const prev = btnSaveAirtable.textContent;
        btnSaveAirtable.disabled = true;
        btnSaveAirtable.textContent = 'Saving…';
        try {
            setAirtableOutput('');
            const pat = String(document.getElementById('set-airtable-pat')?.value || '').trim();
            const baseId = String(document.getElementById('set-airtable-base')?.value || '').trim();
            const clientsTableId = String(document.getElementById('set-airtable-table')?.value || '').trim();
            const clientsViewId = String(document.getElementById('set-airtable-view')?.value || '').trim();
            const requestsTableId = String(document.getElementById('set-airtable-req-table')?.value || '').trim();
            const requestsViewId = String(document.getElementById('set-airtable-req-view')?.value || '').trim();
            if (!baseId) throw new Error('Base ID is required. Paste an Airtable link to auto-fill.');
            if (!clientsTableId && !requestsTableId) throw new Error('Provide a Clients table ID and/or Requests table ID.');

            const body = { baseId, clientsTableId, clientsViewId, requestsTableId, requestsViewId };
            if (pat) body.pat = pat;

            const resp = await apiJson('/api/integrations/airtable/config', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });

            const patEl = document.getElementById('set-airtable-pat');
            if (patEl) patEl.value = '';
            await refreshAirtableStatus();
            alert(resp?.configured ? 'Airtable saved.' : 'Airtable saved (still not configured).');
        } catch (e) {
            alert(e?.message || 'Failed to save Airtable');
        } finally {
            btnSaveAirtable.disabled = false;
            btnSaveAirtable.textContent = prev;
        }
    };

    if (btnTestAirtable) btnTestAirtable.onclick = async () => {
        const prev = btnTestAirtable.textContent;
        btnTestAirtable.disabled = true;
        btnTestAirtable.textContent = 'Testing…';
        try {
            const r = await apiFetch('/api/integrations/airtable/clients/preview');
            const data = await r.json().catch(() => ({}));
            if (!r.ok || data?.ok === false) throw new Error(data?.error || 'Preview failed');
            const lines = [];
            lines.push(`Preview: ${Number(data.count) || 0} record(s)`);
            for (const rec of (data.records || [])) {
                lines.push(`- ${safeText(rec.name) || '(Unnamed)'} (${safeText(rec.id)})`);
            }
            setAirtableOutput(lines.join('\n'));
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Failed to test Airtable';
            setAirtableOutput(msg);
            alert(msg);
        } finally {
            btnTestAirtable.disabled = false;
            btnTestAirtable.textContent = prev;
        }
    };

    if (btnTestAirtableRequests) btnTestAirtableRequests.onclick = async () => {
        const prev = btnTestAirtableRequests.textContent;
        btnTestAirtableRequests.disabled = true;
        btnTestAirtableRequests.textContent = 'Testing…';
        try {
            const r = await apiFetch('/api/integrations/airtable/requests/preview');
            const data = await r.json().catch(() => ({}));
            if (!r.ok || data?.ok === false) throw new Error(data?.error || 'Preview failed');
            const lines = [];
            lines.push(`Preview: ${Number(data.count) || 0} record(s)`);
            for (const rec of (data.records || [])) {
                lines.push(`- ${safeText(rec.title) || '(Untitled)'} (${safeText(rec.id)})`);
            }
            setAirtableOutput(lines.join('\n'));
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Failed to test Airtable requests';
            setAirtableOutput(msg);
            alert(msg);
        } finally {
            btnTestAirtableRequests.disabled = false;
            btnTestAirtableRequests.textContent = prev;
        }
    };

    if (btnSyncAirtableRequests) btnSyncAirtableRequests.onclick = async () => {
        const prev = btnSyncAirtableRequests.textContent;
        btnSyncAirtableRequests.disabled = true;
        btnSyncAirtableRequests.textContent = 'Syncing…';
        try {
            const r = await apiFetch('/api/integrations/airtable/requests/sync', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ limit: 200 }),
            });
            const data = await r.json().catch(() => ({}));
            if (!r.ok || data?.ok === false) throw new Error(data?.error || 'Sync failed');
            const created = Number(data.created) || 0;
            const updated = Number(data.updated) || 0;
            const skipped = Number(data.skipped) || 0;
            const fetched = Number(data.totalFetched) || 0;
            setAirtableOutput(`Synced requests → projects. Created: ${created}, Updated: ${updated}, Skipped: ${skipped}, Fetched: ${fetched}`);
            await fetchState();
            renderNav();
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Failed to sync Airtable requests';
            setAirtableOutput(msg);
            alert(msg);
        } finally {
            btnSyncAirtableRequests.disabled = false;
            btnSyncAirtableRequests.textContent = prev;
        }
    };

    if (btnSyncAirtable) btnSyncAirtable.onclick = async () => {
        const prev = btnSyncAirtable.textContent;
        btnSyncAirtable.disabled = true;
        btnSyncAirtable.textContent = 'Syncing…';
        try {
            const r = await apiFetch('/api/integrations/airtable/clients/sync', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ limit: 200 }),
            });
            const data = await r.json().catch(() => ({}));
            if (!r.ok || data?.ok === false) throw new Error(data?.error || 'Sync failed');
            const created = Number(data.created) || 0;
            const updated = Number(data.updated) || 0;
            const skipped = Number(data.skipped) || 0;
            const fetched = Number(data.totalFetched) || 0;
            setAirtableOutput(`Synced. Created: ${created}, Updated: ${updated}, Skipped: ${skipped}, Fetched: ${fetched}`);
            await fetchState();
            renderNav();
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Failed to sync Airtable';
            setAirtableOutput(msg);
            alert(msg);
        } finally {
            btnSyncAirtable.disabled = false;
            btnSyncAirtable.textContent = prev;
        }
    };

    if (btnSaveAi) btnSaveAi.onclick = async () => {
        try {
            const key = String(document.getElementById('set-openai-key')?.value || '').trim();
            const model = String(document.getElementById('set-openai-model')?.value || '').trim();
            const applyRoutes = !!document.getElementById('set-openai-apply-routes')?.checked;
            if (!model) {
                alert('OpenAI model is required.');
                return;
            }
            const patch = { openaiModel: model };
            if (key) patch.openaiApiKey = key;
            if (applyRoutes) {
                const routes = (state.settings?.aiRoutes && typeof state.settings.aiRoutes === 'object')
                    ? { ...state.settings.aiRoutes }
                    : {};
                const routeKeys = ['marcusChat', 'operatorBio', 'projectAssistant', 'dashboardPreview'];
                for (const rk of routeKeys) {
                    const existing = (routes[rk] && typeof routes[rk] === 'object') ? routes[rk] : {};
                    const provider = String(existing.provider || 'openai').trim().toLowerCase() || 'openai';
                    if (provider === 'openai') {
                        routes[rk] = { ...existing, provider: 'openai', model };
                    } else {
                        routes[rk] = { ...existing, provider };
                    }
                }
                patch.aiRoutes = routes;
            } else {
                const routes = (state.settings?.aiRoutes && typeof state.settings.aiRoutes === 'object')
                    ? { ...state.settings.aiRoutes }
                    : {};
                const routeKeys = ['marcusChat', 'operatorBio', 'projectAssistant', 'dashboardPreview'];
                for (const rk of routeKeys) {
                    const existing = (routes[rk] && typeof routes[rk] === 'object') ? routes[rk] : {};
                    const provider = String(existing.provider || 'openai').trim().toLowerCase() || 'openai';
                    if (provider !== 'openai') {
                        routes[rk] = { ...existing, provider };
                        continue;
                    }
                    const selectedModel = String(document.getElementById(`set-openai-route-model-${rk}`)?.value || '').trim();
                    routes[rk] = { ...existing, provider: 'openai', model: selectedModel };
                }
                patch.aiRoutes = routes;
            }
            await saveSettingsPatch(patch);
            alert(applyRoutes ? 'AI settings saved and applied to OpenAI routes.' : 'AI settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save AI settings');
        }
    };

    if (btnSaveAgent) btnSaveAgent.onclick = async () => {
        try {
            const operatorBio = String(document.getElementById('set-operator-bio')?.value || '').trimEnd();
            const assistantOperatingDoctrine = String(document.getElementById('set-assistant-operating-doctrine')?.value || '').trimEnd();
            const personalityLayer = String(document.getElementById('set-personality-layer')?.value || '').trimEnd();
            const attentionRadar = String(document.getElementById('set-attention-radar')?.value || '').trimEnd();
            const strategicForecasting = String(document.getElementById('set-strategic-forecasting')?.value || '').trimEnd();
            const executionAuthority = String(document.getElementById('set-execution-authority')?.value || '').trimEnd();
            const knowledgeArchive = String(document.getElementById('set-knowledge-archive')?.value || '').trimEnd();
            const dailyReportingStructure = String(document.getElementById('set-daily-reporting-structure')?.value || '').trimEnd();

            // Keep legacy key in sync so older server builds still pick it up.
            await saveSettingsPatch({
                operatorBio,
                assistantOperatingDoctrine,
                personalityLayer,
                attentionRadar,
                strategicForecasting,
                executionAuthority,
                knowledgeArchive,
                dailyReportingStructure,
                operatorHelpPrompt: assistantOperatingDoctrine,
            });
            alert('Marcus settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save agent settings');
        }
    };

    if (btnClearAi) btnClearAi.onclick = async () => {
        try {
            await saveSettingsPatch({ openaiApiKey: '' });
            alert('Stored AI key cleared.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to clear AI key');
        }
    };

    if (btnSaveGoogle) btnSaveGoogle.onclick = async () => {
        try {
            const clientId = String(document.getElementById('set-google-client-id')?.value || '').trim();
            const clientSecret = String(document.getElementById('set-google-client-secret')?.value || '').trim();
            const readCalendarId = String(document.getElementById('set-google-read-calendar-id')?.value || '').trim();
            const patch = { googleClientId: clientId, googleReadCalendarId: readCalendarId };
            if (clientSecret) patch.googleClientSecret = clientSecret;
            await saveSettingsPatch(patch);
            alert('Google settings saved.');
            await refreshGoogleStatus();
        } catch (e) {
            alert(e?.message || 'Failed to save Google settings');
        }
    };

    if (btnSaveEmail) btnSaveEmail.onclick = async () => {
        try {
            const patch = {
                imapHost: String(document.getElementById('set-imap-host')?.value || '').trim(),
                imapPort: Math.max(1, Number(document.getElementById('set-imap-port')?.value || 993) || 993),
                imapSecure: !!document.getElementById('set-imap-secure')?.checked,
                imapUsername: String(document.getElementById('set-imap-username')?.value || '').trim(),
                imapSyncFolders: splitTextareaList(document.getElementById('set-imap-sync-folders')?.value || 'INBOX'),
                imapArchiveFolders: splitTextareaList(document.getElementById('set-imap-archive-folders')?.value || 'Archive\nAll Mail'),
                smtpHost: String(document.getElementById('set-smtp-host')?.value || '').trim(),
                smtpPort: Math.max(1, Number(document.getElementById('set-smtp-port')?.value || 465) || 465),
                smtpSecure: !!document.getElementById('set-smtp-secure')?.checked,
                smtpUsername: String(document.getElementById('set-smtp-username')?.value || '').trim(),
                smtpFromAddress: String(document.getElementById('set-smtp-from-address')?.value || '').trim(),
                emailSyncEnabled: !!document.getElementById('set-email-sync-enabled')?.checked,
                emailArchiveKnowledgeEnabled: !!document.getElementById('set-email-archive-knowledge-enabled')?.checked,
            };
            const imapPassword = String(document.getElementById('set-imap-password')?.value || '').trim();
            const smtpPassword = String(document.getElementById('set-smtp-password')?.value || '').trim();
            if (imapPassword) patch.imapPassword = imapPassword;
            if (smtpPassword) patch.smtpPassword = smtpPassword;
            await saveSettingsPatch(patch);
            const imapPassEl = document.getElementById('set-imap-password');
            const smtpPassEl = document.getElementById('set-smtp-password');
            if (imapPassEl) imapPassEl.value = '';
            if (smtpPassEl) smtpPassEl.value = '';
            alert('Email settings saved.');
            await refreshEmailStatus();
        } catch (e) {
            alert(e?.message || 'Failed to save email settings');
        }
    };

    if (btnTestEmail) btnTestEmail.onclick = async () => {
        const prev = btnTestEmail.textContent;
        btnTestEmail.disabled = true;
        btnTestEmail.textContent = 'Testing…';
        try {
            setEmailOutput('Testing IMAP / SMTP...');
            const data = await apiJson('/api/integrations/email/test', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
            setEmailOutput(JSON.stringify(data, null, 2));
            await refreshEmailStatus();
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Email test failed';
            setEmailOutput(`Error: ${msg}`);
            alert(msg);
        } finally {
            btnTestEmail.disabled = false;
            btnTestEmail.textContent = prev;
        }
    };

    if (btnSyncEmail) btnSyncEmail.onclick = async () => {
        const prev = btnSyncEmail.textContent;
        btnSyncEmail.disabled = true;
        btnSyncEmail.textContent = 'Syncing…';
        try {
            setEmailOutput('Syncing email inbox...');
            const data = await apiJson('/api/integrations/email/sync', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ limitPerFolder: 25, sinceDays: 30, unseenOnly: false }),
            });
            setEmailOutput(JSON.stringify(data, null, 2));
            await fetchState();
            renderNav();
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Email sync failed';
            setEmailOutput(`Error: ${msg}`);
            alert(msg);
        } finally {
            btnSyncEmail.disabled = false;
            btnSyncEmail.textContent = prev;
        }
    };

    if (btnSendTestEmail) btnSendTestEmail.onclick = async () => {
        const prev = btnSendTestEmail.textContent;
        btnSendTestEmail.disabled = true;
        btnSendTestEmail.textContent = 'Sending…';
        try {
            const to = String(document.getElementById('set-email-test-recipient')?.value || '').trim();
            if (!to) throw new Error('Test recipient is required');
            setEmailOutput('Sending SMTP test email...');
            const data = await apiJson('/api/integrations/email/send', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    to,
                    subject: `Marcus email test ${new Date().toISOString()}`,
                    text: 'This is a test message from the Marcus IMAP/SMTP integration.',
                }),
            });
            setEmailOutput(JSON.stringify(data, null, 2));
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Failed to send test email';
            setEmailOutput(`Error: ${msg}`);
            alert(msg);
        } finally {
            btnSendTestEmail.disabled = false;
            btnSendTestEmail.textContent = prev;
        }
    };

    if (btnEmailArchiveQdrant) btnEmailArchiveQdrant.onclick = async () => {
        const prev = btnEmailArchiveQdrant.textContent;
        btnEmailArchiveQdrant.disabled = true;
        btnEmailArchiveQdrant.textContent = 'Ingesting…';
        try {
            setEmailOutput('Pulling archived IMAP messages into Qdrant...');
            const data = await apiJson('/api/integrations/email/archive-to-qdrant', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ source: 'imap', limitPerFolder: 50, sinceDays: 3650 }),
            });
            setEmailOutput(JSON.stringify(data, null, 2));
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Archive ingestion failed';
            setEmailOutput(`Error: ${msg}`);
            alert(msg);
        } finally {
            btnEmailArchiveQdrant.disabled = false;
            btnEmailArchiveQdrant.textContent = prev;
        }
    };

    if (btnEmailLocalArchiveQdrant) btnEmailLocalArchiveQdrant.onclick = async () => {
        const prev = btnEmailLocalArchiveQdrant.textContent;
        btnEmailLocalArchiveQdrant.disabled = true;
        btnEmailLocalArchiveQdrant.textContent = 'Ingesting…';
        try {
            setEmailOutput('Pushing locally archived inbox email into Qdrant...');
            const data = await apiJson('/api/integrations/email/archive-to-qdrant', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ source: 'local' }),
            });
            setEmailOutput(JSON.stringify(data, null, 2));
        } catch (e) {
            const msg = safeText(e?.message || '').trim() || 'Local archive ingestion failed';
            setEmailOutput(`Error: ${msg}`);
            alert(msg);
        } finally {
            btnEmailLocalArchiveQdrant.disabled = false;
            btnEmailLocalArchiveQdrant.textContent = prev;
        }
    };

    if (btnConnectGoogle) btnConnectGoogle.onclick = async () => {
        try {
            await openGoogleAuthWindow();
        } catch (e) {
            alert(e?.message || 'Failed to start Google connect');
        } finally {
            await refreshGoogleStatus();
        }
    };

    if (btnDisconnectGoogle) btnDisconnectGoogle.onclick = async () => {
        try {
            await saveSettingsPatch({ googleTokens: null, googleCalendarId: '', googleProjectEventIds: {} });
            alert('Google disconnected.');
            await refreshGoogleStatus();
        } catch (e) {
            alert(e?.message || 'Failed to disconnect');
        }
    };

    if (btnSyncGoogle) btnSyncGoogle.onclick = async () => {
        try {
            const r = await apiFetch('/api/integrations/google/sync', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
            const data = await r.json().catch(() => ({}));
            if (!r.ok || !data.ok) throw new Error(data?.message || data?.error || 'Sync failed');
            alert(`Synced. Pushed: ${data.pushed || 0}, Pulled updates: ${data.pulledUpdates || 0}`);
            await fetchState();
        } catch (e) {
            alert(e?.message || 'Failed to sync');
        }
    };

    if (btnUpcomingGoogle) btnUpcomingGoogle.onclick = async () => {
        try {
            if (googleUpcomingOutput) {
                googleUpcomingOutput.classList.remove('hidden');
                googleUpcomingOutput.textContent = 'Loading upcoming events...';
            }
            const r = await apiFetch('/api/integrations/google/upcoming?days=7&max=25');
            const data = await r.json().catch(() => ({}));
            if (!r.ok || !data.ok) throw new Error(data?.message || data?.error || 'Failed to load events');

            const events = Array.isArray(data.events) ? data.events : [];
            const lines = [];
            lines.push(`Calendar: ${data.calendarId || 'primary'}`);
            lines.push(`Events: ${events.length}`);
            lines.push('');
            for (const ev of events) {
                const when = ev.start ? String(ev.start) : '';
                const title = ev.summary ? String(ev.summary) : '(no title)';
                const link = ev.meetingLink ? ` ${ev.meetingLink}` : '';
                lines.push(`- ${when} :: ${title}${link}`);
            }
            if (googleUpcomingOutput) googleUpcomingOutput.textContent = lines.join('\n');
        } catch (e) {
            if (googleUpcomingOutput) {
                googleUpcomingOutput.classList.remove('hidden');
                googleUpcomingOutput.textContent = `Error: ${e?.message || 'Failed to load events'}`;
            }
        }
    };

    if (btnGenFireflies) btnGenFireflies.onclick = () => {
        const el = document.getElementById('set-fireflies-secret');
        if (el) el.value = generateSecret();
    };

    if (btnSaveFireflies) btnSaveFireflies.onclick = async () => {
        try {
            const secret = String(document.getElementById('set-fireflies-secret')?.value || '').trim();
            if (!secret) throw new Error('Secret is required');
            await saveSettingsPatch({ firefliesSecret: secret });
            alert('Fireflies settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save Fireflies settings');
        }
    };

    if (btnGenCrm) btnGenCrm.onclick = () => {
        const el = document.getElementById('set-crm-webhook-secret');
        if (el) el.value = generateSecret();
    };

    if (btnSaveCrm) btnSaveCrm.onclick = async () => {
        try {
            const crmApiBaseUrl = String(document.getElementById('set-crm-api-base')?.value || '').trim();
            const crmApiKey = String(document.getElementById('set-crm-api-key')?.value || '').trim();
            const crmWebhookSecret = String(document.getElementById('set-crm-webhook-secret')?.value || '').trim();

            const patch = { crmApiBaseUrl };
            if (crmApiKey) patch.crmApiKey = crmApiKey;
            if (crmWebhookSecret) patch.crmWebhookSecret = crmWebhookSecret;

            if (!crmWebhookSecret && !state.settings.crmConfigured) {
                throw new Error('Webhook secret is required');
            }

            await saveSettingsPatch(patch);
            alert('CRM settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save CRM settings');
        }
    };

    if (btnSaveGa4) btnSaveGa4.onclick = async () => {
        try {
            const ga4PropertyId = String(document.getElementById('set-ga4-property-id')?.value || '').trim();
            if (!ga4PropertyId) throw new Error('GA4 Property ID is required');

            const patch = { ga4PropertyId };

            await saveSettingsPatch(patch);
            alert('GA4 settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save GA4 settings');
        }
    };

    if (btnPullGa4) btnPullGa4.onclick = async () => {
        try {
            if (ga4PullOutput) {
                ga4PullOutput.classList.remove('hidden');
                ga4PullOutput.textContent = 'Pulling GA4 daily summary...';
            }
            const r = await apiFetch('/api/integrations/ga4/pull-now', { method: 'POST' });
            const data = await r.json().catch(() => ({}));
            if (!r.ok || !data.ok) throw new Error(data?.error || 'GA4 pull failed');

            if (ga4PullOutput) {
                const lines = [];
                if (data.skipped) lines.push(`Skipped: ${data.reason || 'n/a'}`);
                if (data.date) lines.push(`Date: ${data.date}`);
                if (typeof data.sessions === 'number') lines.push(`Sessions: ${data.sessions}`);
                if (typeof data.users === 'number') lines.push(`Users: ${data.users}`);
                if ('inboxCreated' in (data || {})) lines.push(`Inbox: ${data.inboxCreated ? 'created' : 'already exists (deduped)'}`);
                ga4PullOutput.textContent = lines.join('\n') || 'OK';
            }
            await fetchState();
        } catch (e) {
            if (ga4PullOutput) {
                ga4PullOutput.classList.remove('hidden');
                ga4PullOutput.textContent = `Error: ${e?.message || 'GA4 pull failed'}`;
            } else {
                alert(e?.message || 'GA4 pull failed');
            }
        }
    };

    if (btnSaveSlack) btnSaveSlack.onclick = async () => {
        try {
            const signingSecret = String(document.getElementById('set-slack-signing-secret')?.value || '').trim();
            const clientId = String(document.getElementById('set-slack-client-id')?.value || '').trim();
            const clientSecret = String(document.getElementById('set-slack-client-secret')?.value || '').trim();
            const botToken = String(document.getElementById('set-slack-bot-token')?.value || '').trim();

            const patch = {};
            if (signingSecret) patch.slackSigningSecret = signingSecret;
            if (clientId) patch.slackClientId = clientId;
            if (clientSecret) patch.slackClientSecret = clientSecret;
            if (botToken) patch.slackBotToken = botToken;

            if (!Object.keys(patch).length) {
                if (!state.settings.slackConfigured) throw new Error('Signing secret is required');
                if (!state.settings.slackOAuthConfigured) throw new Error('Slack Client ID/Secret is required for OAuth connect');
                alert('No changes to save.');
                return;
            }

            await saveSettingsPatch(patch);
            alert('Slack settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save Slack settings');
        }
    };

    if (btnDiagSlack) btnDiagSlack.onclick = async () => {
        try {
            if (slackTestOutput) {
                slackTestOutput.classList.remove('hidden');
                slackTestOutput.textContent = 'Loading Slack diagnostics...';
            }
            const r = await apiFetch('/api/integrations/slack/diagnostics');
            const data = await r.json().catch(() => ({}));
            if (!r.ok || !data.ok) throw new Error(data?.error || 'Failed to load diagnostics');
            if (slackTestOutput) slackTestOutput.textContent = JSON.stringify(data, null, 2);
        } catch (e) {
            if (slackTestOutput) {
                slackTestOutput.classList.remove('hidden');
                slackTestOutput.textContent = `Error: ${e?.message || 'Failed to load diagnostics'}`;
            }
        }
    };

    if (btnTestSlack) btnTestSlack.onclick = async () => {
        try {
            const target = String(document.getElementById('set-slack-test-target')?.value || '').trim();
            if (!target) throw new Error('Enter a test target like @yourname (DM) or a channel ID (C123...).');
            if (slackTestOutput) {
                slackTestOutput.classList.remove('hidden');
                slackTestOutput.textContent = 'Sending Slack test message...';
            }
            const payload = {
                text: `Marcus Slack test (${new Date().toISOString()})`,
            };
            payload.channel = target;

            const r = await apiFetch('/api/integrations/slack/send-summary', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const data = await r.json().catch(() => ({}));
            if (!r.ok) throw new Error(data?.error || `Slack test failed (${r.status})`);
            if (slackTestOutput) slackTestOutput.textContent = JSON.stringify(data, null, 2);
        } catch (e) {
            if (slackTestOutput) {
                slackTestOutput.classList.remove('hidden');
                slackTestOutput.textContent = `Error: ${e?.message || 'Slack test failed'}`;
            }
        }
    };

    if (btnConnectSlack) btnConnectSlack.onclick = async () => {
        try {
            await openSlackAuthWindow();
        } catch (e) {
            alert(e?.message || 'Failed to start Slack connect');
        }
    };

    if (btnDisconnectSlack) btnDisconnectSlack.onclick = async () => {
        try {
            await saveSettingsPatch({
                slackBotToken: '',
                slackTeamId: '',
                slackTeamName: '',
                slackBotUserId: '',
                slackAppId: '',
                slackScopes: '',
                slackInstalledAt: '',
            });
            alert('Slack disconnected.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to disconnect Slack');
        }
    };

    if (btnSaveQuo) btnSaveQuo.onclick = async () => {
        try {
            const token = String(document.getElementById('set-quo-auth-token')?.value || '').trim();
            const rawLevel = String(document.getElementById('set-sms-ack-filter-level')?.value || '').trim().toLowerCase();
            const smsAckFilterLevel = (rawLevel === 'off' || rawLevel === 'low' || rawLevel === 'medium' || rawLevel === 'high')
                ? rawLevel
                : 'medium';
            const phoneInputs = Array.from(document.querySelectorAll('[data-quo-map-phone]'));
            const businessInputs = Array.from(document.querySelectorAll('[data-quo-map-business]'));
            const map = {};
            const normalizePhone = (v) => String(v || '').replace(/[^\d+]/g, '').replace(/[^\d]/g, '');
            for (let i = 0; i < Math.max(phoneInputs.length, businessInputs.length); i += 1) {
                const phone = normalizePhone(phoneInputs[i]?.value || '');
                const business = String(businessInputs[i]?.value || '').trim();
                if (!phone || !business) continue;
                map[phone] = business;
            }

            if (!token && !state.settings.quoConfigured && Object.keys(map).length === 0) {
                throw new Error('Add auth token and at least one phone mapping');
            }

            const patch = { phoneBusinessMap: map, smsAckFilterLevel };
            if (token) patch.quoAuthToken = token;
            await saveSettingsPatch(patch);
            await fetchState({ background: false });
            alert('Quo settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save Quo settings');
        }
    };

    if (btnSaveGhl) btnSaveGhl.onclick = async () => {
        try {
            const apiKey = String(document.getElementById('set-ghl-api-key')?.value || '').trim();
            const locationId = String(document.getElementById('set-ghl-location-id')?.value || '').trim();
            const apiBaseUrl = String(document.getElementById('set-ghl-api-base')?.value || '').trim();

            const patch = {
                ghlLocationId: locationId,
                ghlApiBaseUrl: apiBaseUrl,
            };
            if (apiKey) patch.ghlApiKey = apiKey;

            if (!state.settings.ghlConfigured && !apiKey) throw new Error('API key is required for first-time setup');
            if (!locationId) throw new Error('Location ID is required');

            await saveSettingsPatch(patch);
            alert('Mini GHL settings saved.');
            await refreshDashboardGhl({ force: true });
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save Mini GHL settings');
        }
    };

    if (btnTestGhl) btnTestGhl.onclick = async () => {
        try {
            if (ghlTestOutput) {
                ghlTestOutput.classList.remove('hidden');
                ghlTestOutput.textContent = 'Testing Mini GHL snapshot...';
            }

            const r = await apiFetch('/api/integrations/ghl/snapshot');
            const data = await r.json().catch(() => ({}));
            if (!r.ok || !data.ok) throw new Error(data?.error || 'Mini GHL snapshot failed');

            const lines = [];
            lines.push(`Location: ${String(data.locationId || 'n/a')}`);
            lines.push(`Pipeline open: ${Number(data?.pipeline?.open || 0)} / total ${Number(data?.pipeline?.total || 0)}`);
            lines.push(`Conversations unread: ${Number(data?.conversations?.unread || 0)} / total ${Number(data?.conversations?.total || 0)}`);
            lines.push(`Appointments (7d): ${Number(data?.appointments?.upcoming || 0)}`);
            if (Array.isArray(data?.warnings) && data.warnings.length) {
                lines.push('');
                lines.push(`Warnings: ${data.warnings.join(' | ')}`);
            }
            if (ghlTestOutput) ghlTestOutput.textContent = lines.join('\n');

            state.dashboardGhl = { loading: false, fetchedAt: Date.now(), error: '', snapshot: data };
        } catch (e) {
            if (ghlTestOutput) {
                ghlTestOutput.classList.remove('hidden');
                ghlTestOutput.textContent = `Error: ${e?.message || 'Mini GHL snapshot failed'}`;
            }
        }
    };

    if (btnSaveMcp) btnSaveMcp.onclick = async () => {
        try {
            const enabled = !!document.getElementById('set-mcp-enabled')?.checked;
            const command = String(document.getElementById('set-mcp-command')?.value || '').trim();
            const args = String(document.getElementById('set-mcp-args')?.value || '').trim();
            const cwd = String(document.getElementById('set-mcp-cwd')?.value || '').trim();

            await saveSettingsPatch({ mcp: { enabled, command, args, cwd } });
            alert('MCP settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save MCP settings');
        }
    };

    if (btnTestMcp) btnTestMcp.onclick = async () => {
        try {
            if (mcpToolsOutput) {
                mcpToolsOutput.classList.remove('hidden');
                mcpToolsOutput.textContent = 'Testing MCP...';
            }

            const r = await apiFetch('/api/integrations/mcp/tools', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
            const data = await r.json().catch(() => ({}));
            if (!r.ok || !data.ok) throw new Error(data?.error || 'MCP test failed');

            const tools = Array.isArray(data.tools) ? data.tools : [];
            const lines = [];
            lines.push(`OK. Tools: ${tools.length}`);
            for (const t of tools.slice(0, 50)) {
                const name = t && t.name ? String(t.name) : '';
                const desc = t && t.description ? String(t.description) : '';
                lines.push(`- ${name}${desc ? ` :: ${desc}` : ''}`);
            }
            if (tools.length > 50) lines.push(`...and ${tools.length - 50} more`);
            if (mcpToolsOutput) mcpToolsOutput.textContent = lines.join('\n');
        } catch (e) {
            if (mcpToolsOutput) {
                mcpToolsOutput.classList.remove('hidden');
                mcpToolsOutput.textContent = `Error: ${e?.message || 'MCP test failed'}`;
            }
        }
    };

    if (btnSaveUi) btnSaveUi.onclick = async () => {
        try {
            const autoRefreshSeconds = Math.max(10, Number(document.getElementById('set-auto-refresh')?.value || 30));
            const weekStartsOnMonday = !!document.getElementById('set-week-monday')?.checked;
            const defaultShowCompleted = !!document.getElementById('set-show-completed')?.checked;
            await saveSettingsPatch({ autoRefreshSeconds, weekStartsOnMonday, defaultShowCompleted });
            alert('Preferences saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save preferences');
        }
    };

    if (btnSaveAdvanced) btnSaveAdvanced.onclick = async () => {
        try {
            const raw = String(document.getElementById('set-advanced-json')?.value || '');
            const parsed = JSON.parse(raw);
            if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
                throw new Error('JSON must be an object');
            }

            // Never persist read-only fields.
            delete parsed.aiEnabled;
            delete parsed.openaiKeyHint;
            delete parsed.source;
            delete parsed.settingsUpdatedAt;
            delete parsed.googleConfigured;
            delete parsed.googleConnected;
            delete parsed.firefliesConfigured;
            delete parsed.slackConfigured;
            delete parsed.slackOAuthConfigured;
            delete parsed.slackInstalled;
            delete parsed.quoConfigured;
            delete parsed.ghlConfigured;
            delete parsed.imapConfigured;
            delete parsed.smtpConfigured;
            delete parsed.emailSyncEnabled;
            delete parsed.emailArchiveKnowledgeEnabled;

            await saveSettingsPatch(parsed);
            alert('Advanced settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save advanced settings');
        }
    };

    if (btnResetAdvanced) btnResetAdvanced.onclick = () => {
        const el = document.getElementById('set-advanced-json');
        if (el) el.value = JSON.stringify(safeSettings, null, 2);
    };
}

function renderProjects(container) {
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Projects';

    const activeProjects = getActiveProjects();
    const archivedProjects = getArchivedProjects();
    const teamOwnerOptions = [''].concat(getAssignableOwnerNames());

    const wrap = document.createElement('div');
    wrap.className = 'h-full flex flex-col min-h-0';

    const content = document.createElement('div');
    content.className = 'flex-1 min-h-0 overflow-y-auto p-6';

    // New Project Intake
    const intake = document.createElement('div');
    intake.className = 'mb-10 border border-white/10 rounded-2xl bg-white/5 backdrop-blur-xl p-5 shadow-2xl';

    const d = state.newProjectDraft || {};
    intake.innerHTML = `
        <div class="flex items-center justify-between gap-3">
            <div>
                <div class="text-white text-sm font-semibold">New Project Intake</div>
                <div class="text-[11px] text-zinc-500 mt-0.5">Name + type + brief + value + due date + links.</div>
            </div>
            <button id="btn-toggle-intake" class="px-3 py-1.5 rounded-lg border border-zinc-700 text-xs text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">
                ${state.showNewProjectIntake ? 'Hide' : 'Create'}
            </button>
        </div>
        <div id="intake-body" class="${state.showNewProjectIntake ? '' : 'hidden'} mt-4">
            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                    <label class="text-[11px] text-zinc-400">Project name</label>
                    <input id="np-name" type="text" value="${String(d.name || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40" placeholder="e.g. Freedom Scoopers Website" />
                </div>
                <div class="grid grid-cols-2 gap-3">
                    <div>
                        <label class="text-[11px] text-zinc-400">Type</label>
                        <select id="np-type" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40">
                            ${['Build','Rebuild','Revision','Workflow','Cleanup','Other'].map(t => `<option value="${t}" ${String(d.type||'')===t?'selected':''}>${t}</option>`).join('')}
                        </select>
                    </div>
                    <div>
                        <label class="text-[11px] text-zinc-400">Status</label>
                        <select id="np-status" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40">
                            ${['Active','On Hold','Done'].map(s => `<option value="${s}" ${String(d.status||'')===s?'selected':''}>${s}</option>`).join('')}
                        </select>
                    </div>
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Project value</label>
                    <input id="np-value" type="text" value="${String(d.projectValue || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="$5000" />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Due date</label>
                    <input id="np-due" type="date" value="${String(d.dueDate || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Client Name</label>
                    <input id="np-client-name" type="text" value="${String(d.clientName || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="Client Name" />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Client Phone</label>
                    <input id="np-client-phone" type="text" value="${String(d.clientPhone || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="+15551234567" />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Repo URL</label>
                    <input id="np-repo" type="text" value="${String(d.repoUrl || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="https://github.com/..." />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Docs URL</label>
                    <input id="np-docs" type="text" value="${String(d.docsUrl || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="https://..." />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Stripe invoice URL</label>
                    <input id="np-invoice" type="text" value="${String(d.stripeInvoiceUrl || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="https://invoice.stripe.com/..." />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Workspace path</label>
                    <div class="mt-1 flex gap-2">
                        <input id="np-workspace" type="text" value="${String(d.workspacePath || '').replace(/\"/g, '&quot;')}" class="flex-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="C:\\Work\\Client\\Project" />
                        <button id="btn-browse-np-workspace" type="button" class="shrink-0 px-3 py-2 rounded-lg border border-zinc-700 text-xs text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Browse</button>
                    </div>
                </div>
                <div class="md:col-span-2">
                    <label class="text-[11px] text-zinc-400">Agent brief (saved to project Scratchpad for Marcus)</label>
                    <textarea id="np-brief" rows="4" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="What is this project? Scope, constraints, stakeholders, success criteria...">${String(d.agentBrief || '')}</textarea>
                </div>
            </div>
            <div class="flex flex-wrap gap-2 mt-3">
                <button id="btn-create-project" class="px-3 py-2 rounded-lg bg-blue-600 text-white text-xs hover:bg-blue-500">Create project</button>
                <button id="btn-clear-intake" class="px-3 py-2 rounded-lg border border-zinc-700 text-xs text-zinc-300 hover:text-white hover:bg-zinc-800">Clear</button>
            </div>
            <div id="intake-error" class="text-xs text-red-400 mt-2 hidden"></div>
        </div>
    `;
    content.appendChild(intake);

    // Search
    const searchWrap = document.createElement('div');
    searchWrap.className = 'mb-4 flex flex-col md:flex-row md:items-end md:justify-between gap-3';
    searchWrap.innerHTML = `
        <div class="flex-1">
            <div class="text-[11px] text-zinc-500 mb-1">Search projects</div>
            <input id="projects-search" type="text" value="${escapeHtml(state.projectsSearch || '')}" class="w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40" placeholder="Name, client, brief, type, status..." />
        </div>
        <div class="shrink-0 text-xs font-mono text-zinc-500 bg-black/20 px-2 py-1 rounded-full border border-white/5" data-project-search-count></div>
    `;
    content.appendChild(searchWrap);

    const searchKey = (...parts) => parts.map((p) => safeText(p).trim().toLowerCase()).filter(Boolean).join(' ');

    const makeProjectListCard = (title, projects, emptyText) => {
        const wrap = document.createElement('div');
        const list = Array.isArray(projects) ? projects : [];
        wrap.innerHTML = `
            <div class="flex items-center justify-between gap-3 mb-4">
                <div class="text-white text-lg font-semibold tracking-tight">${escapeHtml(title)}</div>
                <div class="text-xs font-mono text-zinc-500 bg-black/20 px-2 py-0.5 rounded-full border border-white/5">${list.length}</div>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4" data-project-list></div>
        `;
        const body = wrap.querySelector('[data-project-list]');
        if (body) {
            if (!list.length) {
                const empty = document.createElement('div');
                empty.className = 'col-span-full p-4 border border-white/5 rounded-xl text-zinc-500 italic text-sm bg-black/10';
                empty.innerText = emptyText || 'None.';
                body.appendChild(empty);
            } else {
                for (const p of list) {
                    const card = document.createElement('div');
                    card.setAttribute('data-project-card', '1');
                    const ownerName = getProjectOwnerName(p);
                    const isActiveProject = !isArchivedProject(p);
                    card.dataset.searchKey = searchKey(p?.name, p?.clientName, p?.agentBrief, p?.type, p?.status, p?.dueDate, ownerName);
                    // Glassy Card
                    card.className = 'group w-full text-left p-4 rounded-xl border border-white/10 bg-white/5 backdrop-blur-md hover:bg-white/10 hover:border-white/20 transition-all duration-300 hover:-translate-y-1 hover:shadow-xl relative overflow-hidden flex flex-col gap-3 min-h-[120px]';
                    card.innerHTML = `
                        <div class="absolute inset-0 bg-gradient-to-br from-blue-500/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity"></div>
                        <div class="relative z-10 flex items-start justify-between gap-3">
                            <div class="min-w-0 flex-1">
                                <div class="text-white text-base font-semibold truncate tracking-tight">${escapeHtml(safeText(p?.name) || 'Untitled')}</div>
                                <div class="text-xs text-zinc-400 mt-0.5 max-w-[90%] truncate">${escapeHtml(safeText(p?.agentBrief) || 'No brief provided...')}</div>
                            </div>
                            <div class="shrink-0 text-[10px] font-mono px-2 py-1 rounded-full border ${safeText(p?.dueDate) ? 'text-blue-300 border-blue-500/30 bg-blue-500/10' : 'text-zinc-500 border-zinc-800 bg-black/20'}">
                                ${safeText(p?.dueDate) ? `Due ${escapeHtml(safeText(p?.dueDate))}` : 'No due date'}
                            </div>
                        </div>
                        <div class="relative z-10 mt-auto flex items-center gap-2 flex-wrap">
                            <span class="text-[10px] font-mono uppercase tracking-wider px-2 py-1 rounded border border-white/10 bg-black/20 text-emerald-300">
                                ${escapeHtml(safeText(p?.status) || 'Active')}
                            </span>
                            <span class="text-[10px] font-mono uppercase tracking-wider px-2 py-1 rounded border border-white/10 bg-black/20 text-purple-300">
                                ${escapeHtml(safeText(p?.type) || 'Other')}
                            </span>
                            <span class="text-[10px] font-mono uppercase tracking-wider px-2 py-1 rounded border ${ownerName ? 'border-blue-500/30 bg-blue-500/10 text-blue-200' : 'border-zinc-800 bg-black/20 text-zinc-500'}">
                                ${escapeHtml(ownerName || 'Unassigned')}
                            </span>
                        </div>
                        <div class="relative z-10 flex items-center gap-2">
                            ${isActiveProject ? `
                            <select data-project-owner="${escapeHtml(safeText(p?.id))}" class="flex-1 bg-zinc-950/40 border border-zinc-800 rounded px-2 py-1.5 text-[10px] font-mono text-zinc-200">
                                ${teamOwnerOptions.map((name) => {
                                    const label = name ? name : 'Unassigned';
                                    const selected = (name ? name : '') === (ownerName ? ownerName : '') ? 'selected' : '';
                                    return `<option value="${escapeHtml(name)}" ${selected}>${escapeHtml(label)}</option>`;
                                }).join('')}
                            </select>` : `<div class="flex-1 text-[10px] text-zinc-500 font-mono">Archived project</div>`}
                            <button type="button" data-project-open="${escapeHtml(safeText(p?.id))}" class="px-2.5 py-1.5 rounded border border-zinc-800 bg-zinc-950/30 hover:bg-zinc-900/40 text-zinc-300 text-[10px] font-mono">Open</button>
                        </div>
                    `;

                    card.querySelectorAll('select,button').forEach((el) => {
                        el.addEventListener('click', (e) => e.stopPropagation());
                        el.addEventListener('mousedown', (e) => e.stopPropagation());
                    });

                    const ownerSelect = card.querySelector('select[data-project-owner]');
                    if (ownerSelect && p?.id) {
                        ownerSelect.addEventListener('change', async (e) => {
                            const nextOwner = safeText(e?.target?.value).trim();
                            ownerSelect.disabled = true;
                            try {
                                await saveProjectPatch(p.id, { owner: nextOwner });
                                renderNav();
                                renderMain();
                            } catch (err) {
                                alert(err?.message || 'Failed to delegate project');
                                ownerSelect.disabled = false;
                            }
                        });
                    }

                    const openBtn = card.querySelector('button[data-project-open]');
                    if (openBtn) {
                        openBtn.addEventListener('click', async () => {
                            if (p?.id) await openProject(p.id);
                        });
                    }

                    card.onclick = async () => {
                        if (p?.id) await openProject(p.id);
                    };
                    body.appendChild(card);
                }
            }
        }
        return wrap;
    };

    const activeCard = makeProjectListCard('Active Projects', activeProjects, 'None.');
    content.appendChild(activeCard);

    const noMatches = document.createElement('div');
    noMatches.className = 'mt-4 p-4 border border-white/5 rounded-xl text-zinc-500 italic text-sm bg-black/10 hidden';
    noMatches.setAttribute('data-project-search-empty', '1');
    noMatches.innerText = 'No matching projects.';
    content.appendChild(noMatches);

    const applyProjectsSearch = () => {
        const q = safeText(state.projectsSearch).trim().toLowerCase();
        const cards = Array.from(content.querySelectorAll('[data-project-card]'));
        let visible = 0;
        for (const c of cards) {
            const key = safeText(c?.dataset?.searchKey).toLowerCase();
            const match = !q || key.includes(q);
            c.classList.toggle('hidden', !match);
            if (match) visible++;
        }
        const countEl = content.querySelector('[data-project-search-count]');
        if (countEl) countEl.textContent = cards.length ? (q ? `${visible}/${cards.length}` : `${cards.length}`) : '0';
        const emptyEl = content.querySelector('[data-project-search-empty]');
        if (emptyEl) emptyEl.classList.toggle('hidden', !(q && cards.length && visible === 0));
    };

    const searchInput = content.querySelector('#projects-search');
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            state.projectsSearch = safeText(e?.target?.value);
            applyProjectsSearch();
        });
        searchInput.addEventListener('keydown', (e) => {
            if (e?.key === 'Escape') {
                e.preventDefault();
                state.projectsSearch = '';
                searchInput.value = '';
                applyProjectsSearch();
            }
        });
    }
    applyProjectsSearch();

    // Wire intake controls
    const toggle = intake.querySelector('#btn-toggle-intake');
    const errEl = intake.querySelector('#intake-error');
    const setError = (msg) => {
        if (!errEl) return;
        const m = safeText(msg).trim();
        errEl.textContent = m;
        errEl.classList.toggle('hidden', !m);
    };

    if (toggle) {
        toggle.onclick = () => {
            state.showNewProjectIntake = !state.showNewProjectIntake;
            renderMain();
            if (state.showNewProjectIntake) {
                setTimeout(() => {
                    const nameEl = document.getElementById('np-name');
                    try { nameEl?.focus(); } catch {}
                }, 50);
            }
        };
    }

    const bind = (id, key) => {
        const el = intake.querySelector('#' + id);
        if (!el) return;
        el.addEventListener('input', () => {
            setNewProjectDraft({ [key]: el.value });
        });
        el.addEventListener('change', () => {
            setNewProjectDraft({ [key]: el.value });
        });
    };
    bind('np-name', 'name');
    bind('np-type', 'type');
    bind('np-status', 'status');
    bind('np-client-name', 'clientName');
    bind('np-client-phone', 'clientPhone');
    bind('np-value', 'projectValue');
    bind('np-due', 'dueDate');
    bind('np-repo', 'repoUrl');
    bind('np-docs', 'docsUrl');
    bind('np-invoice', 'stripeInvoiceUrl');
    bind('np-workspace', 'workspacePath');
    bind('np-brief', 'agentBrief');

    const btnBrowseNpWorkspace = intake.querySelector('#btn-browse-np-workspace');
    if (btnBrowseNpWorkspace) {
        btnBrowseNpWorkspace.onclick = async () => {
            const wsEl = intake.querySelector('#np-workspace');
            btnBrowseNpWorkspace.disabled = true;
            const original = btnBrowseNpWorkspace.textContent;
            btnBrowseNpWorkspace.textContent = '...';
            try {
                const picked = await pickFolderPath();
                if (!picked) {
                    setError('No folder selected. If the picker didn\'t appear, check that the server is running in your desktop session (not as a service) and try again.');
                    return;
                }
                if (wsEl) wsEl.value = picked;
                setNewProjectDraft({ workspacePath: picked });
                try { wsEl?.focus(); } catch {}
            } catch (e) {
                setError(e?.message || 'Failed to pick folder');
            } finally {
                btnBrowseNpWorkspace.disabled = false;
                btnBrowseNpWorkspace.textContent = original;
            }
        };
    }

    const btnCreate = intake.querySelector('#btn-create-project');
    if (btnCreate) {
        btnCreate.onclick = async () => {
            setError('');
            // Snapshot all form fields into draft before validating
            const fields = {np_name:'name',np_type:'type',np_status:'status',np_value:'projectValue',np_due:'dueDate',np_repo:'repoUrl',np_docs:'docsUrl',np_invoice:'stripeInvoiceUrl',np_workspace:'workspacePath',np_brief:'agentBrief',np_priority:'priority',np_importance:'importance',np_risk:'risk'};
            for (const [elId, key] of Object.entries(fields)) {
                const el = intake.querySelector('#' + elId.replace('_','-'));
                if (el) setNewProjectDraft({ [key]: el.value });
            }
            btnCreate.disabled = true;
            const original = btnCreate.textContent;
            btnCreate.textContent = 'Creating...';
            try {
                await createProjectFromDraft();
            } catch (e) {
                setError(e?.message || 'Failed to create project');
                btnCreate.disabled = false;
                btnCreate.textContent = original;
            }
        };
    }

    const btnClear = intake.querySelector('#btn-clear-intake');
    if (btnClear) {
        btnClear.onclick = () => {
            resetNewProjectDraft();
            renderMain();
        };
    }

    wrap.appendChild(content);
    container.appendChild(wrap);
}

function renderRevisions(container) {
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Revisions';

    const all = Array.isArray(state.projects) ? state.projects : [];
    const revisionsActive = all.filter((p) => isRevisionRequestProject(p) && !isContactOnlyProject(p) && !isArchivedProject(p));
    const revisionsArchived = all.filter((p) => isRevisionRequestProject(p) && !isContactOnlyProject(p) && isArchivedProject(p));

    const me = getCurrentUserName();
    const assigneeOptions = [''].concat(getAssignableOwnerNames());

    const latestRevisionSummaryPreview = (projectId) => {
        const notes = Array.isArray(state.projectNoteEntries?.[projectId]) ? state.projectNoteEntries[projectId] : [];
        const match = notes.find((n) => safeText(n?.kind).toLowerCase() === 'airtable' || safeText(n?.title).toLowerCase().includes('revision summary'));
        const content = safeText(match?.content);
        return previewText(content, 180);
    };

    const makeRevisionCard = (p) => {
        const card = document.createElement('div');
        card.setAttribute('data-rev-card', '1');
        card.className = 'group w-full text-left p-4 rounded-xl border border-white/10 bg-white/5 backdrop-blur-md hover:bg-white/10 hover:border-white/20 transition-all duration-300 hover:-translate-y-1 hover:shadow-xl relative overflow-hidden flex flex-col gap-3 min-h-[120px]';

        const owner = getProjectOwnerName(p);
        const ownerLabel = owner || 'Unassigned';
        const ownerTone = owner ? 'text-emerald-300 border-emerald-500/30 bg-emerald-500/10' : 'text-amber-300 border-amber-500/30 bg-amber-500/10';
        const due = safeText(p?.dueDate).trim();
        const status = safeText(p?.status).trim() || 'Active';
        const summaryPreview = latestRevisionSummaryPreview(p?.id);

        card.dataset.searchKey = [
            safeText(p?.name),
            safeText(p?.clientName),
            safeText(p?.agentBrief),
            safeText(ownerLabel),
            safeText(status),
            safeText(due),
            safeText(summaryPreview),
        ].join(' ').trim().toLowerCase();

        card.innerHTML = `
            <div class="absolute inset-0 bg-gradient-to-br from-blue-500/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity"></div>
            <div class="relative z-10 flex items-start justify-between gap-3">
                <div class="min-w-0 flex-1">
                    <div class="text-white text-base font-semibold truncate tracking-tight">${escapeHtml(safeText(p?.name) || 'Untitled')}</div>
                    <div class="text-xs text-zinc-400 mt-0.5 max-w-[90%] truncate">${escapeHtml(summaryPreview || previewText(safeText(p?.agentBrief) || '', 120) || 'No summary yet...')}</div>
                </div>
                <div class="shrink-0 flex flex-col items-end gap-2">
                    <div class="text-[10px] font-mono px-2 py-1 rounded-full border ${due ? 'text-blue-300 border-blue-500/30 bg-blue-500/10' : 'text-zinc-500 border-zinc-800 bg-black/20'}">${due ? `Due ${escapeHtml(due)}` : 'No due date'}</div>
                    <div class="text-[10px] font-mono px-2 py-1 rounded-full border ${ownerTone}" title="Assigned to">${escapeHtml(ownerLabel)}</div>
                </div>
            </div>
            <div class="relative z-10 mt-auto flex items-center justify-between gap-2">
                <div class="flex items-center gap-2">
                    <span class="text-[10px] font-mono uppercase tracking-wider px-2 py-1 rounded border border-white/10 bg-black/20 text-emerald-300">${escapeHtml(status)}</span>
                    <span class="text-[10px] font-mono uppercase tracking-wider px-2 py-1 rounded border border-white/10 bg-black/20 text-purple-300">Revision</span>
                    ${owner === me ? '<span class="text-[10px] font-mono uppercase tracking-wider px-2 py-1 rounded border border-blue-500/30 bg-blue-500/10 text-blue-200">Mine</span>' : ''}
                </div>
                <div class="flex items-center gap-2">
                    <select data-rev-assign class="bg-zinc-950/40 border border-zinc-800 rounded px-2 py-1 text-[10px] font-mono text-zinc-200">
                        ${assigneeOptions.map((name) => {
                            const label = name ? name : 'Unassigned';
                            const selected = (name ? name : '') === (owner ? owner : '') ? 'selected' : '';
                            return `<option value="${escapeHtml(name)}" ${selected}>${escapeHtml(label)}</option>`;
                        }).join('')}
                    </select>
                    <button data-rev-open type="button" class="px-2 py-1 rounded border border-zinc-800 bg-zinc-950/30 hover:bg-zinc-900/40 text-zinc-300 text-[10px] font-mono">Open</button>
                </div>
            </div>
        `;

        // Card interactions
        const openBtn = card.querySelector('[data-rev-open]');
        if (openBtn) {
            openBtn.addEventListener('click', async (e) => {
                e.preventDefault();
                e.stopPropagation();
                if (p?.id) await openProject(p.id);
            });
        }

        const select = card.querySelector('[data-rev-assign]');
        if (select) {
            const stop = (e) => { try { e.stopPropagation(); } catch {} };
            select.addEventListener('click', stop);
            select.addEventListener('mousedown', stop);
            select.addEventListener('touchstart', stop);
            select.addEventListener('change', async (e) => {
                const val = safeText(e?.target?.value).trim();
                try {
                    await saveProjectPatch(p.id, { owner: val });
                    renderNav();
                    renderMain();
                } catch (err) {
                    alert(err?.message || 'Failed to assign revision');
                }
            });
        }

        // Clicking anywhere else opens
        card.addEventListener('click', async () => {
            if (p?.id) await openProject(p.id);
        });

        return card;
    };

    const wrap = document.createElement('div');
    wrap.className = 'h-full flex flex-col min-h-0';

    const content = document.createElement('div');
    content.className = 'flex-1 min-h-0 overflow-y-auto p-6';
    content.innerHTML = `
        <div class="mb-4 flex flex-col md:flex-row md:items-end md:justify-between gap-3">
            <div>
                <div class="text-white text-lg font-semibold tracking-tight">Revision Requests</div>
                <div class="text-[11px] text-zinc-500 mt-0.5">Assign a revision to someone. If assigned to you, it will also appear in Projects.</div>
            </div>
            <div class="w-full md:max-w-sm">
                <div class="text-[11px] text-zinc-500 mb-1">Search revisions</div>
                <input id="revisions-search" type="text" value="${escapeHtml(state.revisionsSearch || '')}" class="w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40" placeholder="Name, notes, owner, status..." />
            </div>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4" data-rev-active></div>
        <div class="mt-10">
            <div class="flex items-center justify-between gap-3 mb-4">
                <div class="text-white text-lg font-semibold tracking-tight">Archived</div>
                <div class="text-xs font-mono text-zinc-500 bg-black/20 px-2 py-0.5 rounded-full border border-white/5" data-rev-archived-count>${revisionsArchived.length}</div>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4" data-rev-archived></div>
        </div>
    `;

    const activeEl = content.querySelector('[data-rev-active]');
    if (activeEl) {
        if (!revisionsActive.length) {
            const empty = document.createElement('div');
            empty.className = 'col-span-full p-4 border border-white/5 rounded-xl text-zinc-500 italic text-sm bg-black/10';
            empty.innerText = 'No active revision requests.';
            activeEl.appendChild(empty);
        } else {
            for (const p of revisionsActive) activeEl.appendChild(makeRevisionCard(p));
        }

        const empty = document.createElement('div');
        empty.className = 'col-span-full p-4 border border-white/5 rounded-xl text-zinc-500 italic text-sm bg-black/10 hidden';
        empty.setAttribute('data-rev-search-empty-active', '1');
        empty.innerText = 'No matching revision requests.';
        activeEl.appendChild(empty);
    }

    const archivedEl = content.querySelector('[data-rev-archived]');
    if (archivedEl) {
        if (!revisionsArchived.length) {
            const empty = document.createElement('div');
            empty.className = 'col-span-full p-4 border border-white/5 rounded-xl text-zinc-500 italic text-sm bg-black/10';
            empty.innerText = 'None.';
            archivedEl.appendChild(empty);
        } else {
            for (const p of revisionsArchived) archivedEl.appendChild(makeRevisionCard(p));
        }

        const empty = document.createElement('div');
        empty.className = 'col-span-full p-4 border border-white/5 rounded-xl text-zinc-500 italic text-sm bg-black/10 hidden';
        empty.setAttribute('data-rev-search-empty-archived', '1');
        empty.innerText = 'No matching archived revisions.';
        archivedEl.appendChild(empty);
    }

    const applyRevisionsSearch = () => {
        const q = safeText(state.revisionsSearch).trim().toLowerCase();
        const activeCards = Array.from(content.querySelectorAll('[data-rev-active] [data-rev-card]'));
        const archivedCards = Array.from(content.querySelectorAll('[data-rev-archived] [data-rev-card]'));

        const filter = (cards) => {
            let visible = 0;
            for (const c of cards) {
                const key = safeText(c?.dataset?.searchKey).toLowerCase();
                const match = !q || key.includes(q);
                c.classList.toggle('hidden', !match);
                if (match) visible++;
            }
            return visible;
        };

        const visActive = filter(activeCards);
        const visArchived = filter(archivedCards);

        const emptyActive = content.querySelector('[data-rev-search-empty-active]');
        if (emptyActive) emptyActive.classList.toggle('hidden', !(q && activeCards.length && visActive === 0));
        const emptyArchived = content.querySelector('[data-rev-search-empty-archived]');
        if (emptyArchived) emptyArchived.classList.toggle('hidden', !(q && archivedCards.length && visArchived === 0));

        const archivedCountEl = content.querySelector('[data-rev-archived-count]');
        if (archivedCountEl) archivedCountEl.textContent = q ? String(visArchived) : String(revisionsArchived.length);
    };

    const searchInput = content.querySelector('#revisions-search');
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            state.revisionsSearch = safeText(e?.target?.value);
            applyRevisionsSearch();
        });
        searchInput.addEventListener('keydown', (e) => {
            if (e?.key === 'Escape') {
                e.preventDefault();
                state.revisionsSearch = '';
                searchInput.value = '';
                applyRevisionsSearch();
            }
        });
    }
    applyRevisionsSearch();

    wrap.appendChild(content);
    container.appendChild(wrap);
}

function renderDashboardLegacy(container) {
    const titleEl = document.getElementById("page-title");
    if(titleEl) titleEl.innerText = "Command Dashboard";
    
    // Stats Banner
    const activeProjects = getActiveProjects();
    const archivedProjects = getArchivedProjects();
    const visibleProjects = state.showArchivedOnDashboard ? [...activeProjects, ...archivedProjects] : activeProjects;
    const totalTasks = state.tasks.length;
    const completedTasks = state.tasks.filter(t => isDoneTask(t)).length;
    const progress = totalTasks === 0 ? 0 : Math.round((completedTasks / totalTasks) * 100);
    
    const portraitCompact = isPortraitCompactMode();

    const wrap = document.createElement('div');
    wrap.className = 'h-full flex flex-col min-h-0';

    const banner = document.createElement("div");
    banner.className = "shrink-0 p-6 border-b border-zinc-800 bg-zinc-900/30";
    banner.innerHTML = `
        <div class="flex items-end justify-between gap-4">
            <div>
                <h2 class="text-2xl text-white font-light leading-tight">Marcus Command Dashboard</h2>
                <div class="text-xs text-zinc-500 mt-1">${activeProjects.length} active projects • rev ${state.revision}</div>
            </div>
            <div class="flex items-center gap-2">
                <button id="btn-toggle-bulk" class="px-3 py-1.5 rounded-lg border border-zinc-700 text-xs font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">
                    ${state.dashboardBulkMode ? 'Done selecting' : 'Bulk select'}
                </button>
                <button id="btn-toggle-archived" class="px-3 py-1.5 rounded-lg border border-zinc-700 text-xs font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">
                    ${state.showArchivedOnDashboard ? `Hide archived (${archivedProjects.length})` : `Show archived (${archivedProjects.length})`}
                </button>
            </div>
        </div>
        <div id="dashboard-bulkbar" class="${state.dashboardBulkMode ? '' : 'hidden'} mt-4 flex flex-wrap items-center justify-between gap-2">
            <div class="text-[11px] text-zinc-500 font-mono">Selected: <span id="dash-selected-count" class="text-white">0</span> / ${visibleProjects.length}</div>
            <div class="flex flex-wrap gap-2">
                <button id="btn-dash-select-all" class="px-3 py-1.5 rounded border border-zinc-800 text-[11px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Select all</button>
                <button id="btn-dash-clear" class="px-3 py-1.5 rounded border border-zinc-800 text-[11px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px" disabled>Clear</button>
                <button id="btn-dash-archive" class="px-3 py-1.5 rounded bg-blue-600/20 border border-blue-600/40 text-[11px] font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px" disabled>Archive selected</button>
                <button id="btn-dash-unarchive" class="px-3 py-1.5 rounded bg-zinc-900/30 border border-zinc-800 text-[11px] font-mono text-zinc-200 hover:bg-zinc-800/40 transition-colors transition-transform duration-150 ease-out active:translate-y-px" disabled>Unarchive selected</button>
                <button id="btn-dash-delete" class="px-3 py-1.5 rounded bg-red-600/20 border border-red-600/40 text-[11px] font-mono text-red-200 hover:bg-red-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px" disabled>Delete selected</button>
            </div>
        </div>
        <div class="mt-4 grid grid-cols-2 md:grid-cols-4 gap-3">
            ${createStatCard("Active Projects", activeProjects.length, "fa-layer-group", "text-blue-500")}
            ${createStatCard("Pending Tasks", totalTasks - completedTasks, "fa-list-check", "text-amber-500")}
            ${createStatCard("Completion Rate", progress + "%", "fa-chart-pie", "text-emerald-500")}
            ${createStatCard("Team Online", state.team.length, "fa-users", "text-indigo-500")}
        </div>
    `;

    const content = document.createElement("div");
    content.className = `flex-1 min-h-0 overflow-y-auto ${portraitCompact ? 'p-3' : 'p-6'}`;
    const buckets = bucketProjectsByDueDate(activeProjects);

    const allInboxItems = getDisplayInboxItems();
    const sourceCounts = {};
    for (const item of allInboxItems) {
        if (String(item?.status || '').toLowerCase() !== 'new') continue;
        const k = normalizeInboxSourceKey(item?.source);
        sourceCounts[k] = Number(sourceCounts[k] || 0) + 1;
    }
    const sourceSummary = Object.entries(sourceCounts)
        .sort((a, b) => Number(b[1]) - Number(a[1]))
        .slice(0, 3)
        .map(([key, count]) => `${inboxSourceMeta(key).label}: ${count}`)
        .join(' • ');

    const pendingDoneEntries = Object.entries(state.taskDoneUndoById && typeof state.taskDoneUndoById === 'object' ? state.taskDoneUndoById : {});
    const pendingDone = pendingDoneEntries.length
        ? { id: pendingDoneEntries[0][0], ...(pendingDoneEntries[0][1] || {}) }
        : null;

    const nextActions = getTodayNextActions().filter((task) => !state.taskDoneUndoById?.[safeText(task?.id)]).slice(0, 3);
    const nextAction = nextActions[0] || null;
    const callsConnected = !!state.settings?.googleConnected;
    const calls = Array.isArray(state.dashboardCalls?.events) ? state.dashboardCalls.events : [];
    if (callsConnected) setTimeout(() => refreshDashboardCalls({ force: false }), 0);
    const nextCall = calls[0] || null;
    const ghlConfigured = !!state.settings?.ghlConfigured;
    const ghlSnapshot = (state.dashboardGhl && typeof state.dashboardGhl.snapshot === 'object') ? state.dashboardGhl.snapshot : null;
    const ghlError = safeText(state.dashboardGhl?.error);
    const ghlLoading = !!state.dashboardGhl?.loading;
    if (ghlConfigured) setTimeout(() => refreshDashboardGhl({ force: false }), 0);

    const nowYmd = ymdToday();
    const atRiskProjects = activeProjects.filter((project) => {
        const due = safeText(project?.dueDate);
        const tasks = getProjectTasks(project).filter((t) => !isDoneTask(t));
        if (!tasks.length) return false;
        const hasOverdueTask = tasks.some((t) => {
            const d = safeText(t?.dueDate);
            return d && nowYmd && d < nowYmd;
        });
        if (hasOverdueTask) return true;
        if (due && nowYmd && due <= nowYmd) return true;
        return false;
    });

    const nextTaskRowsHtml = pendingDone
        ? `
            <div class="mt-2 border border-amber-600/30 rounded-lg bg-amber-600/5 px-2 py-2">
                <div class="text-[11px] text-zinc-100 truncate">${escapeHtml(safeText(pendingDone.title) || 'Task marked done')}</div>
                <div class="text-[10px] font-mono text-amber-200 mt-0.5">Auto-archiving in 5s • ${escapeHtml(safeText(pendingDone.project) || 'No project')}</div>
                <div class="mt-1.5"><button data-next-task-undo="${escapeHtml(pendingDone.id)}" class="px-2 py-1 rounded border border-amber-600/40 bg-amber-600/10 text-[10px] font-mono text-amber-200 hover:bg-amber-600/20 transition-colors">Undo</button></div>
            </div>
        `
        : (nextActions.length
            ? `<div class="mt-2 space-y-1.5">${nextActions.map((task) => `
                <div class="border border-zinc-800 rounded-md bg-zinc-950/20 px-2 py-1.5">
                    <div class="flex items-center justify-between gap-2">
                        <div class="min-w-0">
                            <div class="text-[11px] text-zinc-100 truncate" title="${escapeHtml(safeText(task?.title))}">${escapeHtml(safeText(task?.title) || 'Untitled')}</div>
                            <div class="text-[10px] font-mono text-zinc-500 truncate">${escapeHtml(safeText(task?.project) || '—')}</div>
                        </div>
                        <button data-next-task-done="${escapeHtml(safeText(task?.id))}" class="shrink-0 px-2 py-1 rounded border border-emerald-600/40 bg-emerald-600/10 text-[10px] font-mono text-emerald-200 hover:bg-emerald-600/20 transition-colors">Done</button>
                    </div>
                </div>
            `).join('')}</div>`
            : `<div class="mt-2 text-[10px] font-mono text-zinc-600">No action</div>`);

    const controlStrip = document.createElement('div');
    controlStrip.className = `grid grid-cols-1 ${portraitCompact ? 'md:grid-cols-2' : 'md:grid-cols-2 xl:grid-cols-5'} gap-3`;
    controlStrip.innerHTML = `
        <button data-strip-action="focus" class="group text-left border border-zinc-800 rounded-xl bg-zinc-900/30 p-3 hover:bg-zinc-900/50 transition-colors">
            <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Today Outcomes</div>
            <div class="mt-1 text-sm text-zinc-100 truncate">${escapeHtml(safeText(state.settings?.todayOutcomes).split('\n')[0] || 'Set top outcomes')}</div>
            <div class="mt-2 max-h-0 opacity-0 overflow-hidden transition-all duration-200 ease-out group-hover:max-h-24 group-hover:opacity-100 text-[11px] text-zinc-400">Click to jump to Today panel.</div>
        </button>

        <button data-strip-action="next-task" class="group text-left border border-zinc-800 rounded-xl bg-zinc-900/30 p-3 hover:bg-zinc-900/50 transition-colors">
            <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Next Task</div>
            <div class="mt-1 text-sm text-zinc-100 truncate">${escapeHtml(nextAction ? 'Action queue (top 3)' : 'No immediate task')}</div>
            ${nextTaskRowsHtml}
        </button>

        <button data-strip-action="next-call" class="group text-left border border-zinc-800 rounded-xl bg-zinc-900/30 p-3 hover:bg-zinc-900/50 transition-colors">
            <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Next Call</div>
            <div class="mt-1 text-sm text-zinc-100 truncate">${escapeHtml(nextCall ? `${formatTimeFromIso(nextCall.start)} • ${safeText(nextCall.summary)}` : (callsConnected ? 'No upcoming call' : 'Connect Google Calendar'))}</div>
            <div class="mt-2 max-h-0 opacity-0 overflow-hidden transition-all duration-200 ease-out group-hover:max-h-24 group-hover:opacity-100 text-[11px] text-zinc-400">${escapeHtml(nextCall?.meetingLink ? 'Click to refresh and prep calls.' : 'Calls feed refreshes from Google.')}</div>
        </button>

        <button data-strip-action="inbox-radar" class="group text-left border border-zinc-800 rounded-xl bg-zinc-900/30 p-3 hover:bg-zinc-900/50 transition-colors">
            <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Inbox Radar</div>
            <div class="mt-1 text-sm text-zinc-100 truncate">${escapeHtml(sourceSummary || 'No new inbox items')}</div>
            <div class="mt-2 max-h-0 opacity-0 overflow-hidden transition-all duration-200 ease-out group-hover:max-h-24 group-hover:opacity-100 text-[11px] text-zinc-400">Click to open Inbox with source-aware triage.</div>
        </button>

        <button data-strip-action="risk" class="group text-left border border-zinc-800 rounded-xl bg-zinc-900/30 p-3 hover:bg-zinc-900/50 transition-colors">
            <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">At Risk Projects</div>
            <div class="mt-1 text-sm text-zinc-100 truncate">${atRiskProjects.length} flagged</div>
            <div class="mt-2 max-h-0 opacity-0 overflow-hidden transition-all duration-200 ease-out group-hover:max-h-24 group-hover:opacity-100 text-[11px] text-zinc-400">${escapeHtml(atRiskProjects.slice(0, 2).map((p) => safeText(p?.name)).filter(Boolean).join(' • ') || 'No immediate risk detected')}</div>
        </button>
    `;

    const missionContainer = document.createElement('div');
    missionContainer.className = `${portraitCompact ? 'mb-3' : 'mb-6'} border border-zinc-800 rounded-xl bg-zinc-900/20 p-3`;
    missionContainer.innerHTML = `<div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500 mb-2">Mission Control</div>`;
    missionContainer.appendChild(controlStrip);

    const miniGhlCard = document.createElement('div');
    miniGhlCard.className = 'mt-3 border border-zinc-800 rounded-lg bg-zinc-950/20 p-3';
    const pipelineOpen = Number(ghlSnapshot?.pipeline?.open || 0);
    const pipelineTotal = Number(ghlSnapshot?.pipeline?.total || 0);
    const conversationsUnread = Number(ghlSnapshot?.conversations?.unread || 0);
    const appointmentsUpcoming = Number(ghlSnapshot?.appointments?.upcoming || 0);
    const warningText = Array.isArray(ghlSnapshot?.warnings) && ghlSnapshot.warnings.length
        ? safeText(ghlSnapshot.warnings[0])
        : '';
    miniGhlCard.innerHTML = `
        <div class="flex items-start justify-between gap-3">
            <div>
                <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Mini GHL</div>
                <div class="text-sm text-zinc-100 mt-1">${escapeHtml(ghlConfigured ? (ghlLoading ? 'Refreshing snapshot...' : 'GoHighLevel snapshot') : 'Not configured')}</div>
                <div class="text-[10px] font-mono text-zinc-500 mt-1">${escapeHtml(ghlConfigured ? (ghlError ? `Error: ${ghlError}` : (warningText || 'Pipeline • Conversations • Appointments (7d)')) : 'Set API key + Location ID in Settings')}</div>
            </div>
            <div class="shrink-0 flex gap-2">
                <button data-mini-ghl-refresh class="px-2.5 py-1.5 rounded border border-zinc-800 text-[10px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors">Refresh</button>
                <button data-mini-ghl-settings class="px-2.5 py-1.5 rounded border border-zinc-800 text-[10px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors">Settings</button>
            </div>
        </div>
        <div class="mt-3 grid grid-cols-1 md:grid-cols-3 gap-2">
            <div class="border border-zinc-800 rounded-md bg-zinc-950/30 px-2.5 py-2">
                <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Pipeline</div>
                <div class="text-sm text-zinc-100 mt-1">${ghlConfigured ? `${pipelineOpen} open / ${pipelineTotal}` : '—'}</div>
            </div>
            <div class="border border-zinc-800 rounded-md bg-zinc-950/30 px-2.5 py-2">
                <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Unread Convos</div>
                <div class="text-sm text-zinc-100 mt-1">${ghlConfigured ? `${conversationsUnread}` : '—'}</div>
            </div>
            <div class="border border-zinc-800 rounded-md bg-zinc-950/30 px-2.5 py-2">
                <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Appointments (7d)</div>
                <div class="text-sm text-zinc-100 mt-1">${ghlConfigured ? `${appointmentsUpcoming}` : '—'}</div>
            </div>
        </div>
    `;
    missionContainer.appendChild(miniGhlCard);
    content.appendChild(missionContainer);

    // New Project Intake
    const intake = document.createElement('div');
    intake.className = 'mb-10 border border-white/10 rounded-2xl bg-white/5 backdrop-blur-xl p-5 shadow-2xl';

    const d = state.newProjectDraft || {};
    intake.innerHTML = `
        <div class="flex items-center justify-between gap-3">
            <div>
                <div class="text-white text-sm font-semibold">New Project Intake</div>
                <div class="text-[11px] text-zinc-500 mt-0.5">Name + type + brief + value + due date + links.</div>
            </div>
            <button id="btn-toggle-intake" class="px-3 py-1.5 rounded-lg border border-zinc-700 text-xs text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">
                ${state.showNewProjectIntake ? 'Hide' : 'Create'}
            </button>
        </div>
        <div id="intake-body" class="${state.showNewProjectIntake ? '' : 'hidden'} mt-4">
            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                    <label class="text-[11px] text-zinc-400">Project name</label>
                    <input id="np-name" type="text" value="${String(d.name || '').replace(/"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40" placeholder="e.g. Freedom Scoopers Website" />
                </div>
                <div class="grid grid-cols-2 gap-3">
                    <div>
                        <label class="text-[11px] text-zinc-400">Type</label>
                        <select id="np-type" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40">
                            ${['Build','Rebuild','Revision','Workflow','Cleanup','Other'].map(t => `<option value="${t}" ${String(d.type||'')===t?'selected':''}>${t}</option>`).join('')}
                        </select>
                    </div>
                    <div>
                        <label class="text-[11px] text-zinc-400">Status</label>
                        <select id="np-status" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40">
                            ${['Active','On Hold','Done'].map(s => `<option value="${s}" ${String(d.status||'')===s?'selected':''}>${s}</option>`).join('')}
                        </select>
                    </div>
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Project value</label>
                    <input id="np-value" type="text" value="${String(d.projectValue || '').replace(/"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="$5000" />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Due date</label>
                    <input id="np-due" type="date" value="${String(d.dueDate || '').replace(/"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Client Name</label>
                    <input id="np-client-name" type="text" value="${String(d.clientName || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="Client Name" />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Client Phone</label>
                    <input id="np-client-phone" type="text" value="${String(d.clientPhone || '').replace(/\"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="+15551234567" />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Repo URL</label>
                    <input id="np-repo" type="text" value="${String(d.repoUrl || '').replace(/"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="https://github.com/..." />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Docs URL</label>
                    <input id="np-docs" type="text" value="${String(d.docsUrl || '').replace(/"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="https://..." />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Stripe invoice URL</label>
                    <input id="np-invoice" type="text" value="${String(d.stripeInvoiceUrl || '').replace(/"/g, '&quot;')}" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="https://invoice.stripe.com/..." />
                </div>
                <div>
                    <label class="text-[11px] text-zinc-400">Workspace path</label>
                    <div class="mt-1 flex gap-2">
                        <input id="np-workspace" type="text" value="${String(d.workspacePath || '').replace(/"/g, '&quot;')}" class="flex-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="C:\\Work\\Client\\Project" />
                        <button id="btn-browse-np-workspace" type="button" class="shrink-0 px-3 py-2 rounded-lg border border-zinc-700 text-xs text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Browse</button>
                    </div>
                </div>
                <div class="md:col-span-2">
                    <label class="text-[11px] text-zinc-400">Agent brief (saved to project Scratchpad for Marcus)</label>
                    <textarea id="np-brief" rows="4" class="mt-1 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white" placeholder="What is this project? Scope, constraints, stakeholders, success criteria...">${String(d.agentBrief || '')}</textarea>
                </div>
            </div>
            <div class="flex flex-wrap gap-2 mt-3">
                <button id="btn-create-project" class="px-3 py-2 rounded-lg bg-blue-600 text-white text-xs hover:bg-blue-500">Create project</button>
                <button id="btn-clear-intake" class="px-3 py-2 rounded-lg border border-zinc-700 text-xs text-zinc-300 hover:text-white hover:bg-zinc-800">Clear</button>
            </div>
            <div id="intake-error" class="text-xs text-red-400 mt-2 hidden"></div>
        </div>
    `;

    content.appendChild(intake);

    // Unreads (Inbox)
    const unreadItems = getDisplayInboxItems()
        .filter((x) => String(x?.status || '') === 'New')
        .slice()
        .sort((a, b) => String(b?.createdAt || '').localeCompare(String(a?.createdAt || '')));

    const unreadGroups = groupInboxItemsByBusiness(unreadItems)
        .slice(0, portraitCompact ? 2 : 6);

    const unreadPanel = document.createElement('div');
    unreadPanel.className = 'mb-6 border border-zinc-800 rounded-xl bg-zinc-900/30 p-4';
    unreadPanel.innerHTML = `
        <div class="flex items-center justify-between gap-3">
            <div>
                <div class="text-white text-sm font-semibold">Inbox Radar</div>
                <div class="text-[11px] text-zinc-500 mt-0.5">${unreadItems.length} new inbox item${unreadItems.length === 1 ? '' : 's'} • hover cards for details</div>
            </div>
            <button id="btn-open-inbox" class="px-3 py-1.5 rounded-lg border border-zinc-700 text-xs font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Open Inbox</button>
        </div>

        <div class="mt-3 ${unreadItems.length ? '' : 'hidden'} grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            ${unreadGroups.map((group) => {
                const newest = group.items[0];
                const newestTime = safeText(newest?.createdAt) ? formatTimeFromIso(newest.createdAt) : '';
                const sourceSummary = (() => {
                    const counts = {};
                    for (const item of group.items) {
                        const sKey = normalizeInboxSourceKey(item?.source);
                        counts[sKey] = Number(counts[sKey] || 0) + 1;
                    }
                    return Object.entries(counts)
                        .sort((a, b) => Number(b[1]) - Number(a[1]))
                        .slice(0, 2)
                        .map(([k, n]) => `${inboxSourceMeta(k).label}: ${n}`)
                        .join(' • ');
                })();
                const previewButtons = group.items.slice(0, portraitCompact ? 2 : 4).map((item) => {
                    const id = safeText(item?.id);
                    const time = safeText(item?.createdAt) ? formatTimeFromIso(item.createdAt) : '';
                    const text = safeText(item?.text).replace(/\s+/g, ' ').trim();
                    return `
                        <button data-dash-open-inbox="${escapeHtml(id)}" class="w-full text-left border border-zinc-800 rounded-md bg-zinc-950/20 px-2.5 py-1.5 hover:bg-zinc-800/40 transition-colors">
                            <div class="flex items-center justify-between gap-2">
                                <span class="text-[10px] font-mono text-zinc-500">${escapeHtml(time || '—')}</span>
                                ${inboxStatusBadge('New')}
                            </div>
                            <div class="mt-0.5 text-[11px] text-zinc-200 truncate" title="${escapeHtml(text)}">${escapeHtml(text || '(empty)')}</div>
                        </button>
                    `;
                }).join('');

                return `
                    <div class="group border border-zinc-800 rounded-lg bg-zinc-950/20 px-3 py-2 transition-colors hover:bg-zinc-900/40">
                        <div class="flex items-center justify-between gap-3">
                            <div class="min-w-0">
                                <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Business</div>
                                <div class="text-sm text-zinc-100 truncate mt-0.5">${escapeHtml(group.label)}</div>
                            </div>
                            <div class="shrink-0 px-2 py-1 rounded border border-zinc-700 bg-zinc-900/40 text-xs font-mono text-zinc-200">${group.items.length}</div>
                        </div>
                        <div class="mt-1 text-[10px] font-mono text-zinc-500">${escapeHtml(sourceSummary || 'Mixed sources')} • Latest: ${escapeHtml(newestTime || '—')}</div>
                        <div class="mt-2 max-h-0 opacity-0 overflow-hidden transition-all duration-200 ease-out group-hover:max-h-64 group-hover:opacity-100 space-y-1.5">
                            ${previewButtons}
                            <button data-dash-open-inbox="group-${escapeHtml(group.label)}" class="w-full text-center px-2 py-1 rounded border border-zinc-800 text-[10px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors">Open full Inbox</button>
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
        <div class="mt-3 ${unreadItems.length ? 'hidden' : ''} text-xs text-zinc-500">No new inbox items.</div>
    `;
    const commsContainer = document.createElement('details');
    commsContainer.className = `${portraitCompact ? 'mb-3' : 'mb-6'} border border-zinc-800 rounded-xl bg-zinc-900/20`;
    if (!portraitCompact) commsContainer.open = true;
    commsContainer.innerHTML = `
        <summary class="cursor-pointer list-none px-3 py-2 flex items-center justify-between gap-3">
            <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Comms Radar</div>
            <div class="text-[10px] font-mono text-zinc-500">${unreadItems.length} new</div>
        </summary>
        <div class="px-3 pb-3 border-t border-zinc-800/60"></div>
    `;
    const commsBody = commsContainer.querySelector('div');
    if (commsBody) commsBody.appendChild(unreadPanel);

    const triageStatuses = ['New', 'Triaged', 'Done', 'Archived'];
    const triageBoard = document.createElement('div');
    triageBoard.className = 'mb-6 border border-zinc-800 rounded-xl bg-zinc-900/30 p-4';
    triageBoard.innerHTML = `
        <div class="flex items-center justify-between gap-3 mb-3">
            <div>
                <div class="text-white text-sm font-semibold">Inbox Triage Board</div>
                <div class="text-[11px] text-zinc-500 mt-0.5">Fast status flow with source-aware cards.</div>
            </div>
            <button data-triage-open-inbox class="px-3 py-1.5 rounded-lg border border-zinc-700 text-xs font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Open Inbox</button>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
            ${triageStatuses.map((status) => {
                const items = allInboxItems
                    .filter((x) => String(x?.status || '').toLowerCase() === status.toLowerCase())
                    .slice(0, portraitCompact ? 2 : 4);
                return `
                    <div class="border border-zinc-800 rounded-lg bg-zinc-950/20 p-2.5">
                        <div class="flex items-center justify-between gap-2 mb-2">
                            <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">${escapeHtml(status)}</div>
                            <div class="text-[10px] font-mono text-zinc-400">${items.length}</div>
                        </div>
                        <div class="space-y-2 min-h-[4rem]">
                            ${items.length ? items.map((item) => {
                                const id = safeText(item?.id);
                                const src = normalizeInboxSourceKey(item?.source);
                                const srcMeta = inboxSourceMeta(src);
                                const biz = inboxBusinessLabel(item);
                                const text = safeText(item?.text).replace(/\s+/g, ' ').trim();
                                const when = safeText(item?.updatedAt || item?.createdAt);
                                return `
                                    <div class="border border-zinc-800 rounded-md bg-zinc-950/20 px-2 py-1.5">
                                        <button data-dash-open-inbox="${escapeHtml(id)}" class="w-full text-left">
                                            <div class="flex items-center justify-between gap-2">
                                                <div class="text-[10px] font-mono ${srcMeta.tone}">${escapeHtml(srcMeta.label)}</div>
                                                <div class="text-[10px] font-mono text-zinc-500">${escapeHtml(formatTimeFromIso(when) || '—')}</div>
                                            </div>
                                            <div class="text-[10px] font-mono text-zinc-500 truncate mt-0.5">${escapeHtml(biz)}</div>
                                            <div class="text-[11px] text-zinc-200 truncate mt-0.5" title="${escapeHtml(text)}">${escapeHtml(text || '(empty)')}</div>
                                        </button>
                                        <div class="mt-1.5 flex gap-1.5">
                                            ${status !== 'Triaged' ? `<button data-triage-set-status="Triaged" data-triage-id="${escapeHtml(id)}" class="px-1.5 py-1 rounded border border-zinc-800 text-[10px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors">Triage</button>` : ''}
                                            ${status !== 'Done' ? `<button data-triage-set-status="Done" data-triage-id="${escapeHtml(id)}" class="px-1.5 py-1 rounded border border-emerald-600/40 bg-emerald-600/10 text-[10px] font-mono text-emerald-300 hover:bg-emerald-600/20 transition-colors">Done</button>` : ''}
                                            ${status !== 'Archived' ? `<button data-triage-set-status="Archived" data-triage-id="${escapeHtml(id)}" class="px-1.5 py-1 rounded border border-zinc-800 text-[10px] font-mono text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800 transition-colors">Archive</button>` : ''}
                                        </div>
                                    </div>
                                `;
                            }).join('') : `<div class="text-[11px] text-zinc-600 italic">No items.</div>`}
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
    `;
    const deliveryContainer = document.createElement('div');
    deliveryContainer.className = `${portraitCompact ? 'mb-3' : 'mb-6'} border border-zinc-800 rounded-xl bg-zinc-900/20 p-3`;
    deliveryContainer.innerHTML = `<div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500 mb-2">Delivery Board</div>`;
    deliveryContainer.appendChild(renderProjectBuckets(buckets, { bulkMode: !!state.dashboardBulkMode }));

    if (state.showArchivedOnDashboard) {
        const archivedCard = renderProjectBucket('Archived', archivedProjects, { bulkMode: !!state.dashboardBulkMode });
        archivedCard.classList.add('mt-6');
        deliveryContainer.appendChild(archivedCard);
    }

    // Today panel (secondary)
    deliveryContainer.appendChild(renderTodayPanel());
    content.appendChild(deliveryContainer);

    if (commsBody) commsBody.appendChild(triageBoard);
    content.appendChild(commsContainer);

    wrap.appendChild(banner);
    wrap.appendChild(content);
    container.appendChild(wrap);

    const btnToggleArchived = banner.querySelector('#btn-toggle-archived');
    if (btnToggleArchived) {
        btnToggleArchived.onclick = () => {
            state.showArchivedOnDashboard = !state.showArchivedOnDashboard;
            renderMain();
        };
    }

    const updateDashBulkUi = () => {
        const selectedIds = Object.keys(state.dashboardSelectedProjectById || {}).filter((id) => state.dashboardSelectedProjectById[id]);
        const countEl = banner.querySelector('#dash-selected-count');
        if (countEl) countEl.textContent = String(selectedIds.length);
        const btnClear = banner.querySelector('#btn-dash-clear');
        const btnArchive = banner.querySelector('#btn-dash-archive');
        const btnUnarchive = banner.querySelector('#btn-dash-unarchive');
        const btnDelete = banner.querySelector('#btn-dash-delete');
        const has = selectedIds.length > 0;
        if (btnClear) btnClear.disabled = !has;
        if (btnArchive) btnArchive.disabled = !has;
        if (btnUnarchive) btnUnarchive.disabled = !has;
        if (btnDelete) btnDelete.disabled = !has;
        // sync visible checkboxes
        container.querySelectorAll('input[type="checkbox"][data-dash-proj-sel]').forEach((cb) => {
            const id = safeText(cb.getAttribute('data-dash-proj-sel'));
            cb.checked = !!state.dashboardSelectedProjectById[id];
        });
    };

    const btnToggleBulk = banner.querySelector('#btn-toggle-bulk');
    if (btnToggleBulk) {
        btnToggleBulk.onclick = () => {
            state.dashboardBulkMode = !state.dashboardBulkMode;
            if (!state.dashboardBulkMode) state.dashboardSelectedProjectById = {};
            renderMain();
        };
    }

    if (state.dashboardBulkMode) {
        // wire checkbox handlers after DOM exists
        container.querySelectorAll('input[type="checkbox"][data-dash-proj-sel]').forEach((cb) => {
            cb.addEventListener('change', () => {
                const id = safeText(cb.getAttribute('data-dash-proj-sel'));
                if (!id) return;
                state.dashboardSelectedProjectById[id] = !!cb.checked;
                updateDashBulkUi();
            });
        });

        const btnSelectAll = banner.querySelector('#btn-dash-select-all');
        if (btnSelectAll) {
            btnSelectAll.onclick = () => {
                const ids = visibleProjects.map((p) => safeText(p?.id)).filter(Boolean);
                for (const id of ids) state.dashboardSelectedProjectById[id] = true;
                updateDashBulkUi();
            };
        }

        const btnClear = banner.querySelector('#btn-dash-clear');
        if (btnClear) {
            btnClear.onclick = () => {
                state.dashboardSelectedProjectById = {};
                updateDashBulkUi();
            };
        }

        const btnArchive = banner.querySelector('#btn-dash-archive');
        if (btnArchive) {
            btnArchive.onclick = async () => {
                const ids = Object.keys(state.dashboardSelectedProjectById || {}).filter((id) => state.dashboardSelectedProjectById[id]);
                if (!ids.length) return;
                const prev = btnArchive.textContent;
                btnArchive.disabled = true;
                btnArchive.textContent = 'Archiving…';
                try {
                    await bulkUpdateProjectsByIdList(ids, { status: 'Archived' });
                    state.dashboardSelectedProjectById = {};
                    renderNav();
                    renderMain();
                } catch (e) {
                    alert(e?.message || 'Failed to archive projects');
                } finally {
                    btnArchive.textContent = prev;
                }
            };
        }

        const btnUnarchive = banner.querySelector('#btn-dash-unarchive');
        if (btnUnarchive) {
            btnUnarchive.onclick = async () => {
                const ids = Object.keys(state.dashboardSelectedProjectById || {}).filter((id) => state.dashboardSelectedProjectById[id]);
                if (!ids.length) return;
                const prev = btnUnarchive.textContent;
                btnUnarchive.disabled = true;
                btnUnarchive.textContent = 'Unarchiving…';
                try {
                    await bulkUpdateProjectsByIdList(ids, { status: 'Active' });
                    state.dashboardSelectedProjectById = {};
                    renderNav();
                    renderMain();
                } catch (e) {
                    alert(e?.message || 'Failed to unarchive projects');
                } finally {
                    btnUnarchive.textContent = prev;
                }
            };
        }

        const btnDelete = banner.querySelector('#btn-dash-delete');
        if (btnDelete) {
            btnDelete.onclick = async () => {
                const ids = Object.keys(state.dashboardSelectedProjectById || {}).filter((id) => state.dashboardSelectedProjectById[id]);
                if (!ids.length) return;
                if (!confirm(`Delete ${ids.length} project(s)? This will also remove their tasks, notes, chat, scratchpad, and communications.`)) return;
                const prev = btnDelete.textContent;
                btnDelete.disabled = true;
                btnDelete.textContent = 'Deleting…';
                try {
                    await deleteProjectsByIdList(ids);
                    state.dashboardSelectedProjectById = {};
                    renderNav();
                    renderMain();
                } catch (e) {
                    alert(e?.message || 'Failed to delete projects');
                } finally {
                    btnDelete.textContent = prev;
                }
            };
        }

        updateDashBulkUi();
    }

    const toggle = intake.querySelector('#btn-toggle-intake');
    const body = intake.querySelector('#intake-body');
    const errEl = intake.querySelector('#intake-error');
    const setError = (msg) => {
        if (!errEl) return;
        const m = safeText(msg).trim();
        errEl.textContent = m;
        errEl.classList.toggle('hidden', !m);
    };

    if (toggle) {
        toggle.onclick = () => {
            state.showNewProjectIntake = !state.showNewProjectIntake;
            renderMain();
            if (state.showNewProjectIntake) {
                setTimeout(() => {
                    const nameEl = document.getElementById('np-name');
                    try { nameEl?.focus(); } catch {}
                }, 50);
            }
        };
    }

    const bind = (id, key) => {
        const el = intake.querySelector('#' + id);
        if (!el) return;
        el.addEventListener('input', () => {
            setNewProjectDraft({ [key]: el.value });
        });
        el.addEventListener('change', () => {
            setNewProjectDraft({ [key]: el.value });
        });
    };
    bind('np-name', 'name');
    bind('np-type', 'type');
    bind('np-status', 'status');
    bind('np-value', 'projectValue');
    bind('np-due', 'dueDate');
    bind('np-repo', 'repoUrl');
    bind('np-docs', 'docsUrl');
    bind('np-invoice', 'stripeInvoiceUrl');
    bind('np-workspace', 'workspacePath');
    bind('np-brief', 'agentBrief');

    const btnBrowseNpWorkspace = intake.querySelector('#btn-browse-np-workspace');
    if (btnBrowseNpWorkspace) {
        btnBrowseNpWorkspace.onclick = async () => {
            const wsEl = intake.querySelector('#np-workspace');
            btnBrowseNpWorkspace.disabled = true;
            const original = btnBrowseNpWorkspace.textContent;
            btnBrowseNpWorkspace.textContent = '...';
            try {
                const picked = await pickFolderPath();
                if (!picked) {
                    setError('No folder selected. If the picker didn\'t appear, check that the server is running in your desktop session (not as a service) and try again.');
                    return;
                }
                if (wsEl) wsEl.value = picked;
                setNewProjectDraft({ workspacePath: picked });
                try { wsEl?.focus(); } catch {}
            } catch (e) {
                setError(e?.message || 'Failed to pick folder');
            } finally {
                btnBrowseNpWorkspace.disabled = false;
                btnBrowseNpWorkspace.textContent = original;
            }
        };
    }

    const btnCreate = intake.querySelector('#btn-create-project');
    if (btnCreate) {
        btnCreate.onclick = async () => {
            setError('');
            // Snapshot all form fields into draft before validating
            const fields = {np_name:'name',np_type:'type',np_status:'status',np_value:'projectValue',np_due:'dueDate',np_repo:'repoUrl',np_docs:'docsUrl',np_invoice:'stripeInvoiceUrl',np_workspace:'workspacePath',np_brief:'agentBrief',np_priority:'priority',np_importance:'importance',np_risk:'risk'};
            for (const [elId, key] of Object.entries(fields)) {
                const el = intake.querySelector('#' + elId.replace('_','-'));
                if (el) setNewProjectDraft({ [key]: el.value });
            }
            btnCreate.disabled = true;
            const original = btnCreate.textContent;
            btnCreate.textContent = 'Creating...';
            try {
                await createProjectFromDraft();
            } catch (e) {
                setError(e?.message || 'Failed to create project');
                btnCreate.disabled = false;
                btnCreate.textContent = original;
            }
        };
    }

    const btnClear = intake.querySelector('#btn-clear-intake');
    if (btnClear) {
        btnClear.onclick = () => {
            resetNewProjectDraft();
            renderMain();
        };
    }

    // Wire Unreads -> Inbox
    const btnOpenInbox = unreadPanel.querySelector('#btn-open-inbox');
    if (btnOpenInbox) {
        btnOpenInbox.onclick = async () => {
            await openInbox();
        };
    }

    const btnTriageOpenInbox = triageBoard.querySelector('button[data-triage-open-inbox]');
    if (btnTriageOpenInbox) {
        btnTriageOpenInbox.onclick = async () => {
            await openInbox();
        };
    }

    controlStrip.querySelectorAll('button[data-strip-action]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const action = safeText(btn.getAttribute('data-strip-action'));
            if (action === 'inbox-radar') {
                await openInbox();
                return;
            }
            if (action === 'next-call') {
                await refreshDashboardCalls({ force: true });
                return;
            }
            if (action === 'next-task') {
                if (pendingDone) {
                    await openDashboard();
                    return;
                }
                if (nextAction) {
                    const project = activeProjects.find((p) => safeText(p?.name) === safeText(nextAction?.project));
                    if (project?.id) {
                        await openProject(project.id);
                        return;
                    }
                }
                await openDashboard();
                return;
            }
            if (action === 'risk') {
                if (atRiskProjects.length && atRiskProjects[0]?.id) {
                    await openProject(atRiskProjects[0].id);
                    return;
                }
                await openDashboard();
                return;
            }
            await openDashboard();
        });
    });

    controlStrip.querySelectorAll('button[data-next-task-done]').forEach((btn) => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            const id = safeText(btn.getAttribute('data-next-task-done')).trim();
            if (!id) return;
            const task = (Array.isArray(state.tasks) ? state.tasks : []).find((t) => safeText(t?.id) === id);
            if (!task) return;
            scheduleTaskDoneWithUndo(task);
        });
    });

    controlStrip.querySelectorAll('button[data-next-task-undo]').forEach((btn) => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            const id = safeText(btn.getAttribute('data-next-task-undo')).trim();
            if (!id) return;
            undoTaskDoneWithUndo(id);
        });
    });

    const btnMiniGhlRefresh = missionContainer.querySelector('button[data-mini-ghl-refresh]');
    if (btnMiniGhlRefresh) {
        btnMiniGhlRefresh.onclick = async () => {
            await refreshDashboardGhl({ force: true });
        };
    }

    const btnMiniGhlSettings = missionContainer.querySelector('button[data-mini-ghl-settings]');
    if (btnMiniGhlSettings) {
        btnMiniGhlSettings.onclick = async () => {
            await openSettings();
        };
    }

    container.querySelectorAll('button[data-dash-open-inbox]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            await openInbox();
        });
    });

    triageBoard.querySelectorAll('button[data-triage-set-status][data-triage-id]').forEach((btn) => {
        btn.addEventListener('click', async (e) => {
            e.stopPropagation();
            const id = safeText(btn.getAttribute('data-triage-id')).trim();
            const status = safeText(btn.getAttribute('data-triage-set-status')).trim();
            if (!id || !status) return;
            try {
                await patchInboxItem(id, { status });
                renderMain();
            } catch (err) {
                alert(err?.message || 'Failed to update inbox item');
            }
        });
    });
}

function renderGodView(container) {
    const titleEl = document.getElementById("page-title");
    if(titleEl) titleEl.innerText = "Global Overview";

    const snap = snapshotViewUiState();

    const prefs = getPageElementsPreferences(state.settings).godview;

    const sectionRadar = prefs.businessesRadar ? `
            <div class="flex items-center justify-between mb-2">
                <h2 class="text-xl font-bold tracking-tight text-white flex items-center gap-3">
                    <i class="fa-solid fa-satellite-dish text-ops-accent"></i> Businesses Radar
                </h2>
                <button id="godview-refresh-btn" class="px-3 py-1.5 bg-zinc-800 hover:bg-zinc-700 text-xs font-semibold rounded text-zinc-300 transition flex items-center gap-2">
                    <i class="fa-solid fa-rotate ${state.godViewLoading ? 'animate-spin' : ''}"></i> Refresh
                </button>
            </div>
            
            <div id="godview-radar-grid" class="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-4">
                <div class="col-span-full text-center text-zinc-500 text-sm py-8"><i class="fa-solid fa-circle-notch animate-ping"></i> Scraping data...</div>
            </div>
    ` : '';

    const sectionBrief = prefs.marcusBrief ? `
            <div class="mt-10 mb-2 flex items-center gap-3">
                <h2 class="text-xl font-bold tracking-tight text-white">Marcus Brief</h2>
            </div>
            <div id="godview-brief-list" class="space-y-2"></div>
    ` : '';

    const sectionUpcomingTeam = (prefs.upcoming || prefs.teamComms) ? `
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-12 mb-12">
                ${prefs.upcoming ? `
                <!-- Calendar Section -->
                <div>
                    <div class="flex items-center gap-3 mb-4">
                        <h2 class="text-xl font-bold tracking-tight text-white flex items-center gap-2">
                            <i class="fa-regular fa-calendar text-blue-400"></i> Upcoming
                        </h2>
                    </div>
                    <div id="godview-calendar-list" class="space-y-2"></div>
                </div>
                ` : ''}

                ${prefs.teamComms ? `
                <!-- Slack Section -->
                <div>
                    <div class="flex items-center gap-3 mb-4">
                        <h2 class="text-xl font-bold tracking-tight text-white flex items-center gap-2">
                            <i class="fa-brands fa-slack text-purple-400"></i> Team Comms
                        </h2>
                    </div>
                    <div id="godview-slack-list" class="grid grid-cols-1 gap-2"></div>
                </div>
                ` : ''}
            </div>
    ` : '';

    const sectionFocus = prefs.globalFocus ? `
            <div class="mt-12 mb-2 flex items-center gap-3">
                <h2 class="text-xl font-bold tracking-tight text-white flex items-center gap-3">
                    <i class="fa-solid fa-bolt text-ops-warning"></i> Global Focus
                </h2>
            </div>
            
            <div id="godview-focus-list" class="space-y-3">
            </div>
    ` : '';

    container.innerHTML = `
        <div class="max-w-7xl mx-auto p-4 md:p-6 lg:p-8 space-y-8 animate-fade-in pb-[400px]">
            ${sectionRadar}
            ${sectionBrief}
            ${sectionUpcomingTeam}
            ${sectionFocus}
        </div>
    `;

    const refreshBtn = container.querySelector('#godview-refresh-btn');
    if (refreshBtn) {
        refreshBtn.onclick = async () => {
            await refreshGodView();
        };
    }

    if (!state.godViewData) {
        refreshGodView();
        return;
    }

    const { businesses, focusProjects, slackItems, team, briefs } = state.godViewData;

    // --- Marcus Brief ---
    const briefList = container.querySelector('#godview-brief-list');
    if (briefList) {
        const items = Array.isArray(briefs) ? briefs : [];
        if (!items.length) {
            briefList.innerHTML = `<div class="text-[11px] text-zinc-500 px-3 py-2 border border-zinc-800 border-dashed rounded bg-zinc-950/20">No briefs yet. Scheduled briefs will appear here.</div>`;
        } else {
            briefList.innerHTML = items.slice(0, 3).map((b) => {
                const name = safeText(b?.businessName) || safeText(b?.businessKey) || 'Business';
                const ts = safeText(b?.updatedAt) || safeText(b?.createdAt) || '';
                const tsHtml = ts ? `<div class="text-[10px] font-mono text-zinc-500">${escapeHtml(ts)}</div>` : '';
                const raw = safeText(b?.text || '').replace(/\s+/g, ' ').trim();
                const head = raw.length > 320 ? `${raw.slice(0, 320)}…` : raw;
                return `
                    <div class="border border-zinc-800 rounded-lg bg-zinc-900/40 px-3 py-2 hover:border-zinc-700 transition-colors">
                        <div class="min-w-0">
                            <div class="text-[11px] font-mono text-ops-accent">${escapeHtml(name)}</div>
                            ${tsHtml}
                        </div>
                        <div class="mt-2 text-[11px] text-zinc-300">${escapeHtml(head)}</div>
                    </div>`;
            }).join('');
        }
    }

    const radarGrid = container.querySelector('#godview-radar-grid');
    if (radarGrid) {
        radarGrid.innerHTML = '';

        if (businesses && businesses.length > 0) {
            for (const b of businesses) {
                const hasNew = b.inboxCount > 0;
                const card = document.createElement('div');
                card.className = `dash-card p-4 flex flex-col justify-center cursor-pointer hover:scale-105 transition-transform ${hasNew ? 'border-ops-accent bg-blue-900/10' : ''}`;
                card.innerHTML = `
                    <div class="flex justify-between items-start mb-2">
                        <h3 class="font-bold text-white text-lg truncate">${safeText(b.name)}</h3>
                        ${hasNew ? `<span class="relative flex h-3 w-3"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-blue-500"></span></span>` : ''}
                    </div>
                    <div class="flex items-center gap-2 text-sm">
                        <i class="fa-solid fa-inbox ${hasNew ? 'text-blue-400' : 'text-zinc-500'}"></i>
                        <span class="${hasNew ? 'text-blue-300 font-bold' : 'text-zinc-500'}">${b.inboxCount} new items</span>
                    </div>
                `;
                card.onclick = () => {
                    setActiveBusinessKey(b.key);
                    openInbox();
                };
                radarGrid.appendChild(card);
            }
        } else {
            radarGrid.innerHTML = '<div class="col-span-full text-zinc-500">No businesses configured.</div>';
        }
    }

    // --- Google Calendar ---
    const calList = container.querySelector('#godview-calendar-list');
    const callsConnected = !!state.settings?.googleConnected;
    const calls = Array.isArray(state.dashboardCalls?.events) ? state.dashboardCalls.events : [];
    if (calList) {
        if (!callsConnected) {
            calList.innerHTML = `<div class="text-[11px] text-zinc-500 px-3 py-2 border border-zinc-800 border-dashed rounded bg-zinc-950/20">Google Calendar not connected. Link in Settings.</div>`;
        } else if (state.dashboardCalls?.loading) {
            calList.innerHTML = `<div class="text-[11px] text-zinc-500 px-3 py-2"><i class="fa-solid fa-circle-notch animate-spin mr-2"></i> Syncing agenda...</div>`;
        } else if (calls.length === 0) {
            calList.innerHTML = `<div class="text-[11px] text-zinc-500 px-3 py-2 border border-zinc-800 border-dashed rounded bg-zinc-950/20">No upcoming meetings in the next 24 hours. Clear skies.</div>`;
        } else {
            calList.innerHTML = calls.slice(0, 5).map(ev => {
                const dateObj = new Date(ev.start);
                const timeStr = Number.isNaN(dateObj.getTime()) ? '' : dateObj.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                const link = safeText(ev.meetingLink) || safeText(ev.htmlLink);
                const linkHtml = link ? `<a class="px-2 py-1 rounded bg-blue-500/20 text-blue-300 hover:bg-blue-500/30 transition-colors" href="${escapeHtml(link)}" target="_blank" rel="noopener noreferrer"><i class="fa-solid fa-video text-[10px]"></i></a>` : `<span class="text-zinc-600 px-2 py-1"><i class="fa-solid fa-video-slash text-[10px]"></i></span>`;
                
                return `
                    <div class="flex items-center justify-between gap-3 border border-zinc-800 rounded-lg bg-zinc-900/40 px-3 py-2 hover:border-zinc-700 transition-colors">
                        <div class="min-w-0 flex items-center gap-3">
                            <div class="text-[10px] font-mono text-blue-400 bg-blue-500/10 px-1.5 py-0.5 rounded w-14 text-center shrink-0">${escapeHtml(timeStr || '---')}</div>
                            <div class="text-xs text-white truncate font-medium">${safeText(ev.summary) || 'Untitled'}</div>
                        </div>
                        <div class="shrink-0 flex items-center">${linkHtml}</div>
                    </div>
                `;
            }).join('');
        }
    }

    // --- Team Comms ---
    const slackContainer = container.querySelector('#godview-slack-list');
    if (slackContainer) slackContainer.innerHTML = '';
    
    const teamMembers = Array.isArray(team) ? team : [];
    
    if (slackContainer) {
        if (teamMembers.length === 0) {
            slackContainer.innerHTML = '<div class="text-gray-400 text-sm italic">No team members</div>';
        } else {
            teamMembers.forEach(member => {
                const memberDiv = document.createElement('div');
                memberDiv.className = 'godview-slack-item text-sm flex gap-3 text-gray-300 items-start p-2 rounded';
                memberDiv.innerHTML = '<div class="flex-1">' +
                    '<div class="font-medium">' + (member.name || '') + ' (' + (member.role || '') + ')</div>' +
                    '<div class="text-gray-400">' + (member.email || '') + '</div>' +
                    '</div>';
                slackContainer.appendChild(memberDiv);
            });
        }
    }

    const focusList = container.querySelector('#godview-focus-list');
    if (focusList) focusList.innerHTML = '';

    if (focusList && focusProjects && focusProjects.length > 0) {
        for (const p of focusProjects) {
            const row = document.createElement('div');
            row.className = 'glass-panel rounded-lg p-4 flex items-center justify-between hover:border-ops-accent/50 transition cursor-pointer';
            
            const isDue = p.dueDate && p.dueDate <= new Date().toISOString().split('T')[0];
            const hasUrgent = p.urgentTasks > 0;
            const openTasks = p.totalTasks - p.completedTasks;
            
            let badges = '';
            if (hasUrgent) badges += `<span class="px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-red-500/20 text-red-400 border border-red-500/30">${p.urgentTasks} Urgent</span> `;
            
            if (isDue) badges += `<span class="px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-yellow-500/20 text-yellow-400 border border-yellow-500/30">Due</span> `;

            row.innerHTML = `
                <div class="flex items-center gap-4 overflow-hidden">
                    <div class="flex-shrink-0 w-2 h-2 rounded-full ${hasUrgent ? 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.8)]' : isDue ? 'bg-yellow-500' : 'bg-emerald-500'}"></div>
                    <div class="min-w-0 flex flex-col">
                        <div class="text-white font-bold text-base truncate flex items-center gap-2">${safeText(p.name)} ${badges}</div>
                        <div class="text-xs text-zinc-400 flex items-center gap-2 mt-1">
                            <span class="text-zinc-300 font-medium whitespace-nowrap"><i class="fa-solid fa-briefcase text-zinc-500 mr-1"></i>${safeText(p.businessName)}</span>
                            <span>&bull;</span>
                            <span class="text-zinc-500">${openTasks} Open Tasks</span>
                            ${p.completedTasks > 0 ? `<span class="text-emerald-500/70 ml-2"><i class="fa-solid fa-check mr-1"></i>${p.completedTasks} Done</span>` : ''}
                        </div>
                    </div>
                </div>
                <div class="flex-shrink-0 ml-4 pl-4 border-l border-zinc-800 text-xs text-zinc-500 flex flex-col items-end gap-1">
                    ${p.dueDate ? `<div class="text-yellow-500/80"><i class="fa-regular fa-clock mr-1"></i>${p.dueDate}</div>` : '<div class="opacity-50">No deadline</div>'}
                </div>
            `;
            row.onclick = () => {
                setActiveBusinessKey(p.businessKey);
                state.currentProjectId = p.id;
                state.currentView = 'project';
                renderNav();
                renderMain();
            };
            focusList.appendChild(row);
        }
    } else if (focusList) {
        focusList.innerHTML = `
                <div class="glass flex flex-col items-center justify-center p-12 rounded-xl border border-ops-border border-dashed text-center">
                    <i class="fa-solid fa-check-double text-4xl text-ops-success mb-4 opacity-80"></i>
                    <h3 class="text-white text-lg font-bold">You're all caught up!</h3>
                    <p class="text-ops-light max-w-md mt-2">No active projects require immediate attention right now. Take a breath.</p>
                </div>
            `;
    }

    restoreViewUiState(snap);
}

function renderDashboard(container, sidePort) {
    // Always render the classic dashboard layout.
    // (The command-center layout is intentionally disabled.)
    void sidePort;

    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Dashboard';

    const pagePrefs = getPageElementsPreferences(state.settings).dashboard;

    /* ── Data gathering ─────────────────────────────────────────── */
    const activeProjects = getActiveProjects();
    const buckets = bucketProjectsByDueDate(activeProjects);
    const dueThisWeek =
        (Array.isArray(buckets.today) ? buckets.today.length : 0) +
        (Array.isArray(buckets.tomorrow) ? buckets.tomorrow.length : 0) +
        (Array.isArray(buckets.thisWeek) ? buckets.thisWeek.length : 0);

    const inboxItems = getDisplayInboxItems();
    const inboxNew = inboxItems.filter((x) => String(x?.status || '').trim().toLowerCase() === 'new');
    const teamMembers = Array.isArray(state.team) ? state.team : [];

    const callsConnected = !!state.settings?.googleConnected;
    const calls = Array.isArray(state.dashboardCalls?.events) ? state.dashboardCalls.events : [];
    const callsError = safeText(state.dashboardCalls?.error);
    const callsLoading = !!state.dashboardCalls?.loading;
    if (callsConnected) setTimeout(() => refreshDashboardCalls({ force: false }), 0);

    const nextActions = getTodayNextActions();
    const outcomes = safeText(state.settings?.todayOutcomes);
    const allTasks = Array.isArray(state.tasks) ? state.tasks : [];
    const today = ymdToday();

    const aiPrev = (state.dashboardAiPreviews && typeof state.dashboardAiPreviews === 'object') ? state.dashboardAiPreviews : {};
    const aiTaskMap = (aiPrev.tasks && typeof aiPrev.tasks === 'object') ? aiPrev.tasks : {};
    const aiInboxMap = (aiPrev.inbox && typeof aiPrev.inbox === 'object') ? aiPrev.inbox : {};

    const isBadDashText = (s) => {
        const v = safeText(s).trim().toLowerCase();
        return !v || v === '[object object]' || v === 'item' || v === 'inbox item';
    };

    const hasActionCue = (text) => {
        const s = safeText(text).toLowerCase();
        if (!s) return false;
        if (s.includes('?')) return true;
        return /\b(need|needs|please|can you|could you|follow up|send|call|schedule|review|fix|update|quote|invoice|confirm|ship|deploy|publish|prepare|asap|urgent|today|tomorrow|deadline|due|assign|delegate)\b/.test(s);
    };

    const hasMeaningfulInboxText = (item) => {
        const txt = safeText(item?.text) || safeText(item?.content) || safeText(item?.body) || safeText(item?.message);
        if (!txt) return false;
        const normalized = txt.replace(/\s+/g, ' ').trim();
        if (!normalized) return false;
        if (normalized.length < 18 && !hasActionCue(normalized)) return false;
        const lowNoise = /^(ok|okay|thanks|thank you|got it|received|read|seen|delivered|message sent|sent)$/i.test(normalized.toLowerCase());
        if (lowNoise) return false;
        return true;
    };

    const dashboardInbox = inboxNew.filter((x) => {
        const source = safeText(x?.source).toLowerCase();
        if (source === 'marcus') return false;
        return hasMeaningfulInboxText(x) || !isBadDashText(safeText(x?.title) || safeText(x?.subject));
    });

    const actionOnlyMode = state.settings?.dashboardActionOnly === true;

    const inboxNewCount = dashboardInbox.length;
    const inboxUnassignedNew = dashboardInbox.filter((x) => !safeText(x?.projectId).trim());
    const inboxUnassignedNewCount = inboxUnassignedNew.length;
    const inboxAssignedActionable = dashboardInbox.filter((x) => {
        const pid = safeText(x?.projectId).trim();
        if (!pid) return false;
        if (String(x?.status || '').trim().toLowerCase() === 'done') return false;
        return true;
    });
    const slackNew = dashboardInbox.filter((x) => normalizeInboxSourceKey(x?.source) === 'slack');
    const emailNew = dashboardInbox.filter((x) => normalizeInboxSourceKey(x?.source) === 'email');
    const otherNew = dashboardInbox.filter((x) => { const k = normalizeInboxSourceKey(x?.source); return k !== 'slack' && k !== 'email'; });

    // Best-effort AI previews for what we actually show on the dashboard.
    setTimeout(() => {
        try {
            refreshDashboardAiPreviews({
                taskIds: nextActions.map((t) => safeText(t?.id)).filter(Boolean).slice(0, 10),
                inboxIds: dashboardInbox.map((x) => safeText(x?.id)).filter(Boolean).slice(0, 16),
                force: false,
            });
        } catch {
            // ignore
        }
    }, 0);

    // Overdue
    const overdueTasks = allTasks.filter((t) => { if (isDoneTask(t)) return false; const d = safeText(t?.dueDate).trim(); return d && d < today; });
    const overdueProjects = activeProjects.filter((p) => { const d = safeText(p?.dueDate).trim(); return d && d < today; });
    const totalOverdue = overdueTasks.length + overdueProjects.length;

    const doTodayLane = [
        ...overdueTasks.slice(0, 2).map((t) => ({
            kind: 'task',
            title: safeText(t?.title) || 'Overdue task',
            sub: safeText(t?.project) || 'Task',
        })),
        ...nextActions.slice(0, 3).map((t) => ({
            kind: 'task',
            title: safeText(t?.title) || 'Next action',
            sub: safeText(t?.project) || 'Task',
        })),
    ].slice(0, 4);
    const needsLinkLane = inboxUnassignedNew.slice(0, 4);
    const waitingLane = inboxAssignedActionable.slice(0, 4);

    // Week streaks
    const weekDays = [];
    const now = new Date();
    const dayOfWeek = now.getDay();
    const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
    for (let i = 0; i < 7; i++) { const d = new Date(now); d.setDate(now.getDate() + mondayOffset + i); weekDays.push({ label: ['M','T','W','T','F','S','S'][i], ymd: `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}` }); }
    const doneTasks = allTasks.filter((t) => isDoneTask(t));
    const completionsByDay = weekDays.map((wd) => doneTasks.filter((t) => safeText(t?.completedAt || t?.updatedAt).trim().slice(0,10) === wd.ymd).length);
    const maxC = Math.max(1, ...completionsByDay);
    const totalDoneWeek = completionsByDay.reduce((a,b) => a+b, 0);

    // Activity feed
    const recentActivity = [];
    for (const t of allTasks) {
        const u = safeText(t?.updatedAt || t?.completedAt || t?.createdAt).trim();
        if (!u) continue;
        const d = new Date(u);
        if (Number.isNaN(d.getTime())) continue;
        recentActivity.push({
            kind: 'task',
            id: safeText(t?.id),
            time: d,
            verb: isDoneTask(t) ? 'Completed' : 'Updated',
            rawTitle: safeText(t?.title),
            icon: isDoneTask(t) ? 'fa-check-circle text-emerald-400' : 'fa-pen text-blue-400',
        });
    }
    for (const item of inboxItems.slice(0, 20)) {
        const c = safeText(item?.createdAt).trim();
        if (!c) continue;
        const d = new Date(c);
        if (Number.isNaN(d.getTime())) continue;
        recentActivity.push({
            kind: 'inbox',
            id: safeText(item?.id),
            source: normalizeInboxSourceKey(item?.source),
            time: d,
            rawTitle: safeText(item?.title || item?.subject),
            icon: 'fa-inbox text-amber-400',
        });
    }
    recentActivity.sort((a,b) => b.time - a.time);

    // M.A.R.C.U.S. insights (multi-line, Jarvis-style)
    const marcusInsights = [];
    if (totalOverdue > 0) marcusInsights.push({ icon: 'fa-triangle-exclamation text-red-400', text: `${totalOverdue} overdue item${totalOverdue>1?'s':''}. I\u2019d recommend triaging those first.` });
    const topAction = nextActions[0] || null;
    if (topAction) {
        const pr = Number(topAction?.priority) || 3;
        const id = safeText(topAction?.id).trim();
        const ai = id ? aiTaskMap[id] : null;
        let title = safeText(ai?.title || topAction?.title).trim();
        if (isBadDashText(title)) title = safeText(topAction?.project).trim() ? `Follow up: ${safeText(topAction?.project).trim()}` : 'Top priority task';
        marcusInsights.push({ icon: 'fa-bullseye text-blue-400', text: `Top priority: "${title}" (P${pr}). Focus there next.` });
    }
    if (dueThisWeek > 0) marcusInsights.push({ icon: 'fa-clock text-amber-400', text: `${dueThisWeek} project${dueThisWeek>1?'s':''} due this week \u2014 stay ahead.` });
    if (inboxNewCount > 3) marcusInsights.push({ icon: 'fa-inbox text-purple-400', text: `${inboxNewCount} inbox items accumulating. Consider a quick triage pass.` });
    if (totalDoneWeek > 0) marcusInsights.push({ icon: 'fa-chart-line text-emerald-400', text: `${totalDoneWeek} tasks completed this week. ${totalDoneWeek >= 5 ? 'Strong momentum.' : 'Keep it going.'}` });
    if (!marcusInsights.length) marcusInsights.push({ icon: 'fa-circle-check text-emerald-400', text: 'All clear. Review upcoming projects or set today\u2019s outcomes.' });
    const marcusCheckin = totalOverdue > 0
        ? 'I can run a full cleanup sweep right now and queue only meaningful actions.'
        : (inboxUnassignedNewCount > 0
            ? `I found ${inboxUnassignedNewCount} inbox item${inboxUnassignedNewCount === 1 ? '' : 's'} without a project — want me to coach the next one?`
            : 'I can proactively brief, triage, and queue approvals while you stay in flow.');

    /* ── Helper: expandable card wrapper ─────────────────────────── */
    function makeCard(id, icon, iconColor, label, rightHtml, previewHtml, bodyHtml, opts) {
        const el = document.createElement('div');
        el.className = 'dash-card' + (opts?.extraClass ? ' '+opts.extraClass : '');
        el.dataset.cardId = id;
        const hasBody = !!bodyHtml;
        el.innerHTML = `
            <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
                <div class="flex items-center gap-2 min-w-0">
                    <i class="fa-solid ${icon} ${iconColor} text-[10px] shrink-0"></i>
                    <span class="ops-section-title font-mono uppercase truncate">${label}</span>
                </div>
                <div class="flex items-center gap-2">
                    ${rightHtml || ''}
                    ${hasBody ? '<i class="fa-solid fa-chevron-down expand-chevron"></i>' : ''}
                </div>
            </div>
            <div class="px-3 pb-2.5">${previewHtml}</div>
            ${hasBody ? `<div class="dash-card-body px-3 pb-3">${bodyHtml}</div>` : ''}
        `;
        return el;
    }

    /* ── Build scrollable dashboard ──────────────────────────────── */
    const wrap = document.createElement('div');
    wrap.className = 'h-full min-h-0 overflow-y-auto p-4 space-y-3 dash-stagger';

    // ═══ GREETING + STAT PILLS ═══════════════════════════════════════
    const hour = new Date().getHours();
    const greeting = hour < 12 ? 'Good morning' : hour < 17 ? 'Good afternoon' : 'Good evening';
    const userName = safeText(teamMembers.find((m) => safeText(m?.role).toLowerCase() === 'admin')?.name) || 'Operator';
    const dateStr = new Date().toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });

    const headerEl = document.createElement('div');
    headerEl.className = 'flex flex-col sm:flex-row sm:items-center justify-between gap-2';
    headerEl.innerHTML = `
        <div>
            <div class="text-lg text-white font-semibold tracking-tight">${escapeHtml(greeting)}, ${escapeHtml(userName)}</div>
            <div class="text-[10px] font-mono text-ops-light/60 mt-0.5">${escapeHtml(dateStr)}</div>
        </div>
        <div class="flex flex-wrap items-center gap-1.5">
            ${totalOverdue ? `<span class="stat-pill stat-pill--danger"><i class="fa-solid fa-triangle-exclamation text-[8px]"></i>${totalOverdue} overdue</span>` : ''}
            ${inboxUnassignedNewCount ? `<span class="stat-pill stat-pill--danger"><i class="fa-solid fa-link-slash text-[8px]"></i>${inboxUnassignedNewCount} unassigned</span>` : ''}
            ${dueThisWeek ? `<span class="stat-pill stat-pill--warning"><i class="fa-solid fa-fire text-[8px]"></i>${dueThisWeek} this week</span>` : ''}
            ${calls.length ? `<span class="stat-pill"><i class="fa-solid fa-video text-[8px] text-blue-400"></i>${calls.length} call${calls.length>1?'s':''}</span>` : ''}
            <span class="stat-pill ${inboxNewCount ? 'stat-pill--accent' : 'stat-pill--muted'}"><i class="fa-solid fa-inbox text-[8px] text-purple-400"></i>${inboxNewCount} inbox</span>
            <span class="stat-pill ${totalDoneWeek ? 'stat-pill--success' : 'stat-pill--muted'}"><i class="fa-solid fa-check text-[8px] text-emerald-400"></i>${totalDoneWeek} done</span>
            <button id="dash-action-only-toggle" class="stat-pill ${actionOnlyMode ? 'stat-pill--accent' : 'stat-pill--muted'}" title="Show only high-confidence actionable items">${actionOnlyMode ? 'Action Only: On' : 'Action Only: Off'}</button>
            <span class="stat-pill cursor-pointer hover:text-white" id="dash-shortcuts-btn" title="Keyboard Shortcuts"><i class="fa-solid fa-keyboard text-[8px]"></i>?</span>
        </div>
    `;
    if (pagePrefs.missionControl) wrap.appendChild(headerEl);

    // ═══ M.A.R.C.U.S. AMBIENT INTELLIGENCE BAR ══════════════════════════════
    const marcusBar = document.createElement('div');
    marcusBar.className = 'marcus-ambient dash-card';
    marcusBar.dataset.cardId = 'marcus';
    const primaryInsight = marcusInsights[0];
    const extraInsights = marcusInsights.slice(1);
    marcusBar.innerHTML = `
        <div class="dash-card-head flex items-center gap-3 px-3 py-2.5">
            <div class="marcus-status-dot shrink-0"></div>
            <div class="flex items-center gap-2 min-w-0 flex-1">
                <div class="marcus-orb marcus-dashboard-avatar idle shrink-0" aria-hidden="true"></div>
                <span class="text-[10px] font-mono uppercase tracking-widest text-blue-300">M.A.R.C.U.S.</span>
                <span class="text-[9px] font-mono text-blue-400/40">\u2014 monitoring ${activeProjects.length} projects, ${allTasks.filter(t=>!isDoneTask(t)).length} tasks</span>
            </div>
            <div class="flex items-center gap-1.5 shrink-0">
                <button id="dash-ask-marcus" class="px-2 py-1 rounded border border-blue-500/25 bg-blue-500/10 text-[9px] font-mono text-blue-300 hover:bg-blue-500/20 transition-colors">Ask</button>
                <button id="dash-brief-marcus" class="px-2 py-1 rounded border border-blue-500/25 bg-blue-500/10 text-[9px] font-mono text-blue-300 hover:bg-blue-500/20 transition-colors">Brief me</button>
                <button id="dash-marcus-sweep" class="px-2 py-1 rounded border border-emerald-500/25 bg-emerald-500/10 text-[9px] font-mono text-emerald-300 hover:bg-emerald-500/20 transition-colors">Sweep</button>
                <button id="dash-marcus-coach" class="px-2 py-1 rounded border border-purple-500/25 bg-purple-500/10 text-[9px] font-mono text-purple-300 hover:bg-purple-500/20 transition-colors">Coach</button>
                ${extraInsights.length ? '<i class="fa-solid fa-chevron-down expand-chevron"></i>' : ''}
            </div>
        </div>
        <div class="px-3 pb-2.5">
            <div class="marcus-insight flex items-start gap-2">
                <i class="fa-solid ${primaryInsight.icon} text-[10px] mt-0.5 shrink-0"></i>
                <span class="text-[11px] leading-relaxed">${escapeHtml(primaryInsight.text)}</span>
            </div>
            <div class="mt-1.5 text-[10px] font-mono text-blue-200/70">${escapeHtml(marcusCheckin)}</div>
        </div>
        ${extraInsights.length ? `<div class="dash-card-body px-3 pb-3"><div class="space-y-1.5">${extraInsights.map(ins => `<div class="marcus-insight flex items-start gap-2"><i class="fa-solid ${ins.icon} text-[10px] mt-0.5 shrink-0"></i><span class="text-[11px] leading-relaxed">${escapeHtml(ins.text)}</span></div>`).join('')}</div></div>` : ''}
    `;
    if (pagePrefs.missionControl) wrap.appendChild(marcusBar);

    const actionStrip = document.createElement('div');
    actionStrip.className = 'dash-card';
    actionStrip.dataset.cardId = 'action-strip';
    const laneProjectNameById = new Map((Array.isArray(state.projects) ? state.projects : []).map((p) => [safeText(p?.id), safeText(p?.name)]));
    const toSourceLabel = (src) => {
        const key = normalizeInboxSourceKey(src);
        if (key === 'slack') return 'Slack';
        if (key === 'email') return 'Email';
        if (key === 'sms') return 'SMS';
        return key ? key.toUpperCase() : 'Inbox';
    };
    const cleanActionPreview = (raw) => {
        const text = safeText(raw).replace(/\s+/g, ' ').trim();
        if (!text) return 'Inbox item';
        const withoutBracketDate = text.replace(/^\[(?:19|20)\d{2}-\d{2}-\d{2}[^\]]*\]\s*/i, '');
        const withoutIso = withoutBracketDate.replace(/^(?:19|20)\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s*[-–—:]?\s*/i, '');
        const finalText = withoutIso.trim();
        return finalText || text;
    };
    const renderInboxActionRow = (item) => {
        const id = safeText(item?.id).trim();
        const text = safeText(item?.text || item?.subject || item?.title);
        const preview = cleanActionPreview(text).slice(0, 104) || 'Inbox item';
        const projectId = safeText(item?.projectId).trim();
        const projectName = projectId ? (safeText(laneProjectNameById.get(projectId)).trim() || 'Linked project') : 'No project linked';
        const sourceLabel = toSourceLabel(item?.source);
        return `
            <div class="border border-ops-border rounded bg-ops-bg/40 px-2 py-1.5 flex items-center justify-between gap-2">
                <div class="min-w-0">
                    <div class="text-[10px] text-white truncate">${escapeHtml(preview)}</div>
                    <div class="text-[9px] text-ops-light/70 truncate">${escapeHtml(sourceLabel)} • ${escapeHtml(projectName)}</div>
                </div>
                <div class="flex items-center gap-1 shrink-0">
                    <button data-action-open-inbox="${escapeHtml(id)}" class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white">Review</button>
                    <button data-action-done-inbox="${escapeHtml(id)}" class="px-1.5 py-0.5 rounded border border-emerald-600/40 text-[9px] font-mono text-emerald-200 hover:bg-emerald-600/20">Done</button>
                </div>
            </div>
        `;
    };
    actionStrip.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2 min-w-0">
                <i class="fa-solid fa-bolt text-amber-400 text-[10px] shrink-0"></i>
                <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light truncate">Priority Queue</span>
            </div>
            <button data-open-inbox class="px-2 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white">Open Inbox</button>
        </div>
        <div class="px-3 pb-3 grid grid-cols-1 md:grid-cols-3 gap-2">
            <div class="border border-red-500/25 rounded-lg bg-red-500/5 p-2">
                <div class="flex items-center justify-between gap-2 mb-1.5">
                    <div class="text-[9px] font-mono uppercase tracking-widest text-red-300">Do First</div>
                    <div class="text-[9px] text-red-200/80">${doTodayLane.length}</div>
                </div>
                <div class="space-y-1">
                    ${doTodayLane.length ? doTodayLane.map((x) => `<div class="border border-red-500/20 rounded bg-ops-bg/40 px-2 py-1.5"><div class="text-[10px] text-white truncate">${escapeHtml(x.title)}</div><div class="text-[9px] font-mono text-red-300/70 truncate">${escapeHtml(x.sub)}</div></div>`).join('') : '<div class="text-[10px] text-ops-light/50">No immediate tasks.</div>'}
                </div>
            </div>
            <div class="border border-amber-500/25 rounded-lg bg-amber-500/5 p-2">
                <div class="flex items-center justify-between gap-2 mb-1.5">
                    <div class="text-[9px] font-mono uppercase tracking-widest text-amber-300">Unsorted Inbox</div>
                    <div class="text-[9px] text-amber-200/80">${needsLinkLane.length}</div>
                </div>
                <div class="space-y-1">
                    ${needsLinkLane.length ? needsLinkLane.map(renderInboxActionRow).join('') : '<div class="text-[10px] text-ops-light/50">Nothing unlinked.</div>'}
                </div>
            </div>
            <div class="border border-blue-500/25 rounded-lg bg-blue-500/5 p-2">
                <div class="flex items-center justify-between gap-2 mb-1.5">
                    <div class="text-[9px] font-mono uppercase tracking-widest text-blue-300">Waiting / Follow-up</div>
                    <div class="text-[9px] text-blue-200/80">${waitingLane.length}</div>
                </div>
                <div class="space-y-1">
                    ${waitingLane.length ? waitingLane.map(renderInboxActionRow).join('') : '<div class="text-[10px] text-ops-light/50">No follow-ups queued.</div>'}
                </div>
            </div>
        </div>
    `;
    if (pagePrefs.missionControl) wrap.appendChild(actionStrip);

    // ═══ OVERDUE ALERT ═══════════════════════════════════════════════
    if (!actionOnlyMode && pagePrefs.missionControl && totalOverdue > 0) {
        const alertEl = document.createElement('div');
        alertEl.className = 'border border-red-500/30 rounded-xl bg-red-500/8 px-3 py-2.5 flex items-center gap-3';
        const overdueNames = [...overdueTasks.slice(0,3).map(t=>safeText(t?.title)), ...overdueProjects.slice(0,2).map(p=>safeText(p?.name))];
        alertEl.innerHTML = `
            <div class="w-7 h-7 rounded-lg flex items-center justify-center bg-red-500/15 text-red-400 shrink-0 text-xs"><i class="fa-solid fa-triangle-exclamation"></i></div>
            <div class="min-w-0 flex-1">
                <div class="text-[11px] text-red-300 font-semibold">${totalOverdue} Overdue</div>
                <div class="text-[10px] font-mono text-red-400/60 truncate">${overdueNames.map(n=>escapeHtml(n)).join(' \u00b7 ')}</div>
            </div>
        `;
        wrap.appendChild(alertEl);
    }

    // ═══ QUICK-ADD BAR ═══════════════════════════════════════════════
    const quickAdd = document.createElement('div');
    quickAdd.className = 'flex gap-1.5';
    quickAdd.innerHTML = `
        <input id="dash-quick-input" type="text" class="flex-1 bg-ops-bg/60 border border-ops-border rounded-lg px-3 py-1.5 text-[11px] font-mono text-white placeholder-ops-light/40 focus:outline-none focus:ring-1 focus:ring-ops-accent" placeholder="Quick add \u2014 type & hit Enter\u2026" />
        <button id="dash-quick-project" class="px-2.5 py-1.5 rounded-lg border border-ops-border text-[9px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors shrink-0"><i class="fa-solid fa-folder-plus mr-1"></i>Project</button>
    `;
    if (!actionOnlyMode && pagePrefs.newProjectIntake) wrap.appendChild(quickAdd);

    // ═══ INBOX RADAR (compact banner) ════════════════════════════════
    const radarBanner = document.createElement('div');
    radarBanner.className = `dash-card${inboxUnassignedNewCount ? ' dash-card--danger' : (inboxNewCount ? ' dash-card--accent' : '')}`;
    radarBanner.dataset.cardId = 'radar';
    const formatInboxStamp = (iso) => {
        const s = safeText(iso).trim();
        if (!s) return '';
        const d = new Date(s);
        if (Number.isNaN(d.getTime())) return '';
        return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    };
    const makeRadarHtml = (payloadOrItems) => {
        const items = Array.isArray(payloadOrItems?.items)
            ? payloadOrItems.items
            : (Array.isArray(payloadOrItems) ? payloadOrItems : []);
        const groups = Array.isArray(payloadOrItems?.groups) ? payloadOrItems.groups : null;
        const businessGroups = Array.isArray(payloadOrItems?.businessGroups) ? payloadOrItems.businessGroups : null;

        const list = Array.isArray(items) ? items : [];
        const slack = list.filter((x) => normalizeInboxSourceKey(x?.source) === 'slack');
        const email = list.filter((x) => normalizeInboxSourceKey(x?.source) === 'email');
        const other = list.filter((x) => { const k = normalizeInboxSourceKey(x?.source); return k !== 'slack' && k !== 'email'; });

        const renderGroupRow = (row, { showBusiness = true } = {}) => {
            const business = safeText(row?.businessLabel) || safeText(row?.businessKey) || 'Business';
            const stamp = formatInboxStamp(safeText(row?.latestAt));
            const count = Number(row?.count) || 0;
            const isUnassigned = Boolean(row?.isUnassigned) || (!safeText(row?.projectId).trim());
            const title = isUnassigned ? 'Unassigned' : (safeText(row?.projectName).trim() || 'Project');
            const summary = safeText(row?.summary).trim() || (Array.isArray(row?.sample) ? safeText(row.sample[0]) : '');

            const borderTone = isUnassigned ? 'border-red-500/30 bg-red-500/10' : 'border-ops-border bg-ops-bg/40';
            const countTone = isUnassigned ? 'text-red-300' : 'text-white';

            return `
                <div class="border rounded px-2.5 py-2 ${borderTone}">
                    <div class="flex items-start justify-between gap-2">
                        <div class="min-w-0 flex-1">
                            <div class="flex items-center gap-1.5 flex-wrap">
                                ${showBusiness ? `<span class=\"px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light/50\">${escapeHtml(business)}</span>` : ''}
                                ${stamp ? `<span class="text-[9px] font-mono text-ops-light/40">${escapeHtml(stamp)}</span>` : ''}
                            </div>
                            <div class="mt-1 flex items-center justify-between gap-2">
                                <div class="min-w-0 text-[11px] ${isUnassigned ? 'text-red-200 font-semibold' : 'text-white'} truncate">${escapeHtml(title)}</div>
                                <div class="shrink-0 text-[11px] font-mono font-semibold ${countTone}">${count}</div>
                            </div>
                            ${summary ? `<div class="mt-0.5 text-[10px] text-ops-light/60 truncate">${escapeHtml(summary)}</div>` : ''}
                        </div>
                    </div>
                </div>
            `;
        };

        const renderBusinessRow = (row) => {
            const business = safeText(row?.businessLabel) || safeText(row?.businessKey) || 'Business';
            const stamp = formatInboxStamp(safeText(row?.latestAt));
            const count = Number(row?.count) || 0;
            const summary = safeText(row?.summary).trim() || (Array.isArray(row?.sample) ? safeText(row.sample[0]) : '');
            return `
                <div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-2">
                    <div class="flex items-start justify-between gap-2">
                        <div class="min-w-0 flex-1">
                            <div class="flex items-center gap-1.5 flex-wrap">
                                <span class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light/50">${escapeHtml(business)}</span>
                                ${stamp ? `<span class=\"text-[9px] font-mono text-ops-light/40\">${escapeHtml(stamp)}</span>` : ''}
                            </div>
                            <div class="mt-1 flex items-center justify-between gap-2">
                                <div class="min-w-0 text-[11px] text-white truncate">Messages</div>
                                <div class="shrink-0 text-[11px] font-mono font-semibold text-white">${count}</div>
                            </div>
                            ${summary ? `<div class=\"mt-0.5 text-[10px] text-ops-light/60 truncate\">${escapeHtml(summary)}</div>` : ''}
                        </div>
                    </div>
                </div>
            `;
        };

        const rowsHtml = Array.isArray(businessGroups)
            ? (() => {
                const shown = businessGroups.slice(0, 8);
                const more = businessGroups.length - shown.length;
                return `${shown.map(renderBusinessRow).join('')}${more > 0 ? `<div class=\"text-[9px] font-mono text-ops-light/40 px-1\">+${more} more businesses</div>` : ''}`;
            })()
            : Array.isArray(groups)
                ? (() => {
                    const sections = [];
                    const byKey = new Map();
                    for (const g of groups) {
                        const key = safeText(g?.businessKey) || safeText(g?.businessLabel) || 'business';
                        if (!byKey.has(key)) {
                            const label = safeText(g?.businessLabel) || safeText(g?.businessKey) || 'Business';
                            const sec = { key, label, rows: [] };
                            byKey.set(key, sec);
                            sections.push(sec);
                        }
                        byKey.get(key).rows.push(g);
                    }

                    return sections
                        .map((sec) => {
                            const total = sec.rows.reduce((sum, r) => sum + (Number(r?.count) || 0), 0);
                            const shown = sec.rows.slice(0, 6);
                            const more = sec.rows.length - shown.length;
                            return `
                                <div class="border border-ops-border rounded-lg bg-ops-bg/20 px-2.5 py-2">
                                    <div class="flex items-center justify-between gap-2 mb-1">
                                        <div class="text-[9px] font-mono uppercase tracking-widest text-ops-light/60 truncate">${escapeHtml(sec.label)}</div>
                                        <div class="text-[10px] font-mono text-white">${total}</div>
                                    </div>
                                    <div class="space-y-1">
                                        ${shown.map((r) => renderGroupRow(r, { showBusiness: false })).join('')}
                                        ${more > 0 ? `<div class=\"text-[9px] font-mono text-ops-light/40 px-1\">+${more} more</div>` : ''}
                                    </div>
                                </div>
                            `;
                        })
                        .join('');
                })()
                : list
                    .slice(0, 8)
                    .map((row) => {
                const item = row;
                const status = safeText(item?.status).trim() || 'New';
                const sourceKey = normalizeInboxSourceKey(item?.source);
                const meta = inboxSourceMeta(sourceKey);
                const iconCls = meta.icon === 'fa-slack' ? 'fa-brands fa-slack' : `fa-solid ${meta.icon}`;
                const business = inboxBusinessLabel(item);
                const stamp = formatInboxStamp(safeText(item?.updatedAt) || safeText(item?.createdAt));

                const fullText = safeText(item?.text) || safeText(item?.content) || safeText(item?.body) || safeText(item?.message) || '';
                const snippet = previewText(fullText, 140);
                const explicitTitle = safeText(item?.title) || safeText(item?.subject) || '';
                const titleLine = explicitTitle.trim() || snippet || 'Inbox item';
                const subLine = (explicitTitle.trim() && snippet && snippet !== titleLine) ? snippet : '';

                return `
                    <div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-2">
                        <div class="flex items-start justify-between gap-2">
                            <div class="min-w-0 flex-1">
                                <div class="flex items-center gap-1.5 flex-wrap">
                                    <span class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light/70">${escapeHtml(status)}</span>
                                    <span class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono ${meta.tone} flex items-center gap-1">
                                        <i class="${iconCls} text-[9px]"></i>
                                        ${escapeHtml(meta.label)}
                                    </span>
                                    <span class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light/50">${escapeHtml(business)}</span>
                                    ${stamp ? `<span class="text-[9px] font-mono text-ops-light/40">${escapeHtml(stamp)}</span>` : ''}
                                </div>
                                <div class="mt-1 text-[11px] text-white truncate">${escapeHtml(titleLine)}</div>
                                ${subLine ? `<div class="mt-0.5 text-[10px] text-ops-light/60 truncate">${escapeHtml(subLine)}</div>` : ''}
                            </div>
                        </div>
                    </div>
                `;
            })
            .join('');

        const showCount = Array.isArray(businessGroups)
            ? businessGroups.reduce((sum, g) => sum + (Number(g?.count) || 0), 0)
            : (Array.isArray(groups)
                ? groups.reduce((sum, g) => sum + (Number(g?.count) || 0), 0)
                : list.length);

        return `
            <div class="dash-card-head flex items-center justify-between gap-3 px-3 py-2.5">
                <div class="flex items-center gap-3 min-w-0">
                    <i class="fa-solid fa-satellite-dish text-blue-400 text-xs shrink-0"></i>
                    <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light">Inbox Radar</span>
                    <div class="flex items-center gap-3 text-[10px] font-mono text-ops-light/50">
                        <span class="text-lg font-semibold text-white leading-none">${showCount}</span>
                        <span><i class="fa-brands fa-slack text-purple-400 mr-0.5"></i>${slack.length}</span>
                        <span><i class="fa-solid fa-envelope text-sky-400 mr-0.5"></i>${email.length}</span>
                        <span><i class="fa-solid fa-ellipsis text-ops-light/30 mr-0.5"></i>${other.length}</span>
                    </div>
                </div>
                <div class="flex items-center gap-1.5 shrink-0">
                    <button type="button" data-run-marcus-filter class="px-2.5 py-1 rounded border border-amber-600/40 bg-amber-600/15 text-[9px] font-mono text-amber-200 hover:bg-amber-600/25 transition-colors">Run Marcus Filter</button>
                    <button type="button" data-open-inbox class="px-2.5 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Open Inbox</button>
                    ${showCount ? '<i class="fa-solid fa-chevron-down expand-chevron"></i>' : ''}
                </div>
            </div>
            ${showCount ? `<div class="dash-card-body px-3 pb-2.5"><div class="space-y-1">${rowsHtml}</div></div>` : ''}
        `;
    };

    radarBanner.innerHTML = makeRadarHtml(dashboardInbox);
    if (pagePrefs.commsRadar) {
        wrap.appendChild(radarBanner);

        // Upgrade radar to cross-business feed (best-effort).
        setTimeout(async () => {
            try {
                const data = await apiJson('/api/inbox/radar?status=New&limit=60');
                if (state.currentView !== 'dashboard') return;
                radarBanner.innerHTML = makeRadarHtml(data);

                // Re-wire events (innerHTML replacement removes listeners).
                radarBanner.querySelector('button[data-open-inbox]')?.addEventListener('click', () => openInbox());
                radarBanner.querySelector('button[data-run-marcus-filter]')?.addEventListener('click', async () => {
                    try {
                        const result = await runMarcusInboxFilter();
                        alert(`Marcus filter complete. Archived: ${Number(result?.archived || 0)}. Matched: ${Number(result?.matched || 0)}.`);
                        renderMain();
                    } catch (e) {
                        alert(e?.message || 'Marcus filter failed');
                    }
                });
                const head = radarBanner.querySelector('.dash-card-head');
                const body = radarBanner.querySelector('.dash-card-body');
                if (head && body) {
                    head.addEventListener('click', (e) => {
                        if (e.target.closest('button') || e.target.closest('a')) return;
                        radarBanner.classList.toggle('expanded');
                    });
                }
            } catch {
                // ignore
            }
        }, 0);
    }

    // ═══ URGENT ROW: Calendar + Due Today + Due This Week ════════════
    const urgentRow = document.createElement('div');
    urgentRow.className = 'grid grid-cols-1 md:grid-cols-3 gap-2';

    // Calendar card
    let calPreview = '';
    let calBody = '';
    if (!callsConnected) { calPreview = `<div class="text-[10px] text-ops-light/50">Connect Google Calendar in Settings.</div>`; }
    else if (callsError) { calPreview = `<div class="text-[10px] text-amber-300">${escapeHtml(callsError)}</div>`; }
    else if (callsLoading && !calls.length) { calPreview = `<div class="text-[10px] text-ops-light/50">Loading\u2026</div>`; }
    else if (calls.length) {
        const mkRow = (ev) => { const time = formatTimeFromIso(ev.start); const ti = safeText(ev.summary)||'Untitled'; const link = safeText(ev.meetingLink); return `<div class="flex items-center justify-between gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><div class="min-w-0"><div class="text-[11px] text-white truncate">${escapeHtml(ti)}</div><div class="text-[9px] font-mono text-ops-light/60">${escapeHtml(time||'')}</div></div>${link ? `<a class="shrink-0 text-[9px] font-mono text-ops-accent hover:underline" href="${escapeHtml(link)}" target="_blank" rel="noreferrer">Join</a>` : ''}</div>`; };
        calPreview = calls.slice(0,2).map(mkRow).join('');
        if (calls.length > 2) calBody = `<div class="space-y-1">${calls.slice(2).map(mkRow).join('')}</div>`;
    } else { calPreview = `<div class="text-[10px] text-ops-light/50">No events today.</div>`; }
    const calCardRight = `<div class="flex gap-1"><button type="button" data-refresh-calls class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors"><i class="fa-solid fa-rotate text-[8px]"></i></button><button type="button" data-open-calendar class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open</button></div>`;
    const calCard = makeCard('calendar', 'fa-calendar-days', 'text-blue-400', 'Calendar', calCardRight, `<div class="space-y-1">${calPreview}</div>`, calBody);
    if (pagePrefs.deliveryBoard) urgentRow.appendChild(calCard);

    // Due Today card
    const dueTodayItems = Array.isArray(buckets.today) ? buckets.today : [];
    const mkProjBtn = (p, color) => `<button type="button" class="dash-project-btn w-full text-left px-2.5 py-1.5 rounded border border-ops-border bg-ops-bg/40 hover:bg-ops-surface/60 transition-colors" data-pid="${escapeHtml(safeText(p?.id))}"><div class="text-[11px] text-white truncate">${escapeHtml(safeText(p?.name)||'Untitled')}</div><div class="text-[9px] font-mono ${color}">${escapeHtml(safeText(p?.dueDate)||'')}</div></button>`;
    const dtPreview = dueTodayItems.length ? `<div class="space-y-1">${dueTodayItems.slice(0,2).map(p=>mkProjBtn(p,'text-red-400/70')).join('')}</div>` : `<div class="text-[10px] text-ops-light/50">Nothing due today.</div>`;
    const dtBody = dueTodayItems.length > 2 ? `<div class="space-y-1">${dueTodayItems.slice(2).map(p=>mkProjBtn(p,'text-red-400/70')).join('')}</div>` : '';
    const dueTodayCard = makeCard('due-today', 'fa-fire', 'text-red-400', 'Due Today', `<span class="text-sm font-semibold text-white">${dueTodayItems.length}</span>`, dtPreview, dtBody, { extraClass: dueTodayItems.length ? 'dash-card--danger' : '' });
    if (pagePrefs.deliveryBoard) urgentRow.appendChild(dueTodayCard);

    // Due This Week card
    const dueWeekItems = [...(Array.isArray(buckets.tomorrow)?buckets.tomorrow:[]), ...(Array.isArray(buckets.thisWeek)?buckets.thisWeek:[])];
    const dwPreview = dueWeekItems.length ? `<div class="space-y-1">${dueWeekItems.slice(0,2).map(p=>mkProjBtn(p,'text-amber-400/70')).join('')}</div>` : `<div class="text-[10px] text-ops-light/50">Nothing else this week.</div>`;
    const dwBody = dueWeekItems.length > 2 ? `<div class="space-y-1">${dueWeekItems.slice(2).map(p=>mkProjBtn(p,'text-amber-400/70')).join('')}</div>` : '';
    const dueWeekCard = makeCard('due-week', 'fa-calendar-week', 'text-amber-400', 'Due This Week', `<span class="text-sm font-semibold text-white">${dueWeekItems.length}</span>`, dwPreview, dwBody, { extraClass: dueWeekItems.length ? 'dash-card--warning' : '' });
    if (pagePrefs.deliveryBoard) urgentRow.appendChild(dueWeekCard);
    if (!actionOnlyMode && pagePrefs.deliveryBoard) wrap.appendChild(urgentRow);

    // ═══ MID ROW: Streaks + Focus Timer ══════════════════════════════
    const midRow = document.createElement('div');
    midRow.className = 'grid grid-cols-1 sm:grid-cols-2 gap-2';

    // Streak chart
    const barsHtml = weekDays.map((wd,i) => {
        const count = completionsByDay[i]; const pct = Math.round((count/maxC)*100); const isToday = wd.ymd === today;
        const barColor = isToday ? 'bg-blue-400' : count > 0 ? 'bg-emerald-400/70' : 'bg-ops-border';
        return `<div class="flex flex-col items-center gap-0.5 flex-1"><div class="w-full rounded-sm ${barColor}" style="height:${Math.max(3, pct*0.5)}px" title="${count}"></div><span class="text-[8px] font-mono ${isToday?'text-blue-400 font-bold':'text-ops-light/40'}">${wd.label}</span></div>`;
    }).join('');
    const streakPreview = `<div class="flex items-end gap-0.5 h-12">${barsHtml}</div>`;
    const streakCard = makeCard('streaks', 'fa-chart-bar', 'text-emerald-400', 'Week', `<span class="text-[10px] font-mono text-ops-light/50">${totalDoneWeek} done</span>`, streakPreview, '');
    if (pagePrefs.deliveryBoard) midRow.appendChild(streakCard);

    // Focus Timer
    const mins = Math.floor(state.focusTimer.remaining/60);
    const secs = state.focusTimer.remaining % 60;
    const timerDisp = `${String(mins).padStart(2,'0')}:${String(secs).padStart(2,'0')}`;
    const pctEl = ((state.focusTimer.duration - state.focusTimer.remaining)/state.focusTimer.duration)*100;
    const timerPreview = `
        <div class="flex items-center gap-3">
            <div id="dash-timer-display" class="text-2xl font-mono font-semibold ${state.focusTimer.running?'text-orange-400':'text-white'} tabular-nums">${timerDisp}</div>
            <div class="flex-1 h-1.5 rounded-full bg-ops-border overflow-hidden"><div class="h-full rounded-full bg-orange-400 transition-all" style="width:${pctEl}%"></div></div>
            <div class="flex gap-1 shrink-0">
                <button id="dash-timer-toggle" class="px-2 py-1 rounded border ${state.focusTimer.running?'border-red-500/30 bg-red-500/10 text-red-300':'border-emerald-500/30 bg-emerald-500/10 text-emerald-300'} text-[9px] font-mono transition-colors">${state.focusTimer.running?'Pause':'Start'}</button>
                <button id="dash-timer-reset" class="px-2 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Reset</button>
            </div>
        </div>`;
    const timerCard = makeCard('timer', 'fa-hourglass-half', 'text-orange-400', 'Focus', '', timerPreview, '');
    if (pagePrefs.deliveryBoard) midRow.appendChild(timerCard);
    if (!actionOnlyMode && pagePrefs.deliveryBoard) wrap.appendChild(midRow);

    // ═══ TODAY'S FOCUS (outcomes + next actions) ═════════════════════
    const nextActionsHtml = nextActions.length
        ? nextActions.slice(0, 3).map((t) => {
            const pr = Number(t?.priority) || 3;
            const prColor = pr === 1 ? 'text-red-400' : pr === 2 ? 'text-amber-400' : 'text-ops-light/50';
            const id = safeText(t?.id).trim();
            const ai = id ? aiTaskMap[id] : null;
            let title = safeText(ai?.title || t?.title).trim();
            if (isBadDashText(title)) title = safeText(t?.project).trim() ? `Follow up: ${safeText(t?.project).trim()}` : 'Next action';
            const summary = safeText(ai?.summary).trim();
            const project = safeText(t?.project).trim();
            const sub = summary || project;
            return `<div class="flex items-center gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><span class="${prColor} text-[9px] font-mono font-bold shrink-0">P${pr}</span><div class="min-w-0 flex-1"><div class="text-[11px] text-white truncate">${escapeHtml(title)}</div>${sub ? `<div class="text-[9px] font-mono text-ops-light/60 truncate">${escapeHtml(sub)}</div>` : ''}</div></div>`;
        }).join('')
        : `<div class="text-[10px] text-ops-light/50">No next actions.</div>`;
    const extraActions = nextActions.length > 3
        ? nextActions.slice(3, 10).map((t) => {
            const pr = Number(t?.priority) || 3;
            const prColor = pr === 1 ? 'text-red-400' : pr === 2 ? 'text-amber-400' : 'text-ops-light/50';
            const id = safeText(t?.id).trim();
            const ai = id ? aiTaskMap[id] : null;
            let title = safeText(ai?.title || t?.title).trim();
            if (isBadDashText(title)) title = safeText(t?.project).trim() ? `Follow up: ${safeText(t?.project).trim()}` : 'Next action';
            const summary = safeText(ai?.summary).trim();
            return `<div class="flex items-center gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><span class="${prColor} text-[9px] font-mono font-bold shrink-0">P${pr}</span><div class="min-w-0 flex-1"><div class="text-[11px] text-white truncate">${escapeHtml(title)}</div>${summary ? `<div class="text-[9px] font-mono text-ops-light/60 truncate">${escapeHtml(summary)}</div>` : ''}</div></div>`;
        }).join('')
        : '';

    const focusEl = document.createElement('div');
    focusEl.className = 'dash-card';
    focusEl.dataset.cardId = 'focus';
    focusEl.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2"><i class="fa-solid fa-crosshairs text-ops-accent text-[10px]"></i><span class="text-[10px] font-mono uppercase tracking-widest text-ops-light">Today's Focus</span></div>
            ${extraActions ? '<i class="fa-solid fa-chevron-down expand-chevron"></i>' : ''}
        </div>
        <div class="px-3 pb-2.5">
            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                    <div class="text-[9px] font-mono uppercase tracking-widest text-ops-light/60 mb-1.5">Outcomes</div>
                    <textarea id="today-outcomes" rows="3" class="w-full bg-ops-bg/60 border border-ops-border rounded-lg px-2.5 py-1.5 text-[11px] font-mono text-white resize-none focus:outline-none focus:ring-1 focus:ring-ops-accent" placeholder="1) \u2026  2) \u2026  3) \u2026">${escapeHtml(outcomes)}</textarea>
                    <div class="flex gap-1.5 mt-1.5">
                        <button id="btn-save-today" class="px-2.5 py-1 rounded border border-ops-accent/30 bg-ops-accent/10 text-[9px] font-mono text-blue-300 hover:bg-ops-accent/20 transition-colors">Save</button>
                        <button id="btn-clear-today" class="px-2.5 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Clear</button>
                    </div>
                </div>
                <div>
                    <div class="text-[9px] font-mono uppercase tracking-widest text-ops-light/60 mb-1.5">Next Actions</div>
                    <div class="space-y-1">${nextActionsHtml}</div>
                </div>
            </div>
        </div>
        ${extraActions ? `<div class="dash-card-body px-3 pb-3"><div class="text-[9px] font-mono uppercase tracking-widest text-ops-light/60 mb-1.5">More Actions</div><div class="space-y-1">${extraActions}</div></div>` : ''}
    `;
    if (pagePrefs.deliveryBoard) wrap.appendChild(focusEl);

    // Wire outcomes
    const outcomesTA = focusEl.querySelector('#today-outcomes');
    const btnSave = focusEl.querySelector('#btn-save-today');
    const btnClear = focusEl.querySelector('#btn-clear-today');
    if (btnSave && outcomesTA) { btnSave.onclick = async () => { btnSave.disabled = true; try { const v = safeText(outcomesTA.value); state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {}; state.settings.todayOutcomes = v; state.rerenderPauseUntil = Date.now()+2000; await saveSettingsPatch({todayOutcomes:v}); state.rerenderPauseUntil = 0; } catch(e) { alert(e?.message||'Failed'); state.rerenderPauseUntil = 0; } finally { btnSave.disabled = false; } }; }
    if (btnClear && outcomesTA) { btnClear.onclick = async () => { outcomesTA.value=''; try { state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {}; state.settings.todayOutcomes=''; state.rerenderPauseUntil = Date.now()+2000; await saveSettingsPatch({todayOutcomes:''}); state.rerenderPauseUntil = 0; } catch(e) { alert(e?.message||'Failed'); state.rerenderPauseUntil = 0; } }; }

    // ═══ FEED ROW: Activity + Slack + Inbox + Team ══════════════════
    const feedRow = document.createElement('div');
    feedRow.className = 'grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-2';

    const inboxById = {};
    for (const it of inboxItems) {
        const id = safeText(it?.id).trim();
        if (!id) continue;
        inboxById[id] = it;
    }

    const projectNameById = {};
    for (const p of activeProjects) {
        const id = safeText(p?.id).trim();
        if (!id) continue;
        projectNameById[id] = safeText(p?.name).trim();
    }

    const parseFirstPhone = (text) => {
        const s = safeText(text);
        const m = s.match(/\+\d{7,15}/);
        return m ? m[0] : '';
    };

    const normalizePhoneForLookup = (value) => {
        const raw = safeText(value);
        const digits = raw.replace(/[^0-9]/g, '');
        return digits;
    };

    const extractFirstEmail = (value) => {
        const s = safeText(value);
        const m = s.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
        return m ? safeText(m[0]).trim().toLowerCase() : '';
    };

    // Contacts index (state.clients) for quick sender -> name lookup.
    const contactNameByPhone = {};
    const contactNameByEmail = {};
    for (const c of (Array.isArray(state.clients) ? state.clients : [])) {
        const name = safeText(c?.name).trim();
        if (!name) continue;
        const phone = normalizePhoneForLookup(c?.phone);
        if (phone) {
            contactNameByPhone[phone] = name;
            if (phone.length > 10) contactNameByPhone[phone.slice(-10)] = name;
        }
        const email = safeText(c?.email).trim().toLowerCase();
        if (email) contactNameByEmail[email] = name;
    }

    const resolveInboxContactName = (item) => {
        const explicit = safeText(item?.contactName).trim() || safeText(item?.fromName).trim();
        if (explicit) return explicit;

        const senderBits = [
            safeText(item?.sender).trim(),
            safeText(item?.fromNumber).trim(),
            safeText(item?.from).trim(),
            safeText(item?.phone).trim(),
            safeText(item?.title).trim(),
            safeText(item?.subject).trim(),
        ].filter(Boolean);

        for (const bit of senderBits) {
            const phone = normalizePhoneForLookup(bit);
            if (phone && contactNameByPhone[phone]) return contactNameByPhone[phone];
            if (phone && phone.length > 10 && contactNameByPhone[phone.slice(-10)]) return contactNameByPhone[phone.slice(-10)];

            const email = extractFirstEmail(bit);
            if (email && contactNameByEmail[email]) return contactNameByEmail[email];
        }

        // Fallback: show a readable counterparty (phone, email, or raw sender).
        const phone = normalizePhoneForLookup(item?.sender || item?.fromNumber || item?.phone);
        if (phone) return phone.length > 10 ? phone.slice(-10) : phone;
        const email = extractFirstEmail(item?.from || item?.sender || item?.title || item?.subject);
        if (email) return email;
        return safeText(item?.sender).trim() || safeText(item?.from).trim();
    };

    const formatClock = (d) => {
        try {
            const dt = (d instanceof Date) ? d : new Date(d);
            if (Number.isNaN(dt.getTime())) return '';
            return dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        } catch {
            return '';
        }
    };

    // Activity
    const activityBlocks = [];
    const inboxAggByKey = {};
    for (const a of recentActivity) {
        if (a.kind !== 'inbox') {
            activityBlocks.push({ kind: 'single', time: a.time, a });
            continue;
        }

        const id = safeText(a?.id).trim();
        const item = id ? inboxById[id] : null;
        const sourceKey = normalizeInboxSourceKey(item?.source || a?.source);
        const meta = inboxSourceMeta(sourceKey);
        const label = safeText(meta?.label).trim() || (sourceKey ? sourceKey.toUpperCase() : 'Inbox');
        const phone = parseFirstPhone(safeText(item?.from) || safeText(item?.phone) || safeText(item?.title) || safeText(a?.rawTitle));
        const counterparty = phone || safeText(item?.fromName).trim() || safeText(item?.contactName).trim();
        const groupKey = `inbox:${sourceKey}:${counterparty || 'unknown'}`;

        const stampIso = safeText(item?.createdAt || a?.time?.toISOString?.() || '').trim();
        const stamp = stampIso ? new Date(stampIso) : a.time;
        const existing = inboxAggByKey[groupKey];
        if (!existing) {
            inboxAggByKey[groupKey] = {
                kind: 'inbox-group',
                time: stamp,
                label,
                counterparty: counterparty || '',
                sourceKey,
                count: 1,
                sampleId: id,
                sampleRawTitle: safeText(a?.rawTitle),
                icon: a.icon,
            };
        } else {
            existing.count += 1;
            if (stamp > existing.time) {
                existing.time = stamp;
                existing.sampleId = id;
                existing.sampleRawTitle = safeText(a?.rawTitle);
            }
        }
    }
    for (const k of Object.keys(inboxAggByKey)) {
        activityBlocks.push(inboxAggByKey[k]);
    }
    activityBlocks.sort((x, y) => y.time - x.time);

    const renderActivityBlock = (block) => {
        const ts = formatClock(block.time);

        if (block.kind === 'single') {
            const a = block.a;
            let main = '';
            let sub = '';

            if (a.kind === 'task') {
                const id = safeText(a?.id).trim();
                const ai = id ? aiTaskMap[id] : null;
                main = `${safeText(a.verb)}: ${safeText(ai?.title || a.rawTitle || 'Task')}`;
                sub = safeText(ai?.summary).trim();
            } else {
                main = safeText(a?.text || '');
            }

            return `<div class="flex items-start gap-1.5"><i class="fa-solid ${a.icon} text-[9px] mt-0.5 shrink-0"></i><div class="min-w-0"><div class="text-[10px] text-white truncate">${escapeHtml(main)}</div>${sub ? `<div class="text-[9px] font-mono text-ops-light/50 truncate">${escapeHtml(sub)}</div>` : ''}<div class="text-[8px] font-mono text-ops-light/40">${escapeHtml(ts)}</div></div></div>`;
        }

        if (block.kind === 'inbox-group') {
            const id = safeText(block.sampleId).trim();
            const ai = id ? aiInboxMap[id] : null;
            let title = safeText(ai?.title || block.sampleRawTitle || 'Inbox item').trim();
            if (isBadDashText(title)) title = `${block.label} message`;
            const summary = safeText(ai?.summary).trim();
            const who = block.counterparty ? ` • ${block.counterparty}` : '';
            const count = Number(block.count) || 1;
            const countBadge = count > 1 ? `<span class="ml-1 px-1.5 py-0.5 rounded border border-ops-border text-[8px] font-mono text-ops-light/60">${count}</span>` : '';
            return `<div class="flex items-start gap-1.5"><i class="fa-solid ${block.icon} text-[9px] mt-0.5 shrink-0"></i><div class="min-w-0"><div class="text-[10px] text-white truncate">${escapeHtml(block.label)}${escapeHtml(who)}${countBadge} — ${escapeHtml(title)}</div>${summary ? `<div class="text-[9px] font-mono text-ops-light/50 truncate">${escapeHtml(summary)}</div>` : ''}<div class="text-[8px] font-mono text-ops-light/40">${escapeHtml(ts)}</div></div></div>`;
        }

        return '';
    };

    const actPreview = activityBlocks.slice(0, 3).map(renderActivityBlock).join('');
    const actBody = activityBlocks.length > 3 ? activityBlocks.slice(3, 10).map(renderActivityBlock).join('') : '';
    const actCard = makeCard('activity', 'fa-clock-rotate-left', 'text-sky-400', 'Activity', '', `<div class="space-y-1.5">${actPreview || '<div class="text-[10px] text-ops-light/50">No recent activity.</div>'}</div>`, actBody ? `<div class="space-y-1.5">${actBody}</div>` : '');
    if (pagePrefs.commsRadar) feedRow.appendChild(actCard);

    // Slack (group by channel)
    const parseSlackContext = (item) => {
        const raw = safeText(item?.title || item?.subject || '').trim();
        const channel = (raw.match(/#([a-z0-9_-]{2,})/i) || [])[1] || '';
        const at = (raw.match(/@([a-z0-9_.-]{2,})/i) || [])[1] || '';
        return { channel, at };
    };
    const slackByChannel = {};
    for (const item of slackNew) {
        const ctx = parseSlackContext(item);
        const key = ctx.channel ? `#${ctx.channel}` : 'Slack';
        if (!slackByChannel[key]) slackByChannel[key] = { key, count: 0, sample: item };
        slackByChannel[key].count += 1;
    }
    const slackChannels = Object.values(slackByChannel).sort((a, b) => (b.count - a.count));

    const renderSlackRow = (row) => {
        const item = row.sample;
        const id = safeText(item?.id).trim();
        const ai = id ? aiInboxMap[id] : null;
        const ctx = parseSlackContext(item);
        const who = ctx.at ? `@${ctx.at}` : '';
        let title = safeText(ai?.title || item?.title || item?.subject).trim();
        if (isBadDashText(title)) {
            const full = safeText(item?.content) || safeText(item?.text) || safeText(item?.body) || '';
            title = previewText(full, 90) || 'Slack message';
        }
        const summary = safeText(ai?.summary).trim() || previewText(safeText(item?.content) || safeText(item?.text) || safeText(item?.body) || '', 120);
        const countBadge = row.count > 1 ? `<span class="px-1.5 py-0.5 rounded border border-ops-border text-[8px] font-mono text-ops-light/60">${row.count}</span>` : '';
        return `
            <div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-2">
                <div class="flex items-center justify-between gap-2">
                    <div class="min-w-0 flex-1">
                        <div class="flex items-center gap-2">
                            <div class="text-[11px] text-white truncate">${escapeHtml(row.key)}${who ? ` <span class="text-ops-light/50">${escapeHtml(who)}</span>` : ''}</div>
                        </div>
                    </div>
                    ${countBadge}
                </div>
                <div class="mt-1 text-[10px] text-white truncate">${escapeHtml(title)}</div>
                ${summary ? `<div class="mt-0.5 text-[9px] font-mono text-ops-light/60 truncate">${escapeHtml(summary)}</div>` : ''}
            </div>
        `;
    };

    if (slackNew.length) {
        const slackPreview = slackChannels.slice(0, 2).map(renderSlackRow).join('');
        const slackBody = slackChannels.length > 2
            ? slackChannels.slice(2, 8).map(renderSlackRow).join('')
            : '';
        const slackCard = makeCard('slack', 'fa-slack', 'text-purple-400', 'Slack', `<button type="button" data-open-slack class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open</button>`, `<div class="space-y-1">${slackPreview}</div>`, slackBody ? `<div class="space-y-1">${slackBody}</div>` : '');
        slackCard.querySelector('.dash-card-head i.fa-slack')?.classList.replace('fa-solid', 'fa-brands');
        if (pagePrefs.commsRadar) feedRow.appendChild(slackCard);
    }

    // Inbox (group by assignment)
    const inboxGroupsByKey = {};
    for (const item of dashboardInbox) {
        const id = safeText(item?.id).trim();
        const projectId = safeText(item?.projectId).trim();
        const key = projectId ? `project:${projectId}` : 'unassigned';
        if (!inboxGroupsByKey[key]) {
            inboxGroupsByKey[key] = {
                key,
                projectId,
                label: projectId ? (projectNameById[projectId] || 'Project') : 'Unassigned',
                count: 0,
                sourceCounts: {},
                latestAt: '',
                sample: item,
            };
        }
        const g = inboxGroupsByKey[key];
        g.count += 1;
        const s = normalizeInboxSourceKey(item?.source);
        g.sourceCounts[s] = (g.sourceCounts[s] || 0) + 1;
        const at = safeText(item?.createdAt).trim();
        if (at && (!g.latestAt || at > g.latestAt)) {
            g.latestAt = at;
            g.sample = item;
        }
    }

    const inboxGroups = Object.values(inboxGroupsByKey).sort((a, b) => {
        if (a.key === 'unassigned' && b.key !== 'unassigned') return -1;
        if (b.key === 'unassigned' && a.key !== 'unassigned') return 1;
        return (safeText(b.latestAt) || '').localeCompare(safeText(a.latestAt) || '');
    });

    const renderInboxGroupRow = (g) => {
        const item = g.sample;
        const id = safeText(item?.id).trim();
        const ai = id ? aiInboxMap[id] : null;
        const contactName = resolveInboxContactName(item);
        const projectId = safeText(item?.projectId).trim();

        let title = safeText(ai?.title || item?.title || item?.subject).trim();
        if (isBadDashText(title)) {
            const full = safeText(item?.text) || safeText(item?.content) || safeText(item?.body) || safeText(item?.message) || '';
            title = previewText(full, 90) || 'Inbox item';
        }
        const rawBody = safeText(item?.text) || safeText(item?.content) || safeText(item?.body) || safeText(item?.message) || '';
        const summary = safeText(ai?.summary).trim() || previewText(rawBody, 120);
        const detail = previewText(rawBody, 320);
        const stamp = g.latestAt ? formatClock(g.latestAt) : '';

        const srcBadges = Object.entries(g.sourceCounts)
            .filter(([k, v]) => k && Number(v) > 0)
            .slice(0, 3)
            .map(([k, v]) => {
                const meta = inboxSourceMeta(k);
                const label = safeText(meta?.label).trim() || k.toUpperCase();
                return `<span class="px-1.5 py-0.5 rounded border border-ops-border text-[8px] font-mono text-ops-light/50">${escapeHtml(label)} ${Number(v)}</span>`;
            })
            .join('');

        const tone = g.key === 'unassigned' ? 'border-red-500/30 bg-red-500/10' : 'border-ops-border bg-ops-bg/40';
        const countTone = g.key === 'unassigned' ? 'text-red-300' : 'text-white';

        return `
            <div class="border rounded px-2.5 py-2 ${tone} cursor-pointer select-none" data-inbox-radar-row data-inbox-id="${escapeHtml(id)}" data-project-id="${escapeHtml(projectId)}">
                <div class="flex items-start justify-between gap-2">
                    <div class="min-w-0 flex-1">
                        <div class="flex items-center justify-between gap-2">
                            <div class="min-w-0">
                                <div class="text-[11px] ${g.key === 'unassigned' ? 'text-red-200 font-semibold' : 'text-white'} truncate">${escapeHtml(g.label)}</div>
                                ${contactName ? `<div class="mt-0.5 text-[9px] font-mono text-ops-light/60 truncate">From: ${escapeHtml(contactName)}</div>` : ''}
                                <div class="mt-0.5 text-[10px] text-white truncate">${escapeHtml(title)}</div>
                            </div>
                            <div class="shrink-0 text-[11px] font-mono font-semibold ${countTone}">${g.count}</div>
                        </div>
                        <div class="mt-1 flex items-center gap-1.5 flex-wrap">
                            ${srcBadges}
                            ${stamp ? `<span class="text-[8px] font-mono text-ops-light/40">${escapeHtml(stamp)}</span>` : ''}
                            <span class="text-[8px] font-mono text-ops-light/30">Click to expand</span>
                        </div>
                        ${summary ? `<div class="mt-1 text-[9px] font-mono text-ops-light/60 truncate">${escapeHtml(summary)}</div>` : ''}
                        <div class="mt-2 hidden" data-inbox-radar-details>
                            ${detail ? `<div class="text-[10px] font-mono text-ops-light/70 leading-relaxed">${escapeHtml(detail)}</div>` : ''}
                            <div class="mt-2 flex items-center gap-2 flex-wrap">
                                <button type="button" data-inbox-radar-open class="px-2 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open Inbox</button>
                                ${projectId ? `<button type="button" data-inbox-radar-open-project class="px-2 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open Project</button>` : ''}
                                ${id ? `<button type="button" data-inbox-radar-triage class="px-2 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Triage</button>` : ''}
                                ${id ? `<button type="button" data-inbox-radar-done class="px-2 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Done</button>` : ''}
                                ${id ? `<button type="button" data-inbox-radar-archive class="px-2 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Archive</button>` : ''}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    };

    if (inboxGroups.length) {
        const inboxPreview = inboxGroups.slice(0, 2).map(renderInboxGroupRow).join('');
        const inboxBody = inboxGroups.length > 2
            ? inboxGroups.slice(2, 10).map(renderInboxGroupRow).join('')
            : '';
        const inboxCard = makeCard('inbox', 'fa-inbox', 'text-amber-400', 'Inbox', `<button type="button" data-open-inbox2 class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open</button>`, `<div class="space-y-1">${inboxPreview}</div>`, inboxBody ? `<div class="space-y-1">${inboxBody}</div>` : '');
        if (pagePrefs.commsRadar) feedRow.appendChild(inboxCard);
    }

    // Team (presence + WIP/overdue signals)
    const humanMembers = teamMembers.filter(m => safeText(m?.role).toLowerCase() !== 'ai');
    const openByOwner = getOpenTaskCountByOwner();
    const overdueByOwner = {};
    for (const t of allTasks) {
        if (isDoneTask(t)) continue;
        const owner = safeText(t?.owner).trim();
        if (!owner) continue;
        const d = safeText(t?.dueDate).trim();
        if (d && d < today) overdueByOwner[owner] = (overdueByOwner[owner] || 0) + 1;
    }

    const teamRows = humanMembers.map((m) => {
        const name = safeText(m?.name).trim() || 'Member';
        const role = safeText(m?.role).trim();
        const memberId = safeText(m?.id).trim();
        const pres = memberId && state.teamPresenceByMemberId?.[memberId];
        const online = pres && Object.prototype.hasOwnProperty.call(pres, 'online') ? pres.online : null;
        const dot = online === true
            ? '<span class="w-1.5 h-1.5 rounded-full bg-emerald-400 shrink-0"></span>'
            : (online === false
                ? '<span class="w-1.5 h-1.5 rounded-full bg-zinc-600 shrink-0"></span>'
                : '<span class="w-1.5 h-1.5 rounded-full bg-amber-300 shrink-0"></span>');

        const openCount = Number(openByOwner[name]) || 0;
        const overdueCount = Number(overdueByOwner[name]) || 0;
        const wipLimit = getWipLimitForOwner(name);
        const overLimit = Number.isFinite(wipLimit) && openCount > wipLimit;

        const wipTone = overLimit ? 'border-red-500/30 bg-red-500/10 text-red-200' : 'border-ops-border bg-ops-bg/40 text-ops-light/70';
        const overdueTone = overdueCount > 0 ? 'border-amber-500/30 bg-amber-500/10 text-amber-200' : 'border-ops-border bg-ops-bg/40 text-ops-light/50';
        const wipText = Number.isFinite(wipLimit) ? `WIP ${openCount}/${wipLimit}` : `WIP ${openCount}`;

        const score = (overLimit ? 1000 : 0) + (overdueCount * 10) + openCount;

        return {
            score,
            html: `
                <div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-2">
                    <div class="flex items-start justify-between gap-2">
                        <div class="min-w-0 flex-1">
                            <div class="flex items-center gap-2">
                                ${dot}
                                <div class="min-w-0">
                                    <div class="text-[11px] text-white truncate">${escapeHtml(name)}</div>
                                    ${role ? `<div class="text-[9px] font-mono text-ops-light/50 truncate">${escapeHtml(role)}</div>` : ''}
                                </div>
                            </div>
                            <div class="mt-2 flex items-center gap-1.5 flex-wrap">
                                <span class="px-1.5 py-0.5 rounded border text-[8px] font-mono ${wipTone}">${escapeHtml(wipText)}</span>
                                <span class="px-1.5 py-0.5 rounded border text-[8px] font-mono ${overdueTone}">${overdueCount ? `${overdueCount} overdue` : 'No overdue'}</span>
                            </div>
                        </div>
                    </div>
                </div>
            `,
        };
    }).sort((a, b) => b.score - a.score);

    const teamPreview = teamRows.length
        ? teamRows.slice(0, 3).map((r) => r.html).join('')
        : '<div class="text-[10px] text-ops-light/50">No team members.</div>';
    const teamBody = teamRows.length > 3
        ? teamRows.slice(3, 10).map((r) => r.html).join('')
        : '';
    const teamCard = makeCard('team', 'fa-users', 'text-emerald-400', 'Team', `<button type="button" data-open-team class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open</button>`, `<div class="space-y-1">${teamPreview}</div>`, teamBody ? `<div class="space-y-1">${teamBody}</div>` : '');
    if (pagePrefs.commsRadar) feedRow.appendChild(teamCard);
    if (!actionOnlyMode && pagePrefs.commsRadar) wrap.appendChild(feedRow);

    // ═══ LATER ROW: Next Week + Future Projects ═════════════════════
    const nextWeekItems = Array.isArray(buckets.nextWeek) ? buckets.nextWeek : [];
    const laterRow = document.createElement('div');
    laterRow.className = 'grid grid-cols-1 sm:grid-cols-2 gap-2';

    const nwPreview = nextWeekItems.length ? `<div class="space-y-1">${nextWeekItems.slice(0,2).map(p=>mkProjBtn(p,'text-ops-light/60')).join('')}</div>` : '<div class="text-[10px] text-ops-light/50">Nothing due next week.</div>';
    const nwBody = nextWeekItems.length > 2 ? `<div class="space-y-1">${nextWeekItems.slice(2).map(p=>mkProjBtn(p,'text-ops-light/60')).join('')}</div>` : '';
    const nwCard = makeCard('next-week', 'fa-calendar-check', 'text-sky-400', 'Next Week', `<span class="text-sm font-semibold text-white">${nextWeekItems.length}</span>`, nwPreview, nwBody);
    if (pagePrefs.deliveryBoard) laterRow.appendChild(nwCard);

    const upcoming = (Array.isArray(buckets.upcoming)?buckets.upcoming:[]).slice(0,6);
    const upPreview = upcoming.length ? `<div class="space-y-1">${upcoming.slice(0,2).map(p=>mkProjBtn(p,'text-ops-light/60')).join('')}</div>` : '<div class="text-[10px] text-ops-light/50">No future projects.</div>';
    const upBody = upcoming.length > 2 ? `<div class="space-y-1">${upcoming.slice(2).map(p=>mkProjBtn(p,'text-ops-light/60')).join('')}</div>` : '';
    const upCard = makeCard('upcoming', 'fa-forward', 'text-ops-light/40', 'Future Projects', `<button type="button" data-open-projects class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">All</button>`, upPreview, upBody);
    if (pagePrefs.deliveryBoard) laterRow.appendChild(upCard);
    if (!actionOnlyMode && pagePrefs.deliveryBoard) wrap.appendChild(laterRow);

    // ═══ KEYBOARD SHORTCUTS (hidden) ════════════════════════════════
    const shortcutsPanel = document.createElement('div');
    shortcutsPanel.id = 'dash-shortcuts-panel';
    shortcutsPanel.className = 'hidden dash-card p-3';
    shortcutsPanel.innerHTML = `
        <div class="flex items-center justify-between gap-2 mb-2">
            <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light flex items-center gap-2"><i class="fa-solid fa-keyboard text-ops-light/50"></i> Shortcuts</span>
            <button id="dash-shortcuts-close" class="text-ops-light hover:text-white text-xs"><i class="fa-solid fa-xmark"></i></button>
        </div>
        <div class="grid grid-cols-3 gap-x-4 gap-y-0.5 text-[10px]">
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">Dashboard</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">G D</kbd></div>
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">Inbox</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">G I</kbd></div>
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">Projects</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">G P</kbd></div>
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">Calendar</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">G C</kbd></div>
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">Team</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">G T</kbd></div>
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">Settings</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">G S</kbd></div>
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">New item</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">N</kbd></div>
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">Command</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">\u2318K</kbd></div>
            <div class="flex justify-between gap-1"><span class="text-ops-light/60">Sync</span><kbd class="font-mono text-white bg-ops-bg/60 px-1 rounded text-[9px]">R</kbd></div>
        </div>
    `;
    if (pagePrefs.missionControl) wrap.appendChild(shortcutsPanel);

    // ═══ MOUNT ═══════════════════════════════════════════════════════
    container.appendChild(wrap);

    // ═══ EVENT WIRING ════════════════════════════════════════════════
    // Expandable cards
    wrap.querySelectorAll('.dash-card').forEach(card => {
        const head = card.querySelector('.dash-card-head');
        const body = card.querySelector('.dash-card-body');
        if (head && body) {
            head.addEventListener('click', (e) => {
                if (e.target.closest('button') || e.target.closest('a')) return;
                card.classList.toggle('expanded');
            });
        }
    });

    // Nav buttons
    wrap.querySelectorAll('button[data-open-inbox]').forEach(b => b.addEventListener('click', () => openInbox()));
    wrap.querySelectorAll('button[data-action-open-inbox]').forEach((b) => {
        b.addEventListener('click', (e) => {
            e.preventDefault();
            openInbox();
        });
    });
    wrap.querySelectorAll('button[data-action-done-inbox]').forEach((b) => {
        b.addEventListener('click', async (e) => {
            e.preventDefault();
            const id = safeText(b.getAttribute('data-action-done-inbox')).trim();
            if (!id) return;
            b.disabled = true;
            try {
                await patchInboxItem(id, { status: 'Done' });
                await fetchState().catch(() => {});
                if (state.currentView === 'dashboard') renderMain();
            } catch (err) {
                alert(err?.message || 'Failed to mark done');
            } finally {
                b.disabled = false;
            }
        });
    });
    wrap.querySelectorAll('button[data-run-marcus-filter]').forEach((b) => b.addEventListener('click', async () => {
        b.disabled = true;
        const prev = b.textContent;
        b.textContent = 'Filtering…';
        try {
            const result = await runMarcusInboxFilter();
            alert(`Marcus filter complete. Archived: ${Number(result?.archived || 0)}. Matched: ${Number(result?.matched || 0)}.`);
            renderMain();
        } catch (e) {
            alert(e?.message || 'Marcus filter failed');
        } finally {
            b.disabled = false;
            b.textContent = prev;
        }
    }));
    wrap.querySelector('button[data-open-inbox2]')?.addEventListener('click', () => openInbox());
    wrap.querySelectorAll('button[data-open-slack]').forEach(b => b.addEventListener('click', () => openInbox()));
    wrap.querySelector('button[data-open-calendar]')?.addEventListener('click', () => openCalendar());
    wrap.querySelectorAll('button[data-open-team]').forEach(b => b.addEventListener('click', () => openTeam()));
    wrap.querySelectorAll('button[data-open-projects]').forEach(b => b.addEventListener('click', () => openProjects()));
    calCard.querySelector('button[data-refresh-calls]')?.addEventListener('click', async () => { await refreshDashboardCalls({force:true}); renderMain(); });
    wrap.querySelectorAll('.dash-project-btn').forEach(btn => { btn.addEventListener('click', () => { const pid = btn.dataset.pid; if (pid) openProject(pid); }); });

    // Inbox Radar: expand rows + fast actions
    wrap.querySelectorAll('[data-inbox-radar-row]').forEach((row) => {
        row.addEventListener('click', (e) => {
            if (e.target.closest('button') || e.target.closest('a')) return;
            const details = row.querySelector('[data-inbox-radar-details]');
            if (!details) return;
            details.classList.toggle('hidden');
        });
    });

    wrap.querySelectorAll('button[data-inbox-radar-open]').forEach((b) => {
        b.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            openInbox();
        });
    });
    wrap.querySelectorAll('button[data-inbox-radar-open-project]').forEach((b) => {
        b.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            const row = b.closest('[data-inbox-radar-row]');
            const pid = safeText(row?.getAttribute('data-project-id')).trim();
            if (pid) openProject(pid);
        });
    });
    wrap.querySelectorAll('button[data-inbox-radar-triage]').forEach((b) => {
        b.addEventListener('click', async (e) => {
            e.preventDefault();
            e.stopPropagation();
            const row = b.closest('[data-inbox-radar-row]');
            const id = safeText(row?.getAttribute('data-inbox-id')).trim();
            if (!id) return;
            b.disabled = true;
            try {
                await patchInboxItem(id, { status: 'Triaged' });
                await fetchState().catch(() => {});
                if (state.currentView === 'dashboard') renderMain();
            } catch (err) {
                alert(err?.message || 'Failed to triage');
            } finally {
                b.disabled = false;
            }
        });
    });
    wrap.querySelectorAll('button[data-inbox-radar-done]').forEach((b) => {
        b.addEventListener('click', async (e) => {
            e.preventDefault();
            e.stopPropagation();
            const row = b.closest('[data-inbox-radar-row]');
            const id = safeText(row?.getAttribute('data-inbox-id')).trim();
            if (!id) return;
            b.disabled = true;
            try {
                await patchInboxItem(id, { status: 'Done' });
                await fetchState().catch(() => {});
                if (state.currentView === 'dashboard') renderMain();
            } catch (err) {
                alert(err?.message || 'Failed to mark done');
            } finally {
                b.disabled = false;
            }
        });
    });
    wrap.querySelectorAll('button[data-inbox-radar-archive]').forEach((b) => {
        b.addEventListener('click', async (e) => {
            e.preventDefault();
            e.stopPropagation();
            const row = b.closest('[data-inbox-radar-row]');
            const id = safeText(row?.getAttribute('data-inbox-id')).trim();
            if (!id) return;
            b.disabled = true;
            try {
                await patchInboxItem(id, { status: 'Archived' });
                await fetchState().catch(() => {});
                if (state.currentView === 'dashboard') renderMain();
            } catch (err) {
                alert(err?.message || 'Failed to archive');
            } finally {
                b.disabled = false;
            }
        });
    });

    // Quick-add
    const quickInput = wrap.querySelector('#dash-quick-input');
    if (quickInput) { quickInput.addEventListener('keydown', async (e) => { if (e.key === 'Enter') { const val = safeText(quickInput.value).trim(); if (!val) return; quickInput.value = ''; try { await apiFetch('/api/inbox', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({title:val,source:'quick-add',status:'new'})}); await fetchState(); renderMain(); } catch(err) { alert(err?.message||'Failed'); } } }); }
    wrap.querySelector('#dash-quick-project')?.addEventListener('click', () => createNewProjectPrompt());

    // Focus timer
    const timerToggle = wrap.querySelector('#dash-timer-toggle');
    const timerReset = wrap.querySelector('#dash-timer-reset');
    if (timerToggle) { timerToggle.addEventListener('click', () => { if (state.focusTimer.running) { clearInterval(state.focusTimer.intervalId); state.focusTimer.running = false; state.focusTimer.intervalId = null; } else { state.focusTimer.running = true; state.focusTimer.intervalId = setInterval(() => { if (state.focusTimer.remaining <= 0) { clearInterval(state.focusTimer.intervalId); state.focusTimer.running = false; state.focusTimer.intervalId = null; try { new Audio('data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1fdHmBgYF9eXl+gYaGg36Af4F/fn5+').play(); } catch(ignored) {} alert('Focus session complete!'); if (state.currentView==='dashboard') renderMain(); return; } state.focusTimer.remaining--; const disp = document.getElementById('dash-timer-display'); if (disp) { const mm = Math.floor(state.focusTimer.remaining/60); const ss = state.focusTimer.remaining%60; disp.textContent = `${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`; } }, 1000); } if (state.currentView==='dashboard') renderMain(); }); }
    if (timerReset) { timerReset.addEventListener('click', () => { clearInterval(state.focusTimer.intervalId); state.focusTimer.running = false; state.focusTimer.intervalId = null; state.focusTimer.remaining = state.focusTimer.duration; if (state.currentView==='dashboard') renderMain(); }); }

    // M.A.R.C.U.S. buttons
    wrap.querySelector('#dash-ask-marcus')?.addEventListener('click', () => {
        const inp = document.getElementById('cmd-input');
        if (inp) {
            inp.focus();
            inp.value = 'What should I focus on right now?';
        }
        speakMarcus('I am ready. Ask me for your next best action.');
    });
    wrap.querySelector('#dash-brief-marcus')?.addEventListener('click', () => {
        const inp = document.getElementById('cmd-input');
        if (inp) {
            inp.focus();
            inp.value = 'Give me a brief status update on everything.';
        }
        const briefLine = marcusInsights.slice(0, 2).map((x) => safeText(x?.text)).filter(Boolean).join(' ');
        if (briefLine) speakMarcus(briefLine);
    });
    wrap.querySelector('#dash-marcus-sweep')?.addEventListener('click', async (e) => {
        const btn = e.currentTarget;
        if (!btn) return;
        btn.disabled = true;
        const prev = btn.textContent;
        btn.textContent = 'Sweeping...';
        try {
            const filtered = await runMarcusInboxFilter();
            const triage = await runMarcusInboxTriage({ onlyNew: true, includeArchived: false, limit: 120 });
            const auto = await runMarcusInboxAutomation();
            await fetchMarcusAutomationDigest().catch(() => {});
            const summary = `Sweep complete. Archived ${Number(filtered?.archived || 0)} noise items, triaged ${Number(triage?.count || 0)}, queued ${Number(auto?.proposed || 0)} approvals.`;
            speakMarcus(summary);
            alert(summary);
            if (state.currentView === 'dashboard') renderMain();
        } catch (err) {
            alert(err?.message || 'Marcus sweep failed');
        } finally {
            btn.disabled = false;
            btn.textContent = prev;
        }
    });
    wrap.querySelector('#dash-marcus-coach')?.addEventListener('click', async (e) => {
        const btn = e.currentTarget;
        if (!btn) return;
        btn.disabled = true;
        const prev = btn.textContent;
        btn.textContent = 'Coaching...';
        try {
            await openInbox();
            const result = await coachNextInboxStep();
            if (result?.applied) {
                const msg = `Coaching applied. Linked: ${result.linked ? 'yes' : 'no'}. Tasks created: ${Number(result.tasksCreated || 0)}.`;
                speakMarcus(msg);
                alert(msg);
            }
        } catch (err) {
            alert(err?.message || 'Marcus coach failed');
        } finally {
            btn.disabled = false;
            btn.textContent = prev;
        }
    });

    // Shortcuts
    const shortcutsBtn = wrap.querySelector('#dash-shortcuts-btn');
    const actionOnlyBtn = wrap.querySelector('#dash-action-only-toggle');
    const shortcutsPanelEl = wrap.querySelector('#dash-shortcuts-panel');
    const shortcutsClose = wrap.querySelector('#dash-shortcuts-close');
    if (actionOnlyBtn) {
        actionOnlyBtn.addEventListener('click', async () => {
            const next = !Boolean(state.settings?.dashboardActionOnly === true);
            state.settings = (state.settings && typeof state.settings === 'object') ? state.settings : {};
            state.settings.dashboardActionOnly = next;
            renderMain();
            try {
                await saveSettingsPatch({ dashboardActionOnly: next });
            } catch (err) {
                state.settings.dashboardActionOnly = !next;
                renderMain();
                alert(err?.message || 'Failed to save Action Only mode');
            }
        });
    }
    if (shortcutsBtn && shortcutsPanelEl) shortcutsBtn.addEventListener('click', () => shortcutsPanelEl.classList.toggle('hidden'));
    if (shortcutsClose && shortcutsPanelEl) shortcutsClose.addEventListener('click', () => shortcutsPanelEl.classList.add('hidden'));
}

function renderDashboardCommandCenter(container, sidePort) {
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Dashboard';

    const inboxItems = getDisplayInboxItems();
    const inboxNew = inboxItems.filter((x) => String(x?.status || '').trim().toLowerCase() === 'new');

    const activeProjects = getActiveProjects();
    const buckets = bucketProjectsByDueDate(activeProjects);
    const dueTodayItems = Array.isArray(buckets.today) ? buckets.today : [];
    const dueWeekItems = [...(Array.isArray(buckets.tomorrow) ? buckets.tomorrow : []), ...(Array.isArray(buckets.thisWeek) ? buckets.thisWeek : [])];

    const allTasks = Array.isArray(state.tasks) ? state.tasks : [];
    const today = ymdToday();
    const overdueTasks = allTasks.filter((t) => { if (isDoneTask(t)) return false; const d = safeText(t?.dueDate).trim(); return d && d < today; });
    const overdueProjects = activeProjects.filter((p) => { const d = safeText(p?.dueDate).trim(); return d && d < today; });
    const totalOverdue = overdueTasks.length + overdueProjects.length;

    const nextActions = getTodayNextActions();
    const topAction = nextActions[0] || null;

    const callsConnected = !!state.settings?.googleConnected;
    const calls = Array.isArray(state.dashboardCalls?.events) ? state.dashboardCalls.events : [];
    const callsError = safeText(state.dashboardCalls?.error);
    const callsLoading = !!state.dashboardCalls?.loading;
    if (callsConnected) setTimeout(() => refreshDashboardCalls({ force: false }), 0);

    const hour = new Date().getHours();
    const greeting = hour < 12 ? 'Good morning' : hour < 17 ? 'Good afternoon' : 'Good evening';
    const teamMembers = Array.isArray(state.team) ? state.team : [];
    const userName = safeText(teamMembers.find((m) => safeText(m?.role).toLowerCase() === 'admin')?.name) || 'Operator';
    const dateStr = new Date().toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });

    container.innerHTML = '';
    sidePort.innerHTML = '';

    const root = document.createElement('div');
    root.className = 'h-full min-h-0 p-4';
    root.style.display = 'grid';
    root.style.gridTemplateRows = 'auto 1fr auto';
    root.style.gap = '0.75rem';
    container.appendChild(root);

    // Top row: Welcome + Urgent
    const topRow = document.createElement('div');
    topRow.style.display = 'grid';
    topRow.style.gridTemplateColumns = '1fr 1fr';
    topRow.style.gap = '0.75rem';
    root.appendChild(topRow);

    const welcome = document.createElement('div');
    welcome.className = 'dash-card';
    welcome.dataset.cardId = 'welcome';
    welcome.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2 min-w-0">
                <i class="fa-solid fa-user-shield text-ops-accent text-[10px] shrink-0"></i>
                <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light truncate">Welcome Operator</span>
            </div>
            <div class="text-[10px] font-mono text-ops-light/50">${escapeHtml(dateStr)}</div>
        </div>
        <div class="px-3 pb-3">
            <div class="text-sm text-white font-semibold">${escapeHtml(greeting)}, ${escapeHtml(userName)}</div>
            <div class="mt-2 flex flex-wrap items-center gap-1.5 text-[10px] font-mono text-ops-light/60">
                ${totalOverdue ? `<span class="stat-pill" style="border-color:rgba(239,68,68,0.3);color:#fca5a5"><i class="fa-solid fa-triangle-exclamation text-[8px]"></i>${totalOverdue} overdue</span>` : ''}
                <span class="stat-pill"><i class="fa-solid fa-inbox text-[8px] text-purple-400"></i>${inboxNew.length} inbox new</span>
                <span class="stat-pill"><i class="fa-solid fa-calendar text-[8px] text-blue-400"></i>${calls.length} meetings</span>
            </div>
        </div>
    `;
    topRow.appendChild(welcome);

    const urgent = document.createElement('div');
    urgent.className = 'dash-card';
    urgent.dataset.cardId = 'urgent';

    const urgentLines = [];
    if (totalOverdue) urgentLines.push(`⚠️ ${totalOverdue} overdue item${totalOverdue === 1 ? '' : 's'} — triage first.`);
    if (topAction) urgentLines.push(`🎯 Next action: “${safeText(topAction?.title)}”`);
    if (inboxNew.length) urgentLines.push(`📡 ${inboxNew.length} new inbox item${inboxNew.length === 1 ? '' : 's'} — link or no-home.`);
    if (!urgentLines.length) urgentLines.push('All clear. Keep momentum and stay ahead of due dates.');

    urgent.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2 min-w-0">
                <i class="fa-solid fa-bell text-amber-400 text-[10px] shrink-0"></i>
                <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light truncate">Urgent Notifications</span>
            </div>
            <button type="button" data-open-inbox class="px-2.5 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Open Inbox</button>
        </div>
        <div class="px-3 pb-3">
            <div class="space-y-1">
                ${urgentLines.slice(0, 4).map((t) => `<div class="text-[11px] text-ops-light/80">${escapeHtml(t)}</div>`).join('')}
            </div>
        </div>
    `;
    topRow.appendChild(urgent);

    // Middle: M.A.R.C.U.S.
    const marcusCard = document.createElement('div');
    marcusCard.className = 'dash-card min-h-0 overflow-hidden';
    marcusCard.dataset.cardId = 'marcus-center';
    marcusCard.style.display = 'flex';
    marcusCard.style.flexDirection = 'column';
    marcusCard.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2 min-w-0">
                <div class="marcus-status-dot shrink-0"></div>
                <div class="marcus-orb marcus-dashboard-avatar idle shrink-0" aria-hidden="true"></div>
                <span class="text-[10px] font-mono uppercase tracking-widest text-blue-300">M.A.R.C.U.S.</span>
            </div>
            <div class="text-[9px] font-mono text-ops-light/50">Dashboard Console</div>
        </div>
        <div id="dash-marcus-slot" class="flex-1 min-h-0 overflow-hidden"></div>
    `;
    root.appendChild(marcusCard);

    const marcusSlot = marcusCard.querySelector('#dash-marcus-slot');
    if (marcusSlot) dockMarcusToDashboardSlot(marcusSlot);

    // Bottom row: Due Today | This Week | Pending Meetings
    const bottomRow = document.createElement('div');
    bottomRow.style.display = 'grid';
    bottomRow.style.gridTemplateColumns = '1fr 1fr 1fr';
    bottomRow.style.gap = '0.75rem';
    root.appendChild(bottomRow);

    const mkMiniList = (rows, emptyText) => {
        const list = Array.isArray(rows) ? rows : [];
        if (!list.length) return `<div class="text-[10px] text-ops-light/50">${escapeHtml(emptyText || 'None')}</div>`;
        return `<div class="space-y-1">${list.slice(0, 3).map((r) => `<div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><div class="text-[11px] text-white truncate">${escapeHtml(safeText(r?.name) || safeText(r?.title) || 'Untitled')}</div>${r?.dueDate ? `<div class="text-[9px] font-mono text-ops-light/50">${escapeHtml(safeText(r?.dueDate))}</div>` : ''}</div>`).join('')}</div>`;
    };

    const dueTodayCard = document.createElement('div');
    dueTodayCard.className = 'dash-card';
    dueTodayCard.dataset.cardId = 'due-today-mini';
    dueTodayCard.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2 min-w-0">
                <i class="fa-solid fa-fire text-red-400 text-[10px] shrink-0"></i>
                <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light truncate">Due Today</span>
            </div>
            <div class="text-[11px] font-mono text-white">${dueTodayItems.length}</div>
        </div>
        <div class="px-3 pb-3">${mkMiniList(dueTodayItems, 'Nothing due today.')}</div>
    `;
    bottomRow.appendChild(dueTodayCard);

    const dueWeekCard = document.createElement('div');
    dueWeekCard.className = 'dash-card';
    dueWeekCard.dataset.cardId = 'due-week-mini';
    dueWeekCard.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2 min-w-0">
                <i class="fa-solid fa-calendar-week text-amber-400 text-[10px] shrink-0"></i>
                <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light truncate">Due This Week</span>
            </div>
            <div class="text-[11px] font-mono text-white">${dueWeekItems.length}</div>
        </div>
        <div class="px-3 pb-3">${mkMiniList(dueWeekItems, 'Nothing else due this week.')}</div>
    `;
    bottomRow.appendChild(dueWeekCard);

    const meetingsCard = document.createElement('div');
    meetingsCard.className = 'dash-card';
    meetingsCard.dataset.cardId = 'meetings-mini';
    const meetingsBody = !callsConnected
        ? `<div class="text-[10px] text-ops-light/50">Connect Google Calendar in Settings.</div>`
        : (callsError
            ? `<div class="text-[10px] text-amber-300">${escapeHtml(callsError)}</div>`
            : (callsLoading && !calls.length)
                ? `<div class="text-[10px] text-ops-light/50">Loading…</div>`
                : (calls.length
                    ? `<div class="space-y-1">${calls.slice(0, 3).map((ev) => {
                        const time = formatTimeFromIso(ev.start);
                        const title = safeText(ev.summary) || 'Untitled';
                        return `<div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><div class="text-[11px] text-white truncate">${escapeHtml(title)}</div><div class="text-[9px] font-mono text-ops-light/50">${escapeHtml(time || '')}</div></div>`;
                    }).join('')}</div>`
                    : `<div class="text-[10px] text-ops-light/50">No meetings scheduled.</div>`));
    meetingsCard.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2 min-w-0">
                <i class="fa-solid fa-video text-blue-400 text-[10px] shrink-0"></i>
                <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light truncate">Pending Meetings</span>
            </div>
            <button type="button" data-open-calendar class="px-2.5 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Open</button>
        </div>
        <div class="px-3 pb-3">${meetingsBody}</div>
    `;
    bottomRow.appendChild(meetingsCard);

    // Right column: Inbox Radar + Calendar
    const sideWrap = document.createElement('div');
    sideWrap.className = 'h-full min-h-0 p-4 space-y-3';
    sidePort.appendChild(sideWrap);

    const radarBanner = document.createElement('div');
    radarBanner.className = 'dash-card';
    radarBanner.dataset.cardId = 'radar-side';

    const formatInboxStamp = (iso) => {
        const s = safeText(iso).trim();
        if (!s) return '';
        const d = new Date(s);
        if (Number.isNaN(d.getTime())) return '';
        return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    };

    const makeRadarHtml = (payloadOrItems) => {
        const items = Array.isArray(payloadOrItems?.items)
            ? payloadOrItems.items
            : (Array.isArray(payloadOrItems) ? payloadOrItems : []);
        const groups = Array.isArray(payloadOrItems?.groups) ? payloadOrItems.groups : null;
        const businessGroups = Array.isArray(payloadOrItems?.businessGroups) ? payloadOrItems.businessGroups : null;

        const list = Array.isArray(items) ? items : [];
        const slack = list.filter((x) => normalizeInboxSourceKey(x?.source) === 'slack');
        const email = list.filter((x) => normalizeInboxSourceKey(x?.source) === 'email');
        const other = list.filter((x) => { const k = normalizeInboxSourceKey(x?.source); return k !== 'slack' && k !== 'email'; });

        const renderGroupRow = (row, { showBusiness = true } = {}) => {
            const business = safeText(row?.businessLabel) || safeText(row?.businessKey) || 'Business';
            const stamp = formatInboxStamp(safeText(row?.latestAt));
            const count = Number(row?.count) || 0;
            const isUnassigned = Boolean(row?.isUnassigned) || (!safeText(row?.projectId).trim());
            const title = isUnassigned ? 'Unassigned' : (safeText(row?.projectName).trim() || 'Project');
            const summary = safeText(row?.summary).trim() || (Array.isArray(row?.sample) ? safeText(row.sample[0]) : '');
            const borderTone = isUnassigned ? 'border-red-500/30 bg-red-500/10' : 'border-ops-border bg-ops-bg/40';
            const countTone = isUnassigned ? 'text-red-300' : 'text-white';
            const businessLine = showBusiness ? `${escapeHtml(business)}${stamp ? ` • ${escapeHtml(stamp)}` : ''}` : (stamp ? escapeHtml(stamp) : '');
            return `
                <div class="border rounded px-2.5 py-2 ${borderTone}">
                    <div class="min-w-0">
                        <div class="flex items-center justify-between gap-2">
                            <div class="min-w-0">
                                ${businessLine ? `<div class=\"text-[10px] font-mono text-ops-light/50 truncate\">${businessLine}</div>` : ''}
                                <div class="mt-0.5 text-[11px] ${isUnassigned ? 'text-red-200 font-semibold' : 'text-white'} truncate">${escapeHtml(title)}</div>
                            </div>
                            <div class="shrink-0 text-[11px] font-mono font-semibold ${countTone}">${count}</div>
                        </div>
                        ${summary ? `<div class="mt-0.5 text-[10px] text-ops-light/60 truncate">${escapeHtml(summary)}</div>` : ''}
                    </div>
                </div>
            `;
        };

        const renderBusinessRow = (row) => {
            const business = safeText(row?.businessLabel) || safeText(row?.businessKey) || 'Business';
            const stamp = formatInboxStamp(safeText(row?.latestAt));
            const count = Number(row?.count) || 0;
            const summary = safeText(row?.summary).trim() || (Array.isArray(row?.sample) ? safeText(row.sample[0]) : '');
            return `
                <div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-2">
                    <div class="flex items-start justify-between gap-2">
                        <div class="min-w-0 flex-1">
                            <div class="flex items-center gap-1.5 flex-wrap">
                                <span class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light/50">${escapeHtml(business)}</span>
                                ${stamp ? `<span class=\"text-[9px] font-mono text-ops-light/40\">${escapeHtml(stamp)}</span>` : ''}
                            </div>
                            <div class="mt-1 flex items-center justify-between gap-2">
                                <div class="min-w-0 text-[11px] text-white truncate">Messages</div>
                                <div class="shrink-0 text-[11px] font-mono font-semibold text-white">${count}</div>
                            </div>
                            ${summary ? `<div class=\"mt-0.5 text-[10px] text-ops-light/60 truncate\">${escapeHtml(summary)}</div>` : ''}
                        </div>
                    </div>
                </div>
            `;
        };

        const rowsHtml = Array.isArray(businessGroups)
            ? (() => {
                const shown = businessGroups.slice(0, 6);
                const more = businessGroups.length - shown.length;
                return `${shown.map(renderBusinessRow).join('')}${more > 0 ? `<div class=\"text-[9px] font-mono text-ops-light/40 px-1\">+${more} more businesses</div>` : ''}`;
            })()
            : Array.isArray(groups)
                ? (() => {
                    const sections = [];
                    const byKey = new Map();
                    for (const g of groups) {
                        const key = safeText(g?.businessKey) || safeText(g?.businessLabel) || 'business';
                        if (!byKey.has(key)) {
                            const label = safeText(g?.businessLabel) || safeText(g?.businessKey) || 'Business';
                            const sec = { key, label, rows: [] };
                            byKey.set(key, sec);
                            sections.push(sec);
                        }
                        byKey.get(key).rows.push(g);
                    }

                    return sections
                        .map((sec) => {
                            const total = sec.rows.reduce((sum, r) => sum + (Number(r?.count) || 0), 0);
                            const shown = sec.rows.slice(0, 6);
                            const more = sec.rows.length - shown.length;
                            return `
                                <div class="border border-ops-border rounded-lg bg-ops-bg/20 px-2.5 py-2">
                                    <div class="flex items-center justify-between gap-2 mb-1">
                                        <div class="text-[9px] font-mono uppercase tracking-widest text-ops-light/60 truncate">${escapeHtml(sec.label)}</div>
                                        <div class="text-[10px] font-mono text-white">${total}</div>
                                    </div>
                                    <div class="space-y-1">
                                        ${shown.map((r) => renderGroupRow(r, { showBusiness: false })).join('')}
                                        ${more > 0 ? `<div class=\"text-[9px] font-mono text-ops-light/40 px-1\">+${more} more</div>` : ''}
                                    </div>
                                </div>
                            `;
                        })
                        .join('');
                })()
                : list
                    .slice(0, 6)
                    .map((row) => {
                const item = row;
                const status = safeText(item?.status).trim() || 'New';
                const sourceKey = normalizeInboxSourceKey(item?.source);
                const meta = inboxSourceMeta(sourceKey);
                const iconCls = meta.icon === 'fa-slack' ? 'fa-brands fa-slack' : `fa-solid ${meta.icon}`;
                const business = inboxBusinessLabel(item);
                const stamp = formatInboxStamp(safeText(item?.updatedAt) || safeText(item?.createdAt));
                const fullText = safeText(item?.text) || safeText(item?.content) || safeText(item?.body) || safeText(item?.message) || '';
                const snippet = previewText(fullText, 120);
                const explicitTitle = safeText(item?.title) || safeText(item?.subject) || '';
                const titleLine = explicitTitle.trim() || snippet || 'Inbox item';
                return `
                    <div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-2">
                        <div class="flex items-center gap-1.5 flex-wrap">
                            <span class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light/70">${escapeHtml(status)}</span>
                            <span class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono ${meta.tone} flex items-center gap-1">
                                <i class="${iconCls} text-[9px]"></i>
                                ${escapeHtml(meta.label)}
                            </span>
                            <span class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light/50">${escapeHtml(business)}</span>
                            ${stamp ? `<span class="text-[9px] font-mono text-ops-light/40">${escapeHtml(stamp)}</span>` : ''}
                        </div>
                        <div class="mt-1 text-[11px] text-white truncate">${escapeHtml(titleLine)}</div>
                    </div>
                `;
            }).join('');

        const showCount = Array.isArray(groups) ? groups.length : list.length;
        return `
            <div class="dash-card-head flex items-center justify-between gap-3 px-3 py-2.5">
                <div class="flex items-center gap-2 min-w-0">
                    <i class="fa-solid fa-satellite-dish text-blue-400 text-xs shrink-0"></i>
                    <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light">Inbox Radar</span>
                    <div class="flex items-center gap-3 text-[10px] font-mono text-ops-light/50">
                        <span class="text-lg font-semibold text-white leading-none">${showCount}</span>
                        <span><i class="fa-brands fa-slack text-purple-400 mr-0.5"></i>${slack.length}</span>
                        <span><i class="fa-solid fa-envelope text-sky-400 mr-0.5"></i>${email.length}</span>
                        <span><i class="fa-solid fa-ellipsis text-ops-light/30 mr-0.5"></i>${other.length}</span>
                    </div>
                </div>
                <div class="flex items-center gap-1.5 shrink-0">
                    <button type="button" data-run-marcus-filter class="px-2.5 py-1 rounded border border-amber-600/40 bg-amber-600/15 text-[9px] font-mono text-amber-200 hover:bg-amber-600/25 transition-colors">Run Marcus Filter</button>
                    <button type="button" data-open-inbox class="px-2.5 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Open Inbox</button>
                    ${showCount ? '<i class="fa-solid fa-chevron-down expand-chevron"></i>' : ''}
                </div>
            </div>
            ${showCount ? `<div class="dash-card-body px-3 pb-2.5"><div class="space-y-1">${rowsHtml}</div></div>` : ''}
        `;
    };

    radarBanner.innerHTML = makeRadarHtml({ items: inboxNew, groups: null });
        const showCount = Array.isArray(businessGroups)
            ? businessGroups.reduce((sum, g) => sum + (Number(g?.count) || 0), 0)
            : (Array.isArray(groups)
                ? groups.reduce((sum, g) => sum + (Number(g?.count) || 0), 0)
                : list.length);

    setTimeout(async () => {
        try {
            const data = await apiJson('/api/inbox/radar?status=New&limit=60');
            if (state.currentView !== 'dashboard') return;
            radarBanner.innerHTML = makeRadarHtml(data);
            radarBanner.querySelector('button[data-open-inbox]')?.addEventListener('click', () => openInbox());
            radarBanner.querySelector('button[data-run-marcus-filter]')?.addEventListener('click', async () => {
                try {
                    const result = await runMarcusInboxFilter();
                    alert(`Marcus filter complete. Archived: ${Number(result?.archived || 0)}. Matched: ${Number(result?.matched || 0)}.`);
                    renderMain();
                } catch (e) {
                    alert(e?.message || 'Marcus filter failed');
                }
            });
        } catch {
            // ignore
        }
    }, 0);

    const calCard = document.createElement('div');
    calCard.className = 'dash-card';
    calCard.dataset.cardId = 'calendar-side';
    const calRows = !callsConnected
        ? `<div class="text-[10px] text-ops-light/50">Connect Google Calendar in Settings.</div>`
        : (callsError
            ? `<div class="text-[10px] text-amber-300">${escapeHtml(callsError)}</div>`
            : (callsLoading && !calls.length)
                ? `<div class="text-[10px] text-ops-light/50">Loading…</div>`
                : (calls.length
                    ? `<div class="space-y-1">${calls.slice(0, 6).map((ev) => {
                        const time = formatTimeFromIso(ev.start);
                        const title = safeText(ev.summary) || 'Untitled';
                        const link = safeText(ev.meetingLink);
                        return `<div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><div class="flex items-center justify-between gap-2"><div class="min-w-0"><div class="text-[11px] text-white truncate">${escapeHtml(title)}</div><div class="text-[9px] font-mono text-ops-light/50">${escapeHtml(time || '')}</div></div>${link ? `<a class="shrink-0 text-[9px] font-mono text-ops-accent hover:underline" href="${escapeHtml(link)}" target="_blank" rel="noreferrer">Join</a>` : ''}</div></div>`;
                    }).join('')}</div>`
                    : `<div class="text-[10px] text-ops-light/50">No upcoming meetings.</div>`));

    calCard.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-2 px-3 py-2.5">
            <div class="flex items-center gap-2 min-w-0">
                <i class="fa-solid fa-calendar-days text-blue-400 text-[10px] shrink-0"></i>
                <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light truncate">Calendar</span>
            </div>
            <div class="flex items-center gap-2">
                <button type="button" data-open-calendar class="px-2.5 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Open</button>
            </div>
        </div>
        <div class="px-3 pb-3">${calRows}</div>
    `;
    sideWrap.appendChild(calCard);

    sideWrap.querySelectorAll('button[data-open-inbox]').forEach((b) => b.addEventListener('click', () => openInbox()));
    sideWrap.querySelectorAll('button[data-run-marcus-filter]').forEach((b) => b.addEventListener('click', async () => {
        b.disabled = true;
        const prev = b.textContent;
        b.textContent = 'Filtering…';
        try {
            const result = await runMarcusInboxFilter();
            alert(`Marcus filter complete. Archived: ${Number(result?.archived || 0)}. Matched: ${Number(result?.matched || 0)}.`);
            renderMain();
        } catch (e) {
            alert(e?.message || 'Marcus filter failed');
        } finally {
            b.disabled = false;
            b.textContent = prev;
        }
    }));
    sideWrap.querySelectorAll('button[data-open-calendar]').forEach((b) => b.addEventListener('click', () => openCalendar()));
    root.querySelectorAll('button[data-open-inbox]').forEach((b) => b.addEventListener('click', () => openInbox()));
    root.querySelectorAll('button[data-run-marcus-filter]').forEach((b) => b.addEventListener('click', async () => {
        b.disabled = true;
        const prev = b.textContent;
        b.textContent = 'Filtering…';
        try {
            const result = await runMarcusInboxFilter();
            alert(`Marcus filter complete. Archived: ${Number(result?.archived || 0)}. Matched: ${Number(result?.matched || 0)}.`);
            renderMain();
        } catch (e) {
            alert(e?.message || 'Marcus filter failed');
        } finally {
            b.disabled = false;
            b.textContent = prev;
        }
    }));
    root.querySelectorAll('button[data-open-calendar]').forEach((b) => b.addEventListener('click', () => openCalendar()));
}

function renderTodayPanel() {
    const wrap = document.createElement('div');
    wrap.className = 'mb-6 border border-zinc-800 rounded-xl bg-zinc-900/30 p-4';

    const next = getTodayNextActions();
    const outcomes = safeText(state.settings?.todayOutcomes);
    const callsConnected = !!state.settings?.googleConnected;
    const calls = Array.isArray(state.dashboardCalls?.events) ? state.dashboardCalls.events : [];
    const callsError = safeText(state.dashboardCalls?.error);

    // Opportunistically fetch calls once when connected
    if (callsConnected) {
        setTimeout(() => refreshDashboardCalls({ force: false }), 0);
    }

    const callsHtml = !callsConnected
        ? `<div class="text-[11px] text-zinc-500">Connect Google Calendar in Settings to show calls here.</div>`
        : (callsError
            ? `<div class="text-[11px] text-red-400">${escapeHtml(callsError)}</div>`
            : (calls.length
                ? `<div class="space-y-2">${calls.slice(0, 6).map((ev) => {
                    const time = formatTimeFromIso(ev.start);
                    const title = safeText(ev.summary) || 'Untitled';
                    const link = safeText(ev.meetingLink) || safeText(ev.htmlLink);
                    const linkHtml = link ? `<a class="text-blue-300 hover:text-blue-200 underline" href="${escapeHtml(link)}" target="_blank" rel="noopener noreferrer">join</a>` : `<span class="text-zinc-600">no link</span>`;
                    return `
                        <div class="flex items-center justify-between gap-3 border border-zinc-800 rounded-md bg-zinc-950/20 px-3 py-2">
                            <div class="min-w-0">
                                <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">${escapeHtml(time || '—')}</div>
                                <div class="text-xs text-zinc-200 truncate">${escapeHtml(title)}</div>
                            </div>
                            <div class="shrink-0 text-xs font-mono">${linkHtml}</div>
                        </div>
                    `;
                }).join('')}</div>`
                : `<div class="text-[11px] text-zinc-500">No upcoming calls in the next 24h.</div>`));

    wrap.innerHTML = `
        <div class="flex items-start justify-between gap-3">
            <div>
                <div class="text-white text-sm font-semibold">Today is handled</div>
                <div class="text-[11px] text-zinc-500 mt-0.5">Top outcomes, next actions, and calls — all in one place.</div>
            </div>
        </div>

        <div class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <div class="text-zinc-400 text-xxs font-mono uppercase tracking-widest mb-1">Top outcomes (write 1–3)</div>
                <textarea id="today-outcomes" rows="4" class="w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-xs font-mono text-zinc-200 resize-none" placeholder="1) ...\n2) ...\n3) ...">${escapeHtml(outcomes)}</textarea>
                <div class="flex gap-2 mt-2">
                    <button id="btn-save-today" class="px-3 py-2 rounded-lg bg-blue-600/20 border border-blue-600/40 text-xs font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Save</button>
                    <button id="btn-clear-today" class="px-3 py-2 rounded-lg border border-zinc-700 text-xs font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Clear</button>
                </div>
            </div>

            <div>
                <div class="text-zinc-400 text-xxs font-mono uppercase tracking-widest mb-2">Next actions</div>
                <div class="space-y-2">
                    ${next.length ? next.map((t) => {
                        const project = safeText(t.project);
                        const due = safeText(t.dueDate);
                        const pr = Number(t.priority) || 3;
                        return `
                            <div class="border border-zinc-800 rounded-md bg-zinc-950/20 px-3 py-2">
                                <div class="flex items-center justify-between gap-3">
                                    <div class="min-w-0">
                                        <div class="text-xs text-zinc-200 truncate">${escapeHtml(safeText(t.title))}</div>
                                        <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">${escapeHtml(project || '—')}${due ? ` • due ${escapeHtml(due)}` : ''}</div>
                                    </div>
                                    <div class="shrink-0 text-[10px] font-mono uppercase tracking-widest ${pr === 1 ? 'text-red-300' : pr === 2 ? 'text-amber-300' : 'text-zinc-500'}">P${pr}</div>
                                </div>
                            </div>
                        `;
                    }).join('') : `<div class="text-[11px] text-zinc-500">No next actions found. Nice.</div>`}
                </div>
            </div>
        </div>

        <div class="mt-4">
            <div class="flex items-center justify-between">
                <div class="text-zinc-400 text-xxs font-mono uppercase tracking-widest">Calls today</div>
                <button id="btn-refresh-calls" class="px-2 py-1 rounded border border-zinc-800 text-[10px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Refresh</button>
            </div>
            <div class="mt-2">${callsHtml}</div>
        </div>
    `;

    const btnSave = wrap.querySelector('#btn-save-today');
    const btnClear = wrap.querySelector('#btn-clear-today');
    const ta = wrap.querySelector('#today-outcomes');
    if (btnSave && ta) {
        btnSave.onclick = async () => {
            btnSave.disabled = true;
            try {
                const v = safeText(ta.value);
                state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {};
                state.settings.todayOutcomes = v;
                state.rerenderPauseUntil = Date.now() + 2000;
                await saveSettingsPatch({ todayOutcomes: v });
                state.rerenderPauseUntil = 0;
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to save');
                state.rerenderPauseUntil = 0;
            } finally {
                btnSave.disabled = false;
            }
        };
    }
    if (btnClear && ta) {
        btnClear.onclick = async () => {
            ta.value = '';
            try {
                state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {};
                state.settings.todayOutcomes = '';
                state.rerenderPauseUntil = Date.now() + 2000;
                await saveSettingsPatch({ todayOutcomes: '' });
                state.rerenderPauseUntil = 0;
                renderMain();
            } catch (e) {
                alert(e?.message || 'Failed to clear');
                state.rerenderPauseUntil = 0;
            }
        };
    }
    const btnRefreshCalls = wrap.querySelector('#btn-refresh-calls');
    if (btnRefreshCalls) {
        btnRefreshCalls.onclick = () => refreshDashboardCalls({ force: true });
    }

    return wrap;
}

function getProjectTasks(project) {
    const list = Array.isArray(state.tasks) ? state.tasks : [];
    const projectName = safeText(project?.name);
    const projectId = safeText(project?.id);
    return list.filter(t => {
        const tp = safeText(t?.project);
        const matches = (tp && projectName && tp === projectName) || (tp && projectId && tp === projectId);
        if (!matches) return false;
        if (state.showCompleted) return true;
        return !isDoneTask(t);
    });
}

function getProjectLinkedInboxItems(project) {
    const list = getDisplayInboxItems();
    const projectId = safeText(project?.id).trim();
    const projectName = safeText(project?.name).trim();
    return list
        .filter((item) => {
            const linkedProjectId = safeText(item?.projectId).trim();
            const linkedProjectName = safeText(item?.projectName).trim();
            if (projectId && linkedProjectId === projectId) return true;
            if (!linkedProjectId && projectName && linkedProjectName === projectName) return true;
            return false;
        })
        .sort((a, b) => String(b?.updatedAt || b?.createdAt || '').localeCompare(String(a?.updatedAt || a?.createdAt || '')));
}

function getActiveProjectTab(projectId) {
    const pid = safeText(projectId);
    const t = safeText(state.projectRightTabById?.[pid]);
    return t || 'inbox';
}

function setActiveProjectTab(projectId, tab) {
    const pid = safeText(projectId);
    const t = safeText(tab);
    if (!pid) return;
    if (!state.projectRightTabById || typeof state.projectRightTabById !== 'object') {
        state.projectRightTabById = {};
    }
    state.projectRightTabById[pid] = t;
}

function toLocalDay(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    return d;
}

function ymdFromLocalDate(date) {
    const d = new Date(date);
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
}

function parseYmdToLocalDay(ymd) {
    if (typeof ymd !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(ymd)) return null;
    const d = new Date(`${ymd}T00:00:00`);
    if (Number.isNaN(d.getTime())) return null;
    d.setHours(0, 0, 0, 0);
    return d;
}

function startOfWeekMonday(localDay) {
    const d = new Date(localDay);
    d.setHours(0, 0, 0, 0);
    const day = d.getDay(); // 0=Sun, 1=Mon
    const diff = (day === 0 ? -6 : 1 - day);
    d.setDate(d.getDate() + diff);
    return d;
}

function startOfWeekSunday(localDay) {
    const d = new Date(localDay);
    d.setHours(0, 0, 0, 0);
    const day = d.getDay(); // 0=Sun
    d.setDate(d.getDate() - day);
    return d;
}

function endOfWeekSunday(localDay) {
    const start = startOfWeekMonday(localDay);
    const end = new Date(start);
    end.setDate(end.getDate() + 6);
    end.setHours(0, 0, 0, 0);
    return end;
}

function endOfWeekSaturday(localDay) {
    const start = startOfWeekSunday(localDay);
    const end = new Date(start);
    end.setDate(end.getDate() + 6);
    end.setHours(0, 0, 0, 0);
    return end;
}

function isBetweenInclusive(date, start, end) {
    const t = date.getTime();
    return t >= start.getTime() && t <= end.getTime();
}

function bucketProjectsByDueDate(projects) {
    const today = toLocalDay(new Date());
    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);

    const mondayWeeks = !!state.uiPrefs.weekStartsOnMonday;
    const thisWeekStart = mondayWeeks ? startOfWeekMonday(today) : startOfWeekSunday(today);
    const thisWeekEnd = mondayWeeks ? endOfWeekSunday(today) : endOfWeekSaturday(today);
    const nextWeekStart = new Date(thisWeekStart);
    nextWeekStart.setDate(nextWeekStart.getDate() + 7);
    const nextWeekEnd = new Date(thisWeekEnd);
    nextWeekEnd.setDate(nextWeekEnd.getDate() + 7);

    const out = {
        today: [],
        tomorrow: [],
        thisWeek: [],
        nextWeek: [],
        upcoming: [],
    };

    const list = Array.isArray(projects) ? projects : [];
    for (const p of list) {
        const due = parseYmdToLocalDay(p?.dueDate);
        if (!due) {
            out.upcoming.push(p);
            continue;
        }

        const dueYmd = ymdFromLocalDate(due);
        if (dueYmd === ymdFromLocalDate(today)) {
            out.today.push(p);
            continue;
        }
        if (dueYmd === ymdFromLocalDate(tomorrow)) {
            out.tomorrow.push(p);
            continue;
        }

        if (isBetweenInclusive(due, thisWeekStart, thisWeekEnd)) {
            out.thisWeek.push(p);
            continue;
        }

        if (isBetweenInclusive(due, nextWeekStart, nextWeekEnd)) {
            out.nextWeek.push(p);
            continue;
        }

        out.upcoming.push(p);
    }

    // Sort each bucket by dueDate then name for stability
    const sorter = (a, b) => {
        const ad = String(a?.dueDate || '9999-12-31');
        const bd = String(b?.dueDate || '9999-12-31');
        if (ad < bd) return -1;
        if (ad > bd) return 1;
        return String(a?.name || '').localeCompare(String(b?.name || ''));
    };
    out.today.sort(sorter);
    out.tomorrow.sort(sorter);
    out.thisWeek.sort(sorter);
    out.nextWeek.sort(sorter);
    out.upcoming.sort(sorter);

    return out;
}

function renderProjectBuckets(buckets, opts) {
    const wrap = document.createElement('div');
    wrap.className = 'grid grid-cols-1 lg:grid-cols-2 gap-6';
    const o = opts && typeof opts === 'object' ? opts : {};

    wrap.appendChild(renderProjectBucket('Due Today', buckets.today, o));
    wrap.appendChild(renderProjectBucket('Due Tomorrow', buckets.tomorrow, o));
    wrap.appendChild(renderProjectBucket('Due This Week', buckets.thisWeek, o));
    wrap.appendChild(renderProjectBucket('Due Next Week', buckets.nextWeek, o));
    // Make upcoming span full width on large screens
    const upcoming = renderProjectBucket('Upcoming', buckets.upcoming, o);
    upcoming.classList.add('lg:col-span-2');
    wrap.appendChild(upcoming);

    return wrap;
}

function renderProjectBucket(title, projects, opts) {
    const card = document.createElement('div');
    card.className = 'bg-zinc-900/30 border border-zinc-800 rounded-lg';
    const o = opts && typeof opts === 'object' ? opts : {};

    const header = document.createElement('div');
    header.className = 'px-4 py-3 border-b border-zinc-800 flex items-center justify-between';
    header.innerHTML = `
        <div class="text-zinc-300 text-xs font-bold uppercase tracking-widest">${title}</div>
        <div class="text-zinc-500 text-xs font-mono">${Array.isArray(projects) ? projects.length : 0}</div>
    `;

    const body = document.createElement('div');
    body.className = 'p-2';

    const list = Array.isArray(projects) ? projects : [];
    if (list.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'p-3 text-zinc-600 italic text-sm';
        empty.innerText = 'None.';
        body.appendChild(empty);
    } else {
        for (const p of list) {
            body.appendChild(renderProjectRow(p, o));
        }
    }

    card.appendChild(header);
    card.appendChild(body);
    return card;
}

function renderProjectRow(project, opts) {
    const row = document.createElement('div');
    row.className = 'w-full text-left px-3 py-2 rounded-md hover:bg-zinc-800/60 transition-colors flex items-center justify-between gap-3';

    const o = opts && typeof opts === 'object' ? opts : {};
    const bulkMode = !!o.bulkMode;
    const isArch = isArchivedProject(project);
    const projectId = safeText(project?.id);

    if (bulkMode) {
        row.classList.add('cursor-pointer');
    }

    const left = document.createElement('div');
    left.className = 'min-w-0 flex items-center gap-3';

    if (bulkMode) {
        const cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.setAttribute('data-dash-proj-sel', projectId);
        cb.checked = !!state.dashboardSelectedProjectById?.[projectId];
        cb.className = 'shrink-0';
        cb.onclick = (e) => {
            e.stopPropagation();
        };
        left.appendChild(cb);
    }

    const leftText = document.createElement('div');
    leftText.className = 'min-w-0';

    const name = document.createElement('div');
    name.className = 'text-zinc-200 text-sm font-medium truncate';
    name.innerText = project?.name || 'Untitled';

    const meta = document.createElement('div');
    meta.className = 'text-zinc-500 text-xs flex items-center gap-2';
    const type = project?.type ? String(project.type) : 'Other';
    const status = project?.status ? String(project.status) : 'Active';
    const pName = safeText(project?.name);
    const pId = safeText(project?.id);
    const pending = (Array.isArray(state.tasks) ? state.tasks : []).filter((t) => {
        const tp = safeText(t?.project);
        const matches = (tp && pName && tp === pName) || (tp && pId && tp === pId);
        if (!matches) return false;
        return !isDoneTask(t);
    }).length;
    meta.innerHTML = `<span>${escapeHtml(type)}</span><span class="text-zinc-700">•</span><span>${escapeHtml(status)}</span><span class="text-zinc-700">•</span><span>${pending} open</span>`;

    leftText.appendChild(name);
    leftText.appendChild(meta);
    left.appendChild(leftText);

    const right = document.createElement('div');
    right.className = 'shrink-0 flex items-center gap-2';

    const dueYmd = safeText(project?.dueDate).trim();
    const due = document.createElement('div');
    due.className = `text-[10px] font-mono uppercase tracking-widest px-2 py-1 rounded border ${dueYmd ? 'text-blue-300 border-blue-500/30 bg-zinc-950/30' : 'text-zinc-600 border-zinc-800 bg-zinc-950/20'}`;
    due.innerText = dueYmd ? `Due ${dueYmd}` : 'No due';
    right.appendChild(due);

    if (!bulkMode) {
        const btnArchive = document.createElement('button');
        btnArchive.type = 'button';
        btnArchive.className = 'px-2 py-1 rounded border border-zinc-800 text-[10px] font-mono uppercase tracking-widest text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px';
        btnArchive.innerText = isArch ? 'Unarchive' : 'Archive';
        btnArchive.onclick = async (e) => {
            e.preventDefault();
            e.stopPropagation();
            if (!projectId) return;
            btnArchive.disabled = true;
            try {
                await setProjectArchived(projectId, !isArch);
                renderNav();
                renderMain();
            } catch (err) {
                alert(err?.message || 'Failed to update project');
            } finally {
                btnArchive.disabled = false;
            }
        };

        const btnDelete = document.createElement('button');
        btnDelete.type = 'button';
        btnDelete.className = 'px-2 py-1 rounded border border-red-600/40 text-[10px] font-mono uppercase tracking-widest text-red-300 hover:text-white hover:bg-red-600/20 transition-colors transition-transform duration-150 ease-out active:translate-y-px';
        btnDelete.innerText = 'Delete';
        btnDelete.onclick = async (e) => {
            e.preventDefault();
            e.stopPropagation();
            if (!projectId) return;
            const nm = safeText(project?.name) || 'this project';
            if (!confirm(`Delete \"${nm}\"? This will also remove its tasks, notes, chat, scratchpad, and communications.`)) return;
            btnDelete.disabled = true;
            try {
                await deleteProjectsByIdList([projectId]);
                renderNav();
                renderMain();
            } catch (err) {
                alert(err?.message || 'Failed to delete project');
            } finally {
                btnDelete.disabled = false;
            }
        };

        right.appendChild(btnArchive);
        right.appendChild(btnDelete);
    }

    row.appendChild(left);
    row.appendChild(right);

    if (bulkMode) {
        row.onclick = () => {
            if (!projectId) return;
            if (!state.dashboardSelectedProjectById || typeof state.dashboardSelectedProjectById !== 'object') {
                state.dashboardSelectedProjectById = {};
            }
            const next = !state.dashboardSelectedProjectById?.[projectId];
            state.dashboardSelectedProjectById[projectId] = next;
            // keep checkbox in sync without full re-render
            const cb = row.querySelector('input[type="checkbox"][data-dash-proj-sel]');
            if (cb) cb.checked = next;
            // banner bulk UI is updated by renderDashboard via updateDashBulkUi; force minimal refresh
            const countEl = document.getElementById('dash-selected-count');
            if (countEl) {
                const selectedIds = Object.keys(state.dashboardSelectedProjectById || {}).filter((id) => state.dashboardSelectedProjectById[id]);
                countEl.textContent = String(selectedIds.length);
                const btnClear = document.getElementById('btn-dash-clear');
                const btnArchive = document.getElementById('btn-dash-archive');
                const btnUnarchive = document.getElementById('btn-dash-unarchive');
                const btnDelete = document.getElementById('btn-dash-delete');
                const has = selectedIds.length > 0;
                if (btnClear) btnClear.disabled = !has;
                if (btnArchive) btnArchive.disabled = !has;
                if (btnUnarchive) btnUnarchive.disabled = !has;
                if (btnDelete) btnDelete.disabled = !has;
            }
        };
    } else {
        row.onclick = () => openProject(project.id);
    }
    return row;
}

function createStatCard(label, value, icon, colorClass) {
    return `
        <div class="bg-zinc-900/50 border border-zinc-800 p-4 rounded-lg flex items-center justify-between transition-colors transition-transform duration-150 ease-out hover:bg-zinc-900/70 hover:border-zinc-700 hover:-translate-y-0.5">
            <div>
                <div class="text-zinc-500 text-xs uppercase tracking-wider mb-1">${label}</div>
                <div class="text-2xl text-white font-mono">${value}</div>
            </div>
            <div class="${colorClass} opacity-80 text-xl"><i class="fa-solid ${icon}"></i></div>
        </div>
    `;
}

function renderProjectView(container) {
    const project = state.projects.find(p => p.id === state.currentProjectId);
    if (!project) return;
    
    const titleEl = document.getElementById("page-title");
    if(titleEl) titleEl.innerText = project.name;
    
    const wrap = document.createElement('div');
    wrap.className = 'h-full flex flex-col min-h-0';

    const subheader = document.createElement('div');
    subheader.className = 'shrink-0 px-6 py-3 border-b border-zinc-800 bg-zinc-900/20 flex items-center justify-between gap-3';

    const due = safeText(project?.dueDate).trim();
    const status = safeText(project?.status).trim() || 'Active';
    const type = safeText(project?.type).trim() || 'Other';
    const ws = safeText(project?.workspacePath).trim();
    const airtable = safeText(project?.airtableUrl).trim();

    subheader.innerHTML = `
        <div class="min-w-0">
            <div class="flex flex-wrap items-center gap-2">
                <span class="text-xs font-mono text-zinc-400 border border-zinc-800 rounded px-2 py-1 bg-zinc-950/30">${escapeHtml(type)}</span>
                <span class="text-xs font-mono text-zinc-400 border border-zinc-800 rounded px-2 py-1 bg-zinc-950/30">${escapeHtml(status)}</span>
                <span class="text-xs font-mono ${due ? 'text-blue-300 border-blue-500/30' : 'text-zinc-500 border-zinc-800'} border rounded px-2 py-1 bg-zinc-950/30">${due ? `Due ${escapeHtml(due)}` : 'No due date'}</span>
                <button id="proj-quick-open-code" class="text-xs font-mono px-2 py-1 rounded border border-zinc-800 bg-zinc-950/30 hover:bg-zinc-900/40 text-zinc-300 transition-colors duration-150 ease-out active:translate-y-px ${ws ? '' : 'opacity-40 pointer-events-none'}" title="Open workspace in VS Code">
                    <i class="fa-solid fa-code mr-1"></i>Code
                </button>
                <a id="proj-quick-open-airtable" class="text-xs font-mono px-2 py-1 rounded border border-zinc-800 bg-zinc-950/30 hover:bg-zinc-900/40 text-zinc-300 transition-colors duration-150 ease-out active:translate-y-px ${airtable ? '' : 'opacity-40 pointer-events-none'}" href="${escapeHtml(airtable || '#')}" target="_blank" rel="noopener noreferrer" title="Open Airtable">
                    <i class="fa-solid fa-table mr-1"></i>Airtable
                </a>
            </div>
            <div class="mt-1 text-[10px] text-zinc-500 font-mono truncate">${ws ? `Workspace: ${escapeHtml(ws)}` : 'Workspace: not set'}</div>
        </div>
        <div class="shrink-0 flex items-center gap-2">
            <button class="bg-blue-600 hover:bg-blue-500 text-white px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors flex items-center" id="btn-new-task">
                <i class="fa-solid fa-plus mr-2"></i>Task
            </button>
            <button class="bg-zinc-800 hover:bg-zinc-700 text-zinc-300 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors flex items-center" id="btn-auto-delegate" title="Auto-assign tasks">
                <i class="fa-solid fa-robot"></i>
            </button>
            <label class="flex items-center gap-2 text-[10px] text-zinc-500 font-mono uppercase tracking-widest select-none">
                <input type="checkbox" ${state.showCompleted ? 'checked' : ''} id="chk-show-completed" class="accent-blue-500">
                Done
            </label>
        </div>
    `;

    const body = document.createElement('div');
    body.className = 'flex-1 min-h-0 grid grid-cols-1 xl:grid-cols-3 gap-4 p-6';

    // LEFT: Tasks (scroll inside)
    const tasksCard = document.createElement('div');
    tasksCard.className = 'xl:col-span-2 bg-zinc-900/30 border border-zinc-800 rounded-lg flex flex-col min-h-0';
    const tasksHeader = document.createElement('div');
    tasksHeader.className = 'shrink-0 px-4 py-3 border-b border-zinc-800 flex items-center justify-between';
    const pTasks = getProjectTasks(project);
    tasksHeader.innerHTML = `
        <div class="text-zinc-300 text-xs font-bold uppercase tracking-widest">Tasks</div>
        <div class="text-zinc-500 text-xs font-mono">${pTasks.length} • rev ${state.revision}</div>
    `;
    const taskList = document.createElement('div');
    taskList.className = 'flex-1 min-h-0 overflow-y-auto p-3 space-y-2';
    if (pTasks.length === 0) {
        taskList.innerHTML = `<div class="text-zinc-600 text-center py-10 italic">No active tasks in this sector.</div>`;
    } else {
        pTasks.forEach(t => taskList.appendChild(createTaskRow(t)));
    }
    tasksCard.appendChild(tasksHeader);
    tasksCard.appendChild(taskList);

    // RIGHT: Tabbed command panel (scroll inside)
    const side = document.createElement('div');
    side.className = 'bg-zinc-900/30 border border-zinc-800 rounded-lg flex flex-col min-h-0';

    const tabs = [
        { id: 'details', label: 'Details', icon: 'fa-sliders' },
        { id: 'links', label: 'Links', icon: 'fa-link' },
        { id: 'inbox', label: 'Inbox', icon: 'fa-inbox' },
        { id: 'notes', label: 'Notes', icon: 'fa-note-sticky' },
        { id: 'scratch', label: 'Scratch', icon: 'fa-pen-to-square' },
        { id: 'comms', label: 'Comms', icon: 'fa-message' },
    ];

    const tabRow = document.createElement('div');
    tabRow.className = 'shrink-0 px-2 py-2 border-b border-zinc-800 flex gap-1';
    const activeTab = getActiveProjectTab(project.id);
    tabRow.innerHTML = tabs.map(t => {
        const on = t.id === activeTab;
        return `
            <button type="button" data-tab="${t.id}" class="flex-1 px-2 py-2 rounded-md text-[10px] font-mono uppercase tracking-widest border transition-colors transition-transform duration-150 ease-out active:translate-y-px ${on ? 'bg-blue-600/10 border-blue-600/30 text-blue-200' : 'bg-zinc-950/20 border-zinc-800 text-zinc-400 hover:text-zinc-200 hover:bg-zinc-900/40'}">
                <i class="fa-solid ${t.icon} mr-1"></i>${t.label}
            </button>
        `;
    }).join('');

    const panel = document.createElement('div');
    panel.className = 'flex-1 min-h-0 overflow-y-auto p-4';

    const renderPanel = () => {
        const tab = getActiveProjectTab(project.id);
        panel.innerHTML = '';

        const setLinkButton = (anchorEl, url) => {
            if (!anchorEl) return;
            const u = safeText(url).trim();
            anchorEl.href = u || '#';
            anchorEl.classList.toggle('opacity-50', !u);
            anchorEl.classList.toggle('pointer-events-none', !u);
        };

        if (tab === 'details') {
            const ownerOptions = [''].concat(getAssignableOwnerNames());
            const currentOwner = getProjectOwnerName(project);
            const ownerOptionsHtml = ownerOptions.map((name) => {
                const label = name ? name : 'Unassigned';
                const selected = (name ? name : '') === (currentOwner ? currentOwner : '') ? 'selected' : '';
                return `<option value="${escapeHtml(name)}" ${selected}>${escapeHtml(label)}</option>`;
            }).join('');

            const el = document.createElement('div');
            el.className = 'space-y-3';
            el.innerHTML = `
                <div>
                    <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Assigned To</div>
                    <select id="proj-owner" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200">
                        ${ownerOptionsHtml}
                    </select>
                </div>
                <div class="grid grid-cols-2 gap-2">
                    <div>
                        <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Due Date</div>
                        <input id="proj-due" type="date" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" value="${escapeHtml(safeText(project.dueDate))}">
                    </div>
                    <div>
                        <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Status</div>
                        <select id="proj-status" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200">
                            <option ${project.status === 'Active' ? 'selected' : ''}>Active</option>
                            <option ${project.status === 'On Hold' ? 'selected' : ''}>On Hold</option>
                            <option ${project.status === 'Done' ? 'selected' : ''}>Done</option>
                        </select>
                    </div>
                </div>
                <div>
                    <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Project Value (optional)</div>
                    <input id="proj-value" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="$5,000" value="${escapeHtml(safeText(project.projectValue))}">
                </div>
                <div class="grid grid-cols-2 gap-2">
                    <div>
                        <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Account Manager</div>
                        <input id="proj-am-name" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="Name" value="${escapeHtml(safeText(project.accountManagerName))}">
                    </div>
                    <div>
                        <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">AM Email</div>
                        <input id="proj-am-email" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="name@company.com" value="${escapeHtml(safeText(project.accountManagerEmail))}">
                    </div>
                </div>
                <div class="grid grid-cols-2 gap-2 mt-2">
                    <div>
                        <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Client Name</div>
                        <input id="proj-client-name" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="Client Name" value="${escapeHtml(safeText(project.clientName))}">
                    </div>
                    <div>
                        <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Client Phone</div>
                        <input id="proj-client-phone" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="+15551234567" value="${escapeHtml(safeText(project.clientPhone))}">
                    </div>
                </div>
                <button id="btn-save-details" class="w-full bg-zinc-800 hover:bg-zinc-700 text-zinc-200 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors">Save</button>
            `;
            panel.appendChild(el);

            const saveDetailsBtn = el.querySelector('#btn-save-details');
            if (saveDetailsBtn) {
                saveDetailsBtn.onclick = async () => {
                    saveDetailsBtn.disabled = true;
                    try {
                        const owner = safeText(el.querySelector('#proj-owner')?.value).trim();
                        const dueDate = safeText(el.querySelector('#proj-due')?.value).trim();
                        const status = safeText(el.querySelector('#proj-status')?.value).trim();
                        const projectValue = safeText(el.querySelector('#proj-value')?.value).trim();
                        const accountManagerName = safeText(el.querySelector('#proj-am-name')?.value).trim();
                        const accountManagerEmail = safeText(el.querySelector('#proj-am-email')?.value).trim();
                        const clientName = safeText(el.querySelector('#proj-client-name')?.value).trim();
                        const clientPhone = safeText(el.querySelector('#proj-client-phone')?.value).trim();
                        await saveProjectPatch(project.id, { owner, dueDate, status, projectValue, accountManagerName, accountManagerEmail, clientName, clientPhone });
                        alert('Saved.');
                        renderNav();
                        renderMain();
                    } catch (e) {
                        alert(e?.message || 'Failed to save details');
                    } finally {
                        saveDetailsBtn.disabled = false;
                    }
                };
            }
            return;
        }

        if (tab === 'links') {
            const el = document.createElement('div');
            el.className = 'space-y-3';
            el.innerHTML = `
                <div>
                    <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Workspace Path</div>
                    <div class="flex gap-2">
                        <input id="proj-workspace" class="flex-1 bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="C:\\path\\to\\project" value="${escapeHtml(safeText(project.workspacePath))}">
                        <button id="btn-browse-workspace" class="bg-zinc-950/40 hover:bg-zinc-800 text-zinc-300 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors border border-zinc-800">Browse</button>
                    </div>
                </div>
                <div>
                    <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Airtable URL</div>
                    <input id="proj-airtable" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="https://airtable.com/..." value="${escapeHtml(safeText(project.airtableUrl))}">
                </div>
                <div>
                    <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Stripe Invoice URL (optional)</div>
                    <input id="proj-invoice" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="https://invoice.stripe.com/..." value="${escapeHtml(safeText(project.stripeInvoiceUrl))}">
                </div>
                <div>
                    <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Repo URL (optional)</div>
                    <input id="proj-repo" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="https://github.com/..." value="${escapeHtml(safeText(project.repoUrl))}">
                </div>
                <div>
                    <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest mb-1">Docs URL (optional)</div>
                    <input id="proj-docs" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="https://docs.google.com/..." value="${escapeHtml(safeText(project.docsUrl))}">
                </div>
                <div class="grid grid-cols-2 gap-2">
                    <button id="btn-save-links" class="bg-zinc-800 hover:bg-zinc-700 text-zinc-200 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors">Save</button>
                    <button id="btn-open-code" class="bg-blue-600 hover:bg-blue-500 text-white px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors">Open VS Code</button>
                </div>
                <div class="grid grid-cols-2 gap-2">
                    <button id="btn-copy-path" class="bg-zinc-950/40 hover:bg-zinc-800 text-zinc-300 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors border border-zinc-800">Copy Path</button>
                    <a id="btn-open-airtable" class="bg-zinc-950/40 hover:bg-zinc-800 text-zinc-300 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors border border-zinc-800 text-center" href="#" target="_blank" rel="noopener noreferrer">Open Airtable</a>
                </div>
                <div class="grid grid-cols-2 gap-2">
                    <a id="btn-open-invoice" class="bg-zinc-950/40 hover:bg-zinc-800 text-zinc-300 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors border border-zinc-800 text-center" href="#" target="_blank" rel="noopener noreferrer">Invoice</a>
                    <a id="btn-open-repo" class="bg-zinc-950/40 hover:bg-zinc-800 text-zinc-300 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors border border-zinc-800 text-center" href="#" target="_blank" rel="noopener noreferrer">Repo</a>
                </div>
                <div>
                    <a id="btn-open-docs" class="block bg-zinc-950/40 hover:bg-zinc-800 text-zinc-300 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors border border-zinc-800 text-center" href="#" target="_blank" rel="noopener noreferrer">Open Docs</a>
                </div>
            `;
            panel.appendChild(el);

            const wsInput = el.querySelector('#proj-workspace');
            const browseWorkspaceBtn = el.querySelector('#btn-browse-workspace');
            const atInput = el.querySelector('#proj-airtable');
            const invInput = el.querySelector('#proj-invoice');
            const repoInput = el.querySelector('#proj-repo');
            const docsInput = el.querySelector('#proj-docs');
            const openAirtable = el.querySelector('#btn-open-airtable');
            const openInvoice = el.querySelector('#btn-open-invoice');
            const openRepo = el.querySelector('#btn-open-repo');
            const openDocs = el.querySelector('#btn-open-docs');

            if (browseWorkspaceBtn) {
                browseWorkspaceBtn.onclick = async () => {
                    browseWorkspaceBtn.disabled = true;
                    try {
                        const picked = await pickFolderPath();
                        if (picked && wsInput) wsInput.value = picked;
                    } catch (e) {
                        alert(e?.message || 'Failed to pick folder');
                    } finally {
                        browseWorkspaceBtn.disabled = false;
                    }
                };
            }

            const setAirtableHref = () => {
                const url = safeText(atInput?.value).trim();
                setLinkButton(openAirtable, url);
            };
            setAirtableHref();
            if (atInput) atInput.addEventListener('input', setAirtableHref);

            const setOtherLinks = () => {
                setLinkButton(openInvoice, invInput?.value);
                setLinkButton(openRepo, repoInput?.value);
                setLinkButton(openDocs, docsInput?.value);
            };
            setOtherLinks();
            if (invInput) invInput.addEventListener('input', setOtherLinks);
            if (repoInput) repoInput.addEventListener('input', setOtherLinks);
            if (docsInput) docsInput.addEventListener('input', setOtherLinks);

            const saveLinksBtn = el.querySelector('#btn-save-links');
            if (saveLinksBtn) {
                saveLinksBtn.onclick = async () => {
                    saveLinksBtn.disabled = true;
                    try {
                        await saveProjectPatch(project.id, {
                            workspacePath: safeText(wsInput?.value).trim(),
                            airtableUrl: safeText(atInput?.value).trim(),
                            stripeInvoiceUrl: safeText(invInput?.value).trim(),
                            repoUrl: safeText(repoInput?.value).trim(),
                            docsUrl: safeText(docsInput?.value).trim(),
                        });
                        alert('Saved.');
                        renderNav();
                        renderMain();
                    } catch (e) {
                        alert(e?.message || 'Failed to save links');
                    } finally {
                        saveLinksBtn.disabled = false;
                    }
                };
            }

            const openCodeBtn = el.querySelector('#btn-open-code');
            if (openCodeBtn) {
                openCodeBtn.onclick = async () => {
                    openCodeBtn.disabled = true;
                    try {
                        await launchVsCodeFolder(wsInput?.value);
                    } catch (e) {
                        alert(e?.message || 'Failed to open VS Code');
                    } finally {
                        openCodeBtn.disabled = false;
                    }
                };
            }

            const copyBtn = el.querySelector('#btn-copy-path');
            if (copyBtn) {
                copyBtn.onclick = async () => {
                    const p = safeText(wsInput?.value).trim();
                    if (!p) return alert('Workspace path is empty.');
                    try {
                        await navigator.clipboard.writeText(p);
                        alert('Copied.');
                    } catch {
                        alert(p);
                    }
                };
            }

            return;
        }

        if (tab === 'scratch') {
            const scratchpadText = safeText(state.projectScratchpads?.[project.id]?.text);
            const scratchpadUpdatedAt = safeText(state.projectScratchpads?.[project.id]?.updatedAt);
            const scratchPreview = previewText(scratchpadText, 220);
            const el = document.createElement('div');
            el.className = 'space-y-3';
            el.innerHTML = `
                <div class="flex items-center justify-between">
                    <div class="text-zinc-400 text-xxs font-mono uppercase tracking-widest">Scratchpad</div>
                    <div class="text-zinc-600 text-[10px] font-mono">${scratchpadUpdatedAt ? `updated ${new Date(scratchpadUpdatedAt).toLocaleString()}` : ''}</div>
                </div>
                <textarea id="proj-scratchpad" rows="8" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200 resize-none" placeholder="Quick notes, deliverables, blockers...">${escapeHtml(scratchpadText)}</textarea>
                <button id="btn-save-scratch" class="w-full bg-zinc-800 hover:bg-zinc-700 text-zinc-200 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors">Save</button>
                <div class="h-px bg-zinc-800"></div>
                ${scratchpadText
                    ? `<details class="border border-zinc-800 rounded-md bg-zinc-950/30 p-3">
                        <summary class="cursor-pointer list-none flex items-center justify-between gap-3">
                            <div>
                                <div class="text-zinc-300 text-xxs font-mono uppercase tracking-widest">Scratch snapshot</div>
                                <div class="mt-1 text-xs text-zinc-300">${escapeHtml(scratchPreview || '(empty)')}</div>
                            </div>
                            <div class="text-[10px] text-zinc-500 font-mono">Expand</div>
                        </summary>
                        <div class="mt-2 text-xs text-zinc-200 whitespace-pre-wrap font-mono border-t border-zinc-800 pt-2">${escapeHtml(scratchpadText)}</div>
                    </details>`
                    : `<div class="text-zinc-600 italic text-sm">No saved scratch content yet.</div>`}
            `;
            panel.appendChild(el);

            const saveScratchBtn = el.querySelector('#btn-save-scratch');
            const scratchArea = el.querySelector('#proj-scratchpad');
            if (saveScratchBtn && scratchArea) {
                saveScratchBtn.onclick = async () => {
                    saveScratchBtn.disabled = true;
                    try {
                        await saveScratchpad(project.id, scratchArea.value);
                        await fetchState();
                        renderMain();
                    } catch (e) {
                        alert(e?.message || 'Failed to save scratchpad');
                    } finally {
                        saveScratchBtn.disabled = false;
                    }
                };
            }
            return;
        }

        if (tab === 'notes') {
            const notes = Array.isArray(state.projectNoteEntries?.[project.id]) ? state.projectNoteEntries[project.id] : [];
            const notesListHtml = notes.length
                ? notes.slice(0, 30).map(n => {
                    const kind = safeText(n.kind) || 'Note';
                    const date = safeText(n.date);
                    const title = safeText(n.title);
                    const content = safeText(n.content);
                    const preview = previewText(content, 220);
                    return `
                        <details class="border border-zinc-800 rounded-md bg-zinc-950/30 p-3">
                            <summary class="cursor-pointer list-none flex items-center justify-between gap-3">
                                <div class="min-w-0">
                                    <div class="text-zinc-300 text-xxs font-mono uppercase tracking-widest">${escapeHtml(kind)}${date ? ` • ${escapeHtml(date)}` : ''}${title ? ` • ${escapeHtml(title)}` : ''}</div>
                                    <div class="mt-1 text-xs text-zinc-300 truncate">${escapeHtml(preview || '(empty)')}</div>
                                </div>
                                <div class="text-[10px] text-zinc-500 font-mono">Expand</div>
                            </summary>
                            <div class="mt-2 text-xs text-zinc-200 whitespace-pre-wrap font-mono border-t border-zinc-800 pt-2">${escapeHtml(content)}</div>
                        </details>
                    `;
                }).join('')
                : `<div class="text-zinc-600 italic text-sm">No notes yet.</div>`;

            const el = document.createElement('div');
            el.className = 'space-y-3';
            el.innerHTML = `
                <div class="flex items-center justify-between">
                    <div class="text-zinc-400 text-xxs font-mono uppercase tracking-widest">New note</div>
                    <div class="text-zinc-600 text-[10px] font-mono">${notes.length} total</div>
                </div>
                <div class="grid grid-cols-2 gap-2">
                    <select id="note-kind" class="bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200">
                        <option>Call Note</option>
                        <option>Summary</option>
                    </select>
                    <input id="note-date" type="date" class="bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" value="${ymdToday()}">
                </div>
                <input id="note-title" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="Title (optional)">
                <textarea id="note-content" rows="5" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200 resize-none" placeholder="Write the note..."></textarea>
                <button id="btn-add-note" class="w-full bg-blue-600 hover:bg-blue-500 text-white px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors">Add</button>
                <div class="h-px bg-zinc-800"></div>
                <div class="space-y-2">${notesListHtml}</div>
            `;
            panel.appendChild(el);

            const addNoteBtn = el.querySelector('#btn-add-note');
            if (addNoteBtn) {
                addNoteBtn.onclick = async () => {
                    addNoteBtn.disabled = true;
                    try {
                        const kind = safeText(el.querySelector('#note-kind')?.value).trim() || 'Call Note';
                        const date = safeText(el.querySelector('#note-date')?.value).trim() || ymdToday();
                        const title = safeText(el.querySelector('#note-title')?.value).trim();
                        const content = safeText(el.querySelector('#note-content')?.value);
                        if (!content.trim()) throw new Error('Note content is required');
                        await addProjectNote(project.id, { kind, date, title, content });
                        await fetchState();
                        el.querySelector('#note-content').value = '';
                        el.querySelector('#note-title').value = '';
                        renderMain();
                    } catch (e) {
                        alert(e?.message || 'Failed to add note');
                    } finally {
                        addNoteBtn.disabled = false;
                    }
                };
            }
            return;
        }

        if (tab === 'inbox') {
            const linkedItems = getProjectLinkedInboxItems(project);
            const listHtml = linkedItems.length
                ? linkedItems.slice(0, 50).map((item) => {
                    const text = safeText(item?.text);
                    const stamp = safeText(item?.updatedAt || item?.createdAt);
                    const when = stamp ? formatTimeFromIso(stamp) : '';
                    return `
                        <div class="border border-zinc-800 rounded-md bg-zinc-950/30 p-3 space-y-2">
                            <div class="flex items-center justify-between gap-2">
                                <div class="flex items-center gap-2 min-w-0">
                                    ${inboxSourceBadge(item?.source)}
                                    ${inboxStatusBadge(item?.status)}
                                </div>
                                <div class="text-[10px] font-mono text-zinc-500">${escapeHtml(when)}</div>
                            </div>
                            <div class="text-xs text-zinc-200 whitespace-pre-wrap font-mono">${escapeHtml(text)}</div>
                        </div>
                    `;
                }).join('')
                : `<div class="text-zinc-600 italic text-sm">No linked inbox messages yet.</div>`;

            const el = document.createElement('div');
            el.className = 'space-y-3';
            el.innerHTML = `
                <div class="flex items-center justify-between">
                    <div class="text-zinc-400 text-xxs font-mono uppercase tracking-widest">Linked inbox messages</div>
                    <button id="btn-open-inbox-from-project" class="px-2.5 py-1.5 rounded border border-zinc-800 bg-zinc-950/30 text-[10px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-900/40 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Open Inbox</button>
                </div>
                <div class="text-zinc-600 text-[10px] font-mono">${linkedItems.length} linked item${linkedItems.length === 1 ? '' : 's'}</div>
                <div class="space-y-2">${listHtml}</div>
            `;
            panel.appendChild(el);

            const openInboxBtn = el.querySelector('#btn-open-inbox-from-project');
            if (openInboxBtn) {
                openInboxBtn.onclick = async () => {
                    await openInbox();
                };
            }
            return;
        }

        if (tab === 'comms') {
            const comms = Array.isArray(state.projectCommunications?.[project.id]) ? state.projectCommunications[project.id] : [];
            const commsListHtml = comms.length
                ? comms.slice(0, 25).map(c => {
                    const ctype = safeText(c.type) || 'other';
                    const direction = safeText(c.direction) || 'outbound';
                    const date = safeText(c.date);
                    const subject = safeText(c.subject) || 'No Subject';
                    const bodyText = safeText(c.body);
                    const preview = previewText(bodyText || subject, 220);
                    return `
                        <details class="border border-zinc-800 rounded-md bg-zinc-950/30 p-3">
                            <summary class="cursor-pointer list-none flex items-center justify-between gap-3">
                                <div class="min-w-0">
                                    <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest">${escapeHtml(ctype)} • ${escapeHtml(direction)}${date ? ` • ${escapeHtml(date)}` : ''}</div>
                                    <div class="mt-1 text-xs text-zinc-200 font-mono truncate">${escapeHtml(subject)}</div>
                                    <div class="mt-1 text-xs text-zinc-300 truncate">${escapeHtml(preview || '(empty)')}</div>
                                </div>
                                <div class="text-[10px] text-zinc-500 font-mono">Expand</div>
                            </summary>
                            ${bodyText ? `<div class=\"mt-2 text-xs text-zinc-300 whitespace-pre-wrap font-mono border-t border-zinc-800 pt-2\">${escapeHtml(bodyText)}</div>` : ''}
                        </details>
                    `;
                }).join('')
                : `<div class="text-zinc-600 italic text-sm">No communications yet.</div>`;

            const el = document.createElement('div');
            el.className = 'space-y-3';

            const draft = getTranscriptDraft(project.id);
            const proposal = draft?.proposal && typeof draft.proposal === 'object' ? draft.proposal : null;
            const actionItems = Array.isArray(proposal?.actionItems) ? proposal.actionItems : [];
            const decisions = Array.isArray(proposal?.decisions) ? proposal.decisions : [];
            const transcriptId = `transcript-text-${project.id}`;
            const analyzeId = `btn-transcript-analyze-${project.id}`;
            const applyId = `btn-transcript-apply-${project.id}`;
            const clearId = `btn-transcript-clear-${project.id}`;
            const copyRecapId = `btn-transcript-copy-recap-${project.id}`;
            const errorId = `transcript-error-${project.id}`;

            el.innerHTML = `
                <div class="border border-zinc-800 rounded-xl bg-zinc-900/20 p-4">
                    <div class="flex items-start justify-between gap-3">
                        <div>
                            <div class="text-white text-xs font-semibold">Transcript → Tasks + Recap</div>
                            <div class="text-[11px] text-zinc-500 mt-0.5">Paste a meeting transcript. Analyze, review, then apply (creates tasks + a Summary note + an outbound email draft).</div>
                        </div>
                    </div>
                    <textarea id="${escapeHtml(transcriptId)}" rows="6" class="mt-3 w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-xs font-mono text-zinc-200 resize-none" placeholder="Paste transcript here...">${escapeHtml(safeText(draft?.text))}</textarea>
                    <div class="flex flex-wrap gap-2 mt-2">
                        <button id="${escapeHtml(analyzeId)}" class="px-3 py-2 rounded-lg bg-blue-600/20 border border-blue-600/40 text-xs font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">${draft?.analyzing ? 'Analyzing...' : 'Analyze'}</button>
                        <button id="${escapeHtml(applyId)}" class="px-3 py-2 rounded-lg bg-emerald-600/15 border border-emerald-600/35 text-xs font-mono text-emerald-200 hover:bg-emerald-600/25 transition-colors transition-transform duration-150 ease-out active:translate-y-px">${draft?.applying ? 'Applying...' : `Apply (${actionItems.length || 0})`}</button>
                        <button id="${escapeHtml(clearId)}" class="px-3 py-2 rounded-lg border border-zinc-700 text-xs font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Clear</button>
                    </div>
                    <div id="${escapeHtml(errorId)}" class="mt-2 text-[11px] text-red-400 ${draft?.error ? '' : 'hidden'}">${escapeHtml(safeText(draft?.error))}</div>

                    ${proposal ? `
                        <div class="mt-3 border border-zinc-800 rounded-lg bg-zinc-950/20 p-3">
                            <div class="text-zinc-400 text-xxs font-mono uppercase tracking-widest">Proposal preview</div>
                            <div class="mt-2 text-xs text-zinc-200">${escapeHtml(safeText(proposal.summary))}</div>
                            ${decisions.length ? `
                                <div class="mt-3 text-zinc-400 text-xxs font-mono uppercase tracking-widest">Decisions</div>
                                <div class="mt-1 space-y-1">${decisions.slice(0, 8).map((d) => `<div class=\"text-xs text-zinc-200\">• ${escapeHtml(safeText(d))}</div>`).join('')}</div>
                            ` : ''}
                            ${actionItems.length ? `
                                <div class="mt-3 text-zinc-400 text-xxs font-mono uppercase tracking-widest">Action items</div>
                                <div class="mt-1 space-y-1">${actionItems.slice(0, 10).map((a) => {
                                    const title = safeText(a?.title);
                                    const due = safeText(a?.dueDate);
                                    const pr = Number(a?.priority) || 2;
                                    return `<div class=\"text-xs text-zinc-200\">• [P${pr}] ${escapeHtml(title)}${due ? ` <span class=\\\"text-zinc-500\\\">(due ${escapeHtml(due)})</span>` : ''}</div>`;
                                }).join('')}</div>
                            ` : ''}
                            <div class="mt-3 text-zinc-400 text-xxs font-mono uppercase tracking-widest">Recap email draft</div>
                            <div class="mt-1 text-xs font-mono text-zinc-200">Subject: ${escapeHtml(safeText(proposal.recapSubject))}</div>
                            <div class="mt-2 flex justify-end">
                                <button id="${escapeHtml(copyRecapId)}" class="px-2.5 py-1.5 rounded border border-zinc-800 bg-zinc-950/30 text-[10px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-900/40 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Copy recap</button>
                            </div>
                            <div class="mt-2 text-xs text-zinc-300 whitespace-pre-wrap font-mono">${escapeHtml(safeText(proposal.recapBody))}</div>
                        </div>
                    ` : ''}
                </div>

                <div class="grid grid-cols-2 gap-2">
                    <select id="comms-type" class="bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200">
                        <option value="email">email</option>
                        <option value="call">call</option>
                        <option value="quo">quo</option>
                        <option value="other">other</option>
                    </select>
                    <select id="comms-direction" class="bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200">
                        <option value="outbound">outbound</option>
                        <option value="inbound">inbound</option>
                    </select>
                </div>
                <input id="comms-date" type="date" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" value="${ymdToday()}">
                <input id="comms-subject" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200" placeholder="Subject">
                <textarea id="comms-body" rows="4" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200 resize-none" placeholder="Notes / summary (optional)"></textarea>
                <button id="btn-add-comms" class="w-full bg-zinc-800 hover:bg-zinc-700 text-zinc-200 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors">Add</button>
                <div class="h-px bg-zinc-800"></div>
                <div class="space-y-2">${commsListHtml}</div>
            `;
            panel.appendChild(el);

            // Transcript actions
            const transcriptEl = el.querySelector(`#${CSS.escape(transcriptId)}`);
            if (transcriptEl) {
                transcriptEl.addEventListener('input', () => {
                    setTranscriptDraft(project.id, { text: transcriptEl.value });
                });
            }

            const setDraftError = (msg) => {
                setTranscriptDraft(project.id, { error: safeText(msg) });
                const err = el.querySelector(`#${CSS.escape(errorId)}`);
                if (err) {
                    err.textContent = safeText(msg);
                    err.classList.toggle('hidden', !safeText(msg).trim());
                }
            };

            const analyzeBtn = el.querySelector(`#${CSS.escape(analyzeId)}`);
            if (analyzeBtn) {
                analyzeBtn.disabled = !!draft?.analyzing;
                analyzeBtn.onclick = async () => {
                    const txt = safeText(transcriptEl?.value).trim();
                    if (!txt) return setDraftError('Paste a transcript first.');
                    setDraftError('');
                    setTranscriptDraft(project.id, { analyzing: true, proposal: null });
                    renderMain();
                    try {
                        const data = await apiJson(`/api/projects/${encodeURIComponent(project.id)}/transcript/analyze`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ transcript: txt })
                        });
                        setTranscriptDraft(project.id, { analyzing: false, proposal: data?.proposal || null });
                    } catch (e) {
                        setTranscriptDraft(project.id, { analyzing: false });
                        setDraftError(e?.message || 'Failed to analyze transcript');
                    }
                    renderMain();
                };
            }

            const clearBtn = el.querySelector(`#${CSS.escape(clearId)}`);
            if (clearBtn) {
                clearBtn.onclick = () => {
                    setTranscriptDraft(project.id, { text: '', proposal: null, analyzing: false, applying: false, error: '' });
                    renderMain();
                };
            }

            const applyBtn = el.querySelector(`#${CSS.escape(applyId)}`);
            if (applyBtn) {
                applyBtn.disabled = !!draft?.applying || !proposal || !actionItems.length;
                applyBtn.onclick = async () => {
                    const txt = safeText(transcriptEl?.value).trim();
                    if (!proposal) return setDraftError('Analyze first.');
                    const count = Array.isArray(proposal.actionItems) ? proposal.actionItems.length : 0;
                    if (!count) return setDraftError('No action items to apply.');

                    const ok = confirm(`Apply this proposal?\n\n- Create ${count} task(s)\n- Add a Summary note\n- Draft an outbound email in Comms`);
                    if (!ok) return;

                    setDraftError('');
                    setTranscriptDraft(project.id, { applying: true });
                    renderMain();
                    try {
                        const data = await apiJson(`/api/projects/${encodeURIComponent(project.id)}/transcript/apply`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ baseRevision: state.revision, transcript: txt, proposal })
                        });
                        if (data?.store) applyStore(data.store);
                        setTranscriptDraft(project.id, { text: '', proposal: null, analyzing: false, applying: false, error: '' });
                        renderNav();
                        renderMain();
                    } catch (e) {
                        setTranscriptDraft(project.id, { applying: false });
                        setDraftError(e?.message || 'Failed to apply proposal');
                        renderMain();
                    }
                };
            }

            const copyBtn = el.querySelector(`#${CSS.escape(copyRecapId)}`);
            if (copyBtn) {
                copyBtn.disabled = !proposal;
                copyBtn.onclick = async () => {
                    if (!proposal) return;
                    const subject = safeText(proposal?.recapSubject).trim();
                    const body = safeText(proposal?.recapBody).trimEnd();
                    const combined = `${subject ? `Subject: ${subject}\n\n` : ''}${body}`.trim();
                    if (!combined) return;
                    try {
                        await navigator.clipboard.writeText(combined);
                        copyBtn.textContent = 'Copied';
                        setTimeout(() => { try { copyBtn.textContent = 'Copy recap'; } catch {} }, 900);
                    } catch {
                        prompt('Copy recap:', combined);
                    }
                };
            }

            const addCommsBtn = el.querySelector('#btn-add-comms');
            if (addCommsBtn) {
                addCommsBtn.onclick = async () => {
                    addCommsBtn.disabled = true;
                    try {
                        const ctype = safeText(el.querySelector('#comms-type')?.value).trim();
                        const direction = safeText(el.querySelector('#comms-direction')?.value).trim();
                        const date = safeText(el.querySelector('#comms-date')?.value).trim() || ymdToday();
                        const subject = safeText(el.querySelector('#comms-subject')?.value).trim();
                        const bodyText = safeText(el.querySelector('#comms-body')?.value);
                        await addProjectCommunication(project.id, { type: ctype, direction, date, subject, body: bodyText });
                        el.querySelector('#comms-subject').value = '';
                        el.querySelector('#comms-body').value = '';
                        renderMain();
                    } catch (e) {
                        alert(e?.message || 'Failed to add communication');
                    } finally {
                        addCommsBtn.disabled = false;
                    }
                };
            }
            return;
        }
    };

    side.appendChild(tabRow);
    side.appendChild(panel);

    body.appendChild(tasksCard);
    body.appendChild(side);

    wrap.appendChild(subheader);
    wrap.appendChild(body);
    container.appendChild(wrap);

    // Wire up header actions
    const btnNewTask = subheader.querySelector('#btn-new-task');
    if (btnNewTask) btnNewTask.onclick = () => promptNewTask(project);

    const btnAuto = subheader.querySelector('#btn-auto-delegate');
    if (btnAuto) btnAuto.onclick = () => autoDelegate(project);

    const chk = subheader.querySelector('#chk-show-completed');
    if (chk) {
        chk.onchange = (e) => {
            state.showCompleted = !!e.target.checked;
            renderMain();
        };
    }

    const quickOpenCode = subheader.querySelector('#proj-quick-open-code');
    if (quickOpenCode) {
        quickOpenCode.onclick = async () => {
            quickOpenCode.disabled = true;
            try {
                await launchVsCodeFolder(ws);
            } catch (e) {
                alert(e?.message || 'Failed to open VS Code');
            } finally {
                quickOpenCode.disabled = false;
            }
        };
    }

    // Wire tabs
    tabRow.querySelectorAll('button[data-tab]').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = safeText(btn.getAttribute('data-tab')).trim();
            if (!tab) return;
            setActiveProjectTab(project.id, tab);
            renderMain();
        });
    });

    renderPanel();
}

function escapeHtml(str) {
    return String(str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function createTaskRow(task, showProjectLabel = false) {
    const div = document.createElement("div");
    div.className = `task-row-container relative z-10 group flex items-center gap-4 p-3 rounded border border-zinc-800/50 bg-zinc-900/30 hover:bg-zinc-800/50 hover:border-zinc-700 transition-all ${isDoneTask(task) ? 'opacity-50' : ''}`;

    // Status Checkbox
    const checkbox = document.createElement("div");
    checkbox.className = `min-w-[1.25rem] h-5 rounded border ${isDoneTask(task) ? 'bg-emerald-500 border-emerald-500' : 'border-zinc-600 hover:border-blue-500'} cursor-pointer flex items-center justify-center transition-colors`;
    checkbox.innerHTML = isDoneTask(task) ? '<i class="fa-solid fa-check text-white text-xs"></i>' : '';
    checkbox.onclick = (e) => { e.stopPropagation(); toggleTaskStatus(task); };
    
    // Content
    const content = document.createElement("div");
    content.className = "flex-1 min-w-0";
    
    const titleRow = document.createElement("div");
    titleRow.className = "flex items-center gap-2 mb-0.5";
    
    const title = document.createElement("span");
    title.className = `text-sm font-medium truncate ${isDoneTask(task) ? 'text-zinc-500 line-through' : 'text-zinc-200'}`;
    title.innerText = task.title;
    
    titleRow.appendChild(title);
    
    // Priority Badge
    if (Number(task.priority) === 1) {
        const badge = document.createElement("span");
        badge.className = "px-1.5 py-0.5 rounded text-[10px] bg-red-500/10 text-red-500 border border-red-500/20 uppercase tracking-wide";
        badge.innerText = "P1";
        titleRow.appendChild(badge);
    } else if (Number(task.priority) === 2) {
        const badge = document.createElement("span");
        badge.className = "px-1.5 py-0.5 rounded text-[10px] bg-amber-500/10 text-amber-400 border border-amber-500/20 uppercase tracking-wide";
        badge.innerText = "P2";
        titleRow.appendChild(badge);
    }

    // Due Date Badge
    const dateStr = task.dueDate ? new Date(task.dueDate).toLocaleDateString() : null;
    if (dateStr) {
         const dateBadge = document.createElement("span");
         dateBadge.className = "px-1.5 py-0.5 rounded text-[10px] bg-blue-500/10 text-blue-400 border border-blue-500/20 flex items-center gap-1";
         dateBadge.innerHTML = `<i class="fa-regular fa-clock"></i>${dateStr}`;
         titleRow.appendChild(dateBadge);
    }
    
    content.appendChild(titleRow);

    // Assignee Row & Meta
    const metaRow = document.createElement("div");
    metaRow.className = "flex items-center gap-4 text-xs text-zinc-500";
    
    // Assignee
    const ownerName = (typeof task.owner === 'string' && task.owner.trim()) ? task.owner.trim() : "Unassigned";
    const ownerObj = (Array.isArray(state.team) ? state.team : []).find(u => u.name === ownerName);
    const avatarTxt = ownerObj?.avatar ? ownerObj.avatar : ownerName[0];
    
    const assigneeHtml = `
          <div class="flex items-center gap-1.5 cursor-pointer relative" title="Delegate Task">
              <button class="flex items-center gap-1.5 px-2 py-1 rounded bg-zinc-800/50 hover:bg-zinc-700/80 border border-zinc-700/50 text-xs transition-colors">
                  <div class="w-4 h-4 rounded-full bg-zinc-600 flex items-center justify-center text-[10px] text-white font-bold uppercase">${avatarTxt}</div>
                  <span class="text-zinc-300 font-medium">${ownerName === 'Unassigned' ? 'Delegate' : ownerName}</span>
                  <i class="fa-solid fa-chevron-down text-[8px] text-zinc-500 ml-1"></i>
              </button>
              <div class="assignee-dropdown hidden absolute top-full left-0 mt-1 w-64 bg-zinc-800 border border-zinc-700 rounded-lg shadow-xl z-50 overflow-hidden text-sm max-h-64 overflow-y-auto">
                <div class="px-3 py-2 text-xs font-bold text-zinc-400 uppercase tracking-widest border-b border-zinc-700 bg-zinc-900/50">Delegate to...</div>
                <div class="p-1 dropdown-list flex flex-col gap-0.5"></div>
              </div>
          </div>
      `;

      metaRow.innerHTML = assigneeHtml;
      const reassignEl = metaRow.querySelector('div[title="Delegate Task"]');
      const dropdownEl = metaRow.querySelector('.assignee-dropdown');
      const dropdownListEl = metaRow.querySelector('.dropdown-list');
      
      if (reassignEl && dropdownEl && dropdownListEl) {
          reassignEl.addEventListener('click', async (e) => {
              e.stopPropagation();
              
              // Toggle or close others
              document.querySelectorAll('.assignee-dropdown:not(.hidden)').forEach(el => {
                  if (el !== dropdownEl) el.classList.add('hidden');
              });
                
                // Reset z-indexes globally to prevent clipping
                document.querySelectorAll('.task-row-container').forEach(el => el.style.zIndex = '10');

                const isHidden = dropdownEl.classList.contains('hidden');
                if (!isHidden) {
                    dropdownEl.classList.add('hidden');
                    div.style.zIndex = '10';
                    return;
                }

                dropdownEl.classList.remove('hidden');
                div.style.zIndex = '50';
                dropdownListEl.innerHTML = '';
                
                const humans = getHumanTeamMembers();
                const names = humans.map((m) => m.name);

              if (!names.length) {
                  dropdownListEl.innerHTML = '<div class="px-2 py-2 text-zinc-500 text-xs italic">No team members.</div>';
                  return;
              }
              
              // Close when clicking outside
              const closeDropdown = (ec) => {
                  if (!reassignEl.contains(ec.target)) {
                      dropdownEl.classList.add('hidden');
                      div.style.zIndex = '10';
                      document.removeEventListener('click', closeDropdown);
                  }
              };
              document.addEventListener('click', closeDropdown);

              const counts = getOpenTaskCountByOwner();

              const addOption = (picked, display, subtext) => {
                  const opt = document.createElement('div');
                  opt.className = 'w-full text-left px-2 py-1.5 rounded hover:bg-zinc-700 transition-colors flex items-center justify-between cursor-pointer';
                  opt.innerHTML = `<span class="font-medium">${escapeHtml(display)}</span> ${subtext ? `<span class="text-[10px] text-zinc-500 flex items-center gap-1.5">${subtext}</span>` : ''}`;

                  opt.onclick = async (ev) => {
                      ec = ev || window.event;
                      ec.stopPropagation();
                      dropdownEl.classList.add('hidden');
                      document.removeEventListener('click', closeDropdown);
                      
                      if (!picked) return;
                      
                      const limit = getWipLimitForOwner(picked);
                      const current = Number(counts[picked] || 0);
                      if (current >= limit && picked !== 'Unassigned') {
                          const ok = confirm(`${picked} is at WIP limit (${current}/${limit}). Assign anyway?`);
                          if (!ok) return;
                      }

                      const res = await apiFetch(`/api/tasks/${task.id}`, {
                          method: 'PUT',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({ baseRevision: state.revision, patch: { owner: picked === 'Unassigned' ? '' : picked } })
                      });
                      if (!res.ok) {
                          await fetchState();
                          alert('Failed to reassign (store changed). Try again.');
                          return;
                      }
                      const store = await res.json();
                      applyStore(store);
                      renderNav();
                      renderMain();
                  };
                  dropdownListEl.appendChild(opt);
              };
              
              addOption('Unassigned', 'Unassigned', '');
              
              const scored = humans.map((m) => {
                  const name = safeText(m?.name).trim();
                  const current = Number(counts[name] || 0);
                  const limit = Number(m?.wipLimit) > 0 ? Number(m.wipLimit) : Infinity;
                  const remaining = limit === Infinity ? Infinity : (limit - current);
                  const skill = computeSkillScore(task, m);
                  return { name, current, limit, remaining, skill };
              }).filter((x) => x.name);

              scored.sort((a, b) => {
                  if (b.skill !== a.skill) return b.skill - a.skill;
                  return a.current - b.current;
              });
              
              const topCandidate = (scored.length > 0 && scored[0].remaining > 0) ? scored[0].name : null;

              scored.forEach(c => {
                  let sub = `${c.current} tasks`;
                  if (c.remaining <= 0) sub = `<span class="text-amber-500">${c.current}/${c.limit} max</span>`;
                  
                  let htmlSub = sub;
                  if (c.name === topCandidate) {
                      htmlSub = `<span class="px-1.5 py-0.5 rounded bg-blue-500/20 text-blue-400 text-[9px] uppercase tracking-wider font-bold shadow-[0_0_8px_rgba(59,130,246,0.3)]"><i class="fa-solid fa-bolt mr-1"></i>Rec</span>` + htmlSub;
                  }

                  addOption(c.name, c.name, htmlSub);
              });
          });
      }

    if (showProjectLabel) {
        metaRow.innerHTML += `<span class="text-zinc-600">•</span><span>${task.project || "General"}</span>`;
    }
    
    content.appendChild(metaRow);
    
    // Actions (Delete)
    const actions = document.createElement("div");
    actions.className = "opacity-0 group-hover:opacity-100 transition-opacity flex gap-2";
    
    const delBtn = document.createElement("button");
    delBtn.className = "text-zinc-600 hover:text-red-500 transition-colors p-2 rounded hover:bg-zinc-800";
    delBtn.innerHTML = `<i class="fa-solid fa-trash"></i>`;
    delBtn.onclick = (e) => { e.stopPropagation(); renderDelete(task.id); };
    
    actions.appendChild(delBtn);
    
    div.appendChild(checkbox);
    div.appendChild(content);
    div.appendChild(actions);
    
    return div;
}

/* --- Actions --- */

async function toggleTaskStatus(task) {
    const newStatus = isDoneTask(task) ? 'Next' : 'Done';
    // Optimistic Update
    task.status = newStatus;
    renderMain();
    
    try {
        const res = await apiFetch(`/api/tasks/${task.id}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ baseRevision: state.revision, patch: { status: newStatus } })
        });
        if (res.ok) {
            const store = await res.json();
            applyStore(store);
            renderNav();
            renderMain();
        } else {
            await fetchState();
        }
    } catch (e) {
        console.error("Update failed", e);
        fetchState(); // Revert on fail
    }
}

function setTaskStatusLocal(taskId, status) {
    const id = safeText(taskId).trim();
    const nextStatus = safeText(status).trim();
    if (!id || !nextStatus) return;
    state.tasks = (Array.isArray(state.tasks) ? state.tasks : []).map((task) => {
        if (safeText(task?.id) !== id) return task;
        return { ...task, status: nextStatus };
    });
}

function clearTaskDoneUndoTimer(taskId) {
    const id = safeText(taskId).trim();
    if (!id) return;
    const existing = taskDoneUndoTimers.get(id);
    if (existing) {
        clearTimeout(existing);
        taskDoneUndoTimers.delete(id);
    }
}

async function finalizeTaskDoneWithUndo(taskId) {
    const id = safeText(taskId).trim();
    if (!id) return;
    const pending = state.taskDoneUndoById?.[id];
    if (!pending) return;

    clearTaskDoneUndoTimer(id);
    const nextMap = { ...(state.taskDoneUndoById || {}) };
    delete nextMap[id];
    state.taskDoneUndoById = nextMap;

    try {
        const store = await withRevisionRetry(() => apiJson(`/api/tasks/${encodeURIComponent(id)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ baseRevision: state.revision, patch: { status: 'Done' } })
        }));
        applyStore(store);
        renderNav();
    } catch {
        await fetchState();
    }

    renderMain();
}

function undoTaskDoneWithUndo(taskId) {
    const id = safeText(taskId).trim();
    if (!id) return;
    const pending = state.taskDoneUndoById?.[id];
    if (!pending) return;

    clearTaskDoneUndoTimer(id);
    const nextMap = { ...(state.taskDoneUndoById || {}) };
    delete nextMap[id];
    state.taskDoneUndoById = nextMap;

    setTaskStatusLocal(id, safeText(pending.prevStatus) || 'Next');
    renderMain();
}

function scheduleTaskDoneWithUndo(task) {
    const id = safeText(task?.id).trim();
    if (!id) return;
    if (state.taskDoneUndoById?.[id]) return;

    const prevStatus = safeText(task?.status) || 'Next';
    const expiresAt = Date.now() + 5000;
    const meta = {
        prevStatus,
        expiresAt,
        title: safeText(task?.title),
        project: safeText(task?.project),
    };

    state.taskDoneUndoById = { ...(state.taskDoneUndoById || {}), [id]: meta };
    setTaskStatusLocal(id, 'Done');
    renderMain();

    clearTaskDoneUndoTimer(id);
    const timer = setTimeout(() => {
        finalizeTaskDoneWithUndo(id);
    }, 5000);
    taskDoneUndoTimers.set(id, timer);
}

async function renderDelete(id) {
    if(!confirm("Delete this task?")) return;
    
    // Optimistic
    state.tasks = state.tasks.filter(t => t.id !== id);
    renderMain();
    
    try {
        const res = await apiFetch(`/api/tasks/${id}`, {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ baseRevision: state.revision })
        });
        if (res.ok) {
            const store = await res.json();
            applyStore(store);
            renderNav();
            renderMain();
        } else {
            await fetchState();
        }
    } catch(e) {
        fetchState();
    }
}

async function createNewProjectPrompt(prefill) {
    state.showNewProjectIntake = true;

    const p = (prefill && typeof prefill === 'object') ? prefill : null;
    if (p) {
        const patch = {};
        const name = safeText(p.clientName).trim();
        const phone = safeText(p.clientPhone).trim();
        if (name) patch.clientName = normalizeClientLabel(name);
        if (phone) patch.clientPhone = phone;
        if (Object.keys(patch).length) setNewProjectDraft(patch);
    }

    await openProjects();
    setTimeout(() => {
        const nameEl = document.getElementById('np-name');
        try { nameEl?.focus(); } catch {}
    }, 50);
}

async function promptNewTask(project) {
    const title = prompt("Task Directive:");
    if (!title) return;
    
    // Simple prompt for now, could be a modal later
    const humans = getHumanTeamMembers();
    const names = humans.map((m) => m.name);
    const owner = prompt(names.length ? `Assign Owner (${names.join(', ')}):` : 'Assign Owner:', "");
    // Default next day
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const defaultDate = tomorrow.toISOString().split('T')[0];

    const dueDate = await promptDatePicker({
        title: 'Task Due Date',
        label: 'Due date',
        defaultValue: defaultDate
    });
    
    const res = await apiFetch("/api/tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            baseRevision: state.revision,
            task: {
                title: title.trim(),
                project: project?.name || "Other",
                type: "Other",
                owner: owner || "",
                status: "Next",
                priority: 2,
                dueDate: (dueDate || "").trim()
            }
        })
    });

    if (res.ok) {
        const store = await res.json();
        applyStore(store);
        renderMain();
    } else {
        await fetchState();
    }
}

async function autoDelegate(project) {
    if (!confirm("Run auto-delegation? AI will assign tasks based on workload.")) return;
    
    const btn = document.getElementById("btn-auto-delegate");
    const originalText = btn.innerHTML;
    btn.innerHTML = `<i class="fa-solid fa-circle-notch fa-spin mr-2"></i>Optimizing...`;
    
    try {
        const projectName = project?.name;
        if (!projectName) throw new Error('Missing project');

        const candidates = getHumanTeamMembers();
        const owners = candidates.map(u => u.name).filter(Boolean);
        if (owners.length === 0) throw new Error('No team members configured');

        const unowned = state.tasks.filter(t => t.project === projectName && (!t.owner || !String(t.owner).trim()) && !isDoneTask(t));
        if (unowned.length === 0) {
            alert('No unassigned tasks found.');
            return;
        }

        const counts = getOpenTaskCountByOwner();

        const pickOwnerForTask = (task) => {
            const scored = candidates
                .map((m) => {
                    const name = safeText(m?.name).trim();
                    const current = Number(counts[name] || 0);
                    const limit = Number(m?.wipLimit) > 0 ? Number(m.wipLimit) : Infinity;
                    const remaining = limit === Infinity ? Infinity : (limit - current);
                    const skill = computeSkillScore(task, m);
                    return { name, current, limit, remaining, skill };
                })
                .filter((x) => x.name)
                .filter((x) => x.remaining > 0);

            if (!scored.length) return '';
            scored.sort((a, b) => {
                // Highest skill match first, then least loaded
                if (b.skill !== a.skill) return b.skill - a.skill;
                return a.current - b.current;
            });
            return scored[0].name;
        };

        // Sequentially apply patches, updating baseRevision each time.
        let assigned = 0;
        for (let i = 0; i < unowned.length; i++) {
            const t = unowned[i];
            const nextOwner = pickOwnerForTask(t);
            if (!nextOwner) {
                alert('All team members are at WIP limit. Increase limits or complete tasks, then retry.');
                break;
            }
            const res = await apiFetch(`/api/tasks/${t.id}`,
                {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ baseRevision: state.revision, patch: { owner: nextOwner } })
                }
            );
            if (!res.ok) {
                // likely revision mismatch; reload and abort
                await fetchState();
                alert('Auto-delegate stopped due to revision mismatch. Try again.');
                return;
            }
            const store = await res.json();
            applyStore(store);
            counts[nextOwner] = Number(counts[nextOwner] || 0) + 1;
            assigned += 1;
        }

        if (assigned) alert(`Delegation Complete: ${assigned} tasks assigned.`);
        
    } catch (e) {
        console.error(e);
        alert("Auto-delegation failed.");
    } finally {
        btn.innerHTML = originalText;
        fetchState();
    }
}

/* --- Chat Interface --- */

function toggleChat() {
    const drawer = document.getElementById('neural-drawer');
    const docked = drawer?.dataset?.marcusDocked === '1';

    // When M.A.R.C.U.S. is persistently docked, keep it visible.
    if (docked) {
        state.isChatOpen = true;
        dockMarcusToPersistentSlot();
        applyMarcusOpenState(true);
        setStoredMarcusOpen(true);
        const input = document.getElementById('cmd-input');
        input?.focus?.();
        return;
    }

    state.isChatOpen = !state.isChatOpen;
    applyMarcusOpenState(state.isChatOpen);
    setStoredMarcusOpen(state.isChatOpen);
}

async function handleChatSubmit() {
    const input = document.getElementById("cmd-input");
    const msg = safeText(input?.value).trim();
    if (!msg) return;
    
    if (input) input.value = "";
    
    // Add User Msg
    recordChatMessage("user", msg);
    addChatMessage("user", msg);
    
    // Show Thinking
    const status = document.getElementById("ai-status");
    if(status) status.style.opacity = "1";
    setMarcusPresence('busy');
    showMarcusTypingIndicator();
    
    try {
        const threadId = state.currentProjectId ? 'default' : (state.chatThreadId || 'default');
        const res = await apiFetch("/api/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ 
                message: msg,
                projectId: state.currentProjectId || undefined,
                threadId,
            })
        });

        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data?.error || `Request failed (${res.status})`);
        }

        const reply = data.reply || data.text || "Command Processed.";
        removeMarcusTypingIndicator();
        setMarcusPresence('responding');
        recordChatMessage("ai", reply);
        addChatMessage("ai", reply, true);
        speakMarcus(reply);
        
        // Refresh state in case AI changed things
        await fetchState();
        await loadChatHistory();
        renderChat();
        renderNav();
        renderMain();
        
    } catch (e) {
        removeMarcusTypingIndicator();
        const raw = safeText(e?.message || '').trim();
        const lower = raw.toLowerCase();
        let friendly = raw || 'Connection severed.';
        if (lower.includes('unauthorized') || lower.includes('invalid admin token')) {
            friendly = 'Unauthorized — paste ADMIN_TOKEN when prompted (reload if you dismissed it).';
        }
        recordChatMessage("ai", `Error: ${friendly}`);
        addChatMessage("ai", `Error: ${friendly}`);
    } finally {
        if(status) status.style.opacity = "0";
        setMarcusPresence('idle');
    }
}

function recordChatMessage(role, text) {
    const entry = { role: normalizeRole(role), content: String(text || "") };
    if (state.currentProjectId) {
        state.chatHistory.push(entry);
    } else {
        if ((state.chatThreadId || 'default') === 'operator_bio') {
            state.operatorBioChatHistory.push(entry);
            state.chatHistory = state.operatorBioChatHistory;
        } else {
            state.globalChatHistory.push(entry);
            state.chatHistory = state.globalChatHistory;
        }
    }

    publishMarcusSync('chat-entry', {
        projectId: safeText(state.currentProjectId || ''),
        threadId: safeText(state.currentProjectId ? 'default' : (state.chatThreadId || 'default')),
        entry,
    });
}

function addChatMessage(role, text, animate = false) {
    const stream = document.getElementById("chat-stream");
    if(!stream) return;
    if (role === 'ai') removeMarcusTypingIndicator();

    const div = document.createElement("div");
    div.className = "flex flex-col gap-1.5 mb-5 animate-fade-in";

    // Glass style bubbles
    const bubbleClasses = role === 'ai' 
        ? 'bg-ops-surface/80 backdrop-blur border border-white/10 text-white rounded-br-2xl rounded-bl-sm rounded-t-2xl px-4 py-3 shadow-md'
        : 'bg-blue-600/20 backdrop-blur border border-blue-500/30 text-blue-50 rounded-bl-2xl rounded-br-sm rounded-t-2xl px-4 py-3 self-end shadow-md';

    const header = document.createElement("span");
    header.className = `text-[9px] uppercase font-bold tracking-widest ${role === 'ai' ? 'text-blue-400 ml-1' : 'text-zinc-400 text-right mr-1'}`;
    const threadLabel = state.currentProjectId
        ? 'DIRECT'
        : ((state.chatThreadId || 'default') === 'operator_bio' ? 'BIO' : 'DIRECT');
    header.innerText = role === 'ai' ? `M.A.R.C.U.S. // ${threadLabel}` : 'Operator';

    const bubble = document.createElement("div");
    bubble.className = `text-[13px] leading-relaxed max-w-[85%] break-words ${bubbleClasses}`;
    
    // Parse Markdown safely
    const parsedHTML = typeof marked === 'function' ? marked.parse(text) : escapeHtml(text).replace(/\n/g, '<br/>');

    if (animate && role === 'ai') {
        bubble.innerHTML = '';
        div.appendChild(header);
        div.appendChild(bubble);
        stream.appendChild(div);
        stream.scrollTop = stream.scrollHeight;
        
        // Typewriter effect using HTML safe streaming
        let tempDiv = document.createElement('div');
        tempDiv.innerHTML = parsedHTML;
        
        // Very basic text walker for typewriter
        let textNodes = [];
        let htmlClone = tempDiv.cloneNode(true);
        let currentPos = 0;
        let speed = 8; // ms per char
        
        // Simplified raw typewriter
        bubble.innerHTML = '<span class="typing-cursor"></span>';
        let rawHtml = parsedHTML;
        let i = 0;
        
        function typeWriter() {
            if (i < rawHtml.length) {
                // Skip HTML tags
                if (rawHtml.charAt(i) === '<') {
                    let tagEnd = rawHtml.indexOf('>', i);
                    if (tagEnd !== -1) {
                        i = tagEnd + 1;
                    }
                }
                bubble.innerHTML = rawHtml.substring(0, i) + '<span class="inline-block w-1.5 h-3 ml-0.5 align-middle bg-blue-400 animate-pulse"></span>';
                i++;
                stream.scrollTop = stream.scrollHeight;
                setTimeout(typeWriter, speed);
            } else {
                bubble.innerHTML = rawHtml;
                stream.scrollTop = stream.scrollHeight;
            }
        }
        typeWriter();
    } else {
        bubble.innerHTML = parsedHTML;
        div.appendChild(header);
        div.appendChild(bubble);
        stream.appendChild(div);
    }
    
    stream.scrollTop = stream.scrollHeight;
}

function renderChat() {
    const stream = document.getElementById("chat-stream");
    if(!stream) return;
    
    stream.innerHTML = "";
    if (state.chatHistory.length) {
        state.chatHistory.forEach(c => addChatMessage(c.role, c.content));
    } else {
        stream.innerHTML = `<div class="text-center mt-10 opacity-30">
            <i class="fa-solid fa-terminal text-2xl mb-2"></i>
            <p>M.A.R.C.U.S. is online — sharp, curious, and ready to work.</p>
        </div>`;
    }
}

function computeFocusNudgeSnapshot() {
    const now = Date.now();
    const last = Number(state.lastInteractionAt || 0);
    const idleMs = last ? Math.max(0, now - last) : 60 * 60 * 1000;
    const idleMinutes = Math.floor(idleMs / 60000);

    const today = ymdFromLocalDate(new Date());
    const allTasks = Array.isArray(state.tasks) ? state.tasks : [];
    const openTasks = allTasks.filter((t) => !isDoneTask(t));

    const urgentDue = openTasks
        .filter((t) => {
            const pr = Number(t?.priority) || 3;
            if (pr > 2) return false;
            const d = safeText(t?.dueDate).trim();
            return d && d <= today;
        })
        .sort((a, b) => {
            const ap = Number(a?.priority) || 3;
            const bp = Number(b?.priority) || 3;
            if (ap !== bp) return ap - bp;
            const ad = safeText(a?.dueDate).trim();
            const bd = safeText(b?.dueDate).trim();
            if (ad !== bd) return ad < bd ? -1 : 1;
            return safeText(a?.title).localeCompare(safeText(b?.title));
        });

    const inboxItems = getDisplayInboxItems();
    const inboxNew = inboxItems.filter((x) => String(x?.status || '').trim().toLowerCase() === 'new');

    const topTasks = urgentDue.slice(0, 3).map((t) => {
        const pr = Number(t?.priority) || 3;
        const title = safeText(t?.title).trim() || 'Untitled task';
        const project = safeText(t?.project).trim();
        const dueDate = safeText(t?.dueDate).trim();
        return { priority: pr, title, project, dueDate };
    });

    return {
        idleMinutes,
        inboxNewCount: inboxNew.length,
        openTasksCount: openTasks.length,
        urgentDueCount: urgentDue.length,
        topTasks,
    };
}

function shouldTriggerFocusNudge(snapshot) {
    if (!snapshot) return { ok: false, reason: '' };

    // Don't interrupt when you're already in a deliberate focus session.
    if (state.focusTimer?.running) return { ok: false, reason: '' };

    // Only nudge when there are real signals.
    const idle = Number(snapshot.idleMinutes) || 0;
    const inboxNew = Number(snapshot.inboxNewCount) || 0;
    const urgentDue = Number(snapshot.urgentDueCount) || 0;
    const openTasks = Number(snapshot.openTasksCount) || 0;

    if (idle >= 12 && (urgentDue > 0 || inboxNew >= 2)) {
        return { ok: true, reason: `Idle ${idle}m with ${urgentDue} urgent due tasks and ${inboxNew} new inbox items` };
    }
    if (idle >= 25 && (openTasks >= 12 || inboxNew >= 5)) {
        return { ok: true, reason: `Idle ${idle}m with ${openTasks} open tasks and ${inboxNew} new inbox items` };
    }
    if (urgentDue >= 4 && idle >= 5) {
        return { ok: true, reason: `${urgentDue} urgent tasks due/overdue` };
    }
    if (inboxNew >= 10 && idle >= 5) {
        return { ok: true, reason: `${inboxNew} new inbox items piling up` };
    }

    return { ok: false, reason: '' };
}

async function sendProactiveFocusNudge(reason, snapshot) {
    // Only do this from the main app context; avoid popout duplicating nudges.
    if (IS_MARCUS_POPOUT) return;

    // Skip if operator is in the Bio thread (training mode).
    if (!state.currentProjectId && (state.chatThreadId || 'default') === 'operator_bio') return;

    // Make sure Marcus is visible so this actually initiates a conversation.
    applyMarcusOpenState(true);
    setStoredMarcusOpen(true);

    const status = document.getElementById('ai-status');
    if (status) status.style.opacity = '1';
    setMarcusPresence('busy');
    showMarcusTypingIndicator();

    const top = Array.isArray(snapshot?.topTasks) ? snapshot.topTasks : [];
    const topLines = top.length
        ? top.map((t) => {
            const pr = Number(t?.priority) || 3;
            const title = safeText(t?.title).trim();
            const proj = safeText(t?.project).trim();
            const due = safeText(t?.dueDate).trim();
            return `- P${pr}: ${title}${proj ? ` (${proj})` : ''}${due ? ` due ${due}` : ''}`;
        }).join('\n')
        : '- (No urgent tasks detected.)';

    const prompt =
        `PROACTIVE FOCUS NUDGE (system event; do not mention this tag):\n` +
        `You are initiating the conversation because focus likely drifted.\n\n` +
        `Reason: ${String(reason || '').slice(0, 400)}\n` +
        `Signals: idleMinutes=${Number(snapshot?.idleMinutes) || 0}, inboxNew=${Number(snapshot?.inboxNewCount) || 0}, urgentDue=${Number(snapshot?.urgentDueCount) || 0}, openTasks=${Number(snapshot?.openTasksCount) || 0}\n\n` +
        `Top urgent tasks:\n${topLines}\n\n` +
        `Output format:\n` +
        `1) One blunt sentence that snaps me back.\n` +
        `2) Ask exactly ONE yes/no question to confirm the focus target.\n` +
        `3) Give a 3-step plan for the next 30 minutes.\n` +
        `If I give a weak excuse, push back. If I give logical reasoning for deviation, accept it and update the plan.`;

    try {
        const res = await apiFetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: prompt, threadId: 'default' }),
        });

        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data?.error || `Request failed (${res.status})`);

        const reply = data.reply || data.text || '';
        removeMarcusTypingIndicator();
        setMarcusPresence('responding');
        if (reply) {
            // Record only the AI message so it feels like Marcus initiated it.
            const prevThread = state.chatThreadId;
            state.chatThreadId = 'default';
            recordChatMessage('ai', reply);
            state.chatThreadId = prevThread;
            addChatMessage('ai', reply, true);
            speakMarcus(reply);
        }
    } catch (e) {
        removeMarcusTypingIndicator();
        const friendly = safeText(e?.message || '').trim() || 'Failed to send focus nudge.';
        const prevThread = state.chatThreadId;
        state.chatThreadId = 'default';
        recordChatMessage('ai', `Focus nudge failed: ${friendly}`);
        state.chatThreadId = prevThread;
        addChatMessage('ai', `Focus nudge failed: ${friendly}`);
    } finally {
        if (status) status.style.opacity = '0';
        setMarcusPresence('idle');
    }
}

function startProactiveFocusNudges() {
    try {
        if (state.__focusNudgeIntervalId) return;
        state.__focusNudgeIntervalId = setInterval(async () => {
            try {
                if (document.visibilityState && document.visibilityState !== 'visible') return;
                if (state.auth.required && !state.auth.authenticated) return;
                if (isUserEditingNow()) return;

                // Rate limit: at most once per 20 minutes.
                const now = Date.now();
                const last = getStoredMarcusFocusNudgeLastTs();
                if (last && (now - last) < (20 * 60 * 1000)) return;

                const snapshot = computeFocusNudgeSnapshot();
                const decision = shouldTriggerFocusNudge(snapshot);
                if (!decision.ok) return;

                setStoredMarcusFocusNudgeLastTs(now);
                await sendProactiveFocusNudge(decision.reason, snapshot);
            } catch {
                // ignore
            }
        }, 60 * 1000);

        // small delayed first check
        setTimeout(() => {
            try {
                state.lastInteractionAt = state.lastInteractionAt || Date.now();
            } catch {
                // ignore
            }
        }, 1500);
    } catch {
        // ignore
    }
}

async function loadChatHistory() {
    // Project-specific chat is persisted server-side.
    if (state.currentProjectId) {
        try {
            const res = await apiFetch(`/api/projects/${state.currentProjectId}/chat`);
            if (!res.ok) throw new Error('Failed to load chat');
            const data = await res.json();
            const history = Array.isArray(data.history) ? data.history : [];
            state.chatHistory = history.map(m => ({
                role: normalizeRole(m.role),
                content: typeof m.content === 'string' ? m.content : String(m.content || '')
            }));
        } catch {
            state.chatHistory = [];
        }
        return;
    }

    // Global chat: either local-only (default) or persisted (operator_bio).
    if ((state.chatThreadId || 'default') === 'operator_bio') {
        try {
            const res = await apiFetch('/api/chat/thread/operator_bio');
            if (!res.ok) throw new Error('Failed to load bio thread');
            const data = await res.json().catch(() => ({}));
            const history = Array.isArray(data?.history) ? data.history : [];
            state.operatorBioChatHistory = history.map(m => ({
                role: normalizeRole(m.role),
                content: typeof m.content === 'string' ? m.content : String(m.content || ''),
            }));
        } catch {
            // Keep whatever is currently in memory.
            state.operatorBioChatHistory = Array.isArray(state.operatorBioChatHistory)
                ? state.operatorBioChatHistory
                : [];
        }
        state.chatHistory = state.operatorBioChatHistory;
        return;
    }

    state.chatHistory = state.globalChatHistory;
}


