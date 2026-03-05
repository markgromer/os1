/* =========================================
   NEURAL OPS - CORE LOGIC V2.4 "REBIRTH"
   Restores: Team, Due Dates, Auto-Delegate, Rich Chat
   Compatible with Neural Ops V2 Shell
   ========================================= */

/* --- State Management --- */
const state = {
    revision: 1,
    updatedAt: "",

    currentView: "dashboard",
    currentProjectId: null,

    projects: [],
    tasks: [],
    inboxItems: [],

    projectScratchpads: {},
    projectNoteEntries: {},
    projectCommunications: {},

    projectRightTabById: {},
    dashboardCalls: { loading: false, fetchedAt: 0, error: '', events: [] },
    dashboardGhl: { loading: false, fetchedAt: 0, error: '', snapshot: null },

    chatHistory: [],
    globalChatHistory: [],
    isChatOpen: true,

    settings: {
        openaiModel: "gpt-4o-mini",
    },
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
        { id: "ai", name: "Neural Core", role: "ai", avatar: "AI" },
    ],
};

let pollIntervalId = null;

function stopPolling() {
    if (pollIntervalId) {
        try { clearInterval(pollIntervalId); } catch { /* ignore */ }
        pollIntervalId = null;
    }
}

function startPolling() {
    stopPolling();
    const seconds = Math.max(10, Number(state.uiPrefs.autoRefreshSeconds) || 30);
    pollIntervalId = setInterval(() => fetchState({ background: true }), seconds * 1000);
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
const ADMIN_TOKEN_STORAGE_KEY = 'opsAdminToken';

const MARTY_OPEN_STORAGE_KEY = 'opsMartyOpen';
const MARTY_DETACHED_STORAGE_KEY = 'opsMartyDetached';
const MARTY_PANEL_STORAGE_KEY = 'opsMartyPanel';
const MARTY_SYNC_CHANNEL = 'opsMartySync';
const MARTY_SYNC_STORAGE_KEY = 'opsMartySyncEvent';

const MARTY_PANEL_MIN_WIDTH = 320;
const MARTY_PANEL_MIN_HEIGHT = 420;
const MARTY_TYPING_ID = 'marty-typing-indicator';

const MARTY_THINKING_LINES = [
    'SCANNING',
    'SYNTHESIZING',
    'MODELING',
    'ROUTING',
    'EVALUATING',
];
const MARTY_RESPONDING_LINES = [
    'DRAFTING',
    'COMPILING',
    'TRANSMITTING',
    'CONFIRMING',
];

const IS_MARTY_POPOUT = (() => {
    try {
        return new URLSearchParams(window.location.search).get('martyPopout') === '1';
    } catch {
        return false;
    }
})();

const MARTY_INSTANCE_ID = (() => {
    try {
        const k = 'opsMartyInstanceId';
        const existing = String(sessionStorage.getItem(k) || '').trim();
        if (existing) return existing;
        const id = `marty_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;
        sessionStorage.setItem(k, id);
        return id;
    } catch {
        return `marty_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;
    }
})();

let martySyncChannel = null;
let martyDockRestore = null;

function setStoredMartyOpen(open) {
    try {
        localStorage.setItem(MARTY_OPEN_STORAGE_KEY, open ? '1' : '0');
    } catch {
        // ignore
    }
}

function getStoredMartyOpen() {
    try {
        const raw = String(localStorage.getItem(MARTY_OPEN_STORAGE_KEY) || '').trim().toLowerCase();
        if (!raw) return true;
        return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'open';
    } catch {
        return true;
    }
}

function getStoredMartyDetached() {
    try {
        const raw = String(localStorage.getItem(MARTY_DETACHED_STORAGE_KEY) || '').trim().toLowerCase();
        return raw === '1' || raw === 'true' || raw === 'yes';
    } catch {
        return false;
    }
}

function setStoredMartyDetached(detached) {
    try {
        localStorage.setItem(MARTY_DETACHED_STORAGE_KEY, detached ? '1' : '0');
    } catch {
        // ignore
    }
}

function syncMartyDetachedIndicator() {
    const el = document.getElementById('marty-detached-indicator');
    if (!el) return;
    el.classList.toggle('hidden', !getStoredMartyDetached());
}

function applyMartyOpenState(open) {
    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;
    const isOpen = Boolean(open);
    drawer.classList.toggle('hidden', !isOpen);
    drawer.classList.toggle('flex', isOpen);
    state.isChatOpen = isOpen;
}

function makeMartySyncEvent(type, payload = {}) {
    return {
        type: String(type || '').trim(),
        payload: (payload && typeof payload === 'object') ? payload : {},
        source: MARTY_INSTANCE_ID,
        ts: Date.now(),
    };
}

function publishMartySync(type, payload = {}) {
    const ev = makeMartySyncEvent(type, payload);
    if (martySyncChannel) {
        try { martySyncChannel.postMessage(ev); } catch {}
    }
    try {
        localStorage.setItem(MARTY_SYNC_STORAGE_KEY, JSON.stringify(ev));
    } catch {}
}

function sameChatEntry(a, b) {
    return safeText(a?.role) === safeText(b?.role) && safeText(a?.content) === safeText(b?.content);
}

async function applyMartyRemoteContext(payload) {
    const p = (payload && typeof payload === 'object') ? payload : {};
    const nextProjectId = safeText(p.currentProjectId || '');
    const nextView = safeText(p.currentView || 'dashboard') || 'dashboard';

    if (safeText(state.currentProjectId) === nextProjectId && safeText(state.currentView) === nextView) return;

    state.currentProjectId = nextProjectId || null;
    state.currentView = nextView;
    await loadChatHistory();
    renderChat();
}

function applyMartyRemoteChat(payload) {
    const p = (payload && typeof payload === 'object') ? payload : {};
    const entry = p.entry && typeof p.entry === 'object' ? p.entry : null;
    if (!entry) return;
    const targetProjectId = safeText(p.projectId || '');
    const localProjectId = safeText(state.currentProjectId || '');

    if (targetProjectId !== localProjectId) return;

    const normalizedEntry = { role: normalizeRole(entry.role), content: safeText(entry.content) };
    if (!normalizedEntry.content) return;

    const target = targetProjectId ? (Array.isArray(state.chatHistory) ? state.chatHistory : []) : (Array.isArray(state.globalChatHistory) ? state.globalChatHistory : []);
    const last = target[target.length - 1];
    if (last && sameChatEntry(last, normalizedEntry)) return;

    if (targetProjectId) {
        state.chatHistory = [...target, normalizedEntry];
    } else {
        state.globalChatHistory = [...target, normalizedEntry];
        state.chatHistory = state.globalChatHistory;
    }
    renderChat();
}

async function handleMartySyncEvent(ev) {
    const e = (ev && typeof ev === 'object') ? ev : null;
    if (!e || safeText(e.source) === MARTY_INSTANCE_ID) return;
    const type = safeText(e.type);

    if (type === 'context') {
        await applyMartyRemoteContext(e.payload);
        return;
    }
    if (type === 'chat-entry') {
        applyMartyRemoteChat(e.payload);
        return;
    }
    if (type === 'request-sync') {
        publishMartySync('sync-state', {
            currentProjectId: safeText(state.currentProjectId || ''),
            currentView: safeText(state.currentView || 'dashboard'),
            globalChatHistory: Array.isArray(state.globalChatHistory) ? state.globalChatHistory.slice(-30) : [],
        });
        return;
    }
    if (type === 'sync-state') {
        const payload = e.payload && typeof e.payload === 'object' ? e.payload : {};
        if (!safeText(state.currentProjectId) && safeText(payload.currentProjectId)) {
            await applyMartyRemoteContext(payload);
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
        if (!IS_MARTY_POPOUT) {
            setStoredMartyDetached(false);
            syncMartyDetachedIndicator();
            applyMartyOpenState(true);
            setStoredMartyOpen(true);
        }
        return;
    }
}

function initMartySync() {
    try {
        if (typeof BroadcastChannel === 'function') {
            martySyncChannel = new BroadcastChannel(MARTY_SYNC_CHANNEL);
            martySyncChannel.onmessage = (msg) => {
                handleMartySyncEvent(msg?.data).catch(() => {});
            };
        }
    } catch {
        martySyncChannel = null;
    }

    window.addEventListener('storage', (evt) => {
        if (evt.key !== MARTY_SYNC_STORAGE_KEY || !evt.newValue) return;
        try {
            const parsed = JSON.parse(evt.newValue);
            handleMartySyncEvent(parsed).catch(() => {});
        } catch {}
    });

    if (IS_MARTY_POPOUT) {
        window.addEventListener('beforeunload', () => {
            publishMartySync('popout-closed', {});
        });
    }

    publishMartySync('request-sync', {});
}

function broadcastMartyContext() {
    publishMartySync('context', {
        currentProjectId: safeText(state.currentProjectId || ''),
        currentView: safeText(state.currentView || 'dashboard'),
    });
}

function getDefaultMartyPanelLayout() {
    const width = Math.min(420, Math.max(MARTY_PANEL_MIN_WIDTH, Math.floor(window.innerWidth * 0.33)));
    const height = Math.min(640, Math.max(MARTY_PANEL_MIN_HEIGHT, Math.floor(window.innerHeight * 0.58)));
    return {
        x: Math.max(8, window.innerWidth - width - 24),
        y: Math.max(8, window.innerHeight - height - 24),
        width,
        height,
    };
}

function clampMartyPanelLayout(layout) {
    const l = (layout && typeof layout === 'object') ? layout : {};
    const width = Math.min(window.innerWidth - 8, Math.max(MARTY_PANEL_MIN_WIDTH, Number(l.width) || MARTY_PANEL_MIN_WIDTH));
    const height = Math.min(window.innerHeight - 8, Math.max(MARTY_PANEL_MIN_HEIGHT, Number(l.height) || MARTY_PANEL_MIN_HEIGHT));
    const maxX = Math.max(0, window.innerWidth - width - 8);
    const maxY = Math.max(0, window.innerHeight - height - 8);
    const x = Math.min(maxX, Math.max(0, Number(l.x) || 0));
    const y = Math.min(maxY, Math.max(0, Number(l.y) || 0));
    return { x, y, width, height };
}

function getStoredMartyPanelLayout() {
    try {
        const raw = String(localStorage.getItem(MARTY_PANEL_STORAGE_KEY) || '').trim();
        if (!raw) return getDefaultMartyPanelLayout();
        const parsed = JSON.parse(raw);
        return clampMartyPanelLayout(parsed);
    } catch {
        return getDefaultMartyPanelLayout();
    }
}

function setStoredMartyPanelLayout(layout) {
    try {
        localStorage.setItem(MARTY_PANEL_STORAGE_KEY, JSON.stringify(clampMartyPanelLayout(layout)));
    } catch {
        // ignore
    }
}

function applyMartyPanelLayout(layout) {
    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;
    if (drawer.dataset?.martyDocked === '1') return;
    const next = clampMartyPanelLayout(layout);
    drawer.style.left = `${next.x}px`;
    drawer.style.top = `${next.y}px`;
    drawer.style.width = `${next.width}px`;
    drawer.style.height = `${next.height}px`;
    drawer.style.right = 'auto';
    drawer.style.bottom = 'auto';
}

function dockMartyToDashboardSlot(slotEl) {
    const slot = slotEl && typeof slotEl === 'object' ? slotEl : null;
    const drawer = document.getElementById('neural-drawer');
    if (!slot || !drawer) return;
    if (drawer.dataset?.martyDocked === '1') return;

    const parent = drawer.parentElement;
    if (!parent) return;

    martyDockRestore = {
        parent,
        nextSibling: drawer.nextSibling,
        className: drawer.className,
        style: drawer.getAttribute('style') || '',
    };

    drawer.dataset.martyDocked = '1';
    drawer.className = drawer.className
        .replace(/\bfixed\b/g, '')
        .replace(/\bright-\S+\b/g, '')
        .replace(/\bbottom-\S+\b/g, '');
    drawer.classList.add('relative', 'w-full', 'h-full');
    drawer.style.left = 'auto';
    drawer.style.top = 'auto';
    drawer.style.right = 'auto';
    drawer.style.bottom = 'auto';

    const resizeHandle = document.getElementById('marty-resize-handle');
    if (resizeHandle) resizeHandle.classList.add('hidden');

    const dragHandle = document.getElementById('marty-drag-handle');
    if (dragHandle) dragHandle.classList.remove('cursor-move');

    slot.appendChild(drawer);
}

function ensurePersistentMartyLayout() {
    const main = document.getElementById('main-port');
    if (!main) return null;

    let viewPort = document.getElementById('view-port');
    let martyPort = document.getElementById('marty-port');

    const needsRebuild = !viewPort || !martyPort || viewPort.parentElement !== main || martyPort.parentElement !== main;
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

        martyPort = document.createElement('div');
        martyPort.id = 'marty-port';
        martyPort.className = 'min-h-0 overflow-hidden border-l border-ops-border';

        main.appendChild(viewPort);
        main.appendChild(martyPort);
    }

    return { main, viewPort, martyPort };
}

function dockMartyToPersistentSlot() {
    const ports = ensurePersistentMartyLayout();
    if (!ports) return;

    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;

    const slot = ports.martyPort;
    if (!slot) return;

    // Always force docked & open.
    setStoredMartyDetached(false);
    syncMartyDetachedIndicator();
    applyMartyOpenState(true);
    setStoredMartyOpen(true);

    // Strip all floating / absolute positioning and fill the right column completely.
    drawer.dataset.martyDocked = '1';
    drawer.className = 'flex flex-col overflow-hidden';
    // Force drawer to fill the marty-port slot exactly.
    drawer.style.cssText = 'position:relative; width:100%; height:100%; min-width:0; min-height:0; border:none; border-radius:0; box-shadow:none;';

    const resizeHandle = document.getElementById('marty-resize-handle');
    if (resizeHandle) resizeHandle.classList.add('hidden');
    const dragHandle = document.getElementById('marty-drag-handle');
    if (dragHandle) dragHandle.classList.remove('cursor-move');
    const popoutToggle = document.getElementById('marty-popout-toggle');
    if (popoutToggle) popoutToggle.classList.add('hidden');

    if (drawer.parentElement !== slot) {
        slot.innerHTML = '';
        slot.appendChild(drawer);
    }
}

function undockMartyFromDashboard() {
    const drawer = document.getElementById('neural-drawer');
    if (!drawer) return;
    if (drawer.dataset?.martyDocked !== '1') return;
    if (!martyDockRestore || !martyDockRestore.parent) return;

    drawer.dataset.martyDocked = '0';

    const resizeHandle = document.getElementById('marty-resize-handle');
    if (resizeHandle) resizeHandle.classList.remove('hidden');

    const dragHandle = document.getElementById('marty-drag-handle');
    if (dragHandle) dragHandle.classList.add('cursor-move');

    if (martyDockRestore.nextSibling && martyDockRestore.nextSibling.parentNode === martyDockRestore.parent) {
        martyDockRestore.parent.insertBefore(drawer, martyDockRestore.nextSibling);
    } else {
        martyDockRestore.parent.appendChild(drawer);
    }

    drawer.className = martyDockRestore.className;
    drawer.setAttribute('style', martyDockRestore.style);
    martyDockRestore = null;

    const next = clampMartyPanelLayout(getStoredMartyPanelLayout());
    applyMartyPanelLayout(next);
    setStoredMartyPanelLayout(next);
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

function syncMartyModelUi() {
    const model = String(state.settings?.openaiModel || '').trim();
    const badge = document.getElementById('ai-model-badge');
    const select = document.getElementById('marty-model-select');
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

function setMartyPresence(mode = 'idle') {
    const panel = document.getElementById('neural-drawer');
    const orb = document.getElementById('marty-orb');
    const statusText = document.getElementById('marty-state');
    const normalized = String(mode || '').toLowerCase();
    const busy = normalized === 'busy';
    const responding = normalized === 'responding';

    if (panel) {
        panel.classList.remove('marty-thinking', 'marty-responding');
        if (busy) panel.classList.add('marty-thinking');
        if (responding) panel.classList.add('marty-responding');
    }

    if (orb) {
        orb.classList.remove('idle', 'busy', 'responding');
        if (busy) orb.classList.add('busy');
        else if (responding) orb.classList.add('responding');
        else orb.classList.add('idle');
    }

    if (statusText) {
        if (busy) {
            const line = MARTY_THINKING_LINES[Math.floor(Math.random() * MARTY_THINKING_LINES.length)];
            statusText.textContent = `MARTY THINKING • ${line}`;
        } else if (responding) {
            const line = MARTY_RESPONDING_LINES[Math.floor(Math.random() * MARTY_RESPONDING_LINES.length)];
            statusText.textContent = `MARTY RESPONDING • ${line}`;
        } else {
            statusText.textContent = 'MARTY IDLE • READY FOR ORDERS';
        }
    }
}

function removeMartyTypingIndicator() {
    const stream = document.getElementById('chat-stream');
    if (!stream) return;
    const existing = stream.querySelector(`#${MARTY_TYPING_ID}`);
    if (existing) existing.remove();
}

function showMartyTypingIndicator() {
    const stream = document.getElementById('chat-stream');
    if (!stream) return;
    removeMartyTypingIndicator();

    const line = MARTY_THINKING_LINES[Math.floor(Math.random() * MARTY_THINKING_LINES.length)];
    const wrap = document.createElement('div');
    wrap.id = MARTY_TYPING_ID;
    wrap.className = 'flex flex-col gap-1 mb-4 animate-fade-in';
    wrap.innerHTML = `
        <span class="text-[10px] uppercase font-bold tracking-wider text-blue-400">MARTY</span>
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

function initializeMartyWidget() {
    const drawer = document.getElementById('neural-drawer');
    const dragHandle = document.getElementById('marty-drag-handle');
    const resizeHandle = document.getElementById('marty-resize-handle');
    const popoutToggle = document.getElementById('marty-popout-toggle');
    if (!drawer) return;

    if (IS_MARTY_POPOUT) {
        document.body.classList.add('marty-popout-mode');
        setStoredMartyDetached(true);
    }
    syncMartyDetachedIndicator();

    applyMartyPanelLayout(getStoredMartyPanelLayout());
    applyMartyOpenState(getStoredMartyOpen());
    syncMartyModelUi();
    setMartyPresence('idle');

    if (IS_MARTY_POPOUT) {
        applyMartyOpenState(true);
    }

    if (popoutToggle) {
        const icon = popoutToggle.querySelector('i');
        if (IS_MARTY_POPOUT) {
            popoutToggle.title = 'Return to app window';
            if (icon) icon.className = 'fa-solid fa-down-left-and-up-right-to-center';
        }
        popoutToggle.addEventListener('click', () => {
            const baseUrl = `${window.location.origin}${window.location.pathname}`;
            if (IS_MARTY_POPOUT) {
                setStoredMartyDetached(false);
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
            const target = `${baseUrl}?martyPopout=1`;
            const features = `popup=yes,width=${width},height=${height},left=${left},top=${top},resizable=yes,scrollbars=no`;
            const w = window.open(target, `marty-popout-${Date.now()}`, features);
            if (w) {
                w.focus();
                setStoredMartyDetached(true);
                syncMartyDetachedIndicator();
                applyMartyOpenState(false);
                setStoredMartyOpen(false);
            }
        });
    }

    if (IS_MARTY_POPOUT) return;

    let drag = null;
    let resize = null;

    if (dragHandle) {
        dragHandle.addEventListener('pointerdown', (e) => {
            if (drawer.dataset?.martyDocked === '1') return;
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
                ...getStoredMartyPanelLayout(),
                x: drag.originX + (e.clientX - drag.startX),
                y: drag.originY + (e.clientY - drag.startY),
                width: drawer.getBoundingClientRect().width,
                height: drawer.getBoundingClientRect().height,
            };
            applyMartyPanelLayout(next);
        });

        const stopDrag = (e) => {
            if (!drag || drag.pointerId !== e.pointerId) return;
            const rect = drawer.getBoundingClientRect();
            setStoredMartyPanelLayout({
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
            if (drawer.dataset?.martyDocked === '1') return;
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
            applyMartyPanelLayout(next);
        });

        const stopResize = (e) => {
            if (!resize || resize.pointerId !== e.pointerId) return;
            const rect = drawer.getBoundingClientRect();
            setStoredMartyPanelLayout({
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
        if (drawer.dataset?.martyDocked === '1') return;
        const next = clampMartyPanelLayout(getStoredMartyPanelLayout());
        applyMartyPanelLayout(next);
        setStoredMartyPanelLayout(next);
    });
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
    const ports = ensurePersistentMartyLayout();
    const viewPort = ports?.viewPort;
    if (!viewPort) return null;

    const snap = {
        view: safeText(state.currentView),
        scrollTop: null,
        expandedCards: [],
        scrollSelector: '',
    };

    if (state.currentView === 'dashboard') {
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
    const ports = ensurePersistentMartyLayout();
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

function ensureAiTeamMember() {
    const list = Array.isArray(state.team) ? state.team : [];
    const hasAi = list.some((m) => safeText(m?.id) === 'ai');
    if (hasAi) return;
    state.team = [...list, { id: 'ai', name: 'Neural Core', role: 'ai', avatar: 'AI' }];
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

function isArchivedProject(project) {
    const s = safeText(project?.status).trim().toLowerCase();
    return s === 'done' || s === 'completed' || s === 'complete' || s === 'archived' || s === 'archive';
}

function getActiveProjects() {
    const list = Array.isArray(state.projects) ? state.projects : [];
    return list.filter((p) => !isArchivedProject(p));
}

function getArchivedProjects() {
    const list = Array.isArray(state.projects) ? state.projects : [];
    return list.filter((p) => isArchivedProject(p));
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

async function init() {
    try {
        console.log("Initializing Neural Ops v2.4...");
        applyTheme(getStoredTheme() || 'dark');
        showLoading();

        // Auth status is a public endpoint; check early so we can avoid a broken/empty UI.
        await refreshAuthStatus();
        
        // Initial Fetch
        await Promise.all([
            fetchState(), 
            fetchSettings()
        ]);
        
        // Setup UI
        setupEventListeners();
        initializeMartyWidget();
        initMartySync();
        await loadChatHistory();
        renderNav();
        renderMain();
        renderChat();

        ensureAiTeamMember();
        
        // Polling (Auto-Refresh)
        if (!(state.auth.required && !state.auth.authenticated)) {
            startPolling();
        }
        
        console.log("System Online");
    } catch (e) {
        console.error("Critical Failure:", e);
        showError(e.message);
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
    
    // Chat Input
    const input = document.getElementById("cmd-input");
    const send = document.getElementById("cmd-send");
    const modelSelect = document.getElementById('marty-model-select');

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

    if (modelSelect) {
        modelSelect.addEventListener('change', async () => {
            const model = safeText(modelSelect.value).trim();
            if (!model) return;
            try {
                await saveSettingsPatch({ openaiModel: model });
                syncMartyModelUi();
            } catch (e) {
                alert(e?.message || 'Failed to save model');
            }
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
        <span class="font-mono text-xs tracking-widest">CONNECTING TO NEURAL CORE...</span>
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

            syncMartyModelUi();
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
    
    nav.appendChild(createNavIcon("fa-grip", "Dashboard", () => openDashboard(), state.currentView === "dashboard"));
    nav.appendChild(createNavIcon("fa-inbox", "Inbox", () => openInbox(), state.currentView === "inbox"));
    nav.appendChild(createNavIcon("fa-folder", "Projects", () => openProjects(), state.currentView === "projects" || state.currentView === "project"));
    nav.appendChild(createNavIcon("fa-calendar-days", "Calendar", () => openCalendar(), state.currentView === "calendar"));
    nav.appendChild(createNavIcon("fa-user-group", "Team", () => openTeam(), state.currentView === "team"));

    const sep = document.createElement("div");
    sep.className = "h-px w-8 bg-zinc-800 mx-auto my-2";
    nav.appendChild(sep);

    nav.appendChild(createNavIcon("fa-gear", "Settings", () => openSettings(), state.currentView === "settings"));
}

async function openDashboard() {
    state.currentView = 'dashboard';
    state.currentProjectId = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMartyContext();
}

async function openInbox() {
    state.currentView = 'inbox';
    state.currentProjectId = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMartyContext();
}

async function openProjects() {
    state.currentView = 'projects';
    state.currentProjectId = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMartyContext();
}

async function openProject(projectId) {
    state.currentView = 'project';
    state.currentProjectId = projectId;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMartyContext();
}

async function openSettings() {
    state.currentView = 'settings';
    state.currentProjectId = null;
    await fetchSettings();
    await refreshSlackTeamPresence({ force: true });
    renderNav();
    renderMain();
    renderChat();
    broadcastMartyContext();
}

async function openCalendar() {
    state.currentView = 'calendar';
    state.currentProjectId = null;
    await fetchSettings();
    await refreshDashboardCalls({ force: true }).catch(() => {});
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMartyContext();
}

async function openTeam() {
    state.currentView = 'team';
    state.currentProjectId = null;
    await fetchSettings();
    await refreshSlackTeamPresence({ force: true }).catch(() => {});
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
    broadcastMartyContext();
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
    const ports = ensurePersistentMartyLayout();
    if (!ports) return;

    // Keep MARTY permanently docked on the right.
    dockMartyToPersistentSlot();

    const container = ports.viewPort;
    if (!container) return;

    container.innerHTML = "";

    // If the server requires ADMIN_TOKEN and we're not authenticated,
    // show a clear gate instead of rendering empty/disabled views.
    if (state.auth.required && !state.auth.authenticated) {
        setMainPortScrolling(true);
        const wrap = document.createElement('div');
        wrap.className = 'p-8 max-w-3xl';
        wrap.innerHTML = `
            <div class="border border-ops-border rounded-xl bg-ops-surface/40 p-6">
                <div class="text-white font-semibold">Access Required</div>
                <div class="text-xs text-ops-light mt-1">This server is protected by an admin token. Paste it once in Settings → Access to unlock projects, inbox, and MARTY.</div>
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
    if (state.currentView === 'project' || state.currentView === 'projects' || state.currentView === 'dashboard' || state.currentView === 'inbox' || state.currentView === 'calendar' || state.currentView === 'team') {
        setMainPortScrolling(false);
    } else {
        setMainPortScrolling(true);
    }
    
    if (state.currentView === "dashboard") {
        renderDashboard(container);
    } else if (state.currentView === "inbox") {
        renderInbox(container);
    } else if (state.currentView === "projects") {
        renderProjects(container);
    } else if (state.currentView === "project") {
        renderProjectView(container);
    } else if (state.currentView === "calendar") {
        renderCalendar(container);
    } else if (state.currentView === "team") {
        renderTeam(container);
    } else if (state.currentView === "settings") {
        renderSettings(container);
    }

    renderCommandPaletteOverlay();
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
        const slackUserId = safeText(m?.slackUserId).trim();
        const key = slackUserId || id;
        return key ? (presenceMap[key] || null) : null;
    };

    const rows = humans.length
        ? `<div class="space-y-2">${humans.map((m) => {
            const name = safeText(m?.name) || 'Member';
            const role = safeText(m?.role);
            const avatar = safeText(m?.avatar) || name.slice(0, 1).toUpperCase();
            const p = presenceFor(m);
            const rawStatus = safeText(p?.status).toLowerCase();
            const statusText = slackInstalled ? (rawStatus || 'unknown') : 'not connected';
            const statusClass = !slackInstalled
                ? 'bg-zinc-600'
                : (rawStatus === 'active' || rawStatus === 'online')
                    ? 'bg-emerald-400'
                    : (rawStatus === 'away')
                        ? 'bg-amber-300'
                        : 'bg-zinc-600';
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
    const list = Array.isArray(state.inboxItems) ? state.inboxItems : [];
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

    const all = Array.isArray(state.inboxItems) ? state.inboxItems : [];
    const visible = getInboxItems();
    const newCount = all.filter((x) => String(x?.status || '').toLowerCase() === 'new').length;

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
            <label class="flex items-center gap-2 text-xs text-zinc-400 select-none">
                <input id="inbox-show-archived" type="checkbox" class="accent-blue-500" ${state.inboxShowArchived ? 'checked' : ''} />
                Show archived
            </label>
        </div>

        <div class="mt-4 grid grid-cols-1 md:grid-cols-6 gap-3">
            <div class="md:col-span-5">
                <textarea id="inbox-draft" rows="2" class="w-full bg-zinc-950/40 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500/30 focus:border-blue-500/40" placeholder="Capture anything…">${escapeHtml(state.inboxDraftText || '')}</textarea>
            </div>
            <div class="md:col-span-1 flex items-stretch">
                <button id="btn-inbox-add" class="w-full px-3 py-2 rounded-lg bg-blue-600/20 border border-blue-600/40 text-sm font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Add</button>
            </div>
        </div>
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
                const projectId = safeText((state.inboxConvertProjectById && state.inboxConvertProjectById[id]) || item?.projectId);

                const card = document.createElement('div');
                card.className = 'border border-zinc-800 rounded-xl bg-zinc-900/30 p-4';
                card.innerHTML = `
                    <div class="flex items-start justify-between gap-4">
                        <div class="min-w-0">
                            <div class="flex items-center gap-2 flex-wrap">
                                ${inboxStatusBadge(status)}
                                ${inboxSourceBadge(item?.source)}
                                <span class="px-2 py-0.5 rounded border border-zinc-800 bg-zinc-950/40 text-[10px] font-mono text-zinc-300">${escapeHtml(inboxBusinessLabel(item))}</span>
                                  ${(item?.sender || item?.fromNumber) ? `<span class="px-2 py-0.5 rounded border border-zinc-500/30 bg-zinc-800/40 text-[10px] font-mono text-zinc-300">From: ${escapeHtml(item?.sender || item?.fromNumber)}</span>` : ""}
                                <div class="text-[11px] text-zinc-500 font-mono">${escapeHtml(createdAt ? formatTimeFromIso(createdAt) : '')}${updatedAt && updatedAt !== createdAt ? ` • upd ${escapeHtml(formatTimeFromIso(updatedAt))}` : ''}</div>
                            </div>
                            <div class="mt-2 text-sm text-zinc-200 whitespace-pre-wrap break-words">${escapeHtml(safeText(item?.text))}</div>
                            <div class="mt-3 flex flex-wrap items-center gap-2">
                                <select data-inbox-project="${escapeHtml(id)}" class="bg-zinc-950/40 border border-zinc-800 rounded px-2 py-1 text-xs text-zinc-200">
                                    <option value="">Project (optional)</option>
                                    ${(Array.isArray(state.projects) ? state.projects : []).map((p) => `<option value="${escapeHtml(safeText(p?.id))}" ${safeText(p?.id) === projectId ? 'selected' : ''}>${escapeHtml(safeText(p?.name) || 'Project')}</option>`).join('')}
                                </select>
                                <button data-inbox-link-project="${escapeHtml(id)}" class="px-2 py-1 rounded border border-blue-600/40 bg-blue-600/20 text-[11px] font-mono text-blue-200 hover:bg-blue-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Link</button>
                                <button data-inbox-create-project="${escapeHtml(id)}" class="px-2 py-1 rounded border border-emerald-600/40 bg-emerald-600/20 text-[11px] font-mono text-emerald-200 hover:bg-emerald-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">New Project</button>
                                <button data-inbox-edit="${escapeHtml(id)}" class="px-2 py-1 rounded border border-zinc-800 text-[11px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Edit</button>
                            </div>
                        </div>
                        <div class="shrink-0 flex flex-col gap-2">
                            <button data-inbox-triage="${escapeHtml(id)}" class="px-3 py-1.5 rounded border border-zinc-800 text-[11px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Triage</button>
                            <button data-inbox-done="${escapeHtml(id)}" class="px-3 py-1.5 rounded bg-emerald-600/20 border border-emerald-600/40 text-[11px] font-mono text-emerald-200 hover:bg-emerald-600/30 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Done</button>
                            <button data-inbox-archive="${escapeHtml(id)}" class="px-3 py-1.5 rounded bg-zinc-900/30 border border-zinc-800 text-[11px] font-mono text-zinc-200 hover:bg-zinc-800/40 transition-colors transition-transform duration-150 ease-out active:translate-y-px">Archive</button>
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

    // Wire row actions
    container.querySelectorAll('select[data-inbox-project]').forEach((sel) => {
        sel.addEventListener('change', (e) => {
            const inboxId = safeText(sel.getAttribute('data-inbox-project')).trim();
            if (!inboxId) return;
            const projectId = safeText(e.target.value).trim();
            state.inboxConvertProjectById = { ...(state.inboxConvertProjectById || {}), [inboxId]: projectId };
        });
    });

    container.querySelectorAll('button[data-inbox-edit]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-inbox-edit')).trim();
            const item = (Array.isArray(state.inboxItems) ? state.inboxItems : []).find((x) => safeText(x?.id) === inboxId);
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

    container.querySelectorAll('button[data-inbox-create-project]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const inboxId = safeText(btn.getAttribute('data-inbox-create-project')).trim();
            const item = (Array.isArray(state.inboxItems) ? state.inboxItems : []).find((x) => safeText(x?.id) === inboxId);
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
    wrap.className = 'p-8 max-w-4xl';

    const section = (title, subtitle) => {
        const el = document.createElement('div');
        el.className = 'border border-ops-border rounded-xl bg-ops-surface/40 p-6 mb-6';
        el.innerHTML = `
            <div class="flex items-start justify-between gap-4">
                <div>
                    <div class="text-white font-semibold">${title}</div>
                    <div class="text-xs text-ops-light mt-1">${subtitle || ''}</div>
                </div>
            </div>
            <div class="mt-4" data-slot="body"></div>
        `;
        return el;
    };

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

    // AI
    const ai = section('AI', 'Configure OpenAI key/model used for “Neural Link”.');
    const aiBody = ai.querySelector('[data-slot="body"]');
    aiBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-ops-light">OpenAI API Key (stored locally)</label>
                <input id="set-openai-key" type="password" autocomplete="off" placeholder="sk-..." class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Current: ${maskHint(state.settings.openaiKeyHint)}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Model</label>
                <input id="set-openai-model" type="text" placeholder="gpt-4o-mini" value="${String(state.settings.openaiModel || '')}" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">AI Enabled: ${state.settings.aiEnabled ? 'Yes' : 'No'}</div>
            </div>
        </div>
        <div class="flex gap-2 mt-4">
            <button id="btn-save-ai" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save AI</button>
            <button id="btn-clear-ai" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Clear stored key</button>
        </div>
    `;
    wrap.appendChild(ai);

    // Agent personalization
    const agent = section('Agent', 'Personalize how your agent behaves and what it should remember across chats.');
    const agentBody = agent.querySelector('[data-slot="body"]');
    agentBody.innerHTML = `
        <div class="grid grid-cols-1 gap-4">
            <div>
                <label class="text-xs text-ops-light">System prompt (your rules/style)</label>
                <textarea id="set-agent-system-prompt" rows="5" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: You are Mark's ops copilot. Be blunt, prioritize revenue, always end with next steps.">${escapeHtml(String(state.settings.agentSystemPrompt || ''))}</textarea>
                <div class="text-[11px] text-ops-light mt-1">This is prepended to the agent's built-in OS.1 prompt.</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Memory (facts/preferences to keep in mind)</label>
                <textarea id="set-agent-memory" rows="6" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono" placeholder="Example: I run a web agency. Preferred tone: concise. My timezone: America/Chicago. My VA's name: ...">${escapeHtml(String(state.settings.agentMemory || ''))}</textarea>
                <div class="text-[11px] text-ops-light mt-1">Stored locally in your settings file. Included in every chat context (capped).</div>
            </div>
        </div>
        <div class="flex gap-2 mt-4">
            <button id="btn-save-agent" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Agent</button>
        </div>
    `;
    wrap.appendChild(agent);

    // Projects (bulk delete)
    const projectsSection = section('Projects', 'Bulk select projects and remove them. This also removes their tasks, notes, chat, scratchpad, and communications.');
    const projectsBody = projectsSection.querySelector('[data-slot="body"]');
    const projects = Array.isArray(state.projects) ? state.projects : [];
    const selectedMap = (state.bulkProjectDeleteSelectedById && typeof state.bulkProjectDeleteSelectedById === 'object') ? state.bulkProjectDeleteSelectedById : {};
    const selectedIds = Object.keys(selectedMap).filter((id) => selectedMap[id]);

    projectsBody.innerHTML = `
        <div class="flex items-center justify-between gap-3">
            <div class="text-xs text-ops-light">Projects (<span id="projects-total">${projects.length}</span>) • Selected (<span id="projects-selected">${selectedIds.length}</span>)</div>
            <div class="flex gap-2">
                <button id="btn-projects-clear" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white" ${selectedIds.length ? '' : 'disabled'}>Clear selection</button>
                <button id="btn-projects-delete" class="px-3 py-2 rounded bg-red-600 text-white text-xs hover:bg-red-500" ${selectedIds.length ? '' : 'disabled'}>Delete selected</button>
            </div>
        </div>
        <div class="mt-3 space-y-2">
            ${projects.length ? projects.map((p) => {
                const id = safeText(p.id);
                const nm = safeText(p.name);
                const ty = safeText(p.type) || 'Other';
                const status = safeText(p.status) || (isArchivedProject(p) ? 'Archived' : 'Active');
                const due = safeText(p.dueDate);
                const checked = selectedMap[id] ? 'checked' : '';
                return `
                    <label class="flex items-start gap-3 border border-ops-border rounded-lg bg-ops-bg/30 p-3 cursor-pointer">
                        <input type="checkbox" data-proj-sel="${escapeHtml(id)}" class="mt-0.5" ${checked} />
                        <div class="min-w-0">
                            <div class="text-white text-sm font-semibold truncate">${escapeHtml(nm || '(Unnamed)')}</div>
                            <div class="text-[11px] text-ops-light mt-0.5">${escapeHtml(ty)} • ${escapeHtml(status)}${due ? ` • Due ${escapeHtml(due)}` : ''}</div>
                        </div>
                    </label>
                `;
            }).join('') : `<div class="text-xs text-ops-light italic">No projects yet.</div>`}
        </div>
        <div class="text-[11px] text-ops-light mt-3">Tip: This delete is permanent (it removes the project and its related data).</div>
    `;

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

                // If the current project was deleted, return to dashboard.
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

    // Slack
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
                <div class="text-[11px] text-ops-light mt-1">Redirect URI: <span class="font-mono">/api/integrations/slack/oauth/callback</span></div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Signing Secret</label>
                <input id="set-slack-signing-secret" type="password" autocomplete="off" placeholder="(leave blank to keep existing)" class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-sm" />
                <div class="text-[11px] text-ops-light mt-1">Configured: ${state.settings.slackConfigured ? 'Yes' : 'No'}</div>
            </div>
            <div>
                <label class="text-xs text-ops-light">Webhook</label>
                <div class="mt-1 w-full bg-ops-bg border border-ops-border rounded px-3 py-2 text-white text-xs font-mono">POST /api/integrations/slack/events</div>
                <div class="text-[11px] text-ops-light mt-1">Slack headers: <span class="font-mono">X-Slack-Signature</span>, <span class="font-mono">X-Slack-Request-Timestamp</span></div>
            </div>
        </div>
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-slack" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Slack</button>
            <button id="btn-connect-slack" class="px-3 py-2 rounded bg-emerald-600 text-white text-xs hover:bg-emerald-500">Connect</button>
            <button id="btn-disconnect-slack" class="px-3 py-2 rounded bg-ops-bg border border-ops-border text-ops-light text-xs hover:text-white">Disconnect</button>
        </div>
    `;
    wrap.appendChild(slack);

    // Quo (SMS/Calls)
    const quo = section('Quo (SMS/Calls)', 'Ingest inbound SMS and missed calls into Inbox (Twilio-style signature verified).');
    const quoBody = quo.querySelector('[data-slot="body"]');
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
    const m = section('MCP', 'Connect a local MCP server (stdio) so “Neural Link” can call MCP tools.');
    const mBody = m.querySelector('[data-slot="body"]');
    const mcp = (state.settings && state.settings.mcp && typeof state.settings.mcp === 'object') ? state.settings.mcp : {};
    const mcpEnabled = !!mcp.enabled;
    const mcpCommand = String(mcp.command || '');
    const mcpArgs = Array.isArray(mcp.args) ? mcp.args.map(String).join(' ') : String(mcp.args || '');
    const mcpCwd = String(mcp.cwd || '');
    mBody.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div class="flex items-center gap-2 md:col-span-3">
                <input id="set-mcp-enabled" type="checkbox" class="accent-blue-500" ${mcpEnabled ? 'checked' : ''} />
                <label for="set-mcp-enabled" class="text-xs text-ops-light">Enable MCP</label>
                <div class="text-[11px] text-ops-light ml-3">Configured: ${state.settings.mcpConfigured ? 'Yes' : 'No'}</div>
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
    const btnSaveGoogle = document.getElementById('btn-save-google');
    const btnConnectGoogle = document.getElementById('btn-connect-google');
    const btnDisconnectGoogle = document.getElementById('btn-disconnect-google');
    const btnSyncGoogle = document.getElementById('btn-sync-google');
    const btnUpcomingGoogle = document.getElementById('btn-upcoming-google');
    const googleStatusLine = document.getElementById('google-status-line');
    const googleUpcomingOutput = document.getElementById('google-upcoming-output');
    const btnGenFireflies = document.getElementById('btn-generate-fireflies');
    const btnSaveFireflies = document.getElementById('btn-save-fireflies');
    const btnSaveSlack = document.getElementById('btn-save-slack');
    const btnConnectSlack = document.getElementById('btn-connect-slack');
    const btnDisconnectSlack = document.getElementById('btn-disconnect-slack');
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

    if (btnSaveAi) btnSaveAi.onclick = async () => {
        try {
            const key = String(document.getElementById('set-openai-key')?.value || '').trim();
            const model = String(document.getElementById('set-openai-model')?.value || '').trim();
            const patch = { openaiModel: model };
            if (key) patch.openaiApiKey = key;
            await saveSettingsPatch(patch);
            alert('AI settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save AI settings');
        }
    };

    if (btnSaveAgent) btnSaveAgent.onclick = async () => {
        try {
            const agentSystemPrompt = String(document.getElementById('set-agent-system-prompt')?.value || '').trimEnd();
            const agentMemory = String(document.getElementById('set-agent-memory')?.value || '').trimEnd();
            await saveSettingsPatch({ agentSystemPrompt, agentMemory });
            alert('Agent settings saved.');
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

    if (btnSaveSlack) btnSaveSlack.onclick = async () => {
        try {
            const signingSecret = String(document.getElementById('set-slack-signing-secret')?.value || '').trim();
            const clientId = String(document.getElementById('set-slack-client-id')?.value || '').trim();
            const clientSecret = String(document.getElementById('set-slack-client-secret')?.value || '').trim();

            const patch = {};
            if (signingSecret) patch.slackSigningSecret = signingSecret;
            if (clientId) patch.slackClientId = clientId;
            if (clientSecret) patch.slackClientSecret = clientSecret;

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

            const patch = { phoneBusinessMap: map };
            if (token) patch.quoAuthToken = token;
            await saveSettingsPatch(patch);
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
                    <label class="text-[11px] text-zinc-400">Agent brief (saved to project Scratchpad for Neural Link)</label>
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
                    const card = document.createElement('button');
                    card.type = 'button';
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
                        <div class="relative z-10 mt-auto flex items-center gap-2">
                            <span class="text-[10px] font-mono uppercase tracking-wider px-2 py-1 rounded border border-white/10 bg-black/20 text-emerald-300">
                                ${escapeHtml(safeText(p?.status) || 'Active')}
                            </span>
                            <span class="text-[10px] font-mono uppercase tracking-wider px-2 py-1 rounded border border-white/10 bg-black/20 text-purple-300">
                                ${escapeHtml(safeText(p?.type) || 'Other')}
                            </span>
                        </div>
                    `;
                    card.onclick = async () => {
                        if (p?.id) await openProject(p.id);
                    };
                    body.appendChild(card);
                }
            }
        }
        return wrap;
    };

    content.appendChild(makeProjectListCard('Active Projects', activeProjects, 'None.'));

    if (archivedProjects.length) {
        const archivedCard = makeProjectListCard('Archived Projects', archivedProjects, '');
        archivedCard.classList.add('mt-6');
        content.appendChild(archivedCard);
    }

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
                <h2 class="text-2xl text-white font-light leading-tight">OS.1 Command Dashboard</h2>
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

    const allInboxItems = Array.isArray(state.inboxItems) ? state.inboxItems : [];
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
                    <label class="text-[11px] text-zinc-400">Agent brief (saved to project Scratchpad for Neural Link)</label>
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
    const unreadItems = (Array.isArray(state.inboxItems) ? state.inboxItems : [])
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

function renderDashboard(container) {
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.innerText = 'Dashboard';

    /* ── Data gathering ─────────────────────────────────────────── */
    const activeProjects = getActiveProjects();
    const buckets = bucketProjectsByDueDate(activeProjects);
    const dueThisWeek =
        (Array.isArray(buckets.today) ? buckets.today.length : 0) +
        (Array.isArray(buckets.tomorrow) ? buckets.tomorrow.length : 0) +
        (Array.isArray(buckets.thisWeek) ? buckets.thisWeek.length : 0);

    const inboxItems = Array.isArray(state.inboxItems) ? state.inboxItems : [];
    const inboxNew = inboxItems.filter((x) => String(x?.status || '').trim().toLowerCase() === 'new');
    const inboxNewCount = inboxNew.length;
    const teamMembers = Array.isArray(state.team) ? state.team : [];

    const callsConnected = !!state.settings?.googleConnected;
    const calls = Array.isArray(state.dashboardCalls?.events) ? state.dashboardCalls.events : [];
    const callsError = safeText(state.dashboardCalls?.error);
    const callsLoading = !!state.dashboardCalls?.loading;
    if (callsConnected) setTimeout(() => refreshDashboardCalls({ force: false }), 0);

    const slackNew = inboxNew.filter((x) => normalizeInboxSourceKey(x?.source) === 'slack');
    const emailNew = inboxNew.filter((x) => normalizeInboxSourceKey(x?.source) === 'email');
    const otherNew = inboxNew.filter((x) => { const k = normalizeInboxSourceKey(x?.source); return k !== 'slack' && k !== 'email'; });

    const nextActions = getTodayNextActions();
    const outcomes = safeText(state.settings?.todayOutcomes);
    const allTasks = Array.isArray(state.tasks) ? state.tasks : [];
    const today = ymdToday();

    // Overdue
    const overdueTasks = allTasks.filter((t) => { if (isDoneTask(t)) return false; const d = safeText(t?.dueDate).trim(); return d && d < today; });
    const overdueProjects = activeProjects.filter((p) => { const d = safeText(p?.dueDate).trim(); return d && d < today; });
    const totalOverdue = overdueTasks.length + overdueProjects.length;

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
    for (const t of allTasks) { const u = safeText(t?.updatedAt||t?.completedAt||t?.createdAt).trim(); if (!u) continue; const d = new Date(u); if (Number.isNaN(d.getTime())) continue; recentActivity.push({ time: d, text: isDoneTask(t) ? `Completed "${safeText(t?.title)}"` : `Updated "${safeText(t?.title)}"`, icon: isDoneTask(t) ? 'fa-check-circle text-emerald-400' : 'fa-pen text-blue-400' }); }
    for (const item of inboxItems.slice(0,20)) { const c = safeText(item?.createdAt).trim(); if (!c) continue; const d = new Date(c); if (Number.isNaN(d.getTime())) continue; recentActivity.push({ time: d, text: `Inbox: "${safeText(item?.title||item?.subject)}"`, icon: 'fa-inbox text-amber-400' }); }
    recentActivity.sort((a,b) => b.time - a.time);

    // MARTY insights (multi-line, Jarvis-style)
    const martyInsights = [];
    if (totalOverdue > 0) martyInsights.push({ icon: 'fa-triangle-exclamation text-red-400', text: `${totalOverdue} overdue item${totalOverdue>1?'s':''}. I\u2019d recommend triaging those first.` });
    const topAction = nextActions[0] || null;
    if (topAction) { const pr = Number(topAction?.priority)||3; martyInsights.push({ icon: 'fa-bullseye text-blue-400', text: `Top priority: "${safeText(topAction?.title)}" (P${pr}). Focus there next.` }); }
    if (dueThisWeek > 0) martyInsights.push({ icon: 'fa-clock text-amber-400', text: `${dueThisWeek} project${dueThisWeek>1?'s':''} due this week \u2014 stay ahead.` });
    if (inboxNewCount > 3) martyInsights.push({ icon: 'fa-inbox text-purple-400', text: `${inboxNewCount} inbox items accumulating. Consider a quick triage pass.` });
    if (totalDoneWeek > 0) martyInsights.push({ icon: 'fa-chart-line text-emerald-400', text: `${totalDoneWeek} tasks completed this week. ${totalDoneWeek >= 5 ? 'Strong momentum.' : 'Keep it going.'}` });
    if (!martyInsights.length) martyInsights.push({ icon: 'fa-circle-check text-emerald-400', text: 'All clear. Review upcoming projects or set today\u2019s outcomes.' });

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
                    <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light truncate">${label}</span>
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
            <div class="text-base text-white font-semibold">${escapeHtml(greeting)}, ${escapeHtml(userName)}</div>
            <div class="text-[10px] font-mono text-ops-light/60 mt-0.5">${escapeHtml(dateStr)}</div>
        </div>
        <div class="flex flex-wrap items-center gap-1.5">
            ${totalOverdue ? `<span class="stat-pill" style="border-color:rgba(239,68,68,0.3);color:#fca5a5"><i class="fa-solid fa-triangle-exclamation text-[8px]"></i>${totalOverdue} overdue</span>` : ''}
            ${dueThisWeek ? `<span class="stat-pill"><i class="fa-solid fa-fire text-[8px] text-amber-400"></i>${dueThisWeek} this week</span>` : ''}
            ${calls.length ? `<span class="stat-pill"><i class="fa-solid fa-video text-[8px] text-blue-400"></i>${calls.length} call${calls.length>1?'s':''}</span>` : ''}
            <span class="stat-pill"><i class="fa-solid fa-inbox text-[8px] text-purple-400"></i>${inboxNewCount} inbox</span>
            <span class="stat-pill"><i class="fa-solid fa-check text-[8px] text-emerald-400"></i>${totalDoneWeek} done</span>
            <span class="stat-pill cursor-pointer hover:text-white" id="dash-shortcuts-btn" title="Keyboard Shortcuts"><i class="fa-solid fa-keyboard text-[8px]"></i>?</span>
        </div>
    `;
    wrap.appendChild(headerEl);

    // ═══ MARTY AMBIENT INTELLIGENCE BAR ══════════════════════════════
    const martyBar = document.createElement('div');
    martyBar.className = 'marty-ambient dash-card';
    martyBar.dataset.cardId = 'marty';
    const primaryInsight = martyInsights[0];
    const extraInsights = martyInsights.slice(1);
    martyBar.innerHTML = `
        <div class="dash-card-head flex items-center gap-3 px-3 py-2.5">
            <div class="marty-status-dot shrink-0"></div>
            <div class="flex items-center gap-2 min-w-0 flex-1">
                <span class="text-[10px] font-mono uppercase tracking-widest text-blue-300">MARTY</span>
                <span class="text-[9px] font-mono text-blue-400/40">\u2014 monitoring ${activeProjects.length} projects, ${allTasks.filter(t=>!isDoneTask(t)).length} tasks</span>
            </div>
            <div class="flex items-center gap-1.5 shrink-0">
                <button id="dash-ask-marty" class="px-2 py-1 rounded border border-blue-500/25 bg-blue-500/10 text-[9px] font-mono text-blue-300 hover:bg-blue-500/20 transition-colors">Ask</button>
                <button id="dash-brief-marty" class="px-2 py-1 rounded border border-blue-500/25 bg-blue-500/10 text-[9px] font-mono text-blue-300 hover:bg-blue-500/20 transition-colors">Brief me</button>
                ${extraInsights.length ? '<i class="fa-solid fa-chevron-down expand-chevron"></i>' : ''}
            </div>
        </div>
        <div class="px-3 pb-2.5">
            <div class="marty-insight flex items-start gap-2">
                <i class="fa-solid ${primaryInsight.icon} text-[10px] mt-0.5 shrink-0"></i>
                <span class="text-[11px] leading-relaxed">${escapeHtml(primaryInsight.text)}</span>
            </div>
        </div>
        ${extraInsights.length ? `<div class="dash-card-body px-3 pb-3"><div class="space-y-1.5">${extraInsights.map(ins => `<div class="marty-insight flex items-start gap-2"><i class="fa-solid ${ins.icon} text-[10px] mt-0.5 shrink-0"></i><span class="text-[11px] leading-relaxed">${escapeHtml(ins.text)}</span></div>`).join('')}</div></div>` : ''}
    `;
    wrap.appendChild(martyBar);

    // ═══ OVERDUE ALERT ═══════════════════════════════════════════════
    if (totalOverdue > 0) {
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
    wrap.appendChild(quickAdd);

    // ═══ INBOX RADAR (compact banner) ════════════════════════════════
    const radarBanner = document.createElement('div');
    radarBanner.className = 'dash-card';
    radarBanner.dataset.cardId = 'radar';
    const formatInboxStamp = (iso) => {
        const s = safeText(iso).trim();
        if (!s) return '';
        const d = new Date(s);
        if (Number.isNaN(d.getTime())) return '';
        return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    };
    const radarExtraRows = inboxNew.slice(0, 8).map(item => {
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
    }).join('');
    radarBanner.innerHTML = `
        <div class="dash-card-head flex items-center justify-between gap-3 px-3 py-2.5">
            <div class="flex items-center gap-3 min-w-0">
                <i class="fa-solid fa-satellite-dish text-blue-400 text-xs shrink-0"></i>
                <span class="text-[10px] font-mono uppercase tracking-widest text-ops-light">Inbox Radar</span>
                <div class="flex items-center gap-3 text-[10px] font-mono text-ops-light/50">
                    <span class="text-lg font-semibold text-white leading-none">${inboxNewCount}</span>
                    <span><i class="fa-brands fa-slack text-purple-400 mr-0.5"></i>${slackNew.length}</span>
                    <span><i class="fa-solid fa-envelope text-sky-400 mr-0.5"></i>${emailNew.length}</span>
                    <span><i class="fa-solid fa-ellipsis text-ops-light/30 mr-0.5"></i>${otherNew.length}</span>
                </div>
            </div>
            <div class="flex items-center gap-1.5 shrink-0">
                <button type="button" data-open-inbox class="px-2.5 py-1 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white hover:bg-ops-surface/60 transition-colors">Open Inbox</button>
                ${inboxNew.length ? '<i class="fa-solid fa-chevron-down expand-chevron"></i>' : ''}
            </div>
        </div>
        ${inboxNew.length ? `<div class="dash-card-body px-3 pb-2.5"><div class="space-y-1">${radarExtraRows}</div></div>` : ''}
    `;
    wrap.appendChild(radarBanner);

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
    urgentRow.appendChild(calCard);

    // Due Today card
    const dueTodayItems = Array.isArray(buckets.today) ? buckets.today : [];
    const mkProjBtn = (p, color) => `<button type="button" class="dash-project-btn w-full text-left px-2.5 py-1.5 rounded border border-ops-border bg-ops-bg/40 hover:bg-ops-surface/60 transition-colors" data-pid="${escapeHtml(safeText(p?.id))}"><div class="text-[11px] text-white truncate">${escapeHtml(safeText(p?.name)||'Untitled')}</div><div class="text-[9px] font-mono ${color}">${escapeHtml(safeText(p?.dueDate)||'')}</div></button>`;
    const dtPreview = dueTodayItems.length ? `<div class="space-y-1">${dueTodayItems.slice(0,2).map(p=>mkProjBtn(p,'text-red-400/70')).join('')}</div>` : `<div class="text-[10px] text-ops-light/50">Nothing due today.</div>`;
    const dtBody = dueTodayItems.length > 2 ? `<div class="space-y-1">${dueTodayItems.slice(2).map(p=>mkProjBtn(p,'text-red-400/70')).join('')}</div>` : '';
    const dueTodayCard = makeCard('due-today', 'fa-fire', 'text-red-400', 'Due Today', `<span class="text-sm font-semibold text-white">${dueTodayItems.length}</span>`, dtPreview, dtBody);
    urgentRow.appendChild(dueTodayCard);

    // Due This Week card
    const dueWeekItems = [...(Array.isArray(buckets.tomorrow)?buckets.tomorrow:[]), ...(Array.isArray(buckets.thisWeek)?buckets.thisWeek:[])];
    const dwPreview = dueWeekItems.length ? `<div class="space-y-1">${dueWeekItems.slice(0,2).map(p=>mkProjBtn(p,'text-amber-400/70')).join('')}</div>` : `<div class="text-[10px] text-ops-light/50">Nothing else this week.</div>`;
    const dwBody = dueWeekItems.length > 2 ? `<div class="space-y-1">${dueWeekItems.slice(2).map(p=>mkProjBtn(p,'text-amber-400/70')).join('')}</div>` : '';
    const dueWeekCard = makeCard('due-week', 'fa-calendar-week', 'text-amber-400', 'Due This Week', `<span class="text-sm font-semibold text-white">${dueWeekItems.length}</span>`, dwPreview, dwBody);
    urgentRow.appendChild(dueWeekCard);
    wrap.appendChild(urgentRow);

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
    midRow.appendChild(streakCard);

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
    midRow.appendChild(timerCard);
    wrap.appendChild(midRow);

    // ═══ TODAY'S FOCUS (outcomes + next actions) ═════════════════════
    const nextActionsHtml = nextActions.length
        ? nextActions.slice(0,3).map((t) => {
            const pr = Number(t?.priority)||3;
            const prColor = pr===1?'text-red-400':pr===2?'text-amber-400':'text-ops-light/50';
            const project = safeText(t?.project)||'';
            return `<div class="flex items-center gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><span class="${prColor} text-[9px] font-mono font-bold shrink-0">P${pr}</span><div class="min-w-0 flex-1"><div class="text-[11px] text-white truncate">${escapeHtml(safeText(t?.title)||'Untitled')}</div>${project?`<div class="text-[9px] font-mono text-ops-light/50 truncate">${escapeHtml(project)}</div>`:''}</div></div>`;
        }).join('')
        : `<div class="text-[10px] text-ops-light/50">No next actions.</div>`;
    const extraActions = nextActions.length > 3 ? nextActions.slice(3,10).map((t) => {
        const pr = Number(t?.priority)||3; const prColor = pr===1?'text-red-400':pr===2?'text-amber-400':'text-ops-light/50';
        return `<div class="flex items-center gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><span class="${prColor} text-[9px] font-mono font-bold shrink-0">P${pr}</span><div class="min-w-0 flex-1"><div class="text-[11px] text-white truncate">${escapeHtml(safeText(t?.title)||'Untitled')}</div></div></div>`;
    }).join('') : '';

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
    wrap.appendChild(focusEl);

    // Wire outcomes
    const outcomesTA = focusEl.querySelector('#today-outcomes');
    const btnSave = focusEl.querySelector('#btn-save-today');
    const btnClear = focusEl.querySelector('#btn-clear-today');
    if (btnSave && outcomesTA) { btnSave.onclick = async () => { btnSave.disabled = true; try { const v = safeText(outcomesTA.value); state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {}; state.settings.todayOutcomes = v; state.rerenderPauseUntil = Date.now()+2000; await saveSettingsPatch({todayOutcomes:v}); state.rerenderPauseUntil = 0; } catch(e) { alert(e?.message||'Failed'); state.rerenderPauseUntil = 0; } finally { btnSave.disabled = false; } }; }
    if (btnClear && outcomesTA) { btnClear.onclick = async () => { outcomesTA.value=''; try { state.settings = state.settings && typeof state.settings === 'object' ? state.settings : {}; state.settings.todayOutcomes=''; state.rerenderPauseUntil = Date.now()+2000; await saveSettingsPatch({todayOutcomes:''}); state.rerenderPauseUntil = 0; } catch(e) { alert(e?.message||'Failed'); state.rerenderPauseUntil = 0; } }; }

    // ═══ FEED ROW: Activity + Slack + Inbox + Team ══════════════════
    const feedRow = document.createElement('div');
    feedRow.className = 'grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-2';

    // Activity
    const actPreview = recentActivity.slice(0,3).map(a => { const ts = a.time.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}); return `<div class="flex items-start gap-1.5"><i class="fa-solid ${a.icon} text-[9px] mt-0.5 shrink-0"></i><div class="min-w-0"><div class="text-[10px] text-white truncate">${escapeHtml(a.text)}</div><div class="text-[8px] font-mono text-ops-light/40">${escapeHtml(ts)}</div></div></div>`; }).join('');
    const actBody = recentActivity.length > 3 ? recentActivity.slice(3,10).map(a => { const ts = a.time.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}); return `<div class="flex items-start gap-1.5"><i class="fa-solid ${a.icon} text-[9px] mt-0.5 shrink-0"></i><div class="min-w-0"><div class="text-[10px] text-white truncate">${escapeHtml(a.text)}</div><div class="text-[8px] font-mono text-ops-light/40">${escapeHtml(ts)}</div></div></div>`; }).join('') : '';
    const actCard = makeCard('activity', 'fa-clock-rotate-left', 'text-sky-400', 'Activity', '', `<div class="space-y-1.5">${actPreview || '<div class="text-[10px] text-ops-light/50">No recent activity.</div>'}</div>`, actBody ? `<div class="space-y-1.5">${actBody}</div>` : '');
    feedRow.appendChild(actCard);

    // Slack
    const slackPreview = slackNew.length ? slackNew.slice(0,2).map(item => { const t = safeText(item?.title)||safeText(item?.subject)||'Slack'; const p = previewText(safeText(item?.content)||safeText(item?.text)||safeText(item?.body)||'',60); return `<div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><div class="text-[11px] text-white truncate">${escapeHtml(t)}</div>${p?`<div class="text-[9px] text-ops-light/60 truncate mt-0.5">${escapeHtml(p)}</div>`:''}</div>`; }).join('') : '<div class="text-[10px] text-ops-light/50">No Slack messages.</div>';
    const slackBody = slackNew.length > 2 ? slackNew.slice(2,6).map(item => { const t = safeText(item?.title)||'Slack'; return `<div class="border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><div class="text-[11px] text-white truncate">${escapeHtml(t)}</div></div>`; }).join('') : '';
    const slackCard = makeCard('slack', 'fa-slack', 'text-purple-400', 'Slack', `<button type="button" data-open-slack class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open</button>`, `<div class="space-y-1">${slackPreview}</div>`, slackBody ? `<div class="space-y-1">${slackBody}</div>` : '');
    slackCard.querySelector('.dash-card-head i.fa-slack')?.classList.replace('fa-solid', 'fa-brands');
    feedRow.appendChild(slackCard);

    // Inbox
    const inboxPreview = inboxNew.length ? inboxNew.slice(0,2).map(item => { const t = safeText(item?.title)||safeText(item?.subject)||'Item'; const s = safeText(item?.source)||''; return `<div class="flex items-center justify-between gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><span class="text-[11px] text-white truncate">${escapeHtml(t)}</span><span class="text-[9px] font-mono text-ops-light/50 shrink-0">${escapeHtml(s)}</span></div>`; }).join('') : '<div class="text-[10px] text-ops-light/50">Inbox zero.</div>';
    const inboxBody = inboxNew.length > 2 ? inboxNew.slice(2,8).map(item => { const t = safeText(item?.title)||'Item'; const s = safeText(item?.source)||''; return `<div class="flex items-center justify-between gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><span class="text-[11px] text-white truncate">${escapeHtml(t)}</span><span class="text-[9px] font-mono text-ops-light/50 shrink-0">${escapeHtml(s)}</span></div>`; }).join('') : '';
    const inboxCard = makeCard('inbox', 'fa-inbox', 'text-amber-400', 'Inbox', `<button type="button" data-open-inbox2 class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open</button>`, `<div class="space-y-1">${inboxPreview}</div>`, inboxBody ? `<div class="space-y-1">${inboxBody}</div>` : '');
    feedRow.appendChild(inboxCard);

    // Team
    const humanMembers = teamMembers.filter(m => safeText(m?.role).toLowerCase() !== 'ai');
    const teamPreview = humanMembers.length ? humanMembers.slice(0,3).map(m => { const name = safeText(m?.name)||'Member'; const role = safeText(m?.role)||''; const sid = safeText(m?.slackMemberId); const pres = sid && state.teamPresenceByMemberId?.[sid]; const on = pres && String(pres.presence||'').toLowerCase()==='active'; const dot = on ? '<span class="w-1.5 h-1.5 rounded-full bg-emerald-400 shrink-0"></span>' : '<span class="w-1.5 h-1.5 rounded-full bg-zinc-600 shrink-0"></span>'; return `<div class="flex items-center gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5">${dot}<div class="min-w-0"><div class="text-[11px] text-white truncate">${escapeHtml(name)}</div>${role?`<div class="text-[9px] font-mono text-ops-light/50">${escapeHtml(role)}</div>`:''}</div></div>`; }).join('') : '<div class="text-[10px] text-ops-light/50">No team members.</div>';
    const teamBody = humanMembers.length > 3 ? humanMembers.slice(3).map(m => { const name = safeText(m?.name)||'Member'; return `<div class="flex items-center gap-2 border border-ops-border rounded bg-ops-bg/40 px-2.5 py-1.5"><span class="w-1.5 h-1.5 rounded-full bg-zinc-600 shrink-0"></span><div class="text-[11px] text-white truncate">${escapeHtml(name)}</div></div>`; }).join('') : '';
    const teamCard = makeCard('team', 'fa-users', 'text-emerald-400', 'Team', `<button type="button" data-open-team class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">Open</button>`, `<div class="space-y-1">${teamPreview}</div>`, teamBody ? `<div class="space-y-1">${teamBody}</div>` : '');
    feedRow.appendChild(teamCard);
    wrap.appendChild(feedRow);

    // ═══ LATER ROW: Next Week + Future Projects ═════════════════════
    const nextWeekItems = Array.isArray(buckets.nextWeek) ? buckets.nextWeek : [];
    const laterRow = document.createElement('div');
    laterRow.className = 'grid grid-cols-1 sm:grid-cols-2 gap-2';

    const nwPreview = nextWeekItems.length ? `<div class="space-y-1">${nextWeekItems.slice(0,2).map(p=>mkProjBtn(p,'text-ops-light/60')).join('')}</div>` : '<div class="text-[10px] text-ops-light/50">Nothing due next week.</div>';
    const nwBody = nextWeekItems.length > 2 ? `<div class="space-y-1">${nextWeekItems.slice(2).map(p=>mkProjBtn(p,'text-ops-light/60')).join('')}</div>` : '';
    const nwCard = makeCard('next-week', 'fa-calendar-check', 'text-sky-400', 'Next Week', `<span class="text-sm font-semibold text-white">${nextWeekItems.length}</span>`, nwPreview, nwBody);
    laterRow.appendChild(nwCard);

    const upcoming = (Array.isArray(buckets.upcoming)?buckets.upcoming:[]).slice(0,6);
    const upPreview = upcoming.length ? `<div class="space-y-1">${upcoming.slice(0,2).map(p=>mkProjBtn(p,'text-ops-light/60')).join('')}</div>` : '<div class="text-[10px] text-ops-light/50">No future projects.</div>';
    const upBody = upcoming.length > 2 ? `<div class="space-y-1">${upcoming.slice(2).map(p=>mkProjBtn(p,'text-ops-light/60')).join('')}</div>` : '';
    const upCard = makeCard('upcoming', 'fa-forward', 'text-ops-light/40', 'Future Projects', `<button type="button" data-open-projects class="px-1.5 py-0.5 rounded border border-ops-border text-[9px] font-mono text-ops-light hover:text-white transition-colors">All</button>`, upPreview, upBody);
    laterRow.appendChild(upCard);
    wrap.appendChild(laterRow);

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
    wrap.appendChild(shortcutsPanel);

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
    wrap.querySelector('button[data-open-inbox2]')?.addEventListener('click', () => openInbox());
    wrap.querySelectorAll('button[data-open-slack]').forEach(b => b.addEventListener('click', () => openInbox()));
    wrap.querySelector('button[data-open-calendar]')?.addEventListener('click', () => openCalendar());
    wrap.querySelectorAll('button[data-open-team]').forEach(b => b.addEventListener('click', () => openTeam()));
    wrap.querySelectorAll('button[data-open-projects]').forEach(b => b.addEventListener('click', () => openProjects()));
    calCard.querySelector('button[data-refresh-calls]')?.addEventListener('click', async () => { await refreshDashboardCalls({force:true}); renderMain(); });
    wrap.querySelectorAll('.dash-project-btn').forEach(btn => { btn.addEventListener('click', () => { const pid = btn.dataset.pid; if (pid) openProject(pid); }); });

    // Quick-add
    const quickInput = wrap.querySelector('#dash-quick-input');
    if (quickInput) { quickInput.addEventListener('keydown', async (e) => { if (e.key === 'Enter') { const val = safeText(quickInput.value).trim(); if (!val) return; quickInput.value = ''; try { await apiFetch('/api/inbox', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({title:val,source:'quick-add',status:'new'})}); await fetchState(); renderMain(); } catch(err) { alert(err?.message||'Failed'); } } }); }
    wrap.querySelector('#dash-quick-project')?.addEventListener('click', () => createNewProjectPrompt());

    // Focus timer
    const timerToggle = wrap.querySelector('#dash-timer-toggle');
    const timerReset = wrap.querySelector('#dash-timer-reset');
    if (timerToggle) { timerToggle.addEventListener('click', () => { if (state.focusTimer.running) { clearInterval(state.focusTimer.intervalId); state.focusTimer.running = false; state.focusTimer.intervalId = null; } else { state.focusTimer.running = true; state.focusTimer.intervalId = setInterval(() => { if (state.focusTimer.remaining <= 0) { clearInterval(state.focusTimer.intervalId); state.focusTimer.running = false; state.focusTimer.intervalId = null; try { new Audio('data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1fdHmBgYF9eXl+gYaGg36Af4F/fn5+').play(); } catch(ignored) {} alert('Focus session complete!'); if (state.currentView==='dashboard') renderMain(); return; } state.focusTimer.remaining--; const disp = document.getElementById('dash-timer-display'); if (disp) { const mm = Math.floor(state.focusTimer.remaining/60); const ss = state.focusTimer.remaining%60; disp.textContent = `${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`; } }, 1000); } if (state.currentView==='dashboard') renderMain(); }); }
    if (timerReset) { timerReset.addEventListener('click', () => { clearInterval(state.focusTimer.intervalId); state.focusTimer.running = false; state.focusTimer.intervalId = null; state.focusTimer.remaining = state.focusTimer.duration; if (state.currentView==='dashboard') renderMain(); }); }

    // MARTY buttons
    wrap.querySelector('#dash-ask-marty')?.addEventListener('click', () => { const inp = document.getElementById('cmd-input'); if (inp) { inp.focus(); inp.value = 'What should I focus on right now?'; } });
    wrap.querySelector('#dash-brief-marty')?.addEventListener('click', () => { const inp = document.getElementById('cmd-input'); if (inp) { inp.focus(); inp.value = 'Give me a brief status update on everything.'; } });

    // Shortcuts
    const shortcutsBtn = wrap.querySelector('#dash-shortcuts-btn');
    const shortcutsPanelEl = wrap.querySelector('#dash-shortcuts-panel');
    const shortcutsClose = wrap.querySelector('#dash-shortcuts-close');
    if (shortcutsBtn && shortcutsPanelEl) shortcutsBtn.addEventListener('click', () => shortcutsPanelEl.classList.toggle('hidden'));
    if (shortcutsClose && shortcutsPanelEl) shortcutsClose.addEventListener('click', () => shortcutsPanelEl.classList.add('hidden'));
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
    const list = Array.isArray(state.inboxItems) ? state.inboxItems : [];
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
            const el = document.createElement('div');
            el.className = 'space-y-3';
            el.innerHTML = `
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
                <button id="btn-save-details" class="w-full bg-zinc-800 hover:bg-zinc-700 text-zinc-200 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors">Save</button>
            `;
            panel.appendChild(el);

            const saveDetailsBtn = el.querySelector('#btn-save-details');
            if (saveDetailsBtn) {
                saveDetailsBtn.onclick = async () => {
                    saveDetailsBtn.disabled = true;
                    try {
                        const dueDate = safeText(el.querySelector('#proj-due')?.value).trim();
                        const status = safeText(el.querySelector('#proj-status')?.value).trim();
                        const projectValue = safeText(el.querySelector('#proj-value')?.value).trim();
                        const accountManagerName = safeText(el.querySelector('#proj-am-name')?.value).trim();
                        const accountManagerEmail = safeText(el.querySelector('#proj-am-email')?.value).trim();
                        await saveProjectPatch(project.id, { dueDate, status, projectValue, accountManagerName, accountManagerEmail });
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

async function createNewProjectPrompt() {
    state.showNewProjectIntake = true;
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
    const docked = drawer?.dataset?.martyDocked === '1';

    // When MARTY is persistently docked, keep it visible.
    if (docked) {
        state.isChatOpen = true;
        dockMartyToPersistentSlot();
        applyMartyOpenState(true);
        setStoredMartyOpen(true);
        const input = document.getElementById('cmd-input');
        input?.focus?.();
        return;
    }

    state.isChatOpen = !state.isChatOpen;
    applyMartyOpenState(state.isChatOpen);
    setStoredMartyOpen(state.isChatOpen);
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
    setMartyPresence('busy');
    showMartyTypingIndicator();
    
    try {
        const res = await apiFetch("/api/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ 
                message: msg,
                projectId: state.currentProjectId || undefined
            })
        });

        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data?.error || `Request failed (${res.status})`);
        }

        const reply = data.reply || data.text || "Command Processed.";
        removeMartyTypingIndicator();
        setMartyPresence('responding');
        recordChatMessage("ai", reply);
        addChatMessage("ai", reply, true);
        
        // Refresh state in case AI changed things
        await fetchState();
        await loadChatHistory();
        renderChat();
        renderNav();
        renderMain();
        
    } catch (e) {
        removeMartyTypingIndicator();
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
        setMartyPresence('idle');
    }
}

function recordChatMessage(role, text) {
    const entry = { role: normalizeRole(role), content: String(text || "") };
    if (state.currentProjectId) {
        state.chatHistory.push(entry);
    } else {
        state.globalChatHistory.push(entry);
        state.chatHistory = state.globalChatHistory;
    }

    publishMartySync('chat-entry', {
        projectId: safeText(state.currentProjectId || ''),
        entry,
    });
}

function addChatMessage(role, text, animate = false) {
    const stream = document.getElementById("chat-stream");
    if(!stream) return;
    if (role === 'ai') removeMartyTypingIndicator();

    const div = document.createElement("div");
    div.className = "flex flex-col gap-1.5 mb-5 animate-fade-in";

    // Glass style bubbles
    const bubbleClasses = role === 'ai' 
        ? 'bg-ops-surface/80 backdrop-blur border border-white/10 text-white rounded-br-2xl rounded-bl-sm rounded-t-2xl px-4 py-3 shadow-md'
        : 'bg-blue-600/20 backdrop-blur border border-blue-500/30 text-blue-50 rounded-bl-2xl rounded-br-sm rounded-t-2xl px-4 py-3 self-end shadow-md';

    const header = document.createElement("span");
    header.className = `text-[9px] uppercase font-bold tracking-widest ${role === 'ai' ? 'text-blue-400 ml-1' : 'text-zinc-400 text-right mr-1'}`;
    header.innerText = role === 'ai' ? 'MARTY // DIRECT' : 'Operator';

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
            <p>MARTY is online — sharp, curious, and ready to work.</p>
        </div>`;
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

    // Global chat is local-only.
    state.chatHistory = state.globalChatHistory;
}


