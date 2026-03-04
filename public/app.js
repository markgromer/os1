/* =========================================
   NEURAL OPS - CORE LOGIC V2.4 "REBIRTH"
   Restores: Team, Due Dates, Auto-Delegate, Rich Chat
   Compatible with Neural Ops V2 Shell
   ========================================= */

/* --- State Management --- */
const state = {
    revision: 1,
    updatedAt: "",
    projects: [],
    tasks: [],
    inboxItems: [],
    projectScratchpads: {},
    projectNoteEntries: {},
    projectCommunications: {},
    // Mock team if API fails, but we'll try to fetch
    team: [
        { id: "u1", name: "Mark", role: "admin", avatar: "M" },
        { id: "u2", name: "Sarah", role: "designer", avatar: "S" },
        { id: "u3", name: "David", role: "developer", avatar: "D" },
        { id: "ai", name: "Neural Core", role: "ai", avatar: "AI" }
    ],
    settings: {
        openaiKey: "",
        githubToken: "",
    },

    uiPrefs: {
        autoRefreshSeconds: 30,
        weekStartsOnMonday: true,
        defaultShowCompleted: true,
    },
    theme: 'dark',
    deferRerender: false,
    rerenderPauseUntil: 0,
    
    // UI State
    currentView: "dashboard", 
    currentProjectId: null,
    showCompleted: true,
    projectRightTabById: {},
    globalChatHistory: [],
    chatHistory: [],
    isChatOpen: true,

    // Inbox UI state
    inboxDraftText: '',
    inboxShowArchived: false,
    inboxConvertProjectById: {},

    // Bulk project delete selection (Settings)
    bulkProjectDeleteSelectedById: {},

    // Dashboard bulk selection
    dashboardBulkMode: false,
    dashboardSelectedProjectById: {},

    // Dashboard: calls cache
    dashboardCalls: {
        loading: false,
        fetchedAt: 0,
        error: '',
        events: [],
    },

    teamPresenceByMemberId: {},
    teamPresenceFetchedAt: 0,
    teamPresenceLoading: false,
    teamPresenceError: '',

    // Per-project transcript drafts (paste -> analyze -> apply)
    projectTranscriptDraftById: {},

    // New Project Intake
    showNewProjectIntake: false,
    showArchivedOnDashboard: false,
    newProjectDraft: {
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
    },
    
    // System Status
    isLoading: true,
    aiAvailable: false,
    lastSync: null
};

const THEME_STORAGE_KEY = 'opsTheme';
const ADMIN_TOKEN_STORAGE_KEY = 'opsAdminToken';

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

function flushDeferredRerenderIfSafe() {
    if (!state.deferRerender) return;
    if (Date.now() < Number(state.rerenderPauseUntil || 0)) return;
    if (isUserEditingNow()) return;
    state.deferRerender = false;
    renderNav();
    renderMain();
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
    const name = safeText(d.name).trim();
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
    const el = document.getElementById('main-port');
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
        
        // Initial Fetch
        await Promise.all([
            fetchState(), 
            fetchSettings()
        ]);
        
        // Setup UI
        setupEventListeners();
        await loadChatHistory();
        renderNav();
        renderMain();
        renderChat();

        ensureAiTeamMember();
        
        // Polling (Auto-Refresh)
        setInterval(fetchState, Math.max(10, Number(state.uiPrefs.autoRefreshSeconds) || 30) * 1000);
        
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
    
    if (input) {
        input.addEventListener("keypress", (e) => {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                handleChatSubmit();
            }
        });
    }
    
    if (send) {
        send.addEventListener("click", handleChatSubmit);
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
    const container = document.getElementById("main-port");
    if(container) container.innerHTML = `<div class="flex h-full items-center justify-center text-blue-500">
        <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mr-3"></div>
        <span class="font-mono text-xs tracking-widest">CONNECTING TO NEURAL CORE...</span>
    </div>`;
}

function showError(msg) {
    const container = document.getElementById("main-port");
    if(container) container.innerHTML = `<div class="p-8 text-red-500 font-mono">
        <h1 class="text-xl font-bold mb-2">CRITICAL ERROR</h1>
        <pre>${msg}</pre>
    </div>`;
}

/* --- API --- */

async function fetchState() {
    try {
        const res = await apiFetch("/api/tasks");
        if (!res.ok) throw new Error("Failed to load store");
        const store = await res.json();
        applyStore(store);
        
        state.lastSync = new Date();

        if (Date.now() < Number(state.rerenderPauseUntil || 0)) {
            state.deferRerender = true;
            return;
        }

        if (isUserEditingNow()) {
            state.deferRerender = true;
            return;
        }

        state.deferRerender = false;
        renderNav();
        renderMain();
    } catch (e) {
        console.error("Fetch State Error:", e);
    }
}

async function fetchSettings() {
    try {
        const res = await apiFetch("/api/settings");
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

            const badge = document.getElementById('ai-model-badge');
            if (badge) {
                badge.innerText = state.settings.openaiModel || 'AI';
            }
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
    
    // Dashboard
    nav.appendChild(createNavIcon("fa-grip", "Dashboard", () => openDashboard(), state.currentView === "dashboard"));

    // Inbox
    nav.appendChild(createNavIcon("fa-inbox", "Inbox", () => openInbox(), state.currentView === "inbox"));
    
    // Separator
    const sep = document.createElement("div");
    sep.className = "h-px w-8 bg-zinc-800 mx-auto my-2";
    nav.appendChild(sep);
    
    // Projects (Active)
    const activeProjects = getActiveProjects();
    const archivedProjects = getArchivedProjects();

    activeProjects.forEach(p => {
        const isActive = state.currentView === "project" && state.currentProjectId === p.id;
        // First letter or icon
        const label = p.name ? p.name.substring(0, 2).toUpperCase() : "PR";
        nav.appendChild(createNavIcon(null, p.name, () => openProject(p.id), isActive, label));
    });

    // Add Project Button
    const addBtn = document.createElement("button");
    addBtn.className = "w-10 h-10 rounded-lg flex items-center justify-center text-zinc-500 hover:text-white hover:bg-zinc-800 transition-all mt-2 mx-auto";
    addBtn.innerHTML = `<i class="fa-solid fa-plus"></i>`;
    addBtn.onclick = createNewProjectPrompt;
    nav.appendChild(addBtn);

    // Archived Projects (if any)
    if (archivedProjects.length) {
        const sepArchive = document.createElement("div");
        sepArchive.className = "h-px w-8 bg-zinc-800 mx-auto my-2";
        nav.appendChild(sepArchive);

        archivedProjects.forEach(p => {
            const isActive = state.currentView === "project" && state.currentProjectId === p.id;
            const label = p.name ? p.name.substring(0, 2).toUpperCase() : "AR";
            const tip = `${p.name || 'Project'} (Archived)`;
            nav.appendChild(createNavIcon(null, tip, () => openProject(p.id), isActive, label));
        });
    }

    // Separator
    const sep2 = document.createElement("div");
    sep2.className = "h-px w-8 bg-zinc-800 mx-auto my-2";
    nav.appendChild(sep2);

    // Settings
    nav.appendChild(createNavIcon("fa-gear", "Settings", () => openSettings(), state.currentView === "settings"));
}

async function openDashboard() {
    state.currentView = 'dashboard';
    state.currentProjectId = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
}

async function openInbox() {
    state.currentView = 'inbox';
    state.currentProjectId = null;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
}

async function openProject(projectId) {
    state.currentView = 'project';
    state.currentProjectId = projectId;
    await loadChatHistory();
    renderNav();
    renderMain();
    renderChat();
}

async function openSettings() {
    state.currentView = 'settings';
    state.currentProjectId = null;
    await fetchSettings();
    await refreshSlackTeamPresence({ force: true });
    renderNav();
    renderMain();
    renderChat();
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
    const container = document.getElementById("main-port");
    if (!container) return;
    
    container.innerHTML = "";

    // Reduce full-page scrolling: scroll inside panes for data-heavy views.
    if (state.currentView === 'project' || state.currentView === 'dashboard' || state.currentView === 'inbox') {
        setMainPortScrolling(false);
    } else {
        setMainPortScrolling(true);
    }
    
    if (state.currentView === "dashboard") {
        renderDashboard(container);
    } else if (state.currentView === "inbox") {
        renderInbox(container);
    } else if (state.currentView === "project") {
        renderProjectView(container);
    } else if (state.currentView === "settings") {
        renderSettings(container);
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
        for (const item of visible) {
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
                        <div class="flex items-center gap-2">
                            ${inboxStatusBadge(status)}
                            ${inboxSourceBadge(item?.source)}
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
            list.appendChild(card);
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
        <div class="flex flex-wrap gap-2 mt-4">
            <button id="btn-save-quo" class="px-3 py-2 rounded bg-blue-600 text-white text-xs hover:bg-blue-500">Save Quo</button>
        </div>
    `;
    wrap.appendChild(quo);

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
    const btnSaveAgent = document.getElementById('btn-save-agent');
    const btnSaveUi = document.getElementById('btn-save-ui');
    const btnSaveAdvanced = document.getElementById('btn-save-advanced');
    const btnResetAdvanced = document.getElementById('btn-reset-advanced');
    const btnSaveMcp = document.getElementById('btn-save-mcp');
    const btnTestMcp = document.getElementById('btn-test-mcp');
    const mcpToolsOutput = document.getElementById('mcp-tools-output');

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
            if (!token) {
                if (!state.settings.quoConfigured) throw new Error('Auth token is required');
                alert('No changes to save.');
                return;
            }
            await saveSettingsPatch({ quoAuthToken: token });
            alert('Quo settings saved.');
            renderSettings(container);
        } catch (e) {
            alert(e?.message || 'Failed to save Quo settings');
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

function renderDashboard(container) {
    const titleEl = document.getElementById("page-title");
    if(titleEl) titleEl.innerText = "Command Dashboard";
    
    // Stats Banner
    const activeProjects = getActiveProjects();
    const archivedProjects = getArchivedProjects();
    const visibleProjects = state.showArchivedOnDashboard ? [...activeProjects, ...archivedProjects] : activeProjects;
    const totalTasks = state.tasks.length;
    const completedTasks = state.tasks.filter(t => isDoneTask(t)).length;
    const progress = totalTasks === 0 ? 0 : Math.round((completedTasks / totalTasks) * 100);
    
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
    content.className = "flex-1 min-h-0 overflow-y-auto p-6";
    const buckets = bucketProjectsByDueDate(activeProjects);

    // New Project Intake
    const intake = document.createElement('div');
    intake.className = 'mb-6 border border-zinc-800 rounded-xl bg-zinc-900/30 p-4';

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

    const unreadGroupsMap = new Map();
    for (const item of unreadItems) {
        const key = normalizeInboxSourceKey(item?.source);
        if (!unreadGroupsMap.has(key)) unreadGroupsMap.set(key, []);
        unreadGroupsMap.get(key).push(item);
    }
    const unreadGroups = Array.from(unreadGroupsMap.entries())
        .map(([key, items]) => ({ key, items }))
        .sort((a, b) => b.items.length - a.items.length);

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
                const meta = inboxSourceMeta(group.key);
                const newest = group.items[0];
                const newestTime = safeText(newest?.createdAt) ? formatTimeFromIso(newest.createdAt) : '';
                const previewButtons = group.items.slice(0, 4).map((item) => {
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
                                <div class="text-[10px] font-mono uppercase tracking-widest text-zinc-500">Source</div>
                                <div class="flex items-center gap-2 mt-0.5">
                                    <i class="fa-solid ${meta.icon} ${meta.tone}"></i>
                                    <div class="text-sm text-zinc-100 truncate">${escapeHtml(meta.label)}</div>
                                </div>
                            </div>
                            <div class="shrink-0 px-2 py-1 rounded border border-zinc-700 bg-zinc-900/40 text-xs font-mono text-zinc-200">${group.items.length}</div>
                        </div>
                        <div class="mt-1 text-[10px] font-mono text-zinc-500">Latest: ${escapeHtml(newestTime || '—')}</div>
                        <div class="mt-2 max-h-0 opacity-0 overflow-hidden transition-all duration-200 ease-out group-hover:max-h-64 group-hover:opacity-100 space-y-1.5">
                            ${previewButtons}
                            <button data-dash-open-inbox="group-${escapeHtml(group.key)}" class="w-full text-center px-2 py-1 rounded border border-zinc-800 text-[10px] font-mono text-zinc-300 hover:text-white hover:bg-zinc-800 transition-colors">Open full Inbox</button>
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
        <div class="mt-3 ${unreadItems.length ? 'hidden' : ''} text-xs text-zinc-500">No new inbox items.</div>
    `;
    content.appendChild(unreadPanel);

    content.appendChild(renderProjectBuckets(buckets, { bulkMode: !!state.dashboardBulkMode }));

    if (state.showArchivedOnDashboard) {
        const archivedCard = renderProjectBucket('Archived', archivedProjects, { bulkMode: !!state.dashboardBulkMode });
        archivedCard.classList.add('mt-6');
        content.appendChild(archivedCard);
    }

    // Today panel (secondary)
    content.appendChild(renderTodayPanel());

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
        const el = document.getElementById(id);
        if (!el) return;
        el.addEventListener('input', () => {
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
    unreadPanel.querySelectorAll('button[data-dash-open-inbox]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            await openInbox();
        });
    });
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
            const el = document.createElement('div');
            el.className = 'space-y-3';
            el.innerHTML = `
                <div class="flex items-center justify-between">
                    <div class="text-zinc-400 text-xxs font-mono uppercase tracking-widest">Scratchpad</div>
                    <div class="text-zinc-600 text-[10px] font-mono">${scratchpadUpdatedAt ? `updated ${new Date(scratchpadUpdatedAt).toLocaleString()}` : ''}</div>
                </div>
                <textarea id="proj-scratchpad" rows="10" class="w-full bg-zinc-950/40 border border-zinc-800 rounded px-3 py-2 text-xs font-mono text-zinc-200 resize-none" placeholder="Quick notes, deliverables, blockers...">${escapeHtml(scratchpadText)}</textarea>
                <button id="btn-save-scratch" class="w-full bg-zinc-800 hover:bg-zinc-700 text-zinc-200 px-3 py-2 rounded text-xs font-semibold uppercase tracking-wide transition-colors">Save</button>
            `;
            panel.appendChild(el);

            const saveScratchBtn = el.querySelector('#btn-save-scratch');
            const scratchArea = el.querySelector('#proj-scratchpad');
            if (saveScratchBtn && scratchArea) {
                saveScratchBtn.onclick = async () => {
                    saveScratchBtn.disabled = true;
                    try {
                        await saveScratchpad(project.id, scratchArea.value);
                        alert('Scratchpad saved.');
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
                    return `
                        <div class="border border-zinc-800 rounded-md bg-zinc-950/30 p-3">
                            <div class="text-zinc-300 text-xxs font-mono uppercase tracking-widest">${escapeHtml(kind)}${date ? ` • ${escapeHtml(date)}` : ''}${title ? ` • ${escapeHtml(title)}` : ''}</div>
                            <div class="mt-2 text-xs text-zinc-200 whitespace-pre-wrap font-mono">${escapeHtml(content)}</div>
                        </div>
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
                    return `
                        <div class="border border-zinc-800 rounded-md bg-zinc-950/30 p-3">
                            <div class="text-zinc-500 text-xxs font-mono uppercase tracking-widest">${escapeHtml(ctype)} • ${escapeHtml(direction)}${date ? ` • ${escapeHtml(date)}` : ''}</div>
                            <div class="mt-1 text-xs text-zinc-200 font-mono">${escapeHtml(subject)}</div>
                            ${bodyText ? `<div class=\"mt-2 text-xs text-zinc-300 whitespace-pre-wrap font-mono\">${escapeHtml(bodyText)}</div>` : ''}
                        </div>
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
    div.className = `group flex items-center gap-4 p-3 rounded border border-zinc-800/50 bg-zinc-900/30 hover:bg-zinc-800/50 hover:border-zinc-700 transition-all ${isDoneTask(task) ? 'opacity-50' : ''}`;
    
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
        <div class="flex items-center gap-1.5 hover:text-zinc-300 cursor-pointer" title="Reassign">
            <div class="w-4 h-4 rounded-full bg-zinc-700 flex items-center justify-center text-[10px] text-white font-bold uppercase">${avatarTxt}</div>
            <span>${ownerName}</span>
        </div>
    `;
    
    metaRow.innerHTML = assigneeHtml;
    const reassignEl = metaRow.querySelector('div[title="Reassign"]');
    if (reassignEl) {
        reassignEl.addEventListener('click', async (e) => {
            e.stopPropagation();
            const humans = getHumanTeamMembers();
            const names = humans.map((m) => m.name);
            if (!names.length) return alert('No team members configured. Add them in Settings → Team.');
            const next = prompt(`Reassign owner (available: ${names.join(', ')})`, ownerName === 'Unassigned' ? '' : ownerName);
            const picked = safeText(next).trim();
            if (!picked) return;
            if (!names.includes(picked)) return alert('Pick an existing team member name.');

            const counts = getOpenTaskCountByOwner();
            const limit = getWipLimitForOwner(picked);
            const current = Number(counts[picked] || 0);
            if (current >= limit) {
                const ok = confirm(`${picked} is at WIP limit (${current}/${limit}). Assign anyway?`);
                if (!ok) return;
            }

            const res = await apiFetch(`/api/tasks/${task.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ baseRevision: state.revision, patch: { owner: picked } })
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
    await openDashboard();
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
    const drawer = document.getElementById("neural-drawer");
    if (!drawer) return;
    
    state.isChatOpen = !state.isChatOpen;
    
    if (state.isChatOpen) {
        drawer.classList.remove("translate-x-full");
    } else {
        drawer.classList.add("translate-x-full");
    }
}

async function handleChatSubmit() {
    const input = document.getElementById("cmd-input");
    const msg = input.value.trim();
    if (!msg) return;
    
    input.value = "";
    
    // Add User Msg
    recordChatMessage("user", msg);
    addChatMessage("user", msg);
    
    // Show Thinking
    const status = document.getElementById("ai-status");
    if(status) status.style.opacity = "1";
    
    try {
        const res = await apiFetch("/api/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ 
                message: msg,
                projectId: state.currentProjectId || undefined
            })
        });
        
        const data = await res.json();
        const reply = data.reply || data.text || "Command Processed.";
        recordChatMessage("ai", reply);
        addChatMessage("ai", reply);
        
        // Refresh state in case AI changed things
        await fetchState();
        await loadChatHistory();
        renderChat();
        renderNav();
        renderMain();
        
    } catch (e) {
        recordChatMessage("ai", "Error: Connection Severed.");
        addChatMessage("ai", "Error: Connection Severed.");
    } finally {
        if(status) status.style.opacity = "0";
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
}

function addChatMessage(role, text) {
    const stream = document.getElementById("chat-stream");
    if(!stream) return;
    
    const div = document.createElement("div");
    div.className = "flex flex-col gap-1 mb-4 animate-fade-in";
    
    div.innerHTML = `
        <span class="text-[10px] uppercase font-bold tracking-wider ${role === 'ai' ? 'text-blue-500' : 'text-zinc-500 text-right'}">${role === 'ai' ? 'Neural Core' : 'Operator'}</span>
        <div class="p-2 rounded text-xs ${role === 'ai' ? 'bg-zinc-800/50 text-zinc-300 border-l-2 border-blue-500' : 'bg-blue-900/10 text-blue-200 border-r-2 border-blue-500/50 self-end'} max-w-[90%] break-words shadow-sm">
            ${text}
        </div>
    `;
    
    stream.appendChild(div);
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
            <p>Ready for directives.</p>
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
