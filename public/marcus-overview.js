(function attachMarcusOverview(global) {
    const list = (value) => Array.isArray(value) ? value : [];
    const text = (value) => String(value == null ? '' : value).trim();
    const lower = (value) => text(value).toLowerCase();
    const html = (value) => escapeHtml(text(value));
    const projectIdOf = (item) => text(item?.projectId || item?.projectRegistryId || item?.relatedProjectId);

    function statusKey(value) {
        const raw = lower(value).replace(/[\s_]+/g, '-');
        if (/approval|review|needs-mark|decision/.test(raw)) return 'review';
        if (/block|fail|error|cancel/.test(raw)) return 'blocked';
        if (/verify|checking|test/.test(raw)) return 'verifying';
        if (/done|complete|verified|success|closed/.test(raw)) return 'verified';
        if (/wait|pause|queued|draft|planned/.test(raw)) return 'waiting';
        if (/run|progress|active|execut/.test(raw)) return 'running';
        return 'waiting';
    }

    function isClosedProject(project) {
        return /archived|closed|done|complete/.test(lower(project?.status));
    }

    function relativeTime(value) {
        const stamp = Date.parse(value || '');
        if (!Number.isFinite(stamp)) return 'not yet';
        const minutes = Math.max(0, Math.floor((Date.now() - stamp) / 60000));
        if (minutes < 1) return 'just now';
        if (minutes < 60) return `${minutes}m ago`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h ago`;
        return `${Math.floor(hours / 24)}d ago`;
    }

    function timeLabel(value) {
        const date = new Date(value || Date.now());
        if (Number.isNaN(date.getTime())) return '';
        return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
    }

    function itemProjectMatch(item, project) {
        const id = text(project?.id);
        const name = lower(project?.name);
        return (id && projectIdOf(item) === id) || (name && lower(item?.project || item?.projectName) === name);
    }

    function workItemsFor(project) {
        const tasks = list(state.tasks).filter((item) => itemProjectMatch(item, project)).map((task) => ({
            id: text(task.id) || `task:${text(task.title || task.name)}`,
            kind: text(task.kind || task.type || 'Task'),
            name: text(task.title || task.name || task.text) || 'Untitled task',
            status: statusKey(task.status),
            rawStatus: text(task.status) || 'Waiting',
            detail: text(task.currentStep || task.summary || task.notes || task.description || task.nextAction),
            updatedAt: task.updatedAt || task.createdAt || '',
            source: task,
        }));
        const operations = list(state.operations).filter((item) => itemProjectMatch(item, project)).map((operation) => ({
            id: text(operation.id),
            kind: 'Codex session',
            name: text(operation.title || operation.objective || operation.name) || 'Codex operation',
            status: statusKey(operation.status),
            rawStatus: text(operation.status) || 'Running',
            detail: text(operation.currentStep?.title || operation.currentStep || operation.summary || operation.resultSummary),
            updatedAt: operation.updatedAt || operation.createdAt || '',
            source: operation,
        }));
        return [...operations, ...tasks].sort((a, b) => {
            const order = { review: 0, blocked: 1, running: 2, verifying: 3, waiting: 4, verified: 5 };
            return (order[a.status] ?? 9) - (order[b.status] ?? 9) || text(b.updatedAt).localeCompare(text(a.updatedAt));
        });
    }

    function projectAttention(project) {
        const brief = state.activeBrief || {};
        const candidates = [...list(brief.topPriorities), ...list(brief.decisionQueue), ...list(brief.activeActionQueue)];
        return candidates.find((item) => itemProjectMatch(item, project)) || null;
    }

    function projectActivity(project) {
        const brief = state.activeBrief || {};
        return list(brief.projectActivity).find((item) => itemProjectMatch(item, project))
            || list(brief.projects).find((item) => itemProjectMatch(item, project) || lower(item?.name) === lower(project?.name))
            || null;
    }

    function projectSnapshot(project) {
        const items = workItemsFor(project);
        const counts = { running: 0, review: 0, blocked: 0, verifying: 0, verified: 0, waiting: 0 };
        for (const item of items) counts[item.status] = Number(counts[item.status] || 0) + 1;
        const attention = projectAttention(project);
        const activity = projectActivity(project);
        const updatedAt = activity?.updatedAt || activity?.lastActivityAt || project?.updatedAt || project?.createdAt || '';
        const staleDays = updatedAt ? Math.floor((Date.now() - Date.parse(updatedAt)) / 86400000) : 999;
        let stateLabel = 'Steady';
        let stateClass = '';
        if (counts.review || attention?.requiresMark || attention?.approvalRequired) { stateLabel = 'Needs Mark'; stateClass = 'needs-mark'; }
        else if (counts.blocked) { stateLabel = 'Blocked'; stateClass = 'blocked'; }
        else if (counts.running || counts.verifying) stateLabel = 'Moving';
        else if (staleDays >= 3) { stateLabel = 'Cooling'; stateClass = 'cooling'; }
        else if (counts.waiting) { stateLabel = 'Waiting'; stateClass = 'waiting'; }
        return { items, counts, attention, activity, updatedAt, stateLabel, stateClass };
    }

    function selectedProject() {
        const projects = list(state.projects).filter((project) => !isClosedProject(project));
        const selectedId = text(state.overviewSelectedProjectId || state.currentProjectId);
        const explicit = projects.find((project) => text(project.id) === selectedId);
        if (explicit) return explicit;
        return projects
            .map((project) => ({ project, snapshot: projectSnapshot(project) }))
            .sort((a, b) => {
                const rank = (snapshot) => snapshot.counts.review ? 0 : snapshot.counts.blocked ? 1 : snapshot.counts.running ? 2 : snapshot.counts.verifying ? 3 : 4;
                return rank(a.snapshot) - rank(b.snapshot) || text(b.snapshot.updatedAt).localeCompare(text(a.snapshot.updatedAt));
            })[0]?.project || null;
    }

    function voiceState() {
        return global.marcusOverviewVoice?.state || state.overviewVoiceState || 'idle';
    }

    function stateLabel(value) {
        return ({
            idle: 'Voice off', connecting: 'Connecting', armed: 'Voice ready', listening: 'Listening',
            thinking: 'Thinking', speaking: 'Speaking', interrupted: 'Interrupted', muted: 'Muted', error: 'Voice unavailable',
        })[value] || value;
    }

    function renderMessages(messages) {
        if (!messages.length) {
            return `<div class="mo-empty"><div><i class="fa-regular fa-comments"></i><p>No conversation in this scope yet.</p></div></div>`;
        }
        return `<div class="mo-transcript-day">TODAY</div>${messages.map((message, index) => {
            const role = lower(message.role) === 'user' ? 'user' : 'ai';
            const isUnheard = role === 'ai' && message.unheard === true;
            const content = text(message.content);
            return `<article class="mo-message ${role} ${isUnheard ? 'unheard' : ''}" data-message-index="${index}">
                <div class="mo-message-avatar">${role === 'ai' ? '<i class="fa-solid fa-microchip"></i>' : 'M'}</div>
                <div class="mo-message-body">
                    <div class="mo-message-meta">
                        <strong>${role === 'ai' ? 'MARCUS' : 'MARK'}</strong>
                        <span>${html(timeLabel(message.timestamp))}</span>
                        ${message.spoken ? '<span class="mo-spoken"><i class="fa-solid fa-volume-high"></i> Spoken</span>' : ''}
                        ${isUnheard ? '<span class="mo-unheard-tag">UNHEARD</span>' : ''}
                    </div>
                    <div class="mo-message-text">${escapeHtml(content).replace(/\n/g, '<br>')}</div>
                    ${role === 'ai' ? `<div class="mo-message-actions"><button data-replay="${index}"><i class="fa-solid fa-volume-high"></i> Replay</button>${message.evidence ? '<button><i class="fa-regular fa-file-lines"></i> Show evidence</button>' : ''}</div>` : ''}
                </div>
            </article>`;
        }).join('')}`;
    }

    function renderBriefing(project, selectedItem, snapshot) {
        const attention = snapshot?.attention;
        const brief = state.activeBrief || {};
        const observed = selectedItem?.detail || snapshot?.activity?.reason || `${snapshot?.items.length || 0} evidence-backed work items are attached.`;
        const changed = text(brief.sessionBriefing?.changedSummary || snapshot?.activity?.summary) || 'No new verified movement has been recorded since the last read.';
        const risk = text(attention?.detail || attention?.summary) || (snapshot?.counts.blocked ? `${snapshot.counts.blocked} work item is blocked.` : 'No material project risk is currently verified.');
        const recommendation = text(attention?.recommendedAction || attention?.nextAction) || (snapshot?.counts.review ? 'Review the pending decision so execution can continue.' : 'Let active work continue and verify the next checkpoint.');
        return `<div class="mo-briefing">
            <section><h3>Current status</h3><p>${html(observed)}</p></section>
            <section><h3>What changed</h3><p>${html(changed)}</p></section>
            <section><h3>Risk / dependency</h3><p>${html(risk)}</p></section>
            <section><h3>What I would do</h3><p>${html(recommendation)}</p></section>
            <section><h3>Evidence boundary</h3><p>Observed facts come from project records, Codex operations, checkpoints, and the current active brief. Inference and recommendation remain explicitly separate.</p></section>
        </div>`;
    }

    function renderProject(project, selected) {
        const snapshot = projectSnapshot(project);
        const expanded = selected;
        const item = snapshot.items.find((entry) => entry.id === text(state.overviewSelectedWorkId)) || snapshot.items[0] || null;
        const observed = item ? `${item.name}: ${item.detail || item.rawStatus}` : (snapshot.activity?.reason || 'No verified work item has moved yet.');
        const matters = snapshot.attention?.detail || snapshot.attention?.summary || (snapshot.counts.blocked ? 'A blocker is holding downstream work.' : 'Current execution can continue without intervention.');
        const recommendation = snapshot.attention?.recommendedAction || snapshot.attention?.nextAction || (snapshot.counts.review ? 'Review the pending decision.' : 'Keep the project moving to its next checkpoint.');
        const total = snapshot.items.length;
        const live = snapshot.counts.running + snapshot.counts.verifying;
        const segments = Object.entries(snapshot.counts).filter(([, count]) => count).map(([key, count]) => `<span class="${key}" title="${key}: ${count}" style="grid-column:span ${count}"></span>`).join('') || '<span></span>';
        return `<section class="mo-project ${selected ? 'selected' : ''}" data-project-card="${html(project.id)}">
            <div class="mo-project-header" data-select-project="${html(project.id)}">
                <div class="mo-project-icon"><i class="fa-solid ${/website|web/i.test(text(project.type)) ? 'fa-globe' : 'fa-layer-group'}"></i></div>
                <div>
                    <div class="mo-project-title"><strong>${html(project.name || 'Unnamed project')}</strong><span class="mo-state ${snapshot.stateClass}">${html(snapshot.stateLabel)}</span></div>
                    <div class="mo-project-meta">${total} work item${total === 1 ? '' : 's'} &nbsp;•&nbsp; ${live} live &nbsp;•&nbsp; Last verified ${html(relativeTime(snapshot.updatedAt))}</div>
                </div>
                <div class="mo-project-actions">
                    <button class="mo-button" data-ask-project="${html(project.id)}"><i class="fa-regular fa-comment"></i>Ask MARCUS</button>
                    <button class="mo-icon-button" data-project-menu="${html(project.id)}" title="Project lifecycle"><i class="fa-solid fa-ellipsis"></i></button>
                </div>
            </div>
            <div class="mo-healthbar" style="grid-template-columns:repeat(${Math.max(total, 1)},1fr)">${segments}</div>
            ${expanded ? `<div class="mo-brief-card">
                <div class="mo-brief-top"><div><div class="mo-brief-title"><i class="fa-solid fa-wave-square"></i>MARCUS BRIEF</div><div class="mo-brief-summary">${html(snapshot.activity?.summary || snapshot.attention?.summary || `${project.name} is ${lower(snapshot.stateLabel)}. MARCUS is separating verified movement from inferred risk.`)}</div></div><button class="mo-button" data-studio-focus="1" title="Reply to MARCUS"><i class="fa-regular fa-comment"></i><span>Reply to MARCUS</span></button></div>
                <div class="mo-brief-facts"><div class="mo-fact"><small>What changed · observed</small><span>${html(observed)}</span></div><div class="mo-fact"><small>What matters · inferred</small><span>${html(matters)}</span></div><div class="mo-fact"><small>My recommendation</small><span>${html(recommendation)}</span></div></div>
            </div>
            <div class="mo-work-list">${snapshot.items.length ? snapshot.items.map((entry) => `<button class="mo-work-item ${entry.id === item?.id ? 'selected' : ''}" data-select-work="${html(entry.id)}">
                <span class="mo-work-type"><i class="fa-solid ${lower(entry.kind).includes('codex') ? 'fa-code' : lower(entry.kind).includes('depend') ? 'fa-link' : 'fa-square-check'}"></i>${html(entry.kind)}</span>
                <span class="mo-work-name">${html(entry.name)}</span><span class="mo-work-status ${entry.status}">${html(entry.rawStatus)}</span><span class="mo-work-detail">${html(entry.detail || relativeTime(entry.updatedAt))}</span><i class="fa-solid fa-chevron-right"></i>
            </button>`).join('') : '<div class="mo-empty"><div>No sessions, tasks, dependencies, or checkpoints have been observed for this project yet.</div></div>'}</div>` : ''}
        </section>`;
    }

    function renderStudio(project) {
        const snapshot = project ? projectSnapshot(project) : null;
        const selectedItem = snapshot?.items.find((entry) => entry.id === text(state.overviewSelectedWorkId)) || snapshot?.items[0] || null;
        const messages = list(state.chatHistory).slice(-80);
        const tab = state.overviewStudioTab === 'briefing' ? 'briefing' : 'conversation';
        const unheard = messages.filter((message) => message?.unheard === true && lower(message.role) !== 'user').length;
        const vState = voiceState();
        const scope = project ? `${project.name}${selectedItem ? ` / ${selectedItem.name}` : ''}` : 'Everything';
        return `<aside class="mo-pane mo-studio" aria-label="MARCUS Studio">
            <header class="mo-studio-head">
                <div class="mo-studio-kicker"><strong>MARCUS STUDIO</strong><span class="mo-voice-live"><span class="mo-dot ${vState === 'error' ? 'red' : vState === 'idle' ? 'amber' : ''}"></span><span data-voice-label>${html(stateLabel(vState))}</span><i class="fa-solid fa-wave-square"></i></span></div>
                <div class="mo-studio-title"><i class="fa-solid fa-crosshairs"></i><span>${html(scope)}</span></div>
            </header>
            <div class="mo-tabs"><button class="mo-tab ${tab === 'conversation' ? 'active' : ''}" data-studio-tab="conversation"><i class="fa-regular fa-comment"></i> Conversation</button><button class="mo-tab ${tab === 'briefing' ? 'active' : ''}" data-studio-tab="briefing"><i class="fa-regular fa-file-lines"></i> Briefing</button></div>
            <div class="mo-session-controls"><button class="continue" data-session-intent="continue"><i class="fa-solid fa-play"></i> Continue</button><button data-session-intent="redirect"><i class="fa-solid fa-share"></i> Redirect</button><button class="stop" data-session-intent="stop safely"><i class="fa-solid fa-stop"></i> Stop safely</button></div>
            <div class="mo-studio-content" id="mo-studio-content">
                ${tab === 'conversation' ? `${unheard ? `<div class="mo-unheard"><span><i class="fa-solid fa-wave-square"></i> ${unheard} unheard response${unheard === 1 ? '' : 's'}</span><button class="mo-button" data-play-unheard><i class="fa-solid fa-play"></i>Play</button></div>` : ''}${renderMessages(messages)}` : renderBriefing(project, selectedItem, snapshot)}
            </div>
            <div class="mo-composer">
                <div class="mo-scope"><select id="mo-scope"><option value="project" ${project ? 'selected' : ''}>${project ? html(project.name) : 'Everything'}</option><option value="global" ${project ? '' : 'selected'}>Everything</option>${selectedItem ? `<option value="work">This ${html(lower(selectedItem.kind))}</option>` : ''}</select></div>
                <div class="mo-compose-row"><textarea id="mo-compose" rows="1" placeholder="Reply to MARCUS..."></textarea><button class="mo-voice-button" id="mo-voice" data-state="${html(vState)}" title="${vState === 'idle' || vState === 'error' ? 'Start live voice' : 'Stop live voice'}"><i class="fa-solid ${vState === 'speaking' ? 'fa-volume-high' : vState === 'thinking' ? 'fa-circle-notch fa-spin' : vState === 'listening' ? 'fa-wave-square' : 'fa-microphone'}"></i></button><button class="mo-send-button" id="mo-send" title="Send"><i class="fa-solid fa-paper-plane"></i></button></div>
                ${state.overviewVoiceError ? `<div class="mo-voice-error">${html(state.overviewVoiceError)}</div>` : ''}
                <div class="mo-suggestions"><button data-suggestion="What changed?">What changed?</button><button data-suggestion="Is there any risk?">Any risk?</button><button data-suggestion="What happens next?">What happens next?</button></div>
            </div>
        </aside>`;
    }

    function globalBriefText() {
        const brief = state.activeBrief || {};
        return text(brief.narrativeSummary || brief.sessionBriefing?.briefingLine) || (state.activeBriefLoading ? 'MARCUS is reconciling current evidence...' : 'MARCUS is watching for verified movement, decisions, and lost momentum.');
    }

    async function ensureOverviewWakeListener() {
        if (global.marcusOverviewWakeSource || global.marcusOverviewWakeInitializing) return;
        global.marcusOverviewWakeInitializing = true;
        try {
            const response = await apiFetch('/api/marcus/live/session');
            const data = await response.json().catch(() => ({}));
            if (!response.ok || !data?.token) throw new Error(data?.error || 'Voice wake channel is unavailable.');
            const source = new EventSource(`/api/marcus/live?token=${encodeURIComponent(data.token)}`, { withCredentials: true });
            source.onmessage = (event) => {
                try {
                    const payload = JSON.parse(event.data);
                    if (payload?.type !== 'voice_wake') return;
                    const client = ensureOverviewVoice(selectedProject());
                    state.overviewVoiceError = '';
                    if (!client.peer) client.start().catch((error) => {
                        state.overviewVoiceError = text(error?.message) || 'Wake detected, but live voice could not start.';
                        if (state.currentView === 'overview') renderMain();
                    });
                    else if (client.muted) client.toggleMute(false);
                } catch { /* ignore malformed live events */ }
            };
            global.marcusOverviewWakeSource = source;
        } catch {
            // Wake-word satellites are optional; direct Studio voice remains available.
        } finally {
            global.marcusOverviewWakeInitializing = false;
        }
    }

    function renderMarcusOverview(container) {
        ensureOverviewWakeListener().catch(() => {});
        if (!state.activeBrief && !state.activeBriefLoading) refreshActiveBrief({ force: true }).then(() => state.currentView === 'overview' && renderMain()).catch(() => {});
        if (!state.controlPlaneDocument && !state.controlPlaneLoading) refreshControlPlane({ force: true }).then(() => state.currentView === 'overview' && renderMain()).catch(() => {});
        if (!state.operationsLoading && (!state.operationsFetchedAt || Date.now() - state.operationsFetchedAt > 15000)) refreshOperations({ force: true }).then(() => state.currentView === 'overview' && renderMain()).catch(() => {});

        const allProjects = list(state.projects);
        const activeProjects = allProjects.filter((project) => !isClosedProject(project));
        const closedCount = allProjects.length - activeProjects.length;
        const project = selectedProject();
        if (project && text(state.overviewSelectedProjectId) !== text(project.id)) state.overviewSelectedProjectId = text(project.id);
        if (project && text(state.currentProjectId) !== text(project.id) && !state.overviewProjectChatLoading) {
            state.currentProjectId = text(project.id);
            state.overviewProjectChatLoading = true;
            loadChatHistory().then(() => {
                state.overviewProjectChatLoading = false;
                if (state.currentView === 'overview') renderMain();
            }).catch(() => { state.overviewProjectChatLoading = false; });
        }
        const snapshots = activeProjects.map((entry) => ({ project: entry, snapshot: projectSnapshot(entry) })).sort((a, b) => {
            const rank = (snapshot) => snapshot.counts.review ? 0 : snapshot.counts.blocked ? 1 : snapshot.counts.running ? 2 : snapshot.counts.verifying ? 3 : 4;
            return rank(a.snapshot) - rank(b.snapshot) || text(b.snapshot.updatedAt).localeCompare(text(a.snapshot.updatedAt));
        });
        const running = snapshots.filter(({ snapshot }) => snapshot.counts.running || snapshot.counts.verifying).length;
        const needsMark = snapshots.filter(({ snapshot }) => snapshot.counts.review || snapshot.attention?.requiresMark).length;
        const blocked = snapshots.filter(({ snapshot }) => snapshot.counts.blocked).length;
        const filter = state.overviewFilter || 'all';
        const query = lower(state.overviewSearch);
        const visible = snapshots.filter(({ project: entry, snapshot }) => {
            if (query && !lower(`${entry.name} ${entry.type} ${snapshot.items.map((item) => `${item.name} ${item.detail}`).join(' ')}`).includes(query)) return false;
            if (filter === 'active') return snapshot.counts.running || snapshot.counts.verifying;
            if (filter === 'needs') return snapshot.counts.review || snapshot.attention?.requiresMark;
            if (filter === 'verifying') return snapshot.counts.verifying;
            if (filter === 'blocked') return snapshot.counts.blocked;
            return true;
        });
        const liveSessions = list(state.operations).filter((operation) => ['running', 'queued', 'verifying', 'awaiting_provider'].includes(lower(operation.status))).length;

        container.innerHTML = `<div class="marcus-overview">
            <header class="mo-topbar">
                <div class="mo-brand"><div class="mo-mark"><span>M</span></div><div class="mo-wordmark">M.A.R.C.U.S.</div><div class="mo-presence"><span class="mo-dot"></span><span>${activeProjects.length} projects</span><span class="mo-separator">•</span><span>${liveSessions} sessions running</span><span class="mo-separator">•</span><span style="color:var(--marcus-amber)">${needsMark} needs you</span></div></div>
                <label class="mo-search"><i class="fa-solid fa-magnifying-glass"></i><input id="mo-search" value="${html(state.overviewSearch || '')}" placeholder="Search projects, tasks, evidence..."><kbd>⌘K</kbd></label>
                <div class="mo-actions"><button class="mo-icon-button" data-open-control title="Controls"><i class="fa-solid fa-sliders"></i></button><button class="mo-icon-button" title="Notifications"><i class="fa-regular fa-bell"></i></button><button class="mo-icon-button" data-open-settings title="Settings"><i class="fa-solid fa-gear"></i></button><button class="mo-icon-button mo-avatar" title="Mark">M</button></div>
            </header>
            <div class="mo-global-brief"><i class="fa-regular fa-circle-info"></i><span><strong>MARCUS:</strong> ${html(globalBriefText())}</span><i class="fa-solid fa-chevron-right" style="margin-left:auto"></i></div>
            <main class="mo-workspace">
                <section class="mo-pane mo-projects-pane">
                    <header class="mo-pane-head"><h2><i class="fa-solid fa-layer-group"></i>PROJECTS IN MOTION</h2><div class="mo-filters">
                        <button class="mo-filter ${filter === 'all' ? 'active' : ''}" data-filter="all">All <span class="mo-count">${activeProjects.length}</span></button>
                        <button class="mo-filter ${filter === 'active' ? 'active' : ''}" data-filter="active">Active <span class="mo-count">${running}</span></button>
                        <button class="mo-filter ${filter === 'needs' ? 'active' : ''}" data-filter="needs">Needs you <span class="mo-count">${needsMark}</span></button>
                        <button class="mo-filter ${filter === 'verifying' ? 'active' : ''}" data-filter="verifying">Verifying</button>
                        <button class="mo-filter ${filter === 'blocked' ? 'active' : ''}" data-filter="blocked">Blocked <span class="mo-count">${blocked}</span></button>
                    </div></header>
                    <div class="mo-project-scroll">${visible.length ? visible.map(({ project: entry }) => renderProject(entry, text(entry.id) === text(project?.id))).join('') : '<div class="mo-empty"><div><i class="fa-solid fa-radar"></i><p>No project evidence matches this view.</p><p>MARCUS will surface work here when sessions, tasks, repositories, or conversations create movement.</p></div></div>'}</div>
                </section>
                ${renderStudio(project)}
            </main>
            <footer class="mo-bottom"><div class="mo-bottom-group"><button class="mo-bottom-nav active"><i class="fa-solid fa-house"></i><span>Overview</span></button><button class="mo-bottom-nav" data-open-god><i class="fa-solid fa-table-cells-large"></i><span class="mo-wide-label">Project God Mode</span><span class="mo-mobile-label">God Mode</span></button><button class="mo-bottom-nav" data-open-operations><i class="fa-regular fa-square-check"></i><span class="mo-wide-label">Task Control Room</span><span class="mo-mobile-label">Task Room</span></button></div><form class="mo-global-compose" id="mo-global-form"><input id="mo-global-input" placeholder="Talk to MARCUS about anything..."><button title="Send globally"><i class="fa-solid fa-wave-square"></i></button></form><div class="mo-closed"><button class="mo-button" data-open-closed title="Closed projects"><i class="fa-regular fa-folder"></i>Closed projects ${closedCount ? `(${closedCount})` : ''}</button></div></footer>
        </div>`;
        bindOverview(container, project);
        const studio = container.querySelector('#mo-studio-content');
        if (studio && tabIsConversation()) studio.scrollTop = studio.scrollHeight;
    }

    function tabIsConversation() {
        return state.overviewStudioTab !== 'briefing';
    }

    async function selectProject(id) {
        const next = list(state.projects).find((project) => text(project.id) === text(id));
        if (!next) return;
        state.overviewSelectedProjectId = text(next.id);
        state.currentProjectId = text(next.id);
        state.overviewSelectedWorkId = '';
        await loadChatHistory();
        renderMain();
        broadcastMarcusContext();
    }

    async function persistVoiceTranscript(entry, project) {
        const response = await apiFetch('/api/chat/transcript', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ...entry, projectId: project?.id || undefined, scopeId: state.overviewSelectedWorkId || undefined }),
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(data?.error || 'Failed to preserve voice transcript');
        return data.entry;
    }

    async function sendOverviewMessage(message, { globalScope = false, operational = false } = {}) {
        const value = text(message);
        if (!value || state.overviewSending) return;
        const project = globalScope ? null : selectedProject();
        state.overviewSending = true;
        const userEntry = { role: 'user', content: value, timestamp: new Date().toISOString() };
        state.chatHistory = [...list(state.chatHistory), userEntry];
        renderMain();
        try {
            const endpoint = operational ? '/api/marcus/live/chat' : '/api/chat';
            const response = await apiFetch(endpoint, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: value, projectId: project?.id || undefined, threadId: 'default' }),
            });
            const data = await response.json().catch(() => ({}));
            if (!response.ok || data?.ok === false) throw new Error(data?.error || `MARCUS request failed (${response.status})`);
            const reply = text(data.reply || data.text) || 'No response was produced.';
            if (operational) {
                await persistVoiceTranscript(userEntry, project).catch(() => {});
                await persistVoiceTranscript({ role: 'ai', content: reply, timestamp: new Date().toISOString(), evidence: data.operation || null }, project).catch(() => {});
            }
            state.chatHistory = [...list(state.chatHistory), { role: 'ai', content: reply, timestamp: new Date().toISOString(), evidence: data.operation || null }];
            await Promise.all([fetchState({ background: false }), refreshActiveBrief({ force: true })]);
            if (project) await loadChatHistory();
        } catch (error) {
            state.chatHistory = [...list(state.chatHistory), { role: 'ai', content: `Error: ${text(error?.message) || 'MARCUS could not respond.'}`, timestamp: new Date().toISOString() }];
        } finally {
            state.overviewSending = false;
            if (state.currentView === 'overview') renderMain();
        }
    }

    function ensureOverviewVoice(project) {
        if (global.marcusOverviewVoice) return global.marcusOverviewVoice;
        if (typeof global.MarcusRealtimeClient !== 'function') throw new Error('Realtime voice client did not load.');
        const client = new global.MarcusRealtimeClient({
            fetcher: apiFetch,
            getContext: () => ({ projectId: selectedProject()?.id || '', workItemId: state.overviewSelectedWorkId || '', view: 'overview' }),
            onToolCall: async ({ arguments: args }) => {
                const response = await apiFetch('/api/marcus/live/chat', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ message: text(args?.message), projectId: selectedProject()?.id || undefined, voiceTool: true }),
                });
                const data = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(data?.error || 'MARCUS tool failed');
                return data;
            },
        });
        client.addEventListener('statechange', (event) => {
            state.overviewVoiceState = event.detail.state;
            state.overviewVoiceError = event.detail.message || '';
            document.querySelectorAll('[data-voice-label]').forEach((node) => { node.textContent = stateLabel(event.detail.state); });
            const button = document.getElementById('mo-voice');
            if (button) button.dataset.state = event.detail.state;
        });
        client.addEventListener('transcript', async (event) => {
            const entry = { ...event.detail, timestamp: new Date().toISOString(), modality: 'voice' };
            try {
                const saved = await persistVoiceTranscript(entry, selectedProject());
                state.chatHistory = [...list(state.chatHistory), saved || entry];
            } catch {
                state.chatHistory = [...list(state.chatHistory), entry];
            }
            if (state.currentView === 'overview') renderMain();
        });
        client.addEventListener('error', (event) => { state.overviewVoiceError = event.detail.message || 'Voice failed.'; });
        global.marcusOverviewVoice = client;
        return client;
    }

    function bindOverview(container, project) {
        container.querySelectorAll('[data-select-project]').forEach((node) => node.addEventListener('click', (event) => {
            if (event.target.closest('button')) return;
            selectProject(node.dataset.selectProject).catch(() => {});
        }));
        container.querySelectorAll('[data-select-work]').forEach((node) => node.addEventListener('click', () => {
            state.overviewSelectedWorkId = node.dataset.selectWork || '';
            renderMain();
        }));
        container.querySelectorAll('[data-filter]').forEach((node) => node.addEventListener('click', () => { state.overviewFilter = node.dataset.filter; renderMain(); }));
        container.querySelectorAll('[data-studio-tab]').forEach((node) => node.addEventListener('click', () => { state.overviewStudioTab = node.dataset.studioTab; renderMain(); }));
        container.querySelectorAll('[data-suggestion]').forEach((node) => node.addEventListener('click', () => sendOverviewMessage(node.dataset.suggestion)));
        container.querySelectorAll('[data-session-intent]').forEach((node) => node.addEventListener('click', () => {
            const selected = text(state.overviewSelectedWorkId);
            sendOverviewMessage(`${node.dataset.sessionIntent} ${selected ? `the selected work item (${selected})` : `work on ${project?.name || 'the current scope'}`}. Preserve evidence and require approval for consequential changes.`, { operational: true });
        }));
        container.querySelectorAll('[data-ask-project]').forEach((node) => node.addEventListener('click', async () => {
            await selectProject(node.dataset.askProject);
            document.getElementById('mo-compose')?.focus();
        }));
        container.querySelectorAll('[data-project-menu]').forEach((node) => node.addEventListener('click', () => {
            const action = global.prompt('Project lifecycle: type PARK, FINAL BRIEF, or CLOSE. History and evidence will be preserved.');
            if (!text(action)) return;
            sendOverviewMessage(`${action} project ${project?.name || node.dataset.projectMenu}. Preserve all evidence, conversations, unresolved work, and require confirmation before closure.`, { operational: true });
        }));
        container.querySelectorAll('[data-replay]').forEach((node) => node.addEventListener('click', () => {
            const message = list(state.chatHistory)[Number(node.dataset.replay)];
            if (message?.content) speakMarcus(message.content);
            if (message) message.unheard = false;
            renderMain();
        }));
        container.querySelector('[data-play-unheard]')?.addEventListener('click', () => {
            const message = [...list(state.chatHistory)].reverse().find((entry) => entry.unheard && lower(entry.role) !== 'user');
            if (message?.content) speakMarcus(message.content);
            if (message) message.unheard = false;
            renderMain();
        });
        container.querySelector('[data-studio-focus]')?.addEventListener('click', () => document.getElementById('mo-compose')?.focus());

        const compose = container.querySelector('#mo-compose');
        const submit = () => {
            const value = text(compose?.value);
            const globalScope = container.querySelector('#mo-scope')?.value === 'global';
            if (compose) compose.value = '';
            sendOverviewMessage(value, { globalScope });
        };
        container.querySelector('#mo-send')?.addEventListener('click', submit);
        compose?.addEventListener('keydown', (event) => { if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); submit(); } });

        container.querySelector('#mo-voice')?.addEventListener('click', async () => {
            try {
                const client = ensureOverviewVoice(project);
                if (client.peer) client.stop(); else await client.start();
                state.overviewVoiceError = '';
            } catch (error) {
                state.overviewVoiceError = text(error?.message) || 'Voice connection failed.';
                if (state.currentView === 'overview') renderMain();
            }
        });

        const search = container.querySelector('#mo-search');
        search?.addEventListener('input', () => { state.overviewSearch = search.value; });
        search?.addEventListener('keydown', (event) => { if (event.key === 'Enter') renderMain(); });
        container.querySelector('#mo-global-form')?.addEventListener('submit', (event) => {
            event.preventDefault();
            const input = container.querySelector('#mo-global-input');
            const value = text(input?.value);
            if (input) input.value = '';
            sendOverviewMessage(value, { globalScope: true });
        });
        container.querySelector('[data-open-god]')?.addEventListener('click', () => openGodView());
        container.querySelector('[data-open-operations]')?.addEventListener('click', () => openOperations());
        container.querySelector('[data-open-control]')?.addEventListener('click', () => openControl());
        container.querySelector('[data-open-settings]')?.addEventListener('click', () => openSettings());
        container.querySelector('[data-open-closed]')?.addEventListener('click', () => openProjects());
    }

    global.renderMarcusOverview = renderMarcusOverview;
    global.sendOverviewMessage = sendOverviewMessage;
})(globalThis);

