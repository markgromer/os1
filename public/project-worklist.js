import { matchWorkProject } from './work-status-view.js?v=3';
const text = (value) => typeof value === 'string' ? value : '';
const pathKey = (value) => text(value).replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase();
const date = (value) => Date.parse(value || '') || 0;
export const escapeHtml = (value) => text(value).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
export const taskKey = (project) => `task:${project.source}:${project.raw?.sessionId || project.raw?.jobId || project.raw?.id || project.id}`;
export function workStartedAt(project) {
  return [project.raw?.latestUserRequestAt, project.raw?.startedAt, project.raw?.createdAt, ...(project.raw?.followups || []).map((item) => item.createdAt)].filter(Boolean).sort((a, b) => date(b) - date(a))[0] || '';
}
export function isSetAside(key, startedAt, preferences = []) {
  const preference = preferences.find((item) => item.key === key);
  return preference?.hidden === true && date(startedAt) <= date(preference.resumeAfter);
}
export function taskStatus(project) {
  if (project.source === 'codex') {
    if (project.raw?.runtimeState === 'running') return 'Running · last observed';
    if (project.raw?.runtimeState === 'interrupted') return 'Interrupted';
    return project.state === 'blocked' ? 'Blocker reported' : project.raw?.runtimeState === 'idle' ? 'Waiting for your next request' : 'Session update';
  }
  const status = project.raw?.status;
  return ({ running: 'Running', started: 'Running', queued: 'Queued', awaiting_provider: 'Waiting on provider', waiting_for_approval: 'Needs you', blocked: 'Blocked', failed: 'Failed', completed: 'Run finished', cancelled: 'Stopped', verifying: 'Checking' })[status] || 'Recorded work';
}
export function readableSummary(value, limit = 220) {
  const clean = text(value).replace(/\[([^\]]+)\]\([^)]*\)/g, '$1').replace(/[*#>`]/g, '').replace(/\s+/g, ' ').trim();
  if (clean.length <= limit) return clean;
  const end = clean.lastIndexOf(' ', limit);
  return clean.slice(0, end > limit / 2 ? end : limit).trimEnd() + '…';
}
export function nextStep(project) {
  const sentences = text(project.response).replace(/\s+/g, ' ').split(/(?<=[.!?])\s+/);
  const statedNext = sentences.find((sentence) => /^(?:next\b|please\b|you (?:can|need|should)\b|click\b|review\b|refresh\b|choose\b|confirm\b|the remaining\b)/i.test(sentence));
  if (statedNext) return readableSummary(statedNext, 200);
  const status = taskStatus(project);
  if (status === 'Needs you') return 'Open the exact decision and choose how to proceed.';
  if (['Blocked', 'Failed', 'Blocker reported', 'Interrupted'].includes(status)) return 'Read the blocker and give the agent a correction or missing answer.';
  if (['Running', 'Running · last observed', 'Queued', 'Checking', 'Waiting on provider'].includes(status)) return 'Let this run continue; open the conversation if you want to steer it.';
  return 'Review the latest update. Continue the conversation, or mark this task Done for now.';
}
function updateAge(project) {
  const at = date(project.updatedAt);
  if (!at) return 'Update time unknown';
  const minutes = Math.max(0, Math.floor((Date.now() - at) / 60000));
  return minutes < 1 ? 'Updated just now' : minutes < 60 ? `Updated ${minutes}m ago` : minutes < 1440 ? `Updated ${Math.floor(minutes / 60)}h ago` : `Updated ${Math.floor(minutes / 1440)}d ago`;
}
export function visibleWorklistGroups(groups, filter = 'active', query = '', now = Date.now()) {
  return groups.filter((group) => {
    if (query && !group.name.toLowerCase().includes(query.toLowerCase())) return false;
    if (filter === 'aside') return group.hidden || (group.tasks.length > 0 && !group.activeTasks.length);
    if (filter === 'all') return true;
    if (group.hidden || (!group.activeTasks.length && !group.needs)) return false;
    if (filter === 'needs') return group.needs > 0;
    if (filter === 'blocked') return group.status === 'Blocked';
    return date(group.updatedAt) >= now - 7 * 86400000;
  });
}
export function projectRowHtml(group, selected = false) {
  const e = escapeHtml;
  const tasks = group.hidden ? [] : group.activeTasks;
  const running = tasks.filter((item) => ['Running', 'Running · last observed', 'Checking'].includes(taskStatus(item)));
  return `<article class="project-row ${selected ? 'selected' : ''}">
    <button class="project-select" data-select-project="${e(group.representative.id)}" aria-pressed="${selected}"><span class="project-row-title">${e(group.name)}</span><span class="project-row-status">${e(group.hidden ? 'Set aside' : group.status)}</span></button>
    <p class="project-row-summary">${e(readableSummary(group.summary))}</p>
    <p class="project-agent"><strong>Agent:</strong> ${running.length ? `${running.length} Codex / Marcus ${running.length === 1 ? 'run' : 'runs'} last observed active` : 'No running agent confirmed'}</p>
    ${tasks.map((task) => `<section class="project-task-row"><button data-worklist-task="${e(task.id)}"><strong>${e(readableSummary(task.latestRequest || task.request || task.current, 125))}</strong></button><p>${e(readableSummary(task.response || task.current || 'No update captured.', 240))}</p><span>${e(['codex', 'job'].includes(task.source) ? 'Codex' : 'Marcus')} · ${e(taskStatus(task))} · ${e(updateAge(task))}</span><p class="project-next"><strong>Next:</strong> ${e(nextStep(task))}</p><button class="btn" data-worklist-placement="${e(taskKey(task))}" data-hidden="true">Done for now</button></section>`).join('')}
    <div class="context-actions"><button class="btn" data-select-project="${e(group.representative.id)}">Open context</button><button class="btn" data-worklist-placement="${e(group.key)}" data-hidden="${!group.hidden}">${group.hidden ? 'Bring back' : 'Set aside'}</button></div>
  </article>`;
}
export function groupProjects(projects, overview, preferences = []) {
  const groups = new Map();
  for (const project of projects) {
    const linked = matchWorkProject(project, overview);
    const workspace = pathKey(project.workspacePath || project.raw?.localWorkspace?.canonicalPath || project.raw?.localWorkspace?.path);
    const key = linked ? `registry:${linked.id}` : workspace ? `workspace:${workspace}` : `task:${project.source}:${project.id}`;
    if (!groups.has(key)) groups.set(key, { key, name: linked?.name || project.name, linked, tasks: [], representative: project, startedAt: '', updatedAt: '', preferences });
    const group = groups.get(key);
    // Registry summaries are context, not another agent or task.
    if (!['awareness', 'work'].includes(project.source)) group.tasks.push(project);
    if (date(project.updatedAt) > date(group.updatedAt)) group.updatedAt = project.updatedAt;
    if (date(workStartedAt(project)) > date(group.startedAt)) group.startedAt = workStartedAt(project);
    if (project.source === 'codex' && group.representative.source !== 'codex') group.representative = project;
  }
  for (const group of groups.values()) {
    group.tasks = group.tasks.map((item) => {
      if (item.source !== 'job') return item;
      const transcript = group.tasks.find((other) => other.source === 'codex' && other.raw?.sessionId === item.raw?.threadId);
      return transcript ? { ...item, latestRequest: transcript.latestRequest || transcript.request, rollingContext: transcript.rollingContext } : item;
    });
    // A provider job and its discovered native transcript represent the same thread.
    group.tasks = group.tasks.filter((item) => item.source !== 'codex' || !group.tasks.some((other) => other.source === 'job' && other.raw?.threadId && other.raw.threadId === item.raw?.sessionId));
    group.tasks.sort((a, b) => date(b.updatedAt) - date(a.updatedAt));
    group.activeTasks = group.tasks.filter((item) => !isSetAside(taskKey(item), workStartedAt(item), preferences));
    group.hidden = isSetAside(group.key, group.startedAt, preferences);
    const tasks = group.activeTasks;
    group.needs = Number(group.linked?.needsYouCount || 0);
    group.status = group.needs ? 'Needs you' : tasks.some((item) => /^(Blocked|Failed|Blocker reported|Interrupted)$/.test(taskStatus(item))) ? 'Blocked' : tasks.some((item) => /^(Running|Queued|Checking)/.test(taskStatus(item))) ? 'In progress' : 'No live run confirmed';
    group.summary = text(tasks[0]?.latestRequest || tasks[0]?.request || group.linked?.objective || 'No current task recorded.');
  }
  return [...groups.values()].sort((a, b) => Number(b.needs > 0) - Number(a.needs > 0) || date(b.updatedAt) - date(a.updatedAt));
}

export function projectContextHtml(group, { showHandled = false } = {}) {
  const e = escapeHtml;
  if (!group) return '<section class="project-context"><h2>Your project workspace</h2><p>Select a project from the worklist. Its tasks and conversation will appear here.</p></section>';
  const tasks = showHandled ? group.tasks : group.activeTasks;
  return `<section class="project-context">
    <header><span class="context-kicker">PROJECT WORKLIST · V4</span><h2>${e(group.name)}</h2><p>${e(group.status)}</p></header>
    <div class="context-actions"><button class="btn" data-worklist-placement="${e(group.key)}" data-hidden="${!group.hidden}">${group.hidden ? 'Bring back' : 'Set project aside'}</button><button class="btn" data-open-tab="preview">Project details</button></div>
    <p class="context-hint">Set aside removes work from your active list, not its history. A new request brings it back. It does not stop an agent or approve a result.</p>
    <h3>What we’re working on</h3>
    ${tasks.map((item) => `<article class="context-task">
      <div class="context-task-heading"><span>${e(taskStatus(item))}</span><small>${e(item.source === 'codex' ? 'Codex app session' : item.source === 'job' ? 'Desktop Codex job' : 'Marcus execution')}</small></div>
      <p class="task-request">${e(item.latestRequest || item.request || item.current || 'No task description captured.')}</p>
      ${item.response ? `<details><summary>Latest update</summary><p class="task-update">${e(item.response)}</p></details>` : ''}
      <div class="context-actions"><button class="btn primary" data-worklist-task="${e(item.id)}">${['codex', 'job'].includes(item.source) ? 'Conversation' : 'Inspect work'}</button><button class="btn" data-worklist-placement="${e(taskKey(item))}" data-hidden="${!isSetAside(taskKey(item), workStartedAt(item), group.preferences)}">${isSetAside(taskKey(item), workStartedAt(item), group.preferences) ? 'Bring task back' : 'Done for now'}</button></div>
    </article>`).join('') || '<p>No active task is recorded here. Earlier work is available under Show handled.</p>'}
    ${group.tasks.length !== group.activeTasks.length ? `<button class="btn" data-show-handled>${showHandled ? 'Hide handled' : `Show handled (${group.tasks.length - group.activeTasks.length})`}</button>` : ''}
    <p class="context-hint">Only reported sessions and linked executions are listed. Unreported agents cannot be shown.</p>
    <p role="status" data-worklist-feedback></p>
  </section>`;
}
