// A visible session is not necessarily a writable desktop-provider job.
const string = (value) => typeof value === 'string' ? value : '';
const workspace = (value) => string(value).replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase();
export function exactCodexJob(project, jobs, businessKey) {
  const candidates = jobs.filter((job) => job.jobId && job.businessKey === businessKey);
  if (project?.source === 'job') return candidates.find((job) => job.jobId === project.raw?.jobId) || null;
  const sessionId = project?.raw?.sessionId;
  // Never redirect a native session to an unrelated job in the same repository.
  if (!sessionId || !workspace(project?.workspacePath)) return null;
  const matches = candidates.filter((job) => job.threadId === sessionId && workspace(job.workspacePath) === workspace(project.workspacePath));
  return matches.length === 1 ? matches[0] : null;
}

export function composeKey(project, businessKey) {
  return JSON.stringify([businessKey, project?.source === 'job' ? project.raw?.jobId : project?.raw?.sessionId || project?.id, workspace(project?.workspacePath)]);
}

export class CodexComposeState {
  constructor(storage, uuid = () => crypto.randomUUID()) {
    this.storage = storage;
    this.uuid = uuid;
    this.entries = new Map();
    try {
      const saved = JSON.parse(storage?.getItem('marcus.codex-drafts.v1') || '[]');
      for (const [key, value] of saved.slice(-40)) {
        if (typeof key !== 'string' || !value || typeof value.draft !== 'string') continue;
        this.entries.set(key, { ...value, draft: value.draft.slice(0, 8000), phase: value.phase === 'sending' ? 'uncertain' : value.phase });
      }
    } catch { /* Storage can be disabled; in-page preservation still works. */ }
  }
  get(key) {
    if (!this.entries.has(key)) this.entries.set(key, { draft: '', phase: 'idle', notice: '', request: null, receipt: null });
    return this.entries.get(key);
  }
  save() {
    try { this.storage?.setItem('marcus.codex-drafts.v1', JSON.stringify([...this.entries].slice(-40))); } catch { /* Retain in memory. */ }
  }
  draft(key, text) { this.get(key).draft = text; this.save(); }
  async send(key, job, dispatch) {
    const state = this.get(key);
    if (state.phase === 'sending') return;
    if (!job) {
      state.phase = 'blocked';
      state.notice = 'Not sent. This Codex app session is read-only here. Copy your draft into its Codex conversation; no Marcus chat or new job was started.';
      this.save(); return;
    }
    if (!state.draft.trim() && !state.request) return;
    // An uncertain request keeps its identity and exact body. Never silently replay as a new send.
    if (!state.request) { state.request = { requestId: this.uuid(), message: state.draft.trim(), jobId: job.jobId, originalDraft: state.draft }; state.receipt = null; }
    const request = state.request;
    state.phase = 'sending'; state.notice = 'Sending… waiting for a durable queue receipt.'; this.save();
    try {
      const result = await dispatch(request);
      if (result?.ok !== true || result?.receipt?.requestId !== request.requestId || result.receipt.jobId !== request.jobId || result.receipt.phase !== 'queued' || !result.receipt.actionId) throw new Error('No matching delivery receipt was returned.');
      state.phase = 'queued'; state.receipt = result.receipt;
      state.notice = 'Queued for the desktop agent—not yet proof that Codex received or ran it.';
      if (state.draft === request.originalDraft) state.draft = '';
      state.request = null;
    } catch (error) {
      const rejected = error.definitive === true;
      state.phase = rejected ? 'failed' : 'uncertain';
      state.notice = rejected ? `Not sent: ${error.message}. Your draft is retained.` : `Delivery unconfirmed: ${error.message}. Your draft is retained. Check send reuses the same request; it cannot enqueue a duplicate.`;
      if (rejected) state.request = null;
    }
    this.save();
  }
}
