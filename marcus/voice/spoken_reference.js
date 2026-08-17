const MACHINE_KEY_RE = /(?:^|_)(?:id|sha|hash|url|path|number)$/i;
const MACHINE_VALUE_RE = /^(?:op|mem|artifact|step|verify|approval|blocker|decision|project|file)_[A-Za-z0-9_-]{8,}$|^[a-f0-9]{12,64}$|^https?:\/\/\S+$/i;

function text(value, max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function projectName(result) {
  return text(result?.project?.name || result?.project?.canonicalName || result?.projectName, 120);
}

function artifactLabel(result) {
  const project = projectName(result);
  const combined = `${result?.reply || ''} ${result?.status || ''} ${result?.operation?.title || ''}`.toLowerCase();
  const noun = /font/.test(combined) ? 'font update'
    : /voice/.test(combined) ? 'voice update'
      : /deploy/.test(combined) ? 'deployment'
        : /pull request|\bpr\b/.test(combined) ? 'pull request'
          : /file/.test(combined) ? 'file'
            : /operation/.test(combined) ? 'operation'
              : 'work';
  return [project, noun].filter(Boolean).join(' ') || `that ${noun}`;
}

export function humanizeSpokenText(value, context = {}) {
  const label = text(context.label, 180) || 'that item';
  return text(value, 8_000)
    .replace(/https?:\/\/\S+/gi, (match) => `${context.urlLabel || label}${/[.!?]$/.test(match) ? match.at(-1) : ''}`)
    .replace(/\b(?:op|mem|artifact|step|verify|approval|blocker|decision|project|file)_[A-Za-z0-9_-]{8,}\b/gi, label)
    .replace(/\b(?:commit\s+)?[a-f0-9]{12,64}\b/gi, label)
    .replace(/\b(?:PR|pull request)\s*#?\d{2,}\b/gi, (match) => context.pullRequestLabel || label)
    .replace(/\bOperation\s+(that item|that operation|[^.!?]{1,80} operation)\s+is\b/gi, '$1 is')
    .replace(/\s+([.,!?])/g, '$1')
    .replace(/\s+/g, ' ')
    .trim();
}

export function sanitizeOperatorResultForSpeech(result) {
  if (!result || typeof result !== 'object') return { ok: false, reply: humanizeSpokenText(result) };
  const label = artifactLabel(result);
  const pullRequestLabel = `${projectName(result) || 'the project'} ${/font/i.test(result.reply || '') ? 'font ' : ''}pull request`;
  let reply = humanizeSpokenText(result.reply || result.error || '', { label, pullRequestLabel, urlLabel: pullRequestLabel });
  reply = reply
    .replace(new RegExp(`\\bOperation\\s+${label.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}\\s+is\\b`, 'gi'), 'That operation is')
    .replace(new RegExp(`\\b(?:is\\s+)?(?:ready\\s+)?at\\s+${pullRequestLabel.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}`, 'gi'), 'is ready in the pull request')
    .replace(new RegExp(`\\bfor\\s+operation\\s+${pullRequestLabel.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}`, 'gi'), 'for that operation');
  if (projectName(result) && /pull request/i.test(pullRequestLabel)) {
    reply = reply.replace(new RegExp(`^${pullRequestLabel.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}\\s+for\\s+the\\s+${projectName(result).replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}\\s+[^.]+?\\s+is\\s+ready\\s+in\\s+the\\s+pull\\s+request`, 'i'), `The ${pullRequestLabel} is ready`);
  }
  const safe = {
    ok: result.ok !== false,
    reply,
  };
  if (text(result.status, 100)) safe.status = text(result.status, 100);
  if (projectName(result)) safe.project = projectName(result);
  if (result.approvalRequired === true || result.status === 'waiting_for_approval') safe.approvalRequired = true;
  if (result.blocked === true || /blocked|recovery_required/.test(String(result.status || ''))) safe.blocked = true;
  if (result.uncertain === true) safe.uncertain = true;
  return safe;
}

export function containsSpokenMachineReference(value) {
  if (typeof value === 'string') return MACHINE_VALUE_RE.test(value) || /\b(?:PR|pull request)\s*#?\d{2,}\b/i.test(value);
  if (!value || typeof value !== 'object') return false;
  return Object.entries(value).some(([key, item]) => MACHINE_KEY_RE.test(key) || containsSpokenMachineReference(item));
}
