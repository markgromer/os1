const JOBS = Object.freeze([
  { id: 'release_verification', match: /\b(deploy|release|publish|production|ship)\b/i, always: ['project identity and locked decisions', 'current deployment target'], task: ['release checklist', 'verification evidence', 'rollback method'] },
  { id: 'project_status', match: /\b(status|brief me|what.*working|where.*at|latest)\b/i, always: ['project identity', 'current priorities'], task: ['recent operations', 'open blockers', 'latest verified evidence'] },
  { id: 'meeting_follow_up', match: /\b(meeting|call|follow.?up|recap|zoom)\b/i, always: ['people and relationship context', 'active project'], task: ['decisions', 'commitments', 'owners and due dates', 'draft communication rules'] },
  { id: 'marketing_copy', match: /\b(copy|landing page|sales page|campaign|ad|email|headline|offer|lead magnet)\b/i, always: ['brand voice', 'customer language', 'current offer'], task: ['channel playbook', 'proof and objections', 'conversion goal'] },
  { id: 'client_revision', match: /\b(client|revision|feedback|requested change)\b/i, always: ['client context', 'project scope'], task: ['exact request', 'prior decisions', 'acceptance criteria'] },
  { id: 'implementation', match: /\b(build|implement|fix|change|update|audit|review|codex)\b/i, always: ['project identity and authority boundaries', 'locked decisions'], task: ['request-ranked source evidence', 'acceptance criteria', 'required verification'] },
]);

export function selectJobPriming(request) {
  const text = String(request || '');
  return JOBS.find((job) => job.match.test(text)) || { id: 'general_operator', always: ['current priorities', 'relevant durable memory'], task: ['request-specific evidence'] };
}

export function formatJobPrimingManifest(request) {
  const job = selectJobPriming(request);
  return [
    `JOB PRIMING MANIFEST: ${job.id}`,
    `Load first: ${job.always.join('; ')}.`,
    `Then load: ${job.task.join('; ')}.`,
    'Exclude unrelated project history and generic context that does not change this job.',
  ].join('\n');
}
