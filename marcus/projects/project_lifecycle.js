const ARCHIVED_STATUSES = new Set(['archive', 'archived']);
const COMPLETED_STATUSES = new Set(['complete', 'completed', 'done']);
const DORMANT_STATUSES = new Set(['dormant', 'intentionally dormant', 'on hold', 'paused', 'parked']);

export const AWARENESS_LIFECYCLES = Object.freeze([
  'active',
  'monitoring',
  'waiting',
  'dormant',
  'completed',
  'archived',
]);

export function normalizedProjectStatus(value) {
  return String(value || '').trim().toLowerCase().replace(/[_-]+/g, ' ').replace(/\s+/g, ' ');
}

export function isArchivedProjectStatus(value) {
  return ARCHIVED_STATUSES.has(normalizedProjectStatus(value));
}

export function isCompletedProjectStatus(value) {
  return COMPLETED_STATUSES.has(normalizedProjectStatus(value));
}

export function isDormantProjectStatus(value) {
  return DORMANT_STATUSES.has(normalizedProjectStatus(value));
}

export function isHistoricalProjectStatus(value) {
  return isArchivedProjectStatus(value) || isCompletedProjectStatus(value);
}

export function lifecycleFromProjectStatus(value) {
  if (isArchivedProjectStatus(value)) return 'archived';
  if (isCompletedProjectStatus(value)) return 'completed';
  if (isDormantProjectStatus(value)) return 'dormant';
  const status = normalizedProjectStatus(value);
  if (status === 'waiting') return 'waiting';
  if (status === 'monitoring') return 'monitoring';
  return 'active';
}

export function registryStatusForLifecycle(value) {
  const lifecycle = String(value || '').trim().toLowerCase();
  if (lifecycle === 'archived') return 'Archived';
  if (lifecycle === 'completed') return 'Done';
  if (lifecycle === 'dormant') return 'Dormant';
  if (lifecycle === 'waiting') return 'Waiting';
  if (lifecycle === 'monitoring') return 'Monitoring';
  return 'Active';
}
