const MONITORED_OPERATION_STATUSES = Object.freeze([
  'queued',
  'running',
  'awaiting_provider',
  'verifying',
]);

export async function runOperationMonitorPass({ engine, businessKeys = [], maxOperationsPerBusiness = 20, onError = () => {} } = {}) {
  if (!engine || typeof engine.listOperations !== 'function' || typeof engine.tick !== 'function') {
    throw new Error('Operation monitor requires an operations engine.');
  }
  const statuses = new Set(MONITORED_OPERATION_STATUSES);
  const result = { businesses: 0, inspected: 0, ticked: 0, failed: 0 };
  for (const rawKey of Array.isArray(businessKeys) ? businessKeys : []) {
    const businessKey = String(rawKey || '').trim();
    if (!businessKey) continue;
    result.businesses += 1;
    let operations = [];
    try {
      operations = await engine.listOperations(businessKey, {
        status: MONITORED_OPERATION_STATUSES,
        limit: maxOperationsPerBusiness,
      });
    } catch (error) {
      result.failed += 1;
      onError(error, { businessKey, phase: 'list' });
      continue;
    }
    const monitored = operations
      .filter((operation) => statuses.has(String(operation?.status || '')))
      .slice(0, Math.max(1, Number(maxOperationsPerBusiness) || 20));
    result.inspected += monitored.length;
    const settled = await Promise.allSettled(monitored.map((operation) => engine.tick(businessKey, operation.id)));
    settled.forEach((entry, index) => {
      if (entry.status === 'fulfilled') result.ticked += 1;
      else {
        result.failed += 1;
        onError(entry.reason, { businessKey, operationId: monitored[index]?.id || '', phase: 'tick' });
      }
    });
  }
  return result;
}

export function startOperationMonitor({
  engine,
  listBusinessKeys,
  initialDelayMs = 3_000,
  intervalMs = 15_000,
  maxOperationsPerBusiness = 20,
  onError = () => {},
  timers = globalThis,
} = {}) {
  if (typeof listBusinessKeys !== 'function') throw new Error('Operation monitor requires listBusinessKeys.');
  let running = false;
  let stopped = false;
  const tick = async () => {
    if (running || stopped) return null;
    running = true;
    try {
      return await runOperationMonitorPass({
        engine,
        businessKeys: await listBusinessKeys(),
        maxOperationsPerBusiness,
        onError,
      });
    } catch (error) {
      onError(error, { phase: 'pass' });
      return null;
    } finally {
      running = false;
    }
  };
  const first = timers.setTimeout(() => { void tick(); }, Math.max(0, Number(initialDelayMs) || 0));
  const interval = timers.setInterval(() => { void tick(); }, Math.max(1_000, Number(intervalMs) || 15_000));
  if (typeof first?.unref === 'function') first.unref();
  if (typeof interval?.unref === 'function') interval.unref();
  return {
    tick,
    stop() {
      stopped = true;
      timers.clearTimeout(first);
      timers.clearInterval(interval);
    },
  };
}

export { MONITORED_OPERATION_STATUSES };
