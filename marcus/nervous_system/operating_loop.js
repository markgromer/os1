import { createSignal } from './signal.js';

function normalizeSensor(sensor) {
  if (!sensor?.name || typeof sensor.sense !== 'function') throw new Error('An operating-loop sensor requires a name and sense function.');
  return { name: String(sensor.name), sense: sensor.sense, everyCycles: Math.max(1, Number(sensor.everyCycles) || 1) };
}

export async function runOperatingLoopPass({ bus, sensors = [], homeostasis = [], cycle = 1, onError = () => {} } = {}) {
  if (!bus || typeof bus.publish !== 'function') throw new Error('Operating loop requires a signal bus.');
  const result = { cycle, sensors: 0, signals: 0, handled: 0, failed: 0, maintenance: 0 };
  for (const sensor of sensors.map(normalizeSensor)) {
    if ((cycle - 1) % sensor.everyCycles !== 0) continue;
    result.sensors += 1;
    try {
      const sensed = await sensor.sense({ cycle });
      const signals = Array.isArray(sensed) ? sensed : sensed ? [sensed] : [];
      for (const input of signals) {
        const published = await bus.publish(createSignal({ ...input, source: input?.source || sensor.name }));
        result.signals += 1;
        result.handled += published.delivery.handled.length;
        result.failed += published.delivery.failed.length;
      }
    } catch (error) {
      result.failed += 1;
      onError(error, { phase: 'sense', sensor: sensor.name, cycle });
    }
  }
  for (const maintain of homeostasis.filter((item) => typeof item === 'function')) {
    try { await maintain({ cycle, result }); result.maintenance += 1; }
    catch (error) { result.failed += 1; onError(error, { phase: 'homeostasis', cycle }); }
  }
  return result;
}

export function startOperatingLoop({ bus, sensors = [], homeostasis = [], initialDelayMs = 2_000, intervalMs = 15_000, onError = () => {}, onCycle = () => {}, timers = globalThis } = {}) {
  let running = false;
  let stopped = false;
  let cycle = 0;
  const health = { status: 'starting', cycle: 0, running: false, startedAt: new Date().toISOString(), lastStartedAt: '', lastCompletedAt: '', lastDurationMs: 0, lastResult: null, consecutiveFailures: 0, skippedOverlaps: 0 };
  const tick = async () => {
    if (running || stopped) { if (running) health.skippedOverlaps += 1; return null; }
    running = true;
    const started = Date.now();
    health.running = true;
    health.lastStartedAt = new Date(started).toISOString();
    try {
      cycle += 1;
      const result = await runOperatingLoopPass({ bus, sensors, homeostasis, cycle, onError });
      await onCycle(result);
      health.status = result.failed ? 'degraded' : 'healthy';
      health.cycle = cycle;
      health.lastResult = structuredClone(result);
      health.consecutiveFailures = result.failed ? health.consecutiveFailures + 1 : 0;
      return result;
    } catch (error) {
      health.status = 'degraded';
      health.consecutiveFailures += 1;
      onError(error, { phase: 'cycle', cycle });
      return null;
    } finally { running = false; health.running = false; health.lastCompletedAt = new Date().toISOString(); health.lastDurationMs = Date.now() - started; }
  };
  const first = timers.setTimeout(() => { void tick(); }, Math.max(0, Number(initialDelayMs) || 0));
  const interval = timers.setInterval(() => { void tick(); }, Math.max(1_000, Number(intervalMs) || 15_000));
  if (typeof first?.unref === 'function') first.unref();
  if (typeof interval?.unref === 'function') interval.unref();
  return {
    tick,
    trigger: () => tick(),
    get cycle() { return cycle; },
    health: () => structuredClone({ ...health, stopped }),
    stop() { stopped = true; health.status = 'stopped'; timers.clearTimeout(first); timers.clearInterval(interval); },
  };
}
