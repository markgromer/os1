import crypto from 'node:crypto';

import { nowIso, safeBusinessKey, safeIso, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';

export const SIGNAL_SEVERITIES = Object.freeze(['debug', 'info', 'notice', 'warning', 'critical']);

export function createSignal(input = {}) {
  const raw = safeObject(input);
  const type = safeString(raw.type, 160).toLowerCase().replace(/[^a-z0-9._-]+/g, '_');
  const source = safeString(raw.source, 160).toLowerCase().replace(/[^a-z0-9._-]+/g, '_');
  if (!type) throw Object.assign(new Error('Signal type is required.'), { code: 'MARCUS_SIGNAL_TYPE_REQUIRED' });
  if (!source) throw Object.assign(new Error('Signal source is required.'), { code: 'MARCUS_SIGNAL_SOURCE_REQUIRED' });
  const severity = safeString(raw.severity, 40).toLowerCase();
  const subject = safeObject(raw.subject);
  return Object.freeze({
    id: safeString(raw.id, 160) || `sig_${crypto.randomBytes(12).toString('base64url')}`,
    type,
    source,
    businessKey: safeBusinessKey(raw.businessKey),
    subject: {
      type: safeString(subject.type, 80).toLowerCase(),
      id: safeString(subject.id, 240),
    },
    observedAt: safeIso(raw.observedAt) || nowIso(),
    severity: SIGNAL_SEVERITIES.includes(severity) ? severity : 'info',
    confidence: Math.max(0, Math.min(1, Number.isFinite(Number(raw.confidence)) ? Number(raw.confidence) : 1)),
    evidence: (Array.isArray(raw.evidence) ? raw.evidence : []).slice(0, 50).map((item) => sanitizeStructured(item, 4_000)),
    context: sanitizeStructured(raw.context ?? {}, 20_000),
    traceId: safeString(raw.traceId, 160) || safeString(raw.id, 160) || `trace_${crypto.randomBytes(10).toString('base64url')}`,
    causationId: safeString(raw.causationId, 160),
  });
}

export function signalMatches(pattern, type) {
  const expected = safeString(pattern, 160).toLowerCase();
  const actual = safeString(type, 160).toLowerCase();
  if (expected === '*') return true;
  if (expected.endsWith('.*')) return actual === expected.slice(0, -2) || actual.startsWith(expected.slice(0, -1));
  return expected === actual;
}
