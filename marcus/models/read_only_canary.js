import crypto from 'node:crypto';
import { getModelProfile, resolveModelDeployment } from './model_profiles.js';

export const keyFingerprint = (key) => crypto.createHash('sha256').update(String(key)).digest('hex').slice(0, 16);

export function chooseReadOnlyRoute({ baseline, workload, requestId, disabled = false, profile = getModelProfile('openai', 'gpt-6-astra') }) {
  const eligible = !disabled && workload === 'dashboardPreview' && baseline.provider === 'openai'
    && ['canary', 'active'].includes(profile?.rollout?.status)
    && profile.rollout.enabledWorkloads.includes(workload)
    && profile.qualification.qualifiedWorkloads.includes(workload)
    && profile.qualification.shadowPassedWorkloads.includes(workload)
    && profile.rollout.credentialFingerprint === keyFingerprint(baseline.apiKey)
    && profile.qualification.accessStatus === 'verified' && profile.qualification.evaluationStatus === 'passed';
  const bucket = crypto.createHash('sha256').update(String(requestId)).digest().readUInt32BE(0) % 100;
  if (!eligible || bucket >= Math.min(10, Math.max(0, Number(profile.rollout.trafficPercent) || 0))) return { ...baseline, canary: false };
  return { ...baseline, model: profile.model, canary: true };
}

// Only accepts text prompts: no tools or dispatcher can cross this boundary.
export async function runReadOnlyCanary({ baseline, workload, requestId, messages, complete, validate, observe = async () => {}, disabled = false, profile }) {
  // A manually selected candidate cannot become its own unsampled fallback.
  // Fail closed to the caller's heuristic instead of bypassing account/cap gates.
  if (/(^|\/)gpt-6-astra(?:$|[-:])/i.test(String(baseline?.model || '').trim())) {
    return { ok: false, error: 'Choose a qualified non-canary baseline; GPT-6 preview traffic is sampled separately.' };
  }
  const selected = chooseReadOnlyRoute({ baseline, workload, requestId, disabled, profile });
  const attempt = async (route) => {
    const start = Date.now();
    let result;
    try { result = await complete({ route, messages, workload, deadlineMs: 20_000, maxOutputTokens: 2048 }); }
    catch { result = { ok: false, error: 'Preview provider request failed.' }; }
    const checked = validate(result);
    const receipt = { type: 'model.preview.observed', requestId, workload, model: route.model, canary: route.canary === true,
      passed: checked.ok, elapsedMs: Date.now() - start, responseId: result.responseId || '', usage: result.usage || null };
    await observe(receipt).catch(() => {});
    return { ...checked, receipt };
  };
  if (selected.canary && !profile && !resolveModelDeployment({ provider: selected.provider, model: selected.model, workload }).allowed) {
    return attempt({ ...baseline, canary: false });
  }
  const result = await attempt(selected);
  return !result.ok && selected.canary ? attempt({ ...baseline, canary: false }) : result;
}
