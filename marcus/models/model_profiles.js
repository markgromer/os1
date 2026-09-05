import fs from 'node:fs';

const PROFILE_FILE = new URL('./model_profiles.json', import.meta.url);
const VALID_ENDPOINTS = new Set(['chat_completions', 'responses']);
const VALID_ROLLOUT_STATUSES = new Set(['disabled', 'shadow', 'canary', 'active']);
const VALID_ACCESS_STATUSES = new Set(['unverified', 'verified', 'denied']);
const VALID_EVALUATION_STATUSES = new Set(['not_run', 'running', 'passed', 'failed']);
const NON_RUNTIME_PURPOSES = new Set(['probe', 'evaluation']);

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function deepFreeze(value) {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  Object.freeze(value);
  for (const nested of Object.values(value)) deepFreeze(nested);
  return value;
}

export function validateModelProfiles(document) {
  const errors = [];
  if (!document || typeof document !== 'object' || Array.isArray(document)) return ['Model profiles must be a JSON object.'];
  if (document.schemaVersion !== 1) errors.push('model profiles schemaVersion must be 1.');
  if (!text(document.version)) errors.push('model profiles version is required.');
  const profiles = Array.isArray(document.profiles) ? document.profiles : [];
  if (!profiles.length) errors.push('At least one model profile is required.');
  const ids = new Set();
  const providerModels = new Set();

  for (const profile of profiles) {
    const id = text(profile?.id);
    const provider = text(profile?.provider).toLowerCase();
    const model = text(profile?.model).toLowerCase();
    const endpoint = text(profile?.endpoint);
    if (!id) errors.push('Every model profile requires an id.');
    else if (ids.has(id)) errors.push(`Duplicate model profile id: ${id}.`);
    ids.add(id);
    if (!provider || !model) errors.push(`${id || 'Model profile'} requires provider and model.`);
    const providerModel = `${provider}:${model}`;
    if (providerModels.has(providerModel)) errors.push(`Duplicate model profile target: ${providerModel}.`);
    providerModels.add(providerModel);
    if (!VALID_ENDPOINTS.has(endpoint)) errors.push(`${id || providerModel} has invalid endpoint ${endpoint || '(missing)'}.`);
    if (!Number.isFinite(Number(profile?.timeoutMs)) || Number(profile.timeoutMs) < 5_000) {
      errors.push(`${id || providerModel} requires timeoutMs of at least 5000.`);
    }

    const supportedEfforts = Array.isArray(profile?.reasoning?.supported) ? profile.reasoning.supported.map(text).filter(Boolean) : [];
    const defaultEffort = text(profile?.reasoning?.default);
    if (defaultEffort && !supportedEfforts.includes(defaultEffort)) {
      errors.push(`${id || providerModel} reasoning.default must appear in reasoning.supported.`);
    }
    if (model === 'gpt-6-astra' && supportedEfforts.includes('none')) {
      errors.push(`${id || providerModel} cannot use unsupported reasoning effort none.`);
    }

    const accessStatus = text(profile?.qualification?.accessStatus);
    const evaluationStatus = text(profile?.qualification?.evaluationStatus);
    if (!VALID_ACCESS_STATUSES.has(accessStatus)) errors.push(`${id || providerModel} has invalid qualification.accessStatus.`);
    if (!VALID_EVALUATION_STATUSES.has(evaluationStatus)) errors.push(`${id || providerModel} has invalid qualification.evaluationStatus.`);
    const rolloutStatus = text(profile?.rollout?.status);
    if (!VALID_ROLLOUT_STATUSES.has(rolloutStatus)) errors.push(`${id || providerModel} has invalid rollout.status.`);
    const enabledWorkloads = Array.isArray(profile?.rollout?.enabledWorkloads) ? profile.rollout.enabledWorkloads.map(text).filter(Boolean) : [];
    const minimumScore = Number(profile?.rollout?.minimumEvaluationScore);
    if (!Number.isFinite(minimumScore) || minimumScore < 0 || minimumScore > 1) {
      errors.push(`${id || providerModel} rollout.minimumEvaluationScore must be between 0 and 1.`);
    }
    if (['canary', 'active'].includes(rolloutStatus)) {
      if (accessStatus !== 'verified') errors.push(`${id || providerModel} cannot enter ${rolloutStatus} without verified access.`);
      if (evaluationStatus !== 'passed') errors.push(`${id || providerModel} cannot enter ${rolloutStatus} without a passing evaluation.`);
      if (!enabledWorkloads.length) errors.push(`${id || providerModel} cannot enter ${rolloutStatus} without enabled workloads.`);
      if (!text(profile?.qualification?.evaluationVersion) || !profile?.qualification?.evidence?.length) {
        errors.push(`${id || providerModel} cannot enter ${rolloutStatus} without versioned evaluation evidence.`);
      }
      for (const workload of enabledWorkloads) {
        if (!profile?.qualification?.qualifiedWorkloads?.includes(workload) || !profile?.qualification?.shadowPassedWorkloads?.includes(workload)) {
          errors.push(`${id || providerModel} requires passing workload evaluation and shadow evidence for ${workload}.`);
        }
      }
    }
  }

  return [...new Set(errors)];
}

const parsedProfiles = JSON.parse(fs.readFileSync(PROFILE_FILE, 'utf8'));
const profileErrors = validateModelProfiles(parsedProfiles);
if (profileErrors.length) throw new Error(`Invalid model profiles: ${profileErrors.join(' ')}`);

export const MODEL_PROFILES = deepFreeze(parsedProfiles);

export function getModelProfile(provider, model) {
  const providerKey = text(provider).toLowerCase();
  const modelKey = text(model).toLowerCase();
  return MODEL_PROFILES.profiles.find((profile) => (
    text(profile.provider).toLowerCase() === providerKey
    && text(profile.model).toLowerCase() === modelKey
  )) || null;
}

export function resolveModelDeployment({ provider, model, workload = '', purpose = 'runtime' } = {}) {
  const profile = getModelProfile(provider, model);
  if (!profile) {
    if (/(^|\/)gpt-6-astra(?:$|[-:])/i.test(text(model))) {
      return { allowed: false, managed: false, reason: 'This GPT-6 provider or snapshot has no qualified profile.' };
    }
    return {
      allowed: true,
      managed: false,
      endpoint: 'chat_completions',
      reasoningEffort: '',
      timeoutMs: 0,
      reason: 'No managed rollout profile applies; preserve the existing transport.',
    };
  }

  const normalizedPurpose = text(purpose).toLowerCase() || 'runtime';
  const endpoint = text(profile.endpoint);
  const base = {
    managed: true,
    profileId: profile.id,
    endpoint,
    reasoningEffort: text(profile.reasoning?.default),
    timeoutMs: Number(profile.timeoutMs) || 0,
    rolloutStatus: text(profile.rollout?.status),
  };
  if (NON_RUNTIME_PURPOSES.has(normalizedPurpose)) {
    return { ...base, allowed: true, reason: `${normalizedPurpose} requests may inspect the candidate without enabling a runtime workload.` };
  }
  if (normalizedPurpose === 'shadow') {
    const allowed = profile.qualification?.accessStatus === 'verified'
      && profile.qualification?.evaluationStatus === 'passed'
      && ['shadow', 'canary', 'active'].includes(base.rolloutStatus)
      && profile.rollout?.shadowWorkloads?.includes(text(workload));
    return { ...base, allowed: Boolean(allowed), reason: 'Shadow calls require qualified access and an exact shadow workload allowlist entry; callers must not dispatch tools.' };
  }

  if (!['canary', 'active'].includes(base.rolloutStatus)) {
    return { ...base, allowed: false, reason: `Model profile ${profile.id} is ${base.rolloutStatus}; runtime routing is disabled.` };
  }
  if (profile.qualification?.accessStatus !== 'verified') {
    return { ...base, allowed: false, reason: `Model profile ${profile.id} does not have verified account access.` };
  }
  if (profile.qualification?.evaluationStatus !== 'passed') {
    return { ...base, allowed: false, reason: `Model profile ${profile.id} does not have a passing evaluation.` };
  }
  const enabledWorkloads = Array.isArray(profile.rollout?.enabledWorkloads) ? profile.rollout.enabledWorkloads : [];
  if (!enabledWorkloads.includes(text(workload))) {
    return { ...base, allowed: false, reason: `Workload ${text(workload) || '(unspecified)'} is not enabled for ${profile.id}.` };
  }
  return { ...base, allowed: true, reason: `Workload ${text(workload)} is enabled by the ${base.rolloutStatus} profile.` };
}

export function listModelProfilesForClient() {
  return MODEL_PROFILES.profiles.map((profile) => ({
    id: profile.id,
    provider: profile.provider,
    model: profile.model,
    endpoint: profile.endpoint,
    qualification: {
      accessStatus: profile.qualification.accessStatus,
      evaluationStatus: profile.qualification.evaluationStatus,
      evaluationSuite: profile.qualification.evaluationSuite,
      evaluationVersion: profile.qualification.evaluationVersion,
    },
    rollout: {
      status: profile.rollout.status,
      enabledWorkloads: [...profile.rollout.enabledWorkloads],
      shadowWorkloads: [...profile.rollout.shadowWorkloads],
    },
  }));
}

// A shared Settings picker must not offer a route-specific canary as a global
// default. Readiness is advertised separately through modelProfiles.
export function isGeneralModelOption(provider, model) {
  return ['marcusChat', 'operatorBio', 'projectAssistant', 'dashboardPreview'].every((workload) => {
    const deployment = resolveModelDeployment({ provider, model, workload });
    return deployment.allowed && deployment.rolloutStatus !== 'canary';
  });
}
