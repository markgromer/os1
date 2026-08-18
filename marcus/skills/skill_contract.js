const AUTHORITY_LEVELS = new Set(['observe', 'prepare', 'consequential']);

function freezeArray(value) {
  return Object.freeze([...(Array.isArray(value) ? value : [])]);
}

export function defineMarcusSkill(input = {}) {
  const id = String(input.id || '').trim();
  const version = Number(input.version);
  const toolName = String(input.toolName || '').trim();
  const authority = String(input.authority || '').trim();
  const verify = input.verify;
  if (!/^[a-z0-9]+(?:[.-][a-z0-9]+)*$/.test(id)) throw new Error(`Invalid MARCUS skill id: ${id || 'missing'}`);
  if (!Number.isSafeInteger(version) || version < 1) throw new Error(`Invalid version for MARCUS skill ${id}.`);
  if (!toolName) throw new Error(`MARCUS skill ${id} requires a tool name.`);
  if (!AUTHORITY_LEVELS.has(authority)) throw new Error(`Invalid authority for MARCUS skill ${id}.`);
  if (typeof verify !== 'function') throw new Error(`MARCUS skill ${id} requires deterministic verification.`);

  return Object.freeze({
    id,
    version,
    toolName,
    authority,
    purpose: String(input.purpose || '').trim(),
    contexts: freezeArray(input.contexts),
    preconditions: freezeArray(input.preconditions),
    evidence: freezeArray(input.evidence),
    recovery: freezeArray(input.recovery),
    verify,
  });
}

export function verifyMarcusSkillResult(skill, result, input = {}) {
  if (!skill) return { ok: false, error: 'No MARCUS skill contract matched this action.' };
  if (!result?.ok) return { ok: false, error: String(result?.error || `${skill.id} did not complete.`) };
  try {
    const verification = skill.verify(result, input);
    if (verification === true) return { ok: true, skillId: skill.id, version: skill.version };
    if (verification?.ok === true) return { ...verification, skillId: skill.id, version: skill.version };
    return {
      ok: false,
      skillId: skill.id,
      version: skill.version,
      error: String(verification?.error || `${skill.id} returned no completion evidence.`),
    };
  } catch (error) {
    return { ok: false, skillId: skill.id, version: skill.version, error: String(error?.message || error) };
  }
}
