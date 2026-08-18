import assert from 'node:assert/strict';
import test from 'node:test';

import {
  getMarcusBrowserSkill,
  listMarcusBrowserSkills,
  verifyMarcusBrowserSkillResult,
} from '../marcus/skills/browser_skills.js';

test('MARCUS browser skills declare authority, recovery, and deterministic evidence', () => {
  const skills = listMarcusBrowserSkills();
  assert.equal(skills.length, 8);
  for (const skill of skills) {
    assert.match(skill.id, /^[a-z0-9]+(?:[.-][a-z0-9]+)*$/);
    assert.ok(['observe', 'prepare', 'consequential'].includes(skill.authority));
    assert.ok(skill.preconditions.length > 0);
    assert.ok(skill.evidence.length > 0);
    assert.ok(skill.recovery.length > 0);
  }
  assert.equal(getMarcusBrowserSkill('marcus_browser_prepare_post').id, 'skool.prepare-standalone-post');
});

test('standalone post skill rejects relay success without standalone composer proof', () => {
  const verification = verifyMarcusBrowserSkillResult('marcus_browser_prepare_post', {
    ok: true,
    details: { result: { insertedChars: 21, surface: 'thread-comment-editor', verified: true } },
  }, { text: 'Standalone post text.' });
  assert.equal(verification.ok, false);
  assert.match(verification.error, /not proven/i);
});

test('standalone post skill accepts exact root-composer read-back evidence', () => {
  const verification = verifyMarcusBrowserSkillResult('marcus_browser_prepare_post', {
    ok: true,
    details: { result: {
      insertedChars: 21,
      surface: 'standalone-feed-composer',
      verified: true,
      communityRoot: true,
      href: 'https://www.skool.com/localgiants',
    } },
  }, { text: 'Standalone post text.' });
  assert.equal(verification.ok, true);
  assert.equal(verification.skillId, 'skool.prepare-standalone-post');
});

test('publication skill rejects a generic successful click without publication proof', () => {
  const verification = verifyMarcusBrowserSkillResult('marcus_browser_submit', {
    ok: true,
    details: { result: { activated: true, label: 'Post' } },
  });
  assert.equal(verification.ok, false);
  assert.match(verification.error, /did not prove publication/i);
});
