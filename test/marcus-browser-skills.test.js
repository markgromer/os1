import assert from 'node:assert/strict';
import test from 'node:test';

import {
  getMarcusBrowserSkill,
  listMarcusBrowserSkills,
  verifyMarcusBrowserSkillResult,
} from '../marcus/skills/browser_skills.js';

test('MARCUS browser skills declare authority, recovery, and deterministic evidence', () => {
  const skills = listMarcusBrowserSkills();
  assert.equal(skills.length, 10);
  for (const skill of skills) {
    assert.match(skill.id, /^[a-z0-9]+(?:[.-][a-z0-9]+)*$/);
    assert.ok(['observe', 'prepare', 'consequential'].includes(skill.authority));
    assert.ok(skill.preconditions.length > 0);
    assert.ok(skill.evidence.length > 0);
    assert.ok(skill.recovery.length > 0);
  }
  assert.equal(getMarcusBrowserSkill('marcus_browser_prepare_post').id, 'skool.prepare-standalone-post');
  assert.equal(getMarcusBrowserSkill('marcus_browser_observe_community').id, 'skool.observe-community');
  assert.equal(getMarcusBrowserSkill('marcus_browser_inspect_notifications').id, 'skool.inspect-notifications');
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

test('standalone post skill requires exact rich-composition evidence when supplied', () => {
  const input = {
    title: 'What should MARCUS automate next?',
    text: 'A developed standalone post body.',
    category: 'Operations',
    pollOptions: ['Lead follow-up', 'Route planning'],
  };
  const incomplete = verifyMarcusBrowserSkillResult('marcus_browser_prepare_post', {
    ok: true,
    details: { result: {
      insertedChars: input.text.length,
      surface: 'standalone-feed-composer',
      verified: true,
      communityRoot: true,
    } },
  }, input);
  assert.equal(incomplete.ok, false);

  const complete = verifyMarcusBrowserSkillResult('marcus_browser_prepare_post', {
    ok: true,
    details: { result: {
      insertedChars: input.text.length,
      surface: 'standalone-feed-composer',
      verified: true,
      communityRoot: true,
      completeDraft: true,
      title: input.title,
      category: input.category,
      pollOptions: input.pollOptions,
    } },
  }, input);
  assert.equal(complete.ok, true);
  assert.equal(complete.evidence.completeDraft, true);
});

test('publication skill rejects a generic successful click without publication proof', () => {
  const verification = verifyMarcusBrowserSkillResult('marcus_browser_submit', {
    ok: true,
    details: { result: { activated: true, label: 'Post' } },
  });
  assert.equal(verification.ok, false);
  assert.match(verification.error, /did not prove publication/i);
});
