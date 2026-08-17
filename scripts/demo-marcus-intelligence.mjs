import { buildVoiceContinuityBrief } from '../marcus/voice/continuity_brief.js';
import { formatJobPrimingManifest } from '../marcus/jobs/job_priming.js';
import { assessLockedDecisionConflict } from '../marcus/memory/locked_decisions.js';
import { sanitizeOperatorResultForSpeech, containsSpokenMachineReference } from '../marcus/voice/spoken_reference.js';
import { assessPostInterruptionAlignment } from '../marcus/voice/conversation_alignment.js';

const memories = [
  { id: 'mem_relationship_demo', status: 'active', kind: 'preference', title: 'Relationship voice', content: 'Marcus and Mark use earned teasing, shared shorthand, honest pushback, and fierce loyalty.' },
  { id: 'mem_locked_demo', status: 'active', kind: 'decision', title: '[Locked] Voice authority remains narrow', content: '[LOCKED] Realtime voice does not receive direct deployment or GitHub mutation authority.' },
];

const raw = {
  ok: true,
  status: 'codex_prepared',
  operationId: 'op_NfHu37cdF1aSjQ',
  project: { name: 'Reggie', id: 'project_123456789' },
  reply: 'PR #1847 for the Reggie font updates is ready at https://github.com/markgromer/Reggie/pull/1847. Operation op_NfHu37cdF1aSjQ is waiting.',
};
const spoken = sanitizeOperatorResultForSpeech(raw);
const continuity = buildVoiceContinuityBrief({
  memories,
  conversation: { activeProject: { name: 'Reggie' }, messages: [{ role: 'user', content: 'The font is still doing interpretive dance on mobile.' }] },
});
const priming = formatJobPrimingManifest('Deploy the Marcus voice update and verify production.');
const conflict = assessLockedDecisionConflict('Bypass the direct deployment authority boundary for realtime voice.', memories);
const alignment = assessPostInterruptionAlignment({ interruptedRequest: 'Audit the Reggie font', nextRequest: 'Actually check Marcus voice latency', answer: 'Marcus voice latency is healthy.' });

const checks = [
  ['Spoken references are human', spoken.reply.includes('Reggie font pull request') && !containsSpokenMachineReference(spoken)],
  ['Relationship continuity is loaded', continuity.includes('earned teasing') && continuity.includes('Reggie')],
  ['Job priming selects release verification', priming.includes('release_verification') && priming.includes('rollback method')],
  ['Locked decisions fail closed', conflict?.status === 'locked_decision_conflict'],
  ['Interrupted turns stay aligned to the new request', alignment.aligned],
  ['Raw identifiers remain available outside speech', raw.operationId === 'op_NfHu37cdF1aSjQ'],
];

console.log('\nMARCUS INTELLIGENCE DEMO\n');
console.log(`Raw operator result: ${raw.reply}`);
console.log(`Spoken by Marcus:   ${spoken.reply}\n`);
console.log(`Continuity brief:\n${continuity}\n`);
console.log(`Priming manifest:\n${priming}\n`);
console.log(`Locked-decision response: ${conflict?.reply}\n`);
for (const [name, passed] of checks) console.log(`${passed ? 'PASS' : 'FAIL'}  ${name}`);
if (checks.some(([, passed]) => !passed)) process.exitCode = 1;
