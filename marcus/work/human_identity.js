import crypto from 'node:crypto';
import { DomainStore, domainError, newDomainId } from '../operations/domain_store.js';
import { redactSecrets } from '../operations/operation_types.js';
import { emitWorkEvent, workReadiness } from './work_graph.js';

const hash = (value) => crypto.createHash('sha256').update(value).digest('hex');
const safe = (value, limit = 2000) => redactSecrets(String(value || '').trim().slice(0, limit), limit);

export class HumanIdentityService {
  constructor({ dataDir, graph }) {
    this.graph = graph;
    this.store = new DomainStore({ dataDir, name: 'human-identities', empty: () => ({ identities: [], grants: [] }),
      validate: (doc) => Array.isArray(doc.identities) && Array.isArray(doc.grants) });
  }
  async issue(key, { displayName, projectId, role = 'contributor', expiresInHours = 24 }) {
    if (!safe(displayName, 120) || !['reader', 'contributor'].includes(role)) throw domainError('IDENTITY_INVALID', 'A name and reader/contributor role are required. Owner authority cannot be issued.');
    if (!await this.graph.engine.registry.get(key, projectId)) throw domainError('PROJECT_NOT_FOUND', 'Unknown project.');
    const token = `mcollab_${crypto.randomBytes(32).toString('base64url')}`;
    const result = await this.store.mutate(key, (doc) => {
      const identity = { id: newDomainId('human'), displayName: safe(displayName, 120), status: 'active', createdAt: new Date().toISOString() };
      const grant = { id: newDomainId('grant'), identityId: identity.id, projectId, role, tokenHash: hash(token), revision: 1, status: 'active', expiresAt: new Date(Date.now() + Math.max(1, Math.min(168, Number(expiresInHours) || 24)) * 3600000).toISOString() };
      doc.identities.push(identity); doc.grants.push(grant);
      const { tokenHash, ...publicGrant } = grant; return { identity, grant: publicGrant };
    });
    return { ...result, token };
  }
  async authenticate(key, token) {
    if (typeof token !== 'string' || !/^mcollab_[A-Za-z0-9_-]{43}$/.test(token)) throw domainError('AUTH_REQUIRED', 'A valid project credential is required.');
    const doc = await this.store.read(key); const digest = hash(token);
    const grant = doc.grants.find((row) => row.tokenHash.length === digest.length && crypto.timingSafeEqual(Buffer.from(row.tokenHash), Buffer.from(digest)));
    const identity = grant && doc.identities.find((row) => row.id === grant.identityId);
    if (!grant || grant.status !== 'active' || Date.parse(grant.expiresAt) <= Date.now() || identity?.status !== 'active') throw domainError('AUTH_REQUIRED', 'The project credential is invalid, expired, or revoked.');
    return { identityId: identity.id, displayName: identity.displayName, projectId: grant.projectId, role: grant.role, grantId: grant.id, grantRevision: grant.revision };
  }
  async authorize(key, actor, projectId, capability = 'read') {
    const doc = await this.store.read(key);
    const grant = doc.grants.find((row) => row.id === actor?.grantId);
    if (!grant || grant.status !== 'active' || grant.revision !== actor.grantRevision || grant.identityId !== actor.identityId || grant.projectId !== projectId || grant.projectId !== actor.projectId || Date.parse(grant.expiresAt) <= Date.now()
      || !doc.identities.some((row) => row.id === actor.identityId && row.status === 'active') || !['read', 'submit'].includes(capability) || (capability === 'submit' && grant.role !== 'contributor')) throw domainError('IDENTITY_FORBIDDEN', 'The authenticated project grant does not permit this action.');
  }
  async revoke(key, grantId) {
    return this.store.mutate(key, (doc) => { const grant = doc.grants.find((row) => row.id === grantId); if (!grant) throw domainError('GRANT_NOT_FOUND', 'Grant not found.'); grant.status = 'revoked'; grant.revision++; return { id: grant.id, status: grant.status }; });
  }
  async assign(key, workId, identityId) {
    const state = await this.graph.snapshot(key); const item = state.items.find((row) => row.id === workId);
    const identities = await this.store.read(key);
    if (!item || item.kind !== 'human') throw domainError('WORK_NOT_FOUND', 'An existing human request is required.');
    if (!identities.grants.some((row) => row.identityId === identityId && row.projectId === item.projectId && row.role === 'contributor' && row.status === 'active' && Date.parse(row.expiresAt) > Date.now())) throw domainError('IDENTITY_FORBIDDEN', 'Assignee needs a current contributor grant for this project.');
    return this.graph.store.mutate(key, (doc) => { const current = doc.items.find((row) => row.id === workId); if (current.status !== 'ready' || current.submission) throw domainError('WORK_IMMUTABLE', 'Request already submitted or closed.'); current.assigneeId = identityId; current.revision++; emitWorkEvent(doc, 'work.human.assigned', current); return current; });
  }
  async submit(key, actor, workId, { note, evidenceRefs, revision }) {
    const item = (await this.graph.snapshot(key)).items.find((row) => row.id === workId);
    if (!item || item.projectId !== actor.projectId) throw domainError('WORK_NOT_FOUND', 'Request not found in your project.');
    await this.authorize(key, actor, item.projectId, 'submit');
    if (!safe(note) || !Array.isArray(evidenceRefs) || !evidenceRefs.length || evidenceRefs.length > 20 || !evidenceRefs.every((ref) => typeof ref === 'string' && safe(ref, 500))) throw domainError('EVIDENCE_REQUIRED', 'A note and evidence references are required; submissions are not verification.');
    return this.graph.store.mutate(key, (doc) => {
      const current = doc.items.find((row) => row.id === workId);
      if (current.kind !== 'human' || current.assigneeId !== actor.identityId || current.status !== 'ready' || current.submission || current.revision !== revision) throw domainError('IDENTITY_FORBIDDEN', 'Only the assigned contributor can submit the current open request.');
      current.submission = { id: newDomainId('submission'), identityId: actor.identityId, grantId: actor.grantId, note: safe(note), evidenceRefs: evidenceRefs.map((ref) => safe(ref, 500)), at: new Date().toISOString(), trustedForVerification: false };
      current.revision++; emitWorkEvent(doc, 'work.human.submitted', current, { submissionId: current.submission.id }); return current.submission;
    });
  }
  async accept(key, workId, { submissionId, revision, reviewNote }, actor = 'mark') {
    if (!safe(reviewNote) || actor !== 'mark') throw domainError('REVIEW_REQUIRED', 'An authenticated owner review note is required.');
    const operations = await this.graph.operations(key);
    return this.graph.store.mutate(key, (doc) => {
      const item = doc.items.find((row) => row.id === workId);
      if (!item || item.kind !== 'human' || item.status !== 'ready' || item.submission?.id !== submissionId || item.revision !== revision) throw domainError('REVISION_MISMATCH', 'Review the exact current submission.');
      if (workReadiness(doc, item, operations).blockers.length) throw domainError('WORK_NOT_RUNNABLE', 'The request still has unresolved dependencies.');
      item.evidence = [{ type: 'owner_reviewed_human_submission', submissionId, actor, reviewNote: safe(reviewNote), reviewedAt: new Date().toISOString(), evidenceRefs: item.submission.evidenceRefs }];
      item.status = 'completed'; item.completedAt = new Date().toISOString(); item.revision++;
      emitWorkEvent(doc, 'work.completed', item, { submissionId }); return item;
    });
  }
  async projectView(key, actor) {
    await this.authorize(key, actor, actor.projectId);
    const state = await this.graph.snapshot(key, actor.projectId);
    return { projectId: actor.projectId, identityId: actor.identityId, items: state.items.map((item) => ({ id: item.id, objective: item.objective, acceptanceCriteria: item.acceptanceCriteria, kind: item.kind, status: item.readiness.status, revision: item.revision, assigneeId: item.assigneeId || '', submitted: Boolean(item.submission) })) };
  }
}
