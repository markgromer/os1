import { nowIso, redactSecrets, safeBusinessKey, safeObject, safeString, sanitizeStructured } from './operation_types.js';
import { appendEvent } from './operation_service.js';

export class OperationReconciliation {
  constructor({ store, runner = null }) {
    this.store = store;
    this.runner = runner;
  }

  setRunner(runner) { this.runner = runner; }

  async reconcileDesktopResult(input = {}, { runCycle = true } = {}) {
    const raw = safeObject(input);
    const businessKey = safeBusinessKey(raw.businessKey, '');
    const actionId = safeString(raw.id || raw.actionId, 120);
    const actionType = safeString(raw.type, 100);
    if (!businessKey || !actionId || !actionType) throw Object.assign(new Error('businessKey, action id, and action type are required.'), { code: 'DESKTOP_RESULT_INVALID' });
    const operations = await this.store.listAll(businessKey);
    const matches = operations.filter((operation) => operation.desktopCorrelations.some((item) => item.actionId === actionId));
    if (matches.length !== 1) throw Object.assign(new Error(matches.length ? 'Desktop action correlation is ambiguous.' : 'Unknown desktop action id.'), { code: matches.length ? 'DESKTOP_ACTION_AMBIGUOUS' : 'DESKTOP_ACTION_UNKNOWN' });
    const snapshot = matches[0];
    const correlation = snapshot.desktopCorrelations.find((item) => item.actionId === actionId);
    const verification = snapshot.verification.find((item) => item.id === correlation.verificationId);
    if (!verification || verification.stepId !== correlation.stepId || verification.type !== correlation.verificationType
      || correlation.actionType !== actionType || correlation.projectRegistryId !== safeString(raw.projectRegistryId, 160)
      || correlation.desktopAgentId !== safeString(raw.desktopAgentId, 200)) {
      throw Object.assign(new Error('Desktop result does not match its durable operation correlation.'), { code: 'DESKTOP_RESULT_MISMATCH' });
    }
    if (correlation.status === 'completed' || correlation.status === 'failed') {
      return { operation: snapshot, duplicate: true, resumed: false };
    }
    const ok = raw.ok === true;
    const details = sanitizeStructured(raw.details ?? {}, 20_000);
    const errorText = redactSecrets(raw.error ?? '', 4_000);
    const updated = await this.store.update(businessKey, snapshot.id, (operation) => {
      const targetCorrelation = operation.desktopCorrelations.find((item) => item.actionId === actionId);
      const targetVerification = operation.verification.find((item) => item.id === targetCorrelation?.verificationId);
      if (!targetCorrelation || !targetVerification) throw Object.assign(new Error('Desktop correlation changed before reconciliation.'), { code: 'DESKTOP_RESULT_MISMATCH' });
      if (['completed', 'failed'].includes(targetCorrelation.status)) return operation;
      const timestamp = nowIso();
      targetCorrelation.status = ok ? 'completed' : 'failed';
      targetCorrelation.completedAt = timestamp;
      targetVerification.status = ok ? 'passed' : 'failed';
      targetVerification.completedAt = timestamp;
      targetVerification.output = ok ? redactSecrets(details, 20_000) : '';
      targetVerification.error = ok ? '' : (errorText || 'Desktop verification failed.');
      targetVerification.evidence = sanitizeStructured({
        ...safeObject(targetVerification.evidence), actionId, actionType, projectRegistryId: correlation.projectRegistryId,
        desktopAgentId: correlation.desktopAgentId, result: details,
      }, 20_000);
      const step = operation.steps.find((item) => item.id === correlation.stepId);
      if (step && step.status === 'blocked' && step.attemptCount < step.maxAttempts) { step.status = 'ready'; step.error = ''; }
      for (const blocker of operation.blockers) {
        if (blocker.stepId === correlation.stepId && blocker.status === 'active' && ['verification_required', 'verification_failed'].includes(blocker.type)) {
          blocker.status = 'resolved'; blocker.resolvedAt = timestamp; blocker.resolution = 'Desktop verification result reconciled.';
        }
      }
      if (['blocked', 'paused', 'recovery_required'].includes(operation.status)) operation.status = 'queued';
      appendEvent(operation, {
        type: ok ? 'desktop_verification_passed' : 'desktop_verification_failed', actor: 'desktop_agent', stepId: correlation.stepId,
        message: `${correlation.verificationType} verification ${ok ? 'passed' : 'failed'} through the bound desktop agent.`,
        data: { actionId, verificationId: correlation.verificationId, idempotencyKey: correlation.idempotencyKey },
      });
      return operation;
    });
    if (runCycle && updated.status === 'queued' && this.runner) {
      return { operation: await this.runner.tick(businessKey, updated.id), duplicate: false, resumed: true };
    }
    return { operation: updated, duplicate: false, resumed: false };
  }
}
