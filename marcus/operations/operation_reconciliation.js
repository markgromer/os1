import {
  nowIso,
  redactSecrets,
  safeBusinessKey,
  safeObject,
  safeString,
  sanitizeStructured,
  TERMINAL_OPERATION_STATUSES,
} from './operation_types.js';
import { appendEvent } from './operation_service.js';

function mismatch(bindings = []) {
  return Object.assign(new Error('Desktop result does not match its durable operation correlation.'), {
    code: 'DESKTOP_RESULT_MISMATCH', bindings: Array.isArray(bindings) ? bindings.slice(0, 20) : [],
  });
}

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
    if (!businessKey || !actionId || !actionType) {
      throw Object.assign(new Error('businessKey, action id, and action type are required.'), { code: 'DESKTOP_RESULT_INVALID' });
    }
    const operations = await this.store.listAll(businessKey);
    const matches = operations.filter((operation) => operation.desktopCorrelations.some((item) => item.actionId === actionId));
    if (matches.length !== 1) {
      throw Object.assign(new Error(matches.length ? 'Desktop action correlation is ambiguous.' : 'Unknown desktop action id.'), { code: matches.length ? 'DESKTOP_ACTION_AMBIGUOUS' : 'DESKTOP_ACTION_UNKNOWN' });
    }
    const snapshot = matches[0];
    const correlation = snapshot.desktopCorrelations.find((item) => item.actionId === actionId);
    const step = snapshot.steps.find((item) => item.id === correlation.stepId);
    const providerAction = snapshot.providerActions.find((item) => item.stepId === correlation.stepId && item.externalId === actionId);
    const verification = correlation.verificationId
      ? snapshot.verification.find((item) => item.id === correlation.verificationId)
      : null;
    const suppliedAttempt = Number(raw.attemptNumber);
    const failedBindings = Object.entries({
      step: Boolean(step),
      providerAction: Boolean(providerAction),
      storedOperation: correlation.operationId === snapshot.id,
      business: correlation.businessKey === businessKey,
      actionType: correlation.actionType === actionType,
      project: correlation.projectRegistryId === safeString(raw.projectRegistryId, 160),
      agent: correlation.desktopAgentId === safeString(raw.desktopAgentId, 200),
      operation: correlation.operationId === safeString(raw.operationId, 120),
      stepId: correlation.stepId === safeString(raw.stepId, 120),
      idempotency: correlation.idempotencyKey === safeString(raw.idempotencyKey, 240),
      attemptSupplied: Number.isFinite(suppliedAttempt) && correlation.attemptNumber === suppliedAttempt,
      externalAction: Boolean(providerAction && providerAction.externalId === actionId),
    }).filter(([, valid]) => !valid).map(([binding]) => binding);
    if (failedBindings.length) throw mismatch(failedBindings);
    if (verification && (verification.stepId !== correlation.stepId || verification.type !== correlation.verificationType)) throw mismatch();
    if (correlation.verificationId && !verification) throw mismatch();
    if (['completed', 'failed'].includes(correlation.status)) {
      return { operation: snapshot, duplicate: true, resumed: false };
    }
    const attemptBindings = Object.entries({
      stepAttempt: step.attemptCount === correlation.attemptNumber,
      stepIdempotency: verification
        ? correlation.idempotencyKey.startsWith(`${step.idempotencyKey}:`)
        : step.idempotencyKey === correlation.idempotencyKey,
    }).filter(([, valid]) => !valid).map(([binding]) => binding);
    if (attemptBindings.length) throw mismatch(attemptBindings);

    const ok = raw.ok === true;
    const details = sanitizeStructured(raw.details ?? {}, 20_000);
    const outputText = redactSecrets(details, 20_000);
    const errorText = redactSecrets(raw.error ?? '', 4_000);
    const updated = await this.store.update(businessKey, snapshot.id, (operation) => {
      const targetCorrelation = operation.desktopCorrelations.find((item) => item.actionId === actionId);
      const targetStep = operation.steps.find((item) => item.id === targetCorrelation?.stepId);
      const targetAction = operation.providerActions.find((item) => item.stepId === targetCorrelation?.stepId && item.externalId === actionId);
      const targetVerification = targetCorrelation?.verificationId
        ? operation.verification.find((item) => item.id === targetCorrelation.verificationId)
        : null;
      if (!targetCorrelation || !targetStep || !targetAction || (targetCorrelation.verificationId && !targetVerification)) throw mismatch();
      if (['completed', 'failed'].includes(targetCorrelation.status)) return operation;
      const liveBindings = Object.entries({
        business: targetCorrelation.businessKey === businessKey,
        operation: targetCorrelation.operationId === safeString(raw.operationId, 120) && targetCorrelation.operationId === operation.id,
        stepId: targetCorrelation.stepId === safeString(raw.stepId, 120),
        actionType: targetCorrelation.actionType === actionType,
        project: targetCorrelation.projectRegistryId === safeString(raw.projectRegistryId, 160),
        agent: targetCorrelation.desktopAgentId === safeString(raw.desktopAgentId, 200),
        idempotency: targetCorrelation.idempotencyKey === safeString(raw.idempotencyKey, 240),
        attemptSupplied: Number.isFinite(suppliedAttempt) && targetCorrelation.attemptNumber === suppliedAttempt,
        stepAttempt: targetStep.attemptCount === targetCorrelation.attemptNumber,
        stepIdempotency: targetVerification
          ? targetCorrelation.idempotencyKey.startsWith(`${targetStep.idempotencyKey}:`)
          : targetStep.idempotencyKey === targetCorrelation.idempotencyKey,
        externalAction: targetAction.externalId === actionId,
      }).filter(([, valid]) => !valid).map(([binding]) => binding);
      if (liveBindings.length) throw mismatch(liveBindings);
      const timestamp = nowIso();
      targetCorrelation.status = ok ? 'completed' : 'failed';
      targetCorrelation.updatedAt = timestamp;
      targetCorrelation.completedAt = timestamp;
      targetCorrelation.output = ok ? outputText : '';
      targetCorrelation.error = ok ? '' : (errorText || 'Desktop action failed.');
      targetAction.status = ok ? 'completed' : 'failed';
      targetAction.updatedAt = timestamp;
      targetAction.completedAt = timestamp;
      targetAction.metadata = sanitizeStructured({
        ...safeObject(targetAction.metadata), desktopResult: { receivedAt: timestamp, output: details, error: targetCorrelation.error },
      }, 15_000);

      if (TERMINAL_OPERATION_STATUSES.includes(operation.status)) {
        appendEvent(operation, {
          type: 'late_desktop_result_recorded', actor: 'desktop_agent', stepId: targetCorrelation.stepId,
          message: `A matching desktop result arrived after the operation became ${operation.status}; terminal state was retained.`,
          data: { actionId, ok, idempotencyKey: targetCorrelation.idempotencyKey, attemptNumber: targetCorrelation.attemptNumber },
        });
        return operation;
      }

      if (targetVerification) {
        targetVerification.status = ok ? 'passed' : 'failed';
        targetVerification.completedAt = timestamp;
        targetVerification.output = ok ? outputText : '';
        targetVerification.error = ok ? '' : (errorText || 'Desktop verification failed.');
        targetVerification.evidence = sanitizeStructured({
          ...safeObject(targetVerification.evidence), actionId, actionType,
          projectRegistryId: targetCorrelation.projectRegistryId, desktopAgentId: targetCorrelation.desktopAgentId,
          idempotencyKey: targetCorrelation.idempotencyKey, attemptNumber: targetCorrelation.attemptNumber, result: details,
        }, 20_000);
        if (targetStep.status === 'blocked' && targetStep.attemptCount < targetStep.maxAttempts) {
          targetStep.status = 'ready'; targetStep.error = '';
        }
        for (const blocker of operation.blockers) {
          if (blocker.stepId === targetCorrelation.stepId && blocker.status === 'active'
            && ['verification_required', 'verification_failed', 'recovery_required'].includes(blocker.type)) {
            blocker.status = 'resolved'; blocker.resolvedAt = timestamp; blocker.resolution = 'Desktop verification result reconciled.';
          }
        }
      } else {
        targetStep.status = ok ? 'completed' : 'failed';
        targetStep.completedAt = ok ? timestamp : '';
        targetStep.failedAt = ok ? '' : timestamp;
        targetStep.output = ok ? outputText : '';
        targetStep.error = ok ? '' : (errorText || 'Desktop action failed.');
        for (const blocker of operation.blockers) {
          if (blocker.stepId === targetCorrelation.stepId && blocker.status === 'active' && ['provider_result_required', 'recovery_required'].includes(blocker.type)) {
            blocker.status = 'resolved'; blocker.resolvedAt = timestamp; blocker.resolution = 'Desktop provider result reconciled.';
          }
        }
      }

      if (operation.status !== 'paused') {
        operation.status = ok ? 'queued' : (targetVerification ? 'blocked' : 'failed');
        if (!ok && !targetVerification) operation.failedAt = timestamp;
      }
      appendEvent(operation, {
        type: targetVerification
          ? (ok ? 'desktop_verification_passed' : 'desktop_verification_failed')
          : (ok ? 'desktop_step_completed' : 'desktop_step_failed'),
        actor: 'desktop_agent', stepId: targetCorrelation.stepId,
        message: targetVerification
          ? `${targetCorrelation.verificationType} verification ${ok ? 'passed' : 'failed'} through the bound desktop agent.`
          : `Desktop action ${targetCorrelation.actionType} ${ok ? 'completed' : 'failed'} for its exact operation attempt.`,
        data: { actionId, verificationId: targetCorrelation.verificationId, idempotencyKey: targetCorrelation.idempotencyKey, attemptNumber: targetCorrelation.attemptNumber },
      });
      return operation;
    });
    if (runCycle && updated.status === 'queued' && this.runner) {
      return { operation: await this.runner.tick(businessKey, updated.id), duplicate: false, resumed: true };
    }
    return { operation: updated, duplicate: false, resumed: false };
  }
}
