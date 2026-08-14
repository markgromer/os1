import { isStrongConfirmation } from './approval_policy.js';
import { createActivityEvent, createApproval, createBlocker, normalizeApproval, nowIso, safeString } from '../operations/operation_types.js';

function actionLabel(value) {
  return safeString(value, 300) || 'execute the next step';
}

function buildDecisionPackage({ operation, step, classification }) {
  const action = actionLabel(classification.action);
  const project = safeString(operation.projectName, 300) || safeString(operation.projectId, 160) || safeString(operation.projectRegistryId, 160);
  const target = safeString(step?.input?.approvalTarget, 300);
  const exactTarget = target ? `${action} on ${target}` : action;
  const highImpact = ['high', 'critical'].includes(classification.riskLevel);
  return {
    statement: `Decide whether Marcus should ${exactTarget}.`,
    project,
    objective: safeString(operation.objective, 2_000),
    whyNeeded: classification.reason || `Marcus needs authority before ${exactTarget}.`,
    recommendation: highImpact
      ? 'Approve only if the exact target, rollback path, and current evidence match Mark\'s intent.'
      : 'Approve if this remains the intended next step and no constraints have changed.',
    supportingEvidence: [
      { type: 'operation', id: operation.id, title: operation.title, status: operation.status },
      { type: 'step', id: step?.id || '', title: step?.title || '', provider: step?.provider || '', toolName: step?.toolName || step?.type || '' },
      { type: 'policy', riskLevel: classification.riskLevel, approvalRequirement: classification.approvalRequirement, reason: classification.reason },
    ],
    alternativesConsidered: [
      'Approve the exact action.',
      'Defer until more evidence is collected.',
      'Decline and keep the operation blocked.',
      'Modify the instructions and re-plan the operation.',
    ],
    benefit: 'Allows Marcus to continue the durable operation instead of leaving valuable work blocked.',
    cost: highImpact ? 'Consumes authority for a consequential action and may require rollback work if the target is wrong.' : 'Consumes time and execution attention.',
    risk: classification.reason || `${classification.riskLevel} risk action.`,
    consequenceOfWaiting: 'The operation remains waiting for approval and Marcus will not execute this step.',
    reversibility: classification.reversibility,
    rollbackMethod: highImpact
      ? 'Use provider read-back, retained operation artifacts, and the previous known target state to prepare a separate rollback operation if needed.'
      : 'Cancel or retry the operation step; retained activity and artifacts remain auditable.',
    authorityLevel: classification.approvalRequirement,
    actions: [
      'Approve',
      'Approve with conditions',
      'Modify instructions',
      'Ask Marcus to decide',
      'Discuss',
      'Defer',
      'Decline',
      'Cancel underlying work',
    ],
  };
}

export class ApprovalService {
  constructor({ store }) {
    this.store = store;
  }

  buildRequest({ operation, step, classification }) {
    const existing = (operation.approvals || []).find((approval) => approval.stepId === step.id && approval.status === 'pending'
      && (!approval.expiresAt || Date.parse(approval.expiresAt) > Date.now()));
    for (const approval of operation.approvals || []) {
      if (approval.status === 'pending' && approval.expiresAt && Date.parse(approval.expiresAt) <= Date.now()) approval.status = 'expired';
    }
    if (existing) return existing;
    return createApproval({
      operationId: operation.id,
      stepId: step.id,
      action: classification.action,
      riskLevel: classification.riskLevel,
      reason: classification.reason,
      status: 'pending',
      decision: buildDecisionPackage({ operation, step, classification }),
    });
  }

  async approve(businessKey, operationId, approvalId, { approvedBy = 'mark', message = '', conditions = [] } = {}) {
    const snapshot = await this.store.get(businessKey, operationId);
    const expiring = snapshot?.approvals?.find((approval) => approval.id === approvalId);
    if (expiring?.status === 'pending' && expiring.expiresAt && Date.parse(expiring.expiresAt) <= Date.now()) {
      await this.store.update(businessKey, operationId, (operation) => {
        const approval = operation.approvals.find((item) => item.id === approvalId);
        if (approval?.status === 'pending') approval.status = 'expired';
        operation.activityLog.push({
          ...createActivityEvent({ operationId, stepId: approval?.stepId || '', type: 'approval_expired', actor: 'system',
          message: 'Approval expired before it was granted.', data: { approvalId }, timestamp: nowIso(),
        }) });
        return operation;
      });
      throw Object.assign(new Error('Approval has expired.'), { code: 'APPROVAL_EXPIRED' });
    }
    return this.store.update(businessKey, operationId, (operation) => {
      const index = operation.approvals.findIndex((approval) => approval.id === approvalId);
      if (index < 0) throw Object.assign(new Error('Approval not found.'), { code: 'APPROVAL_NOT_FOUND' });
      const current = operation.approvals[index];
      if (current.status !== 'pending') throw Object.assign(new Error(`Approval is already ${current.status}.`), { code: 'APPROVAL_NOT_PENDING' });
      if (current.expiresAt && Date.parse(current.expiresAt) <= Date.now()) {
        throw Object.assign(new Error('Approval has expired.'), { code: 'APPROVAL_EXPIRED' });
      }
      const confirmed = isStrongConfirmation(message);
      if (current.riskLevel === 'critical' && !confirmed) {
        throw Object.assign(new Error('Critical actions require strong confirmation acknowledging the irreversible risk.'), { code: 'STRONG_CONFIRMATION_REQUIRED' });
      }
      operation.approvals[index] = normalizeApproval({
        ...current,
        status: 'approved',
        approvedAt: nowIso(),
        approvedBy: safeString(approvedBy, 200) || 'mark',
        approvalMessage: safeString(message, 2_000),
        strongConfirmation: confirmed,
        decision: {
          ...current.decision,
          outcome: Array.isArray(conditions) && conditions.length ? 'approved_with_conditions' : 'approved',
          decidedAt: nowIso(),
          decidedBy: safeString(approvedBy, 200) || 'mark',
          decisionReasoning: safeString(message, 2_000),
          conditions,
        },
      });
      const step = operation.steps.find((item) => item.id === current.stepId);
      if (step && step.status === 'waiting_for_approval') {
        step.status = 'ready';
        step.approvalId = current.id;
      }
      operation.activityLog.push({
        ...createActivityEvent({ operationId, stepId: current.stepId, type: 'approval_approved', actor: approvedBy,
        message: `Approval granted for ${current.action}.`, data: { approvalId, riskLevel: current.riskLevel, decisionOutcome: operation.approvals[index].decision.outcome }, timestamp: nowIso(),
      }) });
      if (operation.status === 'waiting_for_approval') operation.status = 'queued';
      return operation;
    });
  }

  async reject(businessKey, operationId, approvalId, { rejectedBy = 'mark', message = '' } = {}) {
    return this.store.update(businessKey, operationId, (operation) => {
      const index = operation.approvals.findIndex((approval) => approval.id === approvalId);
      if (index < 0) throw Object.assign(new Error('Approval not found.'), { code: 'APPROVAL_NOT_FOUND' });
      const current = operation.approvals[index];
      if (current.status !== 'pending') throw Object.assign(new Error(`Approval is already ${current.status}.`), { code: 'APPROVAL_NOT_PENDING' });
      operation.approvals[index] = normalizeApproval({
        ...current,
        status: 'rejected',
        rejectedAt: nowIso(),
        approvedBy: safeString(rejectedBy, 200) || 'mark',
        approvalMessage: safeString(message, 2_000),
        decision: {
          ...current.decision,
          outcome: 'declined',
          decidedAt: nowIso(),
          decidedBy: safeString(rejectedBy, 200) || 'mark',
          decisionReasoning: safeString(message, 2_000),
        },
      });
      const step = operation.steps.find((item) => item.id === current.stepId);
      if (step) {
        step.status = 'blocked';
        step.error = 'Approval rejected.';
      }
      operation.status = 'blocked';
      operation.blockers.push(createBlocker({
        operationId, stepId: current.stepId, type: 'approval_rejected',
        message: `Approval rejected for ${current.action}.`, status: 'active', createdAt: nowIso(), resolution: safeString(message, 2_000),
      }));
      operation.activityLog.push({
        ...createActivityEvent({ operationId, stepId: current.stepId, type: 'approval_rejected', actor: rejectedBy,
        message: `Approval rejected for ${current.action}.`, data: { approvalId, reason: message }, timestamp: nowIso(),
      }) });
      return operation;
    });
  }
}
