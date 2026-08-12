import { safeObject, safeString } from '../operations/operation_types.js';

const RISK_RANK = Object.freeze({ low: 0, medium: 1, high: 2, critical: 3 });

const CRITICAL_ACTIONS = [
  /delete.*production|production.*delete|production.*data.*delet|delet.*production.*data|drop.*database|truncate.*database/i,
  /destroy.*infrastructure|irreversible.*account|close.*account/i,
  /billing.*change|change.*billing|legal.*commit/i,
  /credential.*change|credential.*rotation|rotate.*credential|revoke.*credential/i,
  /delete_dns_record|delete.*dns|dns.*delete/i,
];

const HIGH_ACTIONS = [
  /\bpush\b|git_push|publish_project_changes/i,
  /\bmerge\b|pull_request_open|open.*pull request/i,
  /deploy.*production|production.*deploy|deploy_worker_version/i,
  /environment.*variable|env.*change|change.*env/i,
  /\bdns\b|database.*migration|run.*migration/i,
  /send.*client|client.*communication|external.*communication/i,
  /automation.*change|change.*automation/i,
  /permission|access.*change|change.*access/i,
];

const MEDIUM_ACTIONS = [
  /modify.*file|write.*file|edit.*file|apply.*patch|codex.*implement/i,
  /create.*branch|git.*branch|commit.*work|draft.*pull request/i,
  /preview.*deploy|deploy.*preview/i,
  /update.*metadata|internal.*draft/i,
];

const LOW_ACTIONS = [
  /read|inspect|list|analy[sz]e|plan|prepare|context|build|test|lint|typecheck|verify|health|artifact|handoff/i,
];

function maxRisk(a, b) {
  return RISK_RANK[a] >= RISK_RANK[b] ? a : b;
}

function riskFromAction(action) {
  if (CRITICAL_ACTIONS.some((pattern) => pattern.test(action))) return 'critical';
  if (HIGH_ACTIONS.some((pattern) => pattern.test(action))) return 'high';
  if (MEDIUM_ACTIONS.some((pattern) => pattern.test(action))) return 'medium';
  if (LOW_ACTIONS.some((pattern) => pattern.test(action))) return 'low';
  return 'medium';
}

export class ApprovalPolicy {
  classify(input = {}) {
    const raw = safeObject(input);
    const provider = safeString(raw.provider, 100).toLowerCase() || 'internal';
    const action = safeString(raw.action || raw.toolName, 500).toLowerCase() || 'execute_step';
    const environment = safeString(raw.environment, 100).toLowerCase() || 'unknown';
    let riskLevel = riskFromAction(`${provider} ${action} ${environment}`);
    if (environment === 'production' && !/read|inspect|list|health/i.test(action)) riskLevel = maxRisk(riskLevel, 'high');

    const authorization = safeObject(raw.authorization);
    const actionClass = riskFromAction(`${provider} ${action} ${environment}`) === 'low' ? 'read_or_verify' : safeString(raw.actionClass, 160) || action;
    const trustedSource = ['authenticated_request', 'recorded_approval', 'server_policy'].includes(authorization.source);
    const authorizationMatches = trustedSource
      && authorization.businessKey === safeString(raw.business, 200)
      && authorization.projectRegistryId === safeString(raw.projectRegistryId, 160)
      && authorization.environment === environment
      && ((Array.isArray(authorization.providers) && authorization.providers.includes(provider)) || authorization.provider === provider)
      && Array.isArray(authorization.actionClasses)
      && authorization.actionClasses.includes(actionClass);
    const trustedAuthorization = authorizationMatches && authorization.revoked !== true;
    let approvalRequirement = 'none';
    let approvalRequired = false;
    if (riskLevel === 'critical') {
      approvalRequirement = 'explicit_strong_confirmation';
      approvalRequired = true;
    } else if (riskLevel === 'high') {
      approvalRequirement = 'explicit';
      approvalRequired = true;
    } else if (riskLevel === 'medium' && !trustedAuthorization) {
      approvalRequirement = 'explicit_or_configured_autonomy';
      approvalRequired = true;
    }
    if (provider === 'approval') {
      approvalRequirement = riskLevel === 'critical' ? 'explicit_strong_confirmation' : 'explicit';
      approvalRequired = true;
    }

    const externalImpact = riskLevel === 'high' || riskLevel === 'critical'
      || /publish|deploy|send|push|merge|dns|external/i.test(action);
    const financialImpact = /billing|purchase|spend|payment|invoice/i.test(action);
    const dataImpact = /delete|database|migration|write|modify|edit/i.test(action);
    const communicationImpact = /send|message|email|slack|client|communication/i.test(action);
    const reversibility = riskLevel === 'critical' ? 'irreversible' : (riskLevel === 'high' ? 'limited' : 'reversible');
    return {
      business: safeString(raw.business, 200),
      environment,
      provider,
      action,
      riskLevel,
      reversibility,
      externalImpact,
      financialImpact,
      dataImpact,
      communicationImpact,
      approvalRequirement,
      approvalRequired,
      actionClass,
      trustedAuthorization,
      authorizationSource: trustedAuthorization ? authorization.source : '',
      reason: approvalRequired
        ? `${provider === 'approval' ? 'Approval checkpoint' : `${riskLevel} risk action`} requires ${approvalRequirement.replaceAll('_', ' ')}.`
        : `${riskLevel} risk action is allowed by trusted server-side authorization provenance.`,
    };
  }
}

export function isStrongConfirmation(message) {
  const text = safeString(message, 2_000).toLowerCase();
  return /\b(i understand|strong confirmation|confirm irreversible|approve critical|proceed with (the )?critical)\b/.test(text);
}

export { riskFromAction, maxRisk, RISK_RANK };
