import { safeObject, safeString } from '../operations/operation_types.js';

const DAY_MS = 24 * 60 * 60 * 1_000;

export const DEFAULT_ACTIVITY_RULES = Object.freeze({
  staleDays: 21,
  dormantDays: 45,
  abandonedDays: 60,
  stalePullRequestDays: 14,
  staleBranchDays: 30,
  deploymentBottleneckCommitCount: 10,
  deploymentBottleneckWindowDays: 14,
  deploymentMaxAgeDays: 30,
  codexDriftCount: 2,
  codexDriftWindowDays: 14,
  verificationWindowDays: 30,
  commitWithoutTestCount: 3,
  quietHealthyDays: 14,
  attentionSlippingDays: 21,
  atRiskDays: 35,
  decayingDays: 45,
});

export const DEFAULT_SIGNAL_WEIGHTS = Object.freeze({
  git_commit: { weight: 100, halfLifeDays: 21 },
  codex_job_running: { weight: 95, halfLifeDays: 2 },
  codex_job_completed: { weight: 85, halfLifeDays: 14 },
  codex_handoff: { weight: 0, halfLifeDays: 1 },
  desktop_active_session: { weight: 90, halfLifeDays: 1 },
  build_or_test_run: { weight: 90, halfLifeDays: 7 },
  deployment_completed: { weight: 90, halfLifeDays: 30 },
  deployment_activity: { weight: 80, halfLifeDays: 7 },
  pull_request_activity: { weight: 85, halfLifeDays: 14 },
  branch_activity: { weight: 80, halfLifeDays: 14 },
  browser_verification: { weight: 90, halfLifeDays: 30 },
  operation_activity: { weight: 70, halfLifeDays: 7 },
  repository_read: { weight: 25, halfLifeDays: 2 },
  issue_activity: { weight: 40, halfLifeDays: 7 },
  airtable_task_update: { weight: 35, halfLifeDays: 3 },
  manual_note: { weight: 20, halfLifeDays: 3 },
});

function timeMs(value) {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function daysBetween(nowMs, value) {
  const timestamp = timeMs(value);
  return timestamp ? Math.max(0, (nowMs - timestamp) / DAY_MS) : Number.POSITIVE_INFINITY;
}

function newest(items, predicate = () => true) {
  return items.filter(predicate).reduce((latest, item) => !latest || timeMs(item.timestamp) > timeMs(latest.timestamp) ? item : latest, null);
}

function signalForEvidence(item) {
  if (item.type === 'commit') return 'git_commit';
  if (item.type === 'codex_handoff_created' || item.event === 'handoff_created') return 'codex_handoff';
  if (item.type === 'codex_job_completed' || item.event === 'job_completed' || item.event === 'result_verified') return 'codex_job_completed';
  if (['codex_job_started', 'codex_job_updated'].includes(item.type)) return 'codex_job_running';
  if (['workspace_opened', 'workspace_active'].includes(item.type)) return 'desktop_active_session';
  if (['build_run', 'test_run', 'lint_run', 'typecheck_run'].includes(item.type)) return 'build_or_test_run';
  if (['deployment_completed', 'production_published'].includes(item.type)) return 'deployment_completed';
  if (['deployment_started', 'deployment_failed', 'preview_created'].includes(item.type)) return 'deployment_activity';
  if (item.type.startsWith('pull_request_')) return 'pull_request_activity';
  if (['branch_created', 'branch_updated'].includes(item.type)) return 'branch_activity';
  if (['browser_verified', 'browser_failed'].includes(item.type)) return 'browser_verification';
  if (item.type.startsWith('operation_')) return 'operation_activity';
  if (item.type === 'repository_read') return 'repository_read';
  if (item.type === 'issue_updated') return 'issue_activity';
  if (item.type === 'task_updated') return 'airtable_task_update';
  return 'manual_note';
}

export function decayForEvidence(item, { nowMs = Date.now(), weights = DEFAULT_SIGNAL_WEIGHTS } = {}) {
  const signal = signalForEvidence(item);
  const rule = safeObject(weights[signal]);
  const weight = Math.max(0, Number(rule.weight) || 0);
  const halfLifeDays = Math.max(0.1, Number(rule.halfLifeDays) || 1);
  const ageDays = daysBetween(nowMs, item.timestamp);
  const recencyDecay = Number.isFinite(ageDays) ? 2 ** (-ageDays / halfLifeDays) : 0;
  return {
    signal,
    weight,
    halfLifeDays,
    ageDays: Number.isFinite(ageDays) ? Math.round(ageDays * 100) / 100 : null,
    recencyDecay: Math.round(recencyDecay * 10_000) / 10_000,
    contribution: Math.round(weight * recencyDecay * Math.max(0, Math.min(1, Number(item.confidence) || 0.5)) * 100) / 100,
  };
}

function summarizeWeights(items, options) {
  const grouped = new Map();
  for (const item of items) {
    const detail = decayForEvidence(item, options);
    const current = grouped.get(detail.signal) || {
      signal: detail.signal, rawCount: 0, weight: detail.weight, halfLifeDays: detail.halfLifeDays,
      decayedContribution: 0, evidenceIds: [],
    };
    current.rawCount += 1;
    current.decayedContribution += detail.contribution;
    if (current.evidenceIds.length < 20) current.evidenceIds.push(item.id);
    grouped.set(detail.signal, current);
  }
  return [...grouped.values()].map((item) => ({
    ...item, decayedContribution: Math.round(item.decayedContribution * 100) / 100,
  })).sort((a, b) => b.decayedContribution - a.decayedContribution);
}

function eventStatus(item) {
  return safeString(item.metadata?.status || item.deployment?.status || item.event, 100).toLowerCase();
}

function isFailure(item) {
  return item.type.endsWith('_failed') || /fail|error|cancel/.test(eventStatus(item));
}

function isSuccessfulRun(item) {
  return ['build_run', 'test_run', 'lint_run', 'typecheck_run'].includes(item.type) && !isFailure(item)
    && !/queued|running|started|unknown/.test(eventStatus(item));
}

function isMeaningfulMovement(item) {
  if (!item) return false;
  if (item.type === 'codex_handoff_created' || item.type === 'repository_read') return false;
  if (item.type === 'commit') return !/format|formatting|whitespace|dependency|deps|chore|bump|lockfile/i.test(`${item.summary || ''} ${item.metadata?.message || ''}`);
  if (['pull_request_opened', 'pull_request_merged', 'deployment_completed', 'production_published', 'browser_verified', 'operation_completed'].includes(item.type)) return !isFailure(item);
  if (['test_run', 'build_run', 'lint_run', 'typecheck_run'].includes(item.type)) return isSuccessfulRun(item);
  if (item.type === 'codex_job_completed') return Boolean(item.commitSha || item.branch || item.metadata?.hasDiff === true || item.metadata?.hasCommit === true || item.metadata?.status === 'verified');
  if (item.type === 'codex_job_updated') return Boolean(item.commitSha || item.metadata?.hasDiff === true || item.metadata?.hasCommit === true || item.event === 'result_verified');
  if (item.type === 'task_updated') return /done|complete|blocked|unblocked|approved|decided|verified|deployed|launched|accepted|closed/i.test(`${item.summary || ''} ${JSON.stringify(item.metadata || {})}`);
  return false;
}

function isVerifiedEvidence(item) {
  return ['browser_verified', 'test_run', 'build_run', 'lint_run', 'typecheck_run', 'deployment_completed', 'production_published', 'operation_completed'].includes(item?.type)
    && !isFailure(item);
}

function cadenceDays(project, fallback) {
  const raw = safeString(project?.currentObjective?.cadence || project?.metadata?.expectedCadence || project?.metadata?.cadence, 100).toLowerCase();
  if (/daily/.test(raw)) return 1;
  if (/weekly/.test(raw)) return 7;
  if (/biweekly|fortnight/.test(raw)) return 14;
  if (/monthly/.test(raw)) return 30;
  const days = raw.match(/(\d+)\s*(day|days|d)\b/);
  if (days) return Math.max(1, Math.min(365, Number(days[1]) || fallback));
  return fallback;
}

function evidenceRef(item) {
  return {
    id: item.id,
    source: item.source,
    type: item.type,
    event: item.event,
    summary: item.summary,
    timestamp: item.timestamp,
  };
}

function risk(code, summary, threshold, evidence) {
  return { code, summary, threshold, evidence: evidence.filter(Boolean).slice(0, 20).map(evidenceRef) };
}

function confidenceFor(items, project, nowMs) {
  const trusted = items.filter((item) => item.provenance?.trusted === true && item.type !== 'codex_handoff_created');
  const sources = new Set(trusted.map((item) => item.source));
  let score = 0;
  if (trusted.length) score += Math.min(0.5, trusted.length * 0.06);
  score += Math.min(0.35, sources.size * 0.1);
  if (timeMs(project?.createdAt) && daysBetween(nowMs, project.createdAt) >= 21) score += 0.15;
  score = Math.round(Math.min(1, score) * 100) / 100;
  return { score, level: score >= 0.75 ? 'high' : score >= 0.45 ? 'medium' : 'low', sources: [...sources].sort() };
}

function healthFor({ project, items, risks, operations, nowMs, rules }) {
  const objective = safeObject(project?.currentObjective);
  const hasObjective = Boolean(safeString(objective.desiredOutcome, 1_000));
  const hasDone = Boolean(safeString(objective.definitionOfDone || project?.definitionOfDone, 1_000));
  const lastMovement = newest(items, isMeaningfulMovement);
  const lastVerified = newest(items, isVerifiedEvidence);
  const movementAge = daysBetween(nowMs, lastMovement?.timestamp);
  const failedVerification = risks.some((item) => item.code === 'verification_gap' || item.code === 'repeated_failed_verification');
  const blocked = operations.some((item) => ['blocked', 'recovery_required', 'waiting_for_approval'].includes(item.status));
  let score = 100;
  if (!hasObjective) score -= 25;
  if (!hasDone) score -= 15;
  if (!lastMovement) score -= 20;
  else if (movementAge >= rules.attentionSlippingDays) score -= 15;
  if (risks.length) score -= Math.min(35, risks.length * 12);
  if (failedVerification) score -= 20;
  if (blocked) score -= 15;
  score = Math.max(0, Math.min(100, Math.round(score)));
  return {
    score,
    level: score >= 80 ? 'healthy' : score >= 60 ? 'watch' : score >= 40 ? 'at_risk' : 'decaying',
    reasons: [
      hasObjective ? 'Objective is recorded.' : 'Current objective is missing or implicit.',
      hasDone ? 'Definition of done is recorded.' : 'Definition of done is missing or implicit.',
      lastMovement ? `Last meaningful movement was ${Math.floor(movementAge)} day(s) ago.` : 'No meaningful movement evidence has been recorded.',
      risks.length ? `${risks.length} evidence-backed risk(s) are active.` : 'No deterministic risk rule is active.',
    ],
  };
}

function momentumFor({ items, nowMs }) {
  const movement = items.filter(isMeaningfulMovement);
  const recent = movement.filter((item) => daysBetween(nowMs, item.timestamp) <= 14);
  const last = newest(movement);
  const score = Math.min(100, Math.round(recent.reduce((sum, item) => sum + decayForEvidence(item, { nowMs }).contribution, 0)));
  return {
    score,
    level: score >= 70 ? 'strong' : score >= 35 ? 'moving' : score > 0 ? 'weak' : 'none',
    meaningfulEvents14d: recent.length,
    lastMeaningfulMovementAt: last?.timestamp || '',
    evidence: recent.slice(-10).map(evidenceRef),
  };
}

function decayStageFor({ project, items, operations, risks, nowMs, rules }) {
  const status = safeString(project?.status, 100).toLowerCase();
  const paused = /paused|on hold|seasonal|intentionally dormant/.test(`${status} ${project?.metadata?.pauseReason || ''}`);
  const activeOperation = operations.some((item) => ['draft', 'planned', 'queued', 'running', 'verifying', 'awaiting_provider'].includes(item.status));
  const blockingOperation = operations.some((item) => ['blocked', 'recovery_required', 'waiting_for_approval'].includes(item.status));
  const lastMovement = newest(items, isMeaningfulMovement);
  const lastEvidence = newest(items);
  const age = lastMovement ? daysBetween(nowMs, lastMovement.timestamp) : daysBetween(nowMs, project?.createdAt);
  const expectedCadenceDays = cadenceDays(project, rules.attentionSlippingDays);
  if (paused) return { stage: 'quiet_but_healthy', severity: 0, expectedCadenceDays, reason: 'Project is explicitly paused or intentionally dormant.', lastMeaningfulMovementAt: lastMovement?.timestamp || '', lastEvidenceAt: lastEvidence?.timestamp || '' };
  if (blockingOperation || risks.some((item) => ['repeated_failed_builds', 'repeated_failed_verification', 'codex_only_drift'].includes(item.code))) {
    return { stage: 'at_risk', severity: 2, expectedCadenceDays, reason: 'Blocked work, failed verification, or Codex drift is present.', lastMeaningfulMovementAt: lastMovement?.timestamp || '', lastEvidenceAt: lastEvidence?.timestamp || '' };
  }
  if (activeOperation && age <= expectedCadenceDays * 2) return { stage: 'quiet_but_healthy', severity: 0, expectedCadenceDays, reason: 'An active operation is expected to continue.', lastMeaningfulMovementAt: lastMovement?.timestamp || '', lastEvidenceAt: lastEvidence?.timestamp || '' };
  if (!Number.isFinite(age)) return { stage: 'dormant_candidate', severity: 4, expectedCadenceDays, reason: 'No credible movement evidence exists.', lastMeaningfulMovementAt: '', lastEvidenceAt: lastEvidence?.timestamp || '' };
  if (age <= expectedCadenceDays) return { stage: 'quiet_but_healthy', severity: 0, expectedCadenceDays, reason: 'No recent activity is expected yet for this cadence.', lastMeaningfulMovementAt: lastMovement?.timestamp || '', lastEvidenceAt: lastEvidence?.timestamp || '' };
  if (age >= rules.abandonedDays) return { stage: 'dormant_candidate', severity: 4, expectedCadenceDays, reason: 'No credible next movement has appeared inside the abandonment window.', lastMeaningfulMovementAt: lastMovement?.timestamp || '', lastEvidenceAt: lastEvidence?.timestamp || '' };
  if (age >= rules.decayingDays) return { stage: 'decaying', severity: 3, expectedCadenceDays, reason: 'Active ownership appears to have broken down.', lastMeaningfulMovementAt: lastMovement?.timestamp || '', lastEvidenceAt: lastEvidence?.timestamp || '' };
  if (age >= rules.atRiskDays || risks.length) return { stage: 'at_risk', severity: 2, expectedCadenceDays, reason: risks[0]?.summary || 'Expected progress is materially drifting.', lastMeaningfulMovementAt: lastMovement?.timestamp || '', lastEvidenceAt: lastEvidence?.timestamp || '' };
  return { stage: 'attention_slipping', severity: 1, expectedCadenceDays, reason: 'Expected progress or follow-up has not occurred.', lastMeaningfulMovementAt: lastMovement?.timestamp || '', lastEvidenceAt: lastEvidence?.timestamp || '' };
}

function deriveRisks({ items, project, operationRows, nowMs, rules }) {
  const within = (item, days) => daysBetween(nowMs, item.timestamp) <= days;
  const commits14d = items.filter((item) => item.type === 'commit' && within(item, rules.deploymentBottleneckWindowDays));
  const commits7d = items.filter((item) => item.type === 'commit' && within(item, 7));
  const deployments = items.filter((item) => ['deployment_completed', 'production_published'].includes(item.type));
  const lastDeployment = newest(deployments);
  const verifications = items.filter((item) => item.type === 'browser_verified' && !isFailure(item));
  const lastVerification = newest(verifications);
  const meaningfulCodex = items.filter((item) => ['codex_job_started', 'codex_job_updated', 'codex_job_completed'].includes(item.type) && within(item, rules.codexDriftWindowDays));
  const meaningfulCodexJobCount = new Set(meaningfulCodex.map((item) => item.codexJobId || item.externalId || item.id)).size;
  const codexImplementationProof = meaningfulCodex.some((item) => item.commitSha || item.branch || item.metadata?.hasDiff === true || item.metadata?.hasCommit === true);
  const tests = items.filter((item) => item.type === 'test_run' && isSuccessfulRun(item));
  const openPrLatest = new Map();
  for (const item of items.filter((entry) => entry.type.startsWith('pull_request_'))) {
    const key = String(item.pullRequest?.number || item.externalId || item.id);
    if (!openPrLatest.has(key) || timeMs(item.timestamp) > timeMs(openPrLatest.get(key).timestamp)) openPrLatest.set(key, item);
  }
  const stalePrs = [...openPrLatest.values()].filter((item) => item.type !== 'pull_request_merged'
    && item.pullRequest?.state !== 'closed' && daysBetween(nowMs, item.pullRequest?.updatedAt || item.timestamp) >= rules.stalePullRequestDays);
  const failedBuilds = items.filter((item) => ['build_run', 'test_run', 'lint_run', 'typecheck_run'].includes(item.type) && isFailure(item) && within(item, 14));
  const failedVerifications = items.filter((item) => item.type === 'browser_failed' && within(item, rules.verificationWindowDays));
  const desktop7d = items.filter((item) => ['workspace_opened', 'workspace_active'].includes(item.type) && within(item, 7));
  const desktopMinutes = desktop7d.reduce((sum, item) => sum + (Number(item.workspace?.activeMinutes) || 0), 0);
  const airtable30d = items.filter((item) => item.source === 'airtable' && within(item, 30));
  const real30d = items.filter((item) => !['airtable', 'manual'].includes(item.source) && item.type !== 'repository_read'
    && item.type !== 'codex_handoff_created' && within(item, 30));
  const risks = [];

  if (commits14d.length >= rules.deploymentBottleneckCommitCount && (!lastDeployment || daysBetween(nowMs, lastDeployment.timestamp) > rules.deploymentMaxAgeDays)) {
    risks.push(risk('deployment_bottleneck', `${commits14d.length} commits in ${rules.deploymentBottleneckWindowDays} days, but no deployment in ${rules.deploymentMaxAgeDays} days.`, {
      commitCount: rules.deploymentBottleneckCommitCount, commitWindowDays: rules.deploymentBottleneckWindowDays, deploymentMaxAgeDays: rules.deploymentMaxAgeDays,
    }, [...commits14d, lastDeployment]));
  }
  if (lastDeployment && (!lastVerification || timeMs(lastVerification.timestamp) < timeMs(lastDeployment.timestamp))) {
    risks.push(risk('verification_gap', 'A deployment completed without subsequent browser verification.', { verificationMustFollowDeployment: true }, [lastDeployment, lastVerification]));
  }
  if (meaningfulCodexJobCount >= rules.codexDriftCount && commits14d.length === 0 && !codexImplementationProof) {
    risks.push(risk('codex_only_drift', `${meaningfulCodexJobCount} Codex jobs have no matching commit, branch, or diff evidence.`, {
      codexSignalCount: rules.codexDriftCount, windowDays: rules.codexDriftWindowDays,
    }, meaningfulCodex));
  }
  const latestCommit = newest(commits14d);
  const testAfterCommit = latestCommit && tests.some((item) => timeMs(item.timestamp) >= timeMs(latestCommit.timestamp));
  if (commits14d.length >= rules.commitWithoutTestCount && !testAfterCommit) {
    risks.push(risk('commits_without_tests', `${commits14d.length} recent commits have no subsequent successful test evidence.`, {
      commitCount: rules.commitWithoutTestCount, windowDays: rules.deploymentBottleneckWindowDays,
    }, [...commits14d, ...tests.slice(-1)]));
  }
  if (stalePrs.length) risks.push(risk('review_bottleneck', `${stalePrs.length} open pull request(s) have not moved for ${rules.stalePullRequestDays} days.`, { stalePullRequestDays: rules.stalePullRequestDays }, stalePrs));
  if (failedBuilds.length >= 2) risks.push(risk('repeated_failed_builds', `${failedBuilds.length} build or quality runs failed in 14 days.`, { failureCount: 2, windowDays: 14 }, failedBuilds));
  if (failedVerifications.length >= 2) risks.push(risk('repeated_failed_verification', `${failedVerifications.length} browser verifications failed in ${rules.verificationWindowDays} days.`, { failureCount: 2, windowDays: rules.verificationWindowDays }, failedVerifications));
  if (desktopMinutes >= 120 && commits7d.length === 0) risks.push(risk('desktop_without_repository_changes', `${Math.round(desktopMinutes)} desktop-active minutes have no commit evidence in 7 days.`, { activeMinutes: 120, windowDays: 7 }, desktop7d));
  if (airtable30d.length >= 3 && real30d.length === 0) risks.push(risk('task_list_without_real_activity', 'Airtable is moving, but no GitHub, Codex, desktop, operation, deployment, or verification activity is present.', { airtableUpdates: 3, windowDays: 30 }, airtable30d));
  if (real30d.length >= 5 && airtable30d.length === 0) risks.push(risk('real_activity_without_airtable_updates', 'Observed implementation activity is not reflected in Airtable.', { realSignals: 5, windowDays: 30 }, real30d));

  const latestAirtableStatus = newest(airtable30d, (item) => Boolean(item.metadata?.projectStatus));
  const declaredStatus = safeString(latestAirtableStatus?.metadata?.projectStatus, 100).toLowerCase();
  const lastReal = newest(items, (item) => !['airtable', 'manual'].includes(item.source) && item.type !== 'repository_read' && item.type !== 'codex_handoff_created');
  if (latestAirtableStatus && declaredStatus === 'active' && (!lastReal || daysBetween(nowMs, lastReal.timestamp) >= 45)) {
    risks.push(risk('airtable_contradiction', `Airtable says Active, but no meaningful observed activity exists for at least 45 days.`, { inactiveDays: 45, declaredStatus: 'Active' }, [latestAirtableStatus, lastReal]));
  }
  if (latestAirtableStatus && /on hold|paused/.test(declaredStatus) && lastReal && daysBetween(nowMs, lastReal.timestamp) <= 1) {
    risks.push(risk('airtable_contradiction', 'Airtable says On Hold, but observed implementation activity occurred today.', { activeWithinDays: 1, declaredStatus }, [latestAirtableStatus, lastReal]));
  }
  if (operationRows.some((operation) => operation.status === 'failed')) {
    const failed = operationRows.filter((operation) => operation.status === 'failed');
    risks.push({ code: 'failed_operations', summary: `${failed.length} durable operation(s) failed.`, threshold: { failureCount: 1 }, evidence: failed.slice(0, 20).map((operation) => ({ operationId: operation.id, status: operation.status, updatedAt: operation.updatedAt })) });
  }
  return risks;
}

function deriveState({ items, project, operations, risks, nowMs, rules }) {
  const lastMeaningful = newest(items, (item) => !['airtable', 'manual'].includes(item.source)
    && item.type !== 'repository_read' && item.type !== 'codex_handoff_created');
  const idleDays = lastMeaningful ? daysBetween(nowMs, lastMeaningful.timestamp) : daysBetween(nowMs, project?.createdAt);
  const status = safeString(project?.status, 100).toLowerCase();
  const activeOperations = operations.filter((item) => ['draft', 'planned', 'queued', 'running', 'verifying', 'awaiting_provider'].includes(item.status));
  const blockedOperations = operations.filter((item) => ['blocked', 'recovery_required', 'waiting_for_approval'].includes(item.status));
  const commitCount7d = items.filter((item) => item.type === 'commit' && daysBetween(nowMs, item.timestamp) <= 7).length;
  const desktopMinutes7d = items.filter((item) => ['workspace_opened', 'workspace_active'].includes(item.type) && daysBetween(nowMs, item.timestamp) <= 7)
    .reduce((sum, item) => sum + (Number(item.workspace?.activeMinutes) || 0), 0);
  const codexRunning = items.some((item) => ['codex_job_started', 'codex_job_updated'].includes(item.type)
    && /running|started|registered|updated/.test(eventStatus(item)) && daysBetween(nowMs, item.timestamp) <= 2);
  const lastDeployStart = newest(items, (item) => item.type === 'deployment_started');
  const lastDeployComplete = newest(items, (item) => ['deployment_completed', 'production_published'].includes(item.type));
  const lastVerification = newest(items, (item) => item.type === 'browser_verified');
  const recentPr = newest(items, (item) => item.type.startsWith('pull_request_') && daysBetween(nowMs, item.timestamp) <= 7);

  if (/paused|on hold/.test(status)) return { state: activeOperations.length ? 'waiting' : 'dormant', idleDays };
  if (blockedOperations.length || risks.some((item) => ['repeated_failed_builds', 'repeated_failed_verification'].includes(item.code))) return { state: 'blocked', idleDays };
  if (lastDeployStart && daysBetween(nowMs, lastDeployStart.timestamp) <= 2
    && (!lastDeployComplete || timeMs(lastDeployComplete.timestamp) < timeMs(lastDeployStart.timestamp))) return { state: 'deploying', idleDays };
  if (lastDeployComplete && daysBetween(nowMs, lastDeployComplete.timestamp) <= rules.verificationWindowDays
    && (!lastVerification || timeMs(lastVerification.timestamp) < timeMs(lastDeployComplete.timestamp))) return { state: 'verifying', idleDays };
  if (recentPr && commitCount7d < 3) return { state: 'reviewing', idleDays };
  if (commitCount7d >= 5 || (codexRunning && desktopMinutes7d >= 30)) return { state: 'deep_implementation', idleDays };
  if (Number.isFinite(idleDays) && idleDays >= rules.abandonedDays && activeOperations.length === 0 && !recentPr) return { state: 'abandoned_candidate', idleDays };
  if (Number.isFinite(idleDays) && idleDays >= rules.dormantDays) return { state: 'dormant', idleDays };
  if (Number.isFinite(idleDays) && idleDays >= rules.staleDays) return { state: 'stale', idleDays };
  if (activeOperations.length || (lastMeaningful && daysBetween(nowMs, lastMeaningful.timestamp) <= 7)) return { state: 'active', idleDays };
  if (lastMeaningful && daysBetween(nowMs, lastMeaningful.timestamp) <= 30) return { state: 'maintenance', idleDays };
  return { state: 'unknown', idleDays };
}

export function calculateProjectActivitySnapshot({
  businessKey, project, evidence = [], operations = [], nowMs = Date.now(), weights = DEFAULT_SIGNAL_WEIGHTS, rules = DEFAULT_ACTIVITY_RULES,
} = {}) {
  const projectRegistryId = safeString(project?.id, 160);
  const items = evidence.filter((item) => item.projectRegistryId === projectRegistryId);
  const operationRows = operations.filter((item) => item.projectRegistryId === projectRegistryId || (project?.projectId && item.projectId === project.projectId));
  const within = (item, days) => daysBetween(nowMs, item.timestamp) <= days;
  const weightedContributions = summarizeWeights(items, { nowMs, weights });
  const weightedTotal = weightedContributions.reduce((sum, item) => sum + item.decayedContribution, 0);
  const activityScore = Math.round(Math.min(100, 100 * (1 - Math.exp(-weightedTotal / 300))) * 10) / 10;
  const risks = deriveRisks({ items, project, operationRows, nowMs, rules: { ...DEFAULT_ACTIVITY_RULES, ...rules } });
  const normalizedRules = { ...DEFAULT_ACTIVITY_RULES, ...rules };
  const derived = deriveState({ items, project, operations: operationRows, risks, nowMs, rules: normalizedRules });
  const health = healthFor({ project, items, risks, operations: operationRows, nowMs, rules: normalizedRules });
  const momentum = momentumFor({ items, nowMs });
  const decay = decayStageFor({ project, items, operations: operationRows, risks, nowMs, rules: normalizedRules });
  const confidence = confidenceFor(items, project, nowMs);
  const last = (predicate) => newest(items, predicate)?.timestamp || '';
  const latestPullRequests = new Map();
  const latestBranches = new Map();
  for (const item of items) {
    if (item.type.startsWith('pull_request_')) {
      const key = String(item.pullRequest?.number || item.externalId || item.id);
      if (!latestPullRequests.has(key) || timeMs(item.timestamp) > timeMs(latestPullRequests.get(key).timestamp)) latestPullRequests.set(key, item);
    }
    if (['branch_created', 'branch_updated'].includes(item.type) && item.branch) {
      if (!latestBranches.has(item.branch) || timeMs(item.timestamp) > timeMs(latestBranches.get(item.branch).timestamp)) latestBranches.set(item.branch, item);
    }
  }
  const openPullRequests = [...latestPullRequests.values()].filter((item) => item.type !== 'pull_request_merged' && item.pullRequest?.state !== 'closed');
  const stalePullRequests = openPullRequests.filter((item) => daysBetween(nowMs, item.pullRequest?.updatedAt || item.timestamp) >= (rules.stalePullRequestDays || DEFAULT_ACTIVITY_RULES.stalePullRequestDays));
  const branchRows = [...latestBranches.values()];
  const staleBranches = branchRows.filter((item) => item.branch !== project?.repo?.defaultBranch
    && daysBetween(nowMs, item.metadata?.commitDate || item.timestamp) >= (rules.staleBranchDays || DEFAULT_ACTIVITY_RULES.staleBranchDays));
  const missingExpectedSignals = [];
  if (items.some((item) => item.type === 'commit' && within(item, 14)) && !items.some((item) => item.type === 'test_run' && within(item, 14))) missingExpectedSignals.push('test_run');
  if (items.filter((item) => item.type === 'commit' && within(item, 14)).length >= (rules.deploymentBottleneckCommitCount || DEFAULT_ACTIVITY_RULES.deploymentBottleneckCommitCount)
    && !items.some((item) => ['deployment_completed', 'production_published'].includes(item.type) && within(item, 30))) missingExpectedSignals.push('deployment_completed');
  const lastDeployment = newest(items, (item) => ['deployment_completed', 'production_published'].includes(item.type));
  if (lastDeployment && !items.some((item) => item.type === 'browser_verified' && timeMs(item.timestamp) >= timeMs(lastDeployment.timestamp))) missingExpectedSignals.push('browser_verified');
  const reasons = [
    `${items.length} normalized evidence record(s).`,
    Number.isFinite(derived.idleDays) ? `Last meaningful observed activity was ${Math.floor(derived.idleDays)} day(s) ago.` : 'No meaningful observed activity has been recorded.',
    weightedContributions.length ? `Strongest decayed signal: ${weightedContributions[0].signal} (${weightedContributions[0].decayedContribution}).` : 'No weighted activity signals are available.',
  ];
  if (risks.length) reasons.push(risks[0].summary);
  return {
    projectRegistryId,
    projectId: safeString(project?.projectId, 160),
    projectName: safeString(project?.canonicalName, 300),
    businessKey,
    calculatedAt: new Date(nowMs).toISOString(),
    lastEvidenceAt: last(() => true),
    lastCodeActivityAt: last((item) => ['commit', 'branch_created', 'branch_updated', 'pull_request_opened', 'pull_request_updated', 'pull_request_merged'].includes(item.type)),
    lastCodexActivityAt: last((item) => ['codex_job_started', 'codex_job_updated', 'codex_job_completed'].includes(item.type)),
    lastDesktopActivityAt: last((item) => ['workspace_opened', 'workspace_active', 'build_run', 'test_run', 'lint_run', 'typecheck_run'].includes(item.type) && item.source === 'desktop'),
    lastDeploymentAt: last((item) => ['deployment_started', 'deployment_completed', 'deployment_failed', 'preview_created', 'production_published'].includes(item.type)),
    lastVerificationAt: last((item) => ['browser_verified', 'browser_failed'].includes(item.type)),
    commitCount7d: items.filter((item) => item.type === 'commit' && within(item, 7)).length,
    commitCount30d: items.filter((item) => item.type === 'commit' && within(item, 30)).length,
    codexJobs7d: new Set(items.filter((item) => ['codex_job_started', 'codex_job_updated', 'codex_job_completed'].includes(item.type) && within(item, 7)).map((item) => item.codexJobId || item.externalId || item.id)).size,
    desktopActiveMinutes7d: Math.round(items.filter((item) => ['workspace_opened', 'workspace_active'].includes(item.type) && within(item, 7)).reduce((sum, item) => sum + (Number(item.workspace?.activeMinutes) || 0), 0) * 10) / 10,
    deployments30d: items.filter((item) => ['deployment_completed', 'production_published'].includes(item.type) && within(item, 30)).length,
    verificationPasses30d: items.filter((item) => item.type === 'browser_verified' && !isFailure(item) && within(item, 30)).length,
    verificationFailures30d: items.filter((item) => item.type === 'browser_failed' && within(item, 30)).length,
    openPullRequests: openPullRequests.length,
    stalePullRequests: stalePullRequests.length,
    activeBranches: branchRows.length - staleBranches.length,
    staleBranches: staleBranches.length,
    activeOperations: operationRows.filter((item) => ['draft', 'planned', 'queued', 'running', 'verifying', 'awaiting_provider'].includes(item.status)).length,
    blockedOperations: operationRows.filter((item) => ['blocked', 'recovery_required', 'waiting_for_approval'].includes(item.status)).length,
    failedOperations: operationRows.filter((item) => item.status === 'failed').length,
    airtableUpdates30d: items.filter((item) => item.source === 'airtable' && item.type === 'task_updated' && within(item, 30)).length,
    evidenceCount: items.length,
    rawSignalCounts: Object.fromEntries(weightedContributions.map((item) => [item.signal, item.rawCount])),
    weightedContributions,
    recencyDecay: Object.fromEntries(Object.entries(weights).map(([signal, rule]) => [signal, { halfLifeDays: rule.halfLifeDays }])),
    weightedTotal: Math.round(weightedTotal * 100) / 100,
    activityScore,
    focusScore: Math.round(weightedContributions.filter((item) => !['airtable_task_update', 'manual_note', 'codex_handoff'].includes(item.signal)).reduce((sum, item) => sum + item.decayedContribution, 0) * 100) / 100,
    confidence: confidence.score,
    confidenceLevel: confidence.level,
    confidenceSources: confidence.sources,
    state: derived.state,
    operationalState: derived.state === 'blocked' ? 'at_risk'
      : derived.state === 'verifying' ? 'verifying'
        : derived.state === 'abandoned_candidate' ? 'dormant'
          : derived.state === 'stale' ? 'decaying'
            : derived.state === 'dormant' ? 'dormant'
              : /paused|on hold/.test(safeString(project?.status, 100).toLowerCase()) ? 'dormant'
                : 'active',
    health,
    momentum,
    decay,
    currentObjective: safeObject(project?.currentObjective),
    definitionOfDone: safeString(project?.definitionOfDone || project?.currentObjective?.definitionOfDone, 4_000),
    lastMeaningfulMovementAt: momentum.lastMeaningfulMovementAt,
    lastVerifiedEvidenceAt: newest(items, isVerifiedEvidence)?.timestamp || '',
    nextExpectedEvent: decay.stage === 'attention_slipping' ? 'Recovery check or follow-up should be prepared.'
      : decay.stage === 'at_risk' ? 'Resolve blocker, verification failure, or Codex drift.'
        : decay.stage === 'decaying' ? 'Marcus should propose resume, pause, archive, or reassignment.'
          : decay.stage === 'dormant_candidate' ? 'Archive, restore, or define a credible next action.'
            : 'Continue observing until the next expected cadence point.',
    reasons,
    risks,
    missingExpectedSignals,
    suggestedAction: risks[0]?.summary || (derived.state === 'stale' ? 'Confirm whether this project should resume or be intentionally paused.' : derived.state === 'abandoned_candidate' ? 'Review and explicitly archive or reactivate this project.' : 'Continue collecting observed activity.'),
  };
}

export function calculateCurrentFocus({ snapshots = [], evidence = [], previousFocus = null, nowMs = Date.now() } = {}) {
  const eligible = snapshots.filter((item) => item.focusScore > 0 && !['stale', 'abandoned_candidate', 'dormant', 'unknown'].includes(item.state))
    .sort((a, b) => b.focusScore - a.focusScore || b.lastEvidenceAt.localeCompare(a.lastEvidenceAt));
  const current = eligible[0] || null;
  const runnerUp = eligible[1] || null;
  if (!current) return {
    currentFocusProject: null, confidence: 0, confidenceLevel: 'low', focusScore: 0, evidence: [],
    previousFocusProject: previousFocus?.currentFocusProject || null, focusShiftDetectedAt: '', reason: 'No qualifying observed activity is available.',
  };
  const margin = current.focusScore - (runnerUp?.focusScore || 0);
  const ratio = runnerUp?.focusScore ? current.focusScore / runnerUp.focusScore : 3;
  const competitionConfidence = runnerUp ? Math.min(0.15, Math.max(0, ratio - 1) * 0.15) : 0.1;
  const confidence = Math.round(Math.min(1, 0.15 + current.confidence * 0.7 + competitionConfidence) * 100) / 100;
  const priorProject = previousFocus?.currentFocusProject || null;
  const shifted = priorProject?.projectRegistryId && priorProject.projectRegistryId !== current.projectRegistryId && confidence >= 0.55 && ratio >= 1.15;
  const topEvidence = evidence.filter((item) => item.projectRegistryId === current.projectRegistryId)
    .map((item) => ({ item, decay: decayForEvidence(item, { nowMs }) }))
    .filter(({ decay }) => decay.signal !== 'airtable_task_update' && decay.signal !== 'manual_note' && decay.signal !== 'codex_handoff')
    .sort((a, b) => b.decay.contribution - a.decay.contribution)
    .slice(0, 10)
    .map(({ item, decay }) => ({ ...evidenceRef(item), weightedContribution: decay.contribution, recencyDecay: decay.recencyDecay }));
  return {
    currentFocusProject: { projectRegistryId: current.projectRegistryId, projectId: current.projectId, projectName: current.projectName, state: current.state },
    confidence,
    confidenceLevel: confidence >= 0.75 ? 'high' : confidence >= 0.5 ? 'medium' : 'low',
    focusScore: current.focusScore,
    evidence: topEvidence,
    previousFocusProject: shifted ? priorProject : (previousFocus?.previousFocusProject || priorProject),
    focusShiftDetectedAt: shifted ? new Date(nowMs).toISOString() : (previousFocus?.focusShiftDetectedAt || ''),
    reason: shifted
      ? `Current focus shifted from ${priorProject.projectName || priorProject.projectRegistryId} to ${current.projectName}; the decayed evidence score leads by ${Math.round(margin)}.`
      : `${current.projectName} has the strongest recent non-Airtable evidence score (${current.focusScore})${runnerUp ? `, ahead of ${runnerUp.projectName} (${runnerUp.focusScore})` : ''}.`,
  };
}

export function calculateBusinessActivity({ businessKey, projects = [], evidence = [], operations = [], previousFocus = null, nowMs = Date.now(), weights, rules } = {}) {
  const snapshots = projects.map((project) => calculateProjectActivitySnapshot({ businessKey, project, evidence, operations, nowMs, weights, rules }));
  snapshots.sort((a, b) => b.focusScore - a.focusScore || a.projectName.localeCompare(b.projectName));
  const currentFocus = calculateCurrentFocus({ snapshots, evidence, previousFocus, nowMs });
  return {
    businessKey,
    calculatedAt: new Date(nowMs).toISOString(),
    snapshots,
    currentFocus,
    stale: snapshots.filter((item) => ['stale', 'dormant', 'abandoned_candidate'].includes(item.state)),
    bottlenecks: snapshots.filter((item) => item.risks.length).map((item) => ({
      projectRegistryId: item.projectRegistryId, projectName: item.projectName, state: item.state, risks: item.risks,
    })),
    rules: { ...DEFAULT_ACTIVITY_RULES, ...safeObject(rules) },
    weights: weights || DEFAULT_SIGNAL_WEIGHTS,
  };
}
