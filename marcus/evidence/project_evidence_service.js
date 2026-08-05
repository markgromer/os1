import path from 'node:path';

import { safeBusinessKey, safeHttpUrl, safeIso, safeObject, safeString } from '../operations/operation_types.js';
import { calculateBusinessActivity, DEFAULT_ACTIVITY_RULES, DEFAULT_SIGNAL_WEIGHTS } from './activity_engine.js';
import { ProjectEvidenceStore } from './evidence_store.js';
import { normalizeEvidence, normalizeManualEvidence } from './evidence_types.js';
import { GitHubEvidenceIngestor } from './github_evidence.js';
import { BrowserVerificationProvider } from '../providers/browser_verification_provider.js';

const TERMINAL_OPERATIONS = new Set(['completed', 'failed', 'cancelled']);
const CODEX_LIFECYCLE_EVENTS = new Set([
  'handoff_created', 'job_started', 'status_updated', 'follow_up_sent', 'artifact_received', 'diff_received',
  'job_completed', 'job_failed', 'job_cancelled', 'job_paused', 'job_resumed',
]);

function projectPathKey(value) {
  return safeString(value, 2_000).replaceAll('\\', '/').replace(/\/+$/g, '').toLowerCase();
}

function observedFileNames(workspace) {
  const values = [workspace?.activeFile, ...(Array.isArray(workspace?.recentFiles) ? workspace.recentFiles : [])];
  return [...new Set(values.map((item) => {
    const text = safeString(item, 500).replaceAll('\\', '/');
    return safeString(path.posix.basename(text), 300);
  }).filter(Boolean))].slice(0, 100);
}

function operationEventType(eventType) {
  if (eventType === 'operation_created') return 'operation_created';
  if (['operation_queued', 'operation_resumed'].includes(eventType)) return 'operation_started';
  if (eventType === 'operation_completed') return 'operation_completed';
  if (eventType === 'operation_failed') return 'operation_failed';
  return '';
}

function deploymentStatusType(status, environment = '') {
  const state = safeString(status, 100).toLowerCase();
  const env = safeString(environment, 100).toLowerCase();
  if (/fail|cancel|error|deactivat/.test(state)) return 'deployment_failed';
  if (/live|success|complete|ready|active/.test(state)) return env === 'production' ? 'production_published' : (env === 'preview' ? 'preview_created' : 'deployment_completed');
  return 'deployment_started';
}

function projectForLegacyItem(projects, item) {
  const id = safeString(item?.projectId, 160);
  const name = safeString(item?.project || item?.projectName || item?.name, 300).toLowerCase();
  const matches = projects.filter((project) => (id && project.projectId === id)
    || (name && [project.canonicalName, ...(project.aliases || [])].some((value) => safeString(value, 300).toLowerCase() === name)));
  return matches.length === 1 ? matches[0] : null;
}

function evidenceWatermark(items) {
  const list = Array.isArray(items) ? items : [];
  let latestObservedAt = '';
  let latestId = '';
  for (const item of list) {
    const observedAt = safeString(item?.observedAt, 64);
    if (observedAt > latestObservedAt || (observedAt === latestObservedAt && safeString(item?.id, 160) > latestId)) {
      latestObservedAt = observedAt;
      latestId = safeString(item?.id, 160);
    }
  }
  return `${list.length}:${latestObservedAt}:${latestId}`;
}

function operationsWatermark(items) {
  const list = Array.isArray(items) ? items : [];
  let latest = '';
  for (const operation of list) {
    const value = `${safeString(operation?.updatedAt, 64)}:${safeString(operation?.id, 160)}:${Number(operation?.revision) || 0}`;
    if (value > latest) latest = value;
  }
  return `${list.length}:${latest}`;
}

export class ProjectEvidenceService {
  constructor({
    dataDir,
    store = null,
    listProjects,
    listOperations = async () => [],
    getLegacyStore = async () => ({}),
    getSettings = async () => ({}),
    githubApi = null,
    renderApi = null,
    cloudflareApi = null,
    browserAdapter = null,
    syncAirtableDerivedState = null,
    maxHistory,
  } = {}) {
    if (typeof listProjects !== 'function') throw new Error('ProjectEvidenceService requires listProjects.');
    this.store = store || new ProjectEvidenceStore({ dataDir, maxHistory });
    this.listProjects = listProjects;
    this.listOperations = listOperations;
    this.getLegacyStore = getLegacyStore;
    this.getSettings = getSettings;
    this.github = new GitHubEvidenceIngestor({ api: githubApi, store: this.store });
    this.renderApi = renderApi;
    this.cloudflareApi = cloudflareApi;
    this.browser = new BrowserVerificationProvider({ adapter: browserAdapter });
    this.syncAirtableDerivedState = syncAirtableDerivedState;
    this.refreshes = new Map();
  }

  async projects(businessKey) {
    return this.listProjects(safeBusinessKey(businessKey));
  }

  async assertProject(businessKey, projectRegistryId) {
    const project = (await this.projects(businessKey)).find((item) => item.id === projectRegistryId);
    if (!project) throw Object.assign(new Error('Project registry record not found.'), { code: 'PROJECT_REGISTRY_NOT_FOUND' });
    return project;
  }

  async ingestManual(businessKey, input) {
    const key = safeBusinessKey(businessKey);
    const project = await this.assertProject(key, safeString(input?.projectRegistryId, 160));
    const normalized = normalizeManualEvidence({ ...safeObject(input), projectId: project.projectId }, { businessKey: key, actor: input?.actor });
    return this.store.append(key, normalized, { assignedSource: 'manual', trusted: false, actor: normalized.actor, provenanceMethod: normalized.provenance.method });
  }

  async ingestTrusted(businessKey, source, inputs, { actor, provenanceMethod } = {}) {
    const key = safeBusinessKey(businessKey);
    const list = Array.isArray(inputs) ? inputs : [inputs];
    for (const item of list) await this.assertProject(key, safeString(item?.projectRegistryId, 160));
    return this.store.append(key, list, { assignedSource: source, trusted: true, actor, provenanceMethod });
  }

  async recordCodexLifecycle(input = {}) {
    const raw = safeObject(input);
    const event = safeString(raw.event, 80).toLowerCase();
    if (!CODEX_LIFECYCLE_EVENTS.has(event)) throw Object.assign(new Error('Unsupported Codex lifecycle event.'), { code: 'CODEX_LIFECYCLE_EVENT_INVALID' });
    const key = safeBusinessKey(raw.businessKey);
    const project = await this.assertProject(key, safeString(raw.projectRegistryId, 160));
    const timestamp = safeIso(raw.timestamp) || new Date().toISOString();
    const jobId = safeString(raw.codexJobId, 300);
    const operationId = safeString(raw.operationId, 120);
    const stepId = safeString(raw.stepId, 120);
    const status = safeString(raw.status, 80).toLowerCase();
    const type = event === 'handoff_created' ? 'codex_handoff_created'
      : event === 'job_completed' ? 'codex_job_completed'
        : event === 'job_started' ? 'codex_job_started' : 'codex_job_updated';
    const jobStateEvents = new Set(['job_started', 'status_updated', 'job_completed', 'job_failed', 'job_cancelled', 'job_paused', 'job_resumed']);
    const externalId = event === 'handoff_created'
      ? `codex-handoff:${operationId}:${stepId}`
      : jobStateEvents.has(event)
        ? `codex-job:${jobId}:${status}:${timestamp}`
        : `codex-${event}:${jobId}:${timestamp}`;
    const summaries = {
      handoff_created: 'Codex handoff created; implementation is not proven.',
      job_started: 'Codex job started.',
      status_updated: `Codex job status updated to ${status || 'unknown'}.`,
      follow_up_sent: 'A follow-up was sent to the Codex job.',
      artifact_received: 'Codex job artifacts were received.',
      diff_received: 'A Codex job diff was received.',
      job_completed: 'Codex job completed.',
      job_failed: 'Codex job failed.',
      job_cancelled: 'Codex job was cancelled.',
      job_paused: 'Codex job was paused.',
      job_resumed: 'Codex job was resumed.',
    };
    return this.store.append(key, {
      businessKey: key,
      projectRegistryId: project.id,
      projectId: project.projectId,
      source: 'codex',
      type,
      event,
      summary: summaries[event],
      timestamp,
      observedAt: new Date().toISOString(),
      actor: safeString(raw.provider, 100) || 'codex-provider',
      externalId,
      operationId,
      codexJobId: jobId,
      metadata: { ...safeObject(raw.metadata), status, stepId, provider: safeString(raw.provider, 100) },
      confidence: 1,
      provenance: { method: 'codex_provider_lifecycle_callback', externalId },
    }, {
      assignedSource: 'codex', trusted: true, actor: safeString(raw.provider, 100) || 'codex-provider',
      provenanceMethod: 'codex_provider_lifecycle_callback',
    });
  }

  async ingestBrowserResult(businessKey, input = {}) {
    const key = safeBusinessKey(businessKey);
    const raw = safeObject(input);
    const project = await this.assertProject(key, safeString(raw.projectRegistryId, 160));
    const actor = safeString(raw.actor, 200);
    if (!actor) throw Object.assign(new Error('actor is required for browser evidence.'), { code: 'EVIDENCE_ACTOR_REQUIRED' });
    const url = safeHttpUrl(raw.url);
    if (!url) throw Object.assign(new Error('A valid verification URL is required.'), { code: 'BROWSER_URL_REQUIRED' });
    const status = safeString(raw.status, 80).toLowerCase();
    const failed = status === 'failed' || (Array.isArray(raw.consoleErrors) && raw.consoleErrors.length > 0)
      || (Array.isArray(raw.networkErrors) && raw.networkErrors.length > 0) || raw.interactionPassed === false;
    const observedAt = new Date().toISOString();
    const record = normalizeEvidence({
      businessKey: key,
      projectRegistryId: project.id,
      projectId: project.projectId,
      source: 'browser',
      type: failed ? 'browser_failed' : 'browser_verified',
      event: failed ? 'external_browser_verification_failed' : 'external_browser_verification_passed',
      summary: failed ? `External browser verification found problems at ${url}.` : `External browser verification passed at ${url}.`,
      timestamp: safeIso(raw.timestamp) || observedAt,
      observedAt,
      actor,
      externalId: safeString(raw.externalId, 500) || `external-browser:${project.id}:${url}:${safeIso(raw.timestamp) || observedAt}`,
      metadata: {
        mode: 'external_manual', url, viewports: raw.viewports, mobile: raw.mobile === true,
        screenshots: raw.screenshots, consoleErrors: raw.consoleErrors, networkErrors: raw.networkErrors,
        accessibilityResults: raw.accessibilityResults, interactionResults: raw.interactionResults,
        visualRegressionResults: raw.visualRegressionResults, status: failed ? 'failed' : 'passed',
      },
      confidence: 0.7,
      provenance: { method: 'authenticated_external_browser_result', external: true },
    }, { businessKey: key, assignedSource: 'browser', trusted: false, actor, provenanceMethod: 'authenticated_external_browser_result' });
    return this.store.append(key, record, { assignedSource: 'browser', trusted: false, actor, provenanceMethod: 'authenticated_external_browser_result' });
  }

  async collectOperations(businessKey, projects, operations) {
    const projectById = new Map(projects.map((project) => [project.id, project]));
    const evidence = [];
    const add = (operation, project, input) => evidence.push({
      businessKey,
      projectRegistryId: project.id,
      projectId: project.projectId,
      source: input.source || 'operations',
      actor: input.actor || 'operations-engine',
      repository: project.repo?.fullName || '',
      operationId: operation.id,
      observedAt: new Date().toISOString(),
      confidence: input.confidence ?? 1,
      provenance: { method: 'durable_operation_reconciliation', externalId: input.externalId },
      ...input,
    });
    for (const operation of operations) {
      const project = projectById.get(operation.projectRegistryId);
      if (!project) continue;
      for (const event of operation.activityLog || []) {
        const operationType = operationEventType(event.type);
        if (operationType) add(operation, project, {
          type: operationType, event: event.type, timestamp: event.timestamp || operation.updatedAt,
          actor: event.actor || 'operations-engine', summary: event.message || `${operation.title}: ${event.type}`,
          externalId: `operation-event:${operation.id}:${event.id || `${event.type}:${event.timestamp}`}`,
          metadata: { status: operation.status, stepId: event.stepId, eventData: event.data },
        });
        if (event.type === 'external_codex_handoff_ready') add(operation, project, {
          source: 'codex', type: 'codex_handoff_created', event: 'handoff_created', timestamp: event.timestamp,
          actor: event.actor || 'codex-provider', summary: event.message || 'Codex handoff created; implementation is not proven.',
          externalId: `codex-handoff:${operation.id}:${event.stepId || event.id}`, metadata: { status: 'handoff_created', implementationProven: false, stepId: event.stepId }, confidence: 1,
        });
        if (event.type === 'external_codex_job_registered') {
          const job = safeObject(operation.metadata?.codexJobs)[event.stepId] || {};
          add(operation, project, {
            source: 'codex', type: 'codex_job_updated', event: 'job_registered', timestamp: event.timestamp,
            actor: event.actor || 'mark', summary: event.message || 'External Codex job registered.',
            externalId: `codex-registered:${operation.id}:${event.id || event.timestamp}`, codexJobId: job.jobId || job.recordId,
            branch: job.branch, metadata: { status: job.status, hasCommit: event.data?.hasCommit === true, hasDiff: event.data?.hasDiff === true, stepId: event.stepId },
          });
        }
        if (event.type === 'provider_action_issued') {
          const step = (operation.steps || []).find((item) => item.id === event.stepId);
          if (step?.type === 'codex') add(operation, project, {
            source: 'codex', type: 'codex_job_started', event: 'job_started', timestamp: event.timestamp,
            actor: event.actor || 'operations-runner', summary: event.message || 'Codex job started.',
            externalId: `codex-started:${operation.id}:${event.id || event.timestamp}`, codexJobId: safeObject(operation.metadata?.codexJobs)[event.stepId]?.jobId,
            metadata: { status: 'started', stepId: event.stepId },
          });
        }
      }
      for (const [stepId, jobValue] of Object.entries(safeObject(operation.metadata?.codexJobs))) {
        const job = safeObject(jobValue);
        if (!job.recordId && !job.jobId) continue;
        const status = safeString(job.status, 80).toLowerCase();
        add(operation, project, {
          source: 'codex', type: status === 'completed' ? 'codex_job_completed' : (['failed', 'cancelled'].includes(status) ? 'codex_job_updated' : 'codex_job_started'),
          event: status === 'completed' ? 'job_completed' : (status === 'running' ? 'job_running' : 'job_updated'),
          timestamp: job.completedAt || job.updatedAt || job.startedAt || operation.updatedAt,
          actor: job.provider || 'codex-provider', summary: `Codex job ${job.jobId || job.recordId} is ${status || 'registered'}.`,
          externalId: `codex-job:${job.recordId || job.jobId}:${status}:${job.updatedAt || job.completedAt || job.startedAt}`,
          codexJobId: job.jobId || job.recordId, branch: job.branch, metadata: { status, stepId, hasDiff: Boolean(job.diffSummary), provider: job.provider },
        });
      }
      for (const artifact of operation.artifacts || []) {
        if (!['codex_diff', 'codex_result', 'branch', 'commit'].includes(artifact.type)) continue;
        const job = safeObject(operation.metadata?.codexJobs)[artifact.stepId] || {};
        add(operation, project, {
          source: 'codex', type: 'codex_job_updated', event: artifact.type === 'codex_diff' ? 'diff_received' : artifact.type === 'codex_result' ? 'artifact_received' : `${artifact.type}_supplied`,
          timestamp: artifact.createdAt || operation.updatedAt, actor: artifact.createdBy || 'codex-provider', summary: `${artifact.name || artifact.type} received for Codex work.`,
          externalId: `codex-artifact:${artifact.id}`, codexJobId: job.jobId || job.recordId,
          branch: artifact.type === 'branch' ? artifact.content : job.branch,
          commitSha: artifact.type === 'commit' ? artifact.content : '', metadata: { artifactType: artifact.type, hasDiff: artifact.type === 'codex_diff' },
        });
      }
      if (operation.status === 'completed') {
        for (const jobValue of Object.values(safeObject(operation.metadata?.codexJobs))) {
          const job = safeObject(jobValue);
          if (job.status !== 'completed') continue;
          add(operation, project, {
            source: 'codex', type: 'codex_job_updated', event: 'result_verified', timestamp: operation.completedAt || operation.updatedAt,
            actor: 'operations-verification', summary: `Codex result was verified by completed durable operation ${operation.id}.`,
            externalId: `codex-verified:${operation.id}:${job.recordId || job.jobId}`, codexJobId: job.jobId || job.recordId,
            metadata: { status: 'verified', operationStatus: operation.status },
          });
        }
      }
      for (const verification of operation.verification || []) {
        if (!['passed', 'failed'].includes(verification.status) || !/browser|visual|accessibility|interaction/i.test(verification.type)) continue;
        add(operation, project, {
          source: 'browser', type: verification.status === 'passed' ? 'browser_verified' : 'browser_failed',
          event: `operation_${verification.type}_${verification.status}`, timestamp: verification.completedAt || operation.updatedAt,
          actor: verification.evidence?.source || 'operation-verification', summary: `${verification.type} verification ${verification.status}.`,
          externalId: `operation-verification:${operation.id}:${verification.id}:${verification.status}`,
          confidence: verification.evidence?.source === 'authenticated_operator_manual' ? 0.7 : 0.95,
          metadata: { status: verification.status, verificationType: verification.type, mode: verification.evidence?.source === 'authenticated_operator_manual' ? 'external_manual' : 'direct' },
        });
      }
    }
    const externalBrowser = evidence.filter((item) => item.source === 'browser' && item.metadata?.mode === 'external_manual');
    const trusted = evidence.filter((item) => !externalBrowser.includes(item));
    const [trustedResult, externalResult] = await Promise.all([
      this.store.append(businessKey, trusted, { trusted: true, provenanceMethod: 'durable_operation_reconciliation' }),
      this.store.append(businessKey, externalBrowser, { assignedSource: 'browser', trusted: false, provenanceMethod: 'authenticated_manual_operation_evidence' }),
    ]);
    return {
      accepted: [...trustedResult.accepted, ...externalResult.accepted],
      duplicateCount: trustedResult.duplicateCount + externalResult.duplicateCount,
    };
  }

  async collectAirtable(businessKey, projects, legacyStore) {
    const evidence = [];
    const observedAt = new Date().toISOString();
    for (const legacyProject of Array.isArray(legacyStore?.projects) ? legacyStore.projects : []) {
      if (!legacyProject?.airtableUrl && legacyProject?.airtableSource !== 'revision-requests' && !safeString(legacyProject?.id, 200).startsWith('airtable:')) continue;
      const project = projectForLegacyItem(projects, legacyProject);
      if (!project) continue;
      evidence.push({
        businessKey, projectRegistryId: project.id, projectId: project.projectId, source: 'airtable', type: 'task_updated', event: 'project_status_observed',
        summary: `Airtable-derived project status is ${legacyProject.status || 'unknown'}.`, timestamp: safeIso(legacyProject.updatedAt) || observedAt, observedAt,
        actor: 'airtable-sync', externalId: `airtable-project:${legacyProject.id || project.id}:${legacyProject.updatedAt || observedAt}`,
        metadata: { projectStatus: legacyProject.status, airtableSource: legacyProject.airtableSource || 'linked_project' }, confidence: 0.8,
        provenance: { method: 'airtable_materialized_store' },
      });
    }
    for (const task of Array.isArray(legacyStore?.tasks) ? legacyStore.tasks : []) {
      if (!safeString(task?.id, 300).startsWith('airtable:') && task?.airtableSource !== 'revision-requests' && !task?.airtableRecordId) continue;
      const project = projectForLegacyItem(projects, task);
      if (!project) continue;
      evidence.push({
        businessKey, projectRegistryId: project.id, projectId: project.projectId, source: 'airtable', type: 'task_updated', event: 'task_updated',
        summary: `Airtable task updated: ${safeString(task.title, 700) || 'untitled task'}.`, timestamp: safeIso(task.updatedAt) || observedAt, observedAt,
        actor: 'airtable-sync', externalId: `airtable-task:${task.id || task.airtableRecordId}:${task.updatedAt || observedAt}`,
        metadata: { taskStatus: task.status, priority: task.priority, dueDate: task.dueDate, projectStatus: task.projectStatus }, confidence: 0.8,
        provenance: { method: 'airtable_materialized_store' },
      });
    }
    return this.store.append(businessKey, evidence, { assignedSource: 'airtable', trusted: true, provenanceMethod: 'airtable_materialized_store' });
  }

  deploymentMappings(projects, field) {
    const values = new Map();
    for (const project of projects) {
      const value = safeString(project?.deployments?.[field], 300);
      if (!value) continue;
      const matches = values.get(value) || [];
      matches.push(project);
      values.set(value, matches);
    }
    return values;
  }

  async collectDeployments(businessKey, projects) {
    const evidence = [];
    const results = [];
    const observedAt = new Date().toISOString();
    const renderMappings = this.deploymentMappings(projects, 'renderServiceId');
    for (const [serviceId, matches] of renderMappings) {
      if (matches.length !== 1) { results.push({ provider: 'render', externalId: serviceId, skipped: 'low_confidence_mapping' }); continue; }
      if (typeof this.renderApi !== 'function') { results.push({ provider: 'render', externalId: serviceId, skipped: 'not_configured' }); continue; }
      try {
        const data = await this.renderApi(`/services/${encodeURIComponent(serviceId)}/deploys?limit=20`);
        const rows = Array.isArray(data) ? data : Array.isArray(data?.deploys) ? data.deploys : [];
        const project = matches[0];
        for (const row of rows) {
          const deploy = row.deploy || row;
          const status = deploy.status || deploy.state;
          const type = deploymentStatusType(status, 'production');
          const timestamp = safeIso(deploy.finishedAt || deploy.finished_at || deploy.updatedAt || deploy.updated_at || deploy.createdAt || deploy.created_at) || observedAt;
          evidence.push({
            businessKey, projectRegistryId: project.id, projectId: project.projectId, source: 'render', type,
            event: `render_${safeString(status, 100).toLowerCase() || 'deployment_observed'}`, summary: `Render deployment ${deploy.id || ''} is ${status || 'observed'}.`,
            timestamp, observedAt, actor: 'render', repository: project.repo?.fullName, branch: deploy.branch, commitSha: deploy.commit?.id || deploy.commit?.sha || deploy.commitSha,
            externalId: `render-deploy:${serviceId}:${deploy.id}:${status}:${timestamp}`, deployment: { id: deploy.id, provider: 'render', environment: 'production', status, url: project.deployments?.productionUrl, branch: deploy.branch, commitSha: deploy.commit?.id || deploy.commit?.sha },
            metadata: { serviceId }, confidence: 1, provenance: { method: 'render_api_exact_registry_mapping' },
          });
        }
      } catch (error) { results.push({ provider: 'render', externalId: serviceId, error: safeString(error?.message, 1_000) }); }
    }
    const cloudflareMappings = this.deploymentMappings(projects, 'cloudflareProject');
    for (const [projectName, matches] of cloudflareMappings) {
      if (matches.length !== 1) { results.push({ provider: 'cloudflare', externalId: projectName, skipped: 'low_confidence_mapping' }); continue; }
      if (typeof this.cloudflareApi !== 'function') { results.push({ provider: 'cloudflare', externalId: projectName, skipped: 'not_configured' }); continue; }
      const accountId = safeString(matches[0]?.deployments?.cloudflareAccountId || process.env.CLOUDFLARE_ACCOUNT_ID, 300);
      if (!accountId) { results.push({ provider: 'cloudflare', externalId: projectName, skipped: 'account_not_configured' }); continue; }
      try {
        const data = await this.cloudflareApi(`/accounts/${encodeURIComponent(accountId)}/pages/projects/${encodeURIComponent(projectName)}/deployments?per_page=20`);
        const rows = Array.isArray(data?.result) ? data.result : [];
        const project = matches[0];
        for (const deploy of rows) {
          const environment = deploy.environment === 'production' ? 'production' : 'preview';
          const status = deploy.latest_stage?.status || deploy.stages?.slice(-1)?.[0]?.status || 'success';
          const type = deploymentStatusType(status, environment);
          const timestamp = safeIso(deploy.modified_on || deploy.created_on) || observedAt;
          evidence.push({
            businessKey, projectRegistryId: project.id, projectId: project.projectId, source: 'cloudflare', type,
            event: `cloudflare_pages_${safeString(status, 100).toLowerCase()}`, summary: `Cloudflare Pages ${environment} deployment ${deploy.id || ''} is ${status}.`,
            timestamp, observedAt, actor: 'cloudflare', repository: project.repo?.fullName, branch: deploy.deployment_trigger?.metadata?.branch,
            commitSha: deploy.deployment_trigger?.metadata?.commit_hash, externalId: `cloudflare-deploy:${projectName}:${deploy.id}:${status}:${timestamp}`,
            deployment: { id: deploy.id, provider: 'cloudflare', environment, status, url: deploy.url, branch: deploy.deployment_trigger?.metadata?.branch, commitSha: deploy.deployment_trigger?.metadata?.commit_hash },
            metadata: { projectName }, confidence: 1, provenance: { method: 'cloudflare_api_exact_registry_mapping' },
          });
        }
      } catch (error) { results.push({ provider: 'cloudflare', externalId: projectName, error: safeString(error?.message, 1_000) }); }
    }
    const appended = await this.store.append(businessKey, evidence, { trusted: true, provenanceMethod: 'deployment_api_exact_registry_mapping' });
    return { accepted: appended.accepted.length, duplicates: appended.duplicateCount, results };
  }

  async recordDesktopContext(businessKey, { agentId = '', context = {}, nowMs = Date.now() } = {}) {
    const key = safeBusinessKey(businessKey);
    const workspace = safeObject(context.workspace);
    const workspacePath = projectPathKey(workspace.workspacePath);
    if (!workspacePath || Number(context.idleSeconds) > 300) return { recorded: false, reason: 'inactive_or_unmapped' };
    const projects = await this.projects(key);
    const matches = projects.filter((project) => projectPathKey(project.localWorkspace?.canonicalPath || project.localWorkspace?.path) === workspacePath
      && project.localWorkspace?.trustStatus === 'approved'
      && (!agentId || project.localWorkspace?.desktopAgentId === agentId));
    if (matches.length !== 1) return { recorded: false, reason: matches.length ? 'low_confidence_mapping' : 'unmapped_workspace' };
    const project = matches[0];
    const now = new Date(nowMs).toISOString();
    const recent = await this.store.list(key, { projectRegistryId: project.id, source: 'desktop', type: 'workspace_opened,workspace_active', limit: 10 });
    const latest = recent.find((item) => projectPathKey(item.workspace?.path) === workspacePath && item.actor === (agentId || 'desktop-agent'));
    const latestEnd = Date.parse(latest?.workspace?.sessionEnd || latest?.observedAt);
    if (latest && Number.isFinite(latestEnd) && nowMs - latestEnd <= 20 * 60_000) {
      const sessionStart = Date.parse(latest.workspace?.sessionStart || latest.timestamp);
      const activeMinutes = Number.isFinite(sessionStart) ? Math.max(Number(latest.workspace?.activeMinutes) || 0, (nowMs - sessionStart) / 60_000) : Number(latest.workspace?.activeMinutes) || 0;
      const reconciled = await this.store.reconcile(key, latest.id, (item) => ({
        ...item,
        observedAt: now,
        summary: `${project.canonicalName} workspace active for ${Math.max(1, Math.round(activeMinutes))} minute(s).`,
        branch: safeString(workspace.gitBranch, 500),
        workspace: {
          ...item.workspace, sessionEnd: now, activeMinutes,
          filesObserved: [...new Set([...(item.workspace?.filesObserved || []), ...observedFileNames(workspace)])].slice(0, 100),
        },
        metadata: { ...safeObject(item.metadata), processName: safeString(context.processName, 100), changedFileCount: Array.isArray(workspace.gitStatus) ? workspace.gitStatus.length : 0 },
      }));
      return { recorded: true, reconciled: true, evidence: reconciled };
    }
    const appended = await this.store.append(key, {
      businessKey: key, projectRegistryId: project.id, projectId: project.projectId, source: 'desktop', type: 'workspace_opened', event: 'workspace_session_started',
      summary: `${project.canonicalName} workspace opened in ${safeString(context.processName, 100) || 'the desktop editor'}.`, timestamp: now, observedAt: now,
      actor: agentId || 'desktop-agent', repository: project.repo?.fullName, branch: workspace.gitBranch,
      externalId: `desktop-session:${project.id}:${agentId || 'desktop'}:${Math.floor(nowMs / (20 * 60_000))}`,
      workspace: { path: workspace.workspacePath, sessionStart: now, sessionEnd: now, activeMinutes: 0.1, filesObserved: observedFileNames(workspace), commandsRun: [] },
      metadata: { processName: context.processName, changedFileCount: Array.isArray(workspace.gitStatus) ? workspace.gitStatus.length : 0 },
      confidence: 1, provenance: { method: 'trusted_desktop_agent_exact_workspace_mapping' },
    }, { assignedSource: 'desktop', trusted: true, actor: agentId || 'desktop-agent', provenanceMethod: 'trusted_desktop_agent_exact_workspace_mapping' });
    return { recorded: appended.accepted.length > 0, evidence: appended.accepted[0] || null };
  }

  async recordDesktopActionResult(businessKey, result = {}) {
    const key = safeBusinessKey(businessKey);
    const project = await this.assertProject(key, safeString(result.projectRegistryId, 160));
    const agentId = safeString(result.desktopAgentId, 200);
    if (project.localWorkspace?.trustStatus !== 'approved' || project.localWorkspace?.desktopAgentId !== agentId) {
      return { recorded: false, reason: 'untrusted_desktop_mapping' };
    }
    const actionType = safeString(result.type, 100);
    const scriptName = safeString(result.details?.scriptName, 100).toLowerCase();
    const type = actionType === 'run-project-script'
      ? ({ test: 'test_run', lint: 'lint_run', typecheck: 'typecheck_run', build: 'build_run' }[scriptName] || 'build_run')
      : actionType === 'open-vscode' ? 'workspace_opened' : 'repository_read';
    const event = result.ok === true ? `${actionType}_completed` : `${actionType}_failed`;
    const timestamp = safeIso(result.completedAt) || new Date().toISOString();
    return this.store.append(key, {
      businessKey: key, projectRegistryId: project.id, projectId: project.projectId, source: 'desktop', type, event,
      summary: result.ok === true ? `Desktop action ${actionType}${scriptName ? ` (${scriptName})` : ''} completed.` : `Desktop action ${actionType}${scriptName ? ` (${scriptName})` : ''} failed.`,
      timestamp, observedAt: timestamp, actor: agentId, repository: project.repo?.fullName, operationId: result.operationId,
      externalId: `desktop-action:${result.id}:${result.attemptNumber ?? 0}`, workspace: { path: project.localWorkspace?.canonicalPath || project.localWorkspace?.path, commandsRun: scriptName ? [`npm run ${scriptName}`] : [actionType] },
      metadata: { status: result.ok === true ? 'passed' : 'failed', actionType, scriptName, error: result.error, operationId: result.operationId, stepId: result.stepId },
      confidence: 1, provenance: { method: 'trusted_desktop_action_result', idempotencyKey: result.idempotencyKey },
    }, { assignedSource: 'desktop', trusted: true, actor: agentId, provenanceMethod: 'trusted_desktop_action_result' });
  }

  async recalculate(businessKey, { nowMs = Date.now() } = {}) {
    const key = safeBusinessKey(businessKey);
    const [projects, operations, document, settings] = await Promise.all([
      this.projects(key), this.listOperations(key, { limit: 5_000 }), this.store.readDocument(key), this.getSettings(),
    ]);
    const config = safeObject(settings?.projectEvidence);
    const configuredWeights = safeObject(config.weights);
    const weights = Object.fromEntries(Object.entries(DEFAULT_SIGNAL_WEIGHTS).map(([signal, defaults]) => [
      signal,
      { ...defaults, ...safeObject(configuredWeights[signal]) },
    ]));
    const rules = { ...DEFAULT_ACTIVITY_RULES, ...safeObject(config.rules) };
    const previousAnalysis = safeObject(document.analysis);
    const analysis = calculateBusinessActivity({
      businessKey: key, projects, evidence: document.evidence, operations, previousFocus: previousAnalysis.currentFocus, nowMs, weights, rules,
    });
    const focusHistory = Array.isArray(previousAnalysis.focusHistory) ? previousAnalysis.focusHistory.slice(-99) : [];
    if (analysis.currentFocus.focusShiftDetectedAt && analysis.currentFocus.focusShiftDetectedAt !== previousAnalysis.currentFocus?.focusShiftDetectedAt) {
      focusHistory.push({
        detectedAt: analysis.currentFocus.focusShiftDetectedAt,
        from: analysis.currentFocus.previousFocusProject,
        to: analysis.currentFocus.currentFocusProject,
        reason: analysis.currentFocus.reason,
      });
    }
    const saved = await this.store.setAnalysis(key, {
      ...analysis,
      evidenceWatermark: evidenceWatermark(document.evidence),
      operationsWatermark: operationsWatermark(operations),
      focusHistory,
    });
    if (settings?.airtableDerivedStatusSync === true && typeof this.syncAirtableDerivedState === 'function') {
      await this.syncAirtableDerivedState(key, saved.snapshots).catch(() => {});
    }
    return saved;
  }

  async refresh(businessKey, { force = false, sources = null, nowMs = Date.now() } = {}) {
    const key = safeBusinessKey(businessKey);
    const previous = this.refreshes.get(key);
    if (previous) return previous;
    const run = (async () => {
      const selected = new Set((Array.isArray(sources) ? sources : ['operations', 'airtable', 'github', 'render', 'cloudflare']).map((item) => safeString(item, 80).toLowerCase()));
      const [projects, operations, legacyStore] = await Promise.all([this.projects(key), this.listOperations(key, { limit: 5_000 }), this.getLegacyStore(key)]);
      const result = {};
      if (selected.has('operations') || selected.has('codex') || selected.has('browser')) result.operations = await this.collectOperations(key, projects, operations);
      if (selected.has('airtable')) result.airtable = await this.collectAirtable(key, projects, legacyStore);
      if (selected.has('github')) result.github = await this.github.collect({ businessKey: key, projects, force, nowMs });
      if (selected.has('render') || selected.has('cloudflare')) result.deployments = await this.collectDeployments(key, projects);
      result.activity = await this.recalculate(key, { nowMs });
      return result;
    })();
    this.refreshes.set(key, run);
    try { return await run; } finally { if (this.refreshes.get(key) === run) this.refreshes.delete(key); }
  }

  async getActivity(businessKey, { recalculate = false } = {}) {
    const key = safeBusinessKey(businessKey);
    const [document, operations] = await Promise.all([this.store.readDocument(key), this.listOperations(key, { limit: 5_000 })]);
    const analysis = safeObject(document.analysis);
    if (analysis.operationsWatermark !== operationsWatermark(operations)) {
      await this.collectOperations(key, await this.projects(key), operations);
      return this.recalculate(key);
    }
    if (recalculate || !analysis.calculatedAt || !Array.isArray(analysis.snapshots)
      || analysis.evidenceWatermark !== evidenceWatermark(document.evidence)) return this.recalculate(key);
    return structuredClone(analysis);
  }

  async getProjectActivity(businessKey, projectRegistryId, options = {}) {
    await this.assertProject(businessKey, projectRegistryId);
    const analysis = await this.getActivity(businessKey, options);
    return (analysis.snapshots || []).find((item) => item.projectRegistryId === projectRegistryId) || null;
  }

  async listEvidence(businessKey, filters = {}) { return this.store.list(businessKey, filters); }
  async getProjectEvidence(businessKey, projectRegistryId, filters = {}) {
    await this.assertProject(businessKey, projectRegistryId);
    return this.store.list(businessKey, { ...filters, projectRegistryId });
  }
}

export { TERMINAL_OPERATIONS };
