import crypto from 'node:crypto';

import { redactSecrets, safeObject, safeString } from '../operations/operation_types.js';

function parseRepoFullName(value, fallback = '') {
  const raw = safeString(value || fallback, 300).trim();
  if (/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(raw)) return raw;
  const match = raw.match(/^https?:\/\/github\.com\/([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+?)(?:\.git)?\/?$/i);
  return match ? `${match[1]}/${match[2]}` : '';
}

function normalizeTimeout(value, fallback = 30_000) {
  const n = Number(value);
  return Number.isFinite(n) && n >= 1_000 && n <= 300_000 ? n : fallback;
}

function githubHeaders(token) {
  return {
    Accept: 'application/vnd.github+json',
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
    'User-Agent': 'marcus-codex-adapter',
    'X-GitHub-Api-Version': '2022-11-28',
  };
}

async function fetchGitHubJson(url, { token, method = 'GET', body, timeoutMs = 30_000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);
  try {
    const resp = await fetch(url, {
      method,
      signal: controller.signal,
      headers: githubHeaders(token),
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (resp.status === 204) return { ok: true };
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      const error = new Error(data?.message || `GitHub API failed (${resp.status}).`);
      error.status = resp.status;
      error.code = `GITHUB_API_${resp.status}`;
      throw error;
    }
    return data;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeGitHubError(scope, error) {
  return {
    scope,
    status: Number(error?.status) || 0,
    message: redactSecrets(safeString(error?.message, 1_000), 1_000) || 'GitHub evidence request failed.',
  };
}

function normalizeChangedFile(file = {}) {
  const raw = safeObject(file);
  const rawPatch = safeString(raw.patch, 20_000);
  return {
    path: safeString(raw.filename, 1_000),
    previousPath: safeString(raw.previous_filename, 1_000),
    status: safeString(raw.status, 80),
    additions: Math.max(0, Number(raw.additions) || 0),
    deletions: Math.max(0, Number(raw.deletions) || 0),
    changes: Math.max(0, Number(raw.changes) || 0),
    blobUrl: safeString(raw.blob_url, 2_000),
    patch: redactSecrets(rawPatch, 8_000),
    patchAvailable: Boolean(rawPatch),
    patchTruncated: rawPatch.length > 8_000,
  };
}

function normalizeCheckRun(run = {}) {
  const raw = safeObject(run);
  return {
    id: Number(raw.id) || 0,
    name: safeString(raw.name, 300),
    status: safeString(raw.status, 80),
    conclusion: safeString(raw.conclusion, 80),
    url: safeString(raw.html_url, 2_000),
    app: safeString(raw.app?.slug || raw.app?.name, 200),
    startedAt: safeString(raw.started_at, 80),
    completedAt: safeString(raw.completed_at, 80),
  };
}

function normalizeCommitStatus(status = {}) {
  const raw = safeObject(status);
  return {
    id: Number(raw.id) || 0,
    context: safeString(raw.context, 300),
    state: safeString(raw.state, 80),
    description: redactSecrets(safeString(raw.description, 500), 500),
    url: safeString(raw.target_url, 2_000),
    createdAt: safeString(raw.created_at, 80),
    updatedAt: safeString(raw.updated_at, 80),
  };
}

function summarizeEvidence(evidence) {
  const pr = safeObject(evidence.pullRequest);
  const files = Array.isArray(evidence.files) ? evidence.files : [];
  const checks = safeObject(evidence.checks);
  const checkRuns = Array.isArray(checks.checkRuns) ? checks.checkRuns : [];
  const statuses = Array.isArray(checks.statuses) ? checks.statuses : [];
  const checkFailures = checkRuns.filter((item) => item.status === 'completed' && !['success', 'neutral', 'skipped'].includes(item.conclusion));
  const checkPending = checkRuns.filter((item) => item.status !== 'completed');
  const statusFailures = statuses.filter((item) => !['success', 'pending'].includes(item.state));
  return [
    `${evidence.repository}@${evidence.headSha ? evidence.headSha.slice(0, 12) : evidence.branch}`,
    pr.number ? `PR #${pr.number} (${pr.state}${pr.draft ? ', draft' : ''})` : 'no pull request found',
    `${files.length} changed file(s)`,
    `${Number(evidence.totals?.additions || 0)} addition(s)`,
    `${Number(evidence.totals?.deletions || 0)} deletion(s)`,
    `${checkRuns.length + statuses.length} target check/status result(s)`,
    checkPending.length ? `${checkPending.length} pending check(s)` : '',
    checkFailures.length + statusFailures.length ? `${checkFailures.length + statusFailures.length} failed check/status result(s)` : '',
  ].filter(Boolean).join('; ');
}

function makeDispatchJobId({ operationId, stepId, idempotencyKey }) {
  const seed = [operationId, stepId, idempotencyKey].map((item) => safeString(item, 300)).join(':');
  return `ghdispatch_${crypto.createHash('sha256').update(seed || crypto.randomUUID()).digest('hex').slice(0, 24)}`;
}

function normalizeWorkflowRun(run = {}, fallback = {}) {
  const raw = safeObject(run);
  const conclusion = safeString(raw.conclusion, 80).toLowerCase();
  const status = safeString(raw.status, 80).toLowerCase();
  const mappedStatus = conclusion === 'success' ? 'completed'
    : ['failure', 'timed_out', 'startup_failure'].includes(conclusion) ? 'failed'
      : conclusion === 'cancelled' ? 'cancelled'
        : status === 'completed' ? 'unknown'
          : status === 'in_progress' ? 'running'
            : status === 'queued' || status === 'requested' || status === 'waiting' ? 'queued'
              : fallback.status || 'queued';
  return {
    provider: 'github_actions_codex',
    jobId: safeString(fallback.jobId, 300),
    status: mappedStatus,
    branch: safeString(fallback.branch, 500),
    rawMetadata: {
      ...(safeObject(fallback.rawMetadata)),
      workflowRunId: raw.id || '',
      workflowRunUrl: raw.html_url || '',
      workflowStatus: raw.status || '',
      workflowConclusion: raw.conclusion || '',
      workflowName: raw.name || '',
      displayTitle: raw.display_title || '',
      runNumber: raw.run_number || '',
    },
    error: mappedStatus === 'failed' ? safeString(raw.conclusion || 'GitHub Actions workflow failed.', 8_000) : '',
  };
}

export class GitHubActionsCodexAdapter {
  constructor({
    token,
    runnerRepo = 'markgromer/Reggie',
    eventType = 'marcus_codex_job',
    workflowFile = 'marcus-codex-runner.yml',
    timeoutMs = 30_000,
  } = {}) {
    const cleanToken = safeString(token, 2_000).trim();
    if (!cleanToken) throw new Error('GitHubActionsCodexAdapter requires token.');
    const repo = parseRepoFullName(runnerRepo);
    if (!repo) throw new Error('GitHubActionsCodexAdapter requires runnerRepo in owner/name form.');
    this.providerName = 'github_actions_codex';
    this.token = cleanToken;
    this.runnerRepo = repo;
    this.eventType = safeString(eventType, 100).trim() || 'marcus_codex_job';
    this.workflowFile = safeString(workflowFile, 200).trim() || 'marcus-codex-runner.yml';
    this.timeoutMs = normalizeTimeout(timeoutMs);
    this.evidenceCache = new Map();
  }

  async startJob(job, { idempotencyKey = '' } = {}) {
    const jobId = makeDispatchJobId({ operationId: job.operationId, stepId: job.stepId, idempotencyKey });
    await fetchGitHubJson(`https://api.github.com/repos/${this.runnerRepo}/dispatches`, {
      token: this.token,
      method: 'POST',
      timeoutMs: this.timeoutMs,
      body: {
        event_type: this.eventType,
        client_payload: {
          jobId,
          operationId: job.operationId,
          stepId: job.stepId,
          businessKey: job.businessKey,
          projectRegistryId: job.projectRegistryId,
          repository: job.repository,
          branch: job.branch,
          prompt: job.prompt,
          idempotencyKey,
          requestedAt: new Date().toISOString(),
        },
      },
    });
    return {
      provider: 'github_actions_codex',
      jobId,
      status: 'queued',
      branch: job.branch,
      rawMetadata: {
        runnerRepo: this.runnerRepo,
        eventType: this.eventType,
        workflowFile: this.workflowFile,
        repository: job.repository,
      },
    };
  }

  async getJobStatus(job) {
    const metadata = safeObject(job?.rawMetadata);
    const existingRunId = safeString(metadata.workflowRunId, 80);
    if (existingRunId) {
      const run = await fetchGitHubJson(`https://api.github.com/repos/${this.runnerRepo}/actions/runs/${encodeURIComponent(existingRunId)}`, {
        token: this.token,
        timeoutMs: this.timeoutMs,
      });
      return normalizeWorkflowRun(run, job);
    }
    const runs = await fetchGitHubJson(`https://api.github.com/repos/${this.runnerRepo}/actions/workflows/${encodeURIComponent(this.workflowFile)}/runs?event=repository_dispatch&per_page=30`, {
      token: this.token,
      timeoutMs: this.timeoutMs,
    });
    const jobId = safeString(job?.jobId, 300);
    const operationId = safeString(job?.operationId, 120);
    const match = (Array.isArray(runs?.workflow_runs) ? runs.workflow_runs : []).find((run) => {
      const haystack = `${run?.display_title || ''} ${run?.name || ''} ${run?.head_branch || ''}`.toLowerCase();
      return (jobId && haystack.includes(jobId.toLowerCase())) || (operationId && haystack.includes(operationId.toLowerCase()));
    });
    return match ? normalizeWorkflowRun(match, job) : { ...job, provider: 'github_actions_codex', status: 'queued' };
  }

  async sendFollowup(job, message) {
    return {
      ...job,
      provider: 'github_actions_codex',
      status: job?.status || 'queued',
      rawMetadata: {
        ...safeObject(job?.rawMetadata),
        followups: [...(Array.isArray(job?.rawMetadata?.followups) ? job.rawMetadata.followups : []), safeString(message, 8_000)].slice(-20),
      },
    };
  }

  async collectTargetEvidence(job) {
    const metadata = safeObject(job?.rawMetadata);
    const repository = parseRepoFullName(job?.repository, metadata.repository);
    const branch = safeString(job?.branch, 500).trim();
    if (!repository || !branch) {
      throw Object.assign(new Error('Completed Codex job is missing a valid target GitHub repository or branch.'), { code: 'CODEX_TARGET_EVIDENCE_UNRESOLVED' });
    }
    const cacheKey = `${safeString(job?.jobId, 300)}:${repository}:${branch}`;
    if (this.evidenceCache.has(cacheKey)) return this.evidenceCache.get(cacheKey);
    const collecting = this.fetchTargetEvidence({ repository, branch }).catch((error) => {
      this.evidenceCache.delete(cacheKey);
      throw error;
    });
    this.evidenceCache.set(cacheKey, collecting);
    const expire = () => {
      const timer = setTimeout(() => {
        if (this.evidenceCache.get(cacheKey) === collecting) this.evidenceCache.delete(cacheKey);
      }, 10_000);
      if (typeof timer.unref === 'function') timer.unref();
    };
    collecting.then(expire, expire);
    if (this.evidenceCache.size > 200) this.evidenceCache.delete(this.evidenceCache.keys().next().value);
    return collecting;
  }

  invalidateEvidence(job) {
    const metadata = safeObject(job?.rawMetadata);
    const repository = parseRepoFullName(job?.repository, metadata.repository);
    const branch = safeString(job?.branch, 500).trim();
    if (!repository || !branch) return false;
    return this.evidenceCache.delete(`${safeString(job?.jobId, 300)}:${repository}:${branch}`);
  }

  async fetchTargetEvidence({ repository, branch }) {
    const [owner] = repository.split('/');
    const apiBase = `https://api.github.com/repos/${repository}`;
    const errors = [];
    const [repoResult, branchResult, pullsResult] = await Promise.allSettled([
      fetchGitHubJson(apiBase, { token: this.token, timeoutMs: this.timeoutMs }),
      fetchGitHubJson(`${apiBase}/branches/${encodeURIComponent(branch)}`, { token: this.token, timeoutMs: this.timeoutMs }),
      fetchGitHubJson(`${apiBase}/pulls?state=all&head=${encodeURIComponent(`${owner}:${branch}`)}&sort=updated&direction=desc&per_page=10`, { token: this.token, timeoutMs: this.timeoutMs }),
    ]);
    if (repoResult.status === 'rejected') errors.push(normalizeGitHubError('repository', repoResult.reason));
    if (branchResult.status === 'rejected') errors.push(normalizeGitHubError('branch', branchResult.reason));
    if (pullsResult.status === 'rejected') errors.push(normalizeGitHubError('pull_requests', pullsResult.reason));
    const repositoryData = repoResult.status === 'fulfilled' ? safeObject(repoResult.value) : {};
    const branchData = branchResult.status === 'fulfilled' ? safeObject(branchResult.value) : {};
    const pulls = pullsResult.status === 'fulfilled' && Array.isArray(pullsResult.value) ? pullsResult.value : [];
    const pull = pulls.find((item) => safeString(item?.head?.ref, 500) === branch) || pulls[0] || null;
    const baseRef = safeString(pull?.base?.ref || repositoryData.default_branch, 500);
    const headSha = safeString(pull?.head?.sha || branchData.commit?.sha, 100);
    if (!headSha) {
      throw Object.assign(new Error(`GitHub did not return a target commit for ${repository}:${branch}.`), {
        code: 'CODEX_TARGET_COMMIT_UNRESOLVED',
        evidenceErrors: errors,
      });
    }

    const requests = [];
    const scopes = [];
    if (pull?.number) {
      scopes.push('pull_request');
      requests.push(fetchGitHubJson(`${apiBase}/pulls/${Number(pull.number)}`, { token: this.token, timeoutMs: this.timeoutMs }));
      scopes.push('pull_request_files');
      requests.push(fetchGitHubJson(`${apiBase}/pulls/${Number(pull.number)}/files?per_page=100`, { token: this.token, timeoutMs: this.timeoutMs }));
    }
    if (baseRef) {
      scopes.push('compare');
      requests.push(fetchGitHubJson(`${apiBase}/compare/${encodeURIComponent(baseRef)}...${encodeURIComponent(branch)}?per_page=100`, { token: this.token, timeoutMs: this.timeoutMs }));
    }
    scopes.push('check_runs');
    requests.push(fetchGitHubJson(`${apiBase}/commits/${encodeURIComponent(headSha)}/check-runs?filter=latest&per_page=100`, { token: this.token, timeoutMs: this.timeoutMs }));
    scopes.push('commit_status');
    requests.push(fetchGitHubJson(`${apiBase}/commits/${encodeURIComponent(headSha)}/status?per_page=100`, { token: this.token, timeoutMs: this.timeoutMs }));

    const settled = await Promise.allSettled(requests);
    const collected = {};
    settled.forEach((result, index) => {
      const scope = scopes[index];
      if (result.status === 'fulfilled') collected[scope] = result.value;
      else errors.push(normalizeGitHubError(scope, result.reason));
    });
    const pullDetails = safeObject(collected.pull_request || pull);
    const compare = safeObject(collected.compare);
    const rawFiles = Array.isArray(collected.pull_request_files) && collected.pull_request_files.length
      ? collected.pull_request_files
      : (Array.isArray(compare.files) ? compare.files : []);
    let remainingPatchChars = 24_000;
    const files = rawFiles.slice(0, 50).map((rawFile) => {
      const file = normalizeChangedFile(rawFile);
      if (file.patch.length > remainingPatchChars) {
        file.patch = file.patch.slice(0, Math.max(0, remainingPatchChars));
        file.patchTruncated = true;
      }
      remainingPatchChars = Math.max(0, remainingPatchChars - file.patch.length);
      return file;
    });
    const checkRuns = (Array.isArray(collected.check_runs?.check_runs) ? collected.check_runs.check_runs : []).slice(0, 50).map(normalizeCheckRun);
    const statuses = (Array.isArray(collected.commit_status?.statuses) ? collected.commit_status.statuses : []).slice(0, 50).map(normalizeCommitStatus);
    const additions = files.reduce((sum, item) => sum + item.additions, 0);
    const deletions = files.reduce((sum, item) => sum + item.deletions, 0);
    const digestInput = JSON.stringify({
      repository,
      baseRef,
      headSha,
      files: files.map((item) => ({ path: item.path, previousPath: item.previousPath, status: item.status, patch: item.patch })),
      checks: {
        combinedState: safeString(collected.commit_status?.state, 80),
        checkRuns,
        statuses,
      },
    });
    const evidence = {
      source: 'github_api',
      authoritative: true,
      collectedAt: new Date().toISOString(),
      repository,
      repositoryUrl: safeString(repositoryData.html_url, 2_000) || `https://github.com/${repository}`,
      defaultBranch: safeString(repositoryData.default_branch, 500),
      branch,
      baseRef,
      headSha,
      evidenceDigest: crypto.createHash('sha256').update(digestInput).digest('hex'),
      pullRequest: pullDetails.number ? {
        number: Number(pullDetails.number) || 0,
        title: redactSecrets(safeString(pullDetails.title, 1_000), 1_000),
        state: safeString(pullDetails.state, 80),
        draft: pullDetails.draft === true,
        merged: pullDetails.merged === true,
        url: safeString(pullDetails.html_url, 2_000),
        baseRef: safeString(pullDetails.base?.ref, 500),
        headRef: safeString(pullDetails.head?.ref, 500),
        headSha: safeString(pullDetails.head?.sha, 100) || headSha,
        additions: Math.max(0, Number(pullDetails.additions) || additions),
        deletions: Math.max(0, Number(pullDetails.deletions) || deletions),
        changedFiles: Math.max(0, Number(pullDetails.changed_files) || files.length),
      } : {},
      compare: {
        status: safeString(compare.status, 80),
        aheadBy: Math.max(0, Number(compare.ahead_by) || 0),
        behindBy: Math.max(0, Number(compare.behind_by) || 0),
        totalCommits: Math.max(0, Number(compare.total_commits) || 0),
        url: safeString(compare.html_url, 2_000),
      },
      totals: {
        files: files.length,
        reportedFiles: Math.max(files.length, Number(pullDetails.changed_files) || Number(compare.files?.length) || 0),
        additions,
        deletions,
        changes: additions + deletions,
      },
      files,
      checks: {
        combinedState: safeString(collected.commit_status?.state, 80),
        checkRuns,
        statuses,
      },
      errors: errors.slice(0, 20),
    };
    evidence.summary = summarizeEvidence(evidence);
    return evidence;
  }

  async getArtifacts(job) {
    const evidence = await this.collectTargetEvidence(job);
    const artifacts = [];
    if (evidence.pullRequest?.number) {
      artifacts.push({
        type: 'github_pull_request',
        name: `GitHub pull request #${evidence.pullRequest.number}`,
        mimeType: 'application/json',
        content: JSON.stringify(evidence.pullRequest),
        url: evidence.pullRequest.url,
        metadata: { source: 'github_api', authoritative: true, repository: evidence.repository, headSha: evidence.headSha, evidenceDigest: evidence.evidenceDigest },
      });
    }
    artifacts.push({
      type: 'commit',
      name: 'GitHub target commit',
      content: evidence.headSha,
      url: `${evidence.repositoryUrl}/commit/${evidence.headSha}`,
      metadata: { source: 'github_api', authoritative: true, repository: evidence.repository, branch: evidence.branch, evidenceDigest: evidence.evidenceDigest },
    });
    artifacts.push({
      type: 'target_checks',
      name: 'GitHub target checks',
      mimeType: 'application/json',
      content: JSON.stringify({ headSha: evidence.headSha, ...evidence.checks, collectionErrors: evidence.errors.filter((item) => ['check_runs', 'commit_status'].includes(item.scope)) }),
      metadata: { source: 'github_api', authoritative: true, repository: evidence.repository, headSha: evidence.headSha, evidenceDigest: evidence.evidenceDigest },
    });
    artifacts.push({
      type: 'github_result_evidence',
      name: 'GitHub implementation evidence',
      mimeType: 'application/json',
      content: JSON.stringify({
        summary: evidence.summary,
        repository: evidence.repository,
        branch: evidence.branch,
        baseRef: evidence.baseRef,
        headSha: evidence.headSha,
        pullRequest: evidence.pullRequest,
        compare: evidence.compare,
        totals: evidence.totals,
        changedFiles: evidence.files.map((item) => ({ path: item.path, previousPath: item.previousPath, status: item.status, additions: item.additions, deletions: item.deletions })),
        collectionErrors: evidence.errors,
      }),
      url: evidence.pullRequest?.url || evidence.compare?.url || evidence.repositoryUrl,
      metadata: { source: 'github_api', authoritative: true, repository: evidence.repository, headSha: evidence.headSha, evidenceDigest: evidence.evidenceDigest },
    });
    return artifacts;
  }

  async getDiff(job) {
    const evidence = await this.collectTargetEvidence(job);
    return {
      source: 'github_api',
      authoritative: true,
      evidenceDigest: evidence.evidenceDigest,
      summary: evidence.summary,
      repository: evidence.repository,
      baseRef: evidence.baseRef,
      headRef: evidence.branch,
      headSha: evidence.headSha,
      pullRequest: evidence.pullRequest,
      compare: evidence.compare,
      totals: evidence.totals,
      files: evidence.files,
      checks: evidence.checks,
      collectionErrors: evidence.errors,
    };
  }

  async cancelJob(job) {
    return { ...job, provider: 'github_actions_codex', status: 'cancelled' };
  }
}

export function createGitHubActionsCodexAdapterFromEnv(env = process.env) {
  const enabled = safeString(env.MARCUS_CODEX_GITHUB_ACTIONS_ENABLED || env.CODEX_GITHUB_ACTIONS_ENABLED, 20).trim().toLowerCase();
  if (!['1', 'true', 'yes', 'on'].includes(enabled)) return null;
  const token = safeString(env.MARCUS_CODEX_GITHUB_TOKEN || env.CODEX_GITHUB_TOKEN || env.GITHUB_TOKEN, 2_000).trim();
  if (!token) return null;
  return new GitHubActionsCodexAdapter({
    token,
    runnerRepo: env.MARCUS_CODEX_RUNNER_REPO || env.CODEX_RUNNER_REPO || 'markgromer/Reggie',
    eventType: env.MARCUS_CODEX_RUNNER_EVENT_TYPE || env.CODEX_RUNNER_EVENT_TYPE || 'marcus_codex_job',
    workflowFile: env.MARCUS_CODEX_RUNNER_WORKFLOW || env.CODEX_RUNNER_WORKFLOW || 'marcus-codex-runner.yml',
    timeoutMs: env.MARCUS_CODEX_GITHUB_TIMEOUT_MS || env.CODEX_GITHUB_TIMEOUT_MS,
  });
}
