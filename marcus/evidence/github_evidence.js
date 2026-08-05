import { safeString } from '../operations/operation_types.js';

const DAY_MS = 24 * 60 * 60 * 1_000;

function iso(value, fallback) {
  const time = Date.parse(value);
  return Number.isFinite(time) ? new Date(time).toISOString() : fallback;
}

function firstLine(value, max = 500) {
  return safeString(String(value || '').split(/\r?\n/)[0], max);
}

function workflowType(name) {
  const text = safeString(name, 300).toLowerCase();
  if (/type.?check/.test(text)) return 'typecheck_run';
  if (/lint/.test(text)) return 'lint_run';
  if (/test|spec|e2e|integration/.test(text)) return 'test_run';
  return 'build_run';
}

function pullRequestType(pr, previous = null) {
  if (pr?.merged_at) return 'pull_request_merged';
  if (pr?.state === 'open' && !previous) return 'pull_request_opened';
  return 'pull_request_updated';
}

export class GitHubEvidenceIngestor {
  constructor({ api, store, minRefreshMs = 5 * 60_000, historyDays = 90 } = {}) {
    this.api = api;
    this.store = store;
    this.minRefreshMs = Math.max(60_000, Number(minRefreshMs) || 5 * 60_000);
    this.historyDays = Math.max(30, Math.min(365, Number(historyDays) || 90));
  }

  async collectProject({ businessKey, project, force = false, nowMs = Date.now() }) {
    const repository = safeString(project?.repo?.fullName, 500);
    if (!repository || project?.repo?.provider !== 'github') return { projectRegistryId: project?.id || '', skipped: 'not_registered_github' };
    if (typeof this.api !== 'function') return { projectRegistryId: project.id, skipped: 'github_not_configured' };
    const sourceKey = `github:${project.id}`;
    const previous = await this.store.getSourceState(businessKey, sourceKey);
    if (!force && Date.parse(previous.lastRefreshedAt) > nowMs - this.minRefreshMs) {
      return { projectRegistryId: project.id, skipped: 'refresh_cache', lastRefreshedAt: previous.lastRefreshedAt };
    }
    const [owner, repo] = repository.split('/');
    if (!owner || !repo) return { projectRegistryId: project.id, skipped: 'invalid_repository_mapping' };
    const base = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`;
    const since = new Date(nowMs - this.historyDays * DAY_MS).toISOString();
    const endpoints = {
      repository: base,
      commits: `${base}/commits?since=${encodeURIComponent(since)}&per_page=100`,
      branches: `${base}/branches?per_page=100`,
      pulls: `${base}/pulls?state=all&sort=updated&direction=desc&per_page=100`,
      issues: `${base}/issues?state=all&sort=updated&direction=desc&since=${encodeURIComponent(since)}&per_page=100`,
      workflows: `${base}/actions/runs?per_page=100`,
    };
    const results = Object.fromEntries(await Promise.all(Object.entries(endpoints).map(async ([name, endpoint]) => {
      try { return [name, { ok: true, data: await this.api(endpoint) }]; }
      catch (error) { return [name, { ok: false, error: safeString(error?.message, 1_000) || 'GitHub request failed.' }]; }
    })));
    const observedAt = new Date(nowMs).toISOString();
    const evidence = [];
    const add = (input) => evidence.push({
      businessKey,
      projectRegistryId: project.id,
      projectId: project.projectId,
      source: 'github',
      actor: input.actor || 'github',
      repository,
      observedAt,
      confidence: input.confidence ?? 1,
      provenance: { method: 'github_api', endpoint: input.endpoint, externalId: input.externalId },
      ...input,
    });

    const repoData = results.repository?.ok ? results.repository.data : null;
    if (repoData) add({
      type: 'repository_read', event: 'repository_observed', timestamp: iso(repoData.pushed_at || repoData.updated_at, observedAt),
      summary: `${repository} repository metadata observed.`, externalId: `repo:${repoData.id || repository}:${repoData.pushed_at || repoData.updated_at || observedAt}`,
      endpoint: endpoints.repository, branch: repoData.default_branch || project.repo.defaultBranch,
      metadata: { defaultBranch: repoData.default_branch, pushedAt: repoData.pushed_at, updatedAt: repoData.updated_at, archived: Boolean(repoData.archived), openIssues: repoData.open_issues_count },
    });

    const commits = results.commits?.ok && Array.isArray(results.commits.data) ? results.commits.data : [];
    const commitDates = new Map();
    const cachedBranchDates = previous.branchDates && typeof previous.branchDates === 'object' ? previous.branchDates : {};
    for (const commit of commits) {
      const sha = safeString(commit?.sha, 200);
      if (!sha) continue;
      const timestamp = iso(commit?.commit?.author?.date || commit?.commit?.committer?.date, observedAt);
      commitDates.set(sha, timestamp);
      add({
        type: 'commit', event: 'commit_observed', timestamp, actor: commit?.author?.login || commit?.commit?.author?.name || 'github',
        summary: firstLine(commit?.commit?.message) || `Commit ${sha.slice(0, 12)} observed.`, commitSha: sha,
        externalId: `commit:${repository}:${sha}`, endpoint: endpoints.commits,
        metadata: { author: commit?.author?.login || commit?.commit?.author?.name, committer: commit?.committer?.login || commit?.commit?.committer?.name, url: commit?.html_url },
      });
    }

    const branchHeads = {};
    const branchDates = {};
    const oldHeads = previous.branchHeads && typeof previous.branchHeads === 'object' ? previous.branchHeads : {};
    const branches = results.branches?.ok && Array.isArray(results.branches.data) ? results.branches.data : [];
    const missingBranchShas = [...new Set(branches.map((branch) => safeString(branch?.commit?.sha, 200))
      .filter((sha) => sha && !commitDates.has(sha) && !cachedBranchDates[sha]))].slice(0, 25);
    await Promise.all(missingBranchShas.map(async (sha) => {
      try {
        const commit = await this.api(`${base}/commits/${encodeURIComponent(sha)}`);
        const timestamp = iso(commit?.commit?.author?.date || commit?.commit?.committer?.date, '');
        if (timestamp) commitDates.set(sha, timestamp);
      } catch {
        // Branch timestamps remain observation-based when a bounded detail lookup fails.
      }
    }));
    for (const branch of branches) {
      const name = safeString(branch?.name, 300);
      const sha = safeString(branch?.commit?.sha, 200);
      if (!name || !sha) continue;
      branchHeads[name] = sha;
      const created = !oldHeads[name];
      if (!created && oldHeads[name] === sha) continue;
      const commitDate = commitDates.get(sha) || safeString(cachedBranchDates[sha], 64);
      if (commitDate) branchDates[sha] = commitDate;
      add({
        type: created ? 'branch_created' : 'branch_updated', event: created ? 'branch_first_observed' : 'branch_head_updated',
        timestamp: commitDate || observedAt, summary: `${name} ${created ? 'was first observed' : 'advanced'} at ${sha.slice(0, 12)}.`,
        branch: name, commitSha: sha, externalId: `branch:${repository}:${name}:${sha}`, endpoint: endpoints.branches,
        confidence: commitDate ? 0.95 : 0.75, metadata: { protected: Boolean(branch?.protected), commitDate, firstObserved: created, timestampBasis: commitDate ? 'commit' : 'observation' },
      });
    }

    const pulls = results.pulls?.ok && Array.isArray(results.pulls.data) ? results.pulls.data : [];
    const previousPullRequests = previous.pullRequests && typeof previous.pullRequests === 'object' ? previous.pullRequests : {};
    const pullRequests = {};
    for (const pr of pulls) {
      const type = pullRequestType(pr, previousPullRequests[pr.number]);
      const timestamp = iso(pr.merged_at || (type === 'pull_request_opened' ? pr.created_at : pr.updated_at) || pr.created_at, observedAt);
      pullRequests[pr.number] = { state: pr.state, updatedAt: pr.updated_at, mergedAt: pr.merged_at || '' };
      add({
        type, event: type, timestamp, actor: pr?.user?.login || 'github', branch: pr?.head?.ref || '', commitSha: pr?.head?.sha || '',
        summary: `PR #${pr.number}: ${firstLine(pr.title, 700)}`, externalId: `pr:${repository}:${pr.number}:${type}:${pr.updated_at || timestamp}`,
        endpoint: endpoints.pulls,
        pullRequest: { number: pr.number, title: pr.title, state: pr.state, draft: pr.draft, url: pr.html_url, base: pr?.base?.ref, head: pr?.head?.ref, updatedAt: pr.updated_at, mergedAt: pr.merged_at },
        metadata: { createdAt: pr.created_at, closedAt: pr.closed_at, mergedAt: pr.merged_at, labels: (Array.isArray(pr.labels) ? pr.labels : []).slice(0, 20).map((label) => label?.name).filter(Boolean) },
      });
    }

    const issues = results.issues?.ok && Array.isArray(results.issues.data) ? results.issues.data : [];
    for (const issue of issues) {
      if (issue?.pull_request) continue;
      add({
        type: 'issue_updated', event: 'issue_updated', timestamp: iso(issue.updated_at || issue.created_at, observedAt), actor: issue?.user?.login || 'github',
        summary: `Issue #${issue.number}: ${firstLine(issue.title, 700)}`, externalId: `issue:${repository}:${issue.number}:${issue.updated_at || observedAt}`,
        endpoint: endpoints.issues, metadata: { number: issue.number, state: issue.state, url: issue.html_url, createdAt: issue.created_at, updatedAt: issue.updated_at },
      });
    }

    const workflowData = results.workflows?.ok ? results.workflows.data : null;
    const runs = Array.isArray(workflowData?.workflow_runs) ? workflowData.workflow_runs : [];
    for (const run of runs) {
      const type = workflowType(run.name || run.display_title);
      add({
        type, event: safeString(run.conclusion || run.status, 100) || 'workflow_observed', timestamp: iso(run.updated_at || run.created_at, observedAt),
        actor: run?.actor?.login || 'github-actions', branch: run.head_branch || '', commitSha: run.head_sha || '',
        summary: `${run.name || 'GitHub workflow'} ${run.conclusion || run.status || 'observed'}.`, externalId: `workflow:${repository}:${run.id}:${run.run_attempt || 1}:${run.updated_at || observedAt}`,
        endpoint: endpoints.workflows, metadata: { status: run.status, conclusion: run.conclusion, event: run.event, url: run.html_url, runNumber: run.run_number, runAttempt: run.run_attempt },
      });
    }

    const appended = await this.store.append(businessKey, evidence, { assignedSource: 'github', trusted: true, provenanceMethod: 'github_api' });
    const errors = Object.entries(results).filter(([, result]) => !result.ok).map(([name, result]) => ({ endpoint: name, error: result.error }));
    await this.store.setSourceState(businessKey, sourceKey, {
      lastRefreshedAt: observedAt,
      lastSuccessfulAt: evidence.length ? observedAt : previous.lastSuccessfulAt || '',
      repository,
      branchHeads: Object.keys(branchHeads).length ? branchHeads : oldHeads,
      branchDates: Object.keys(branchDates).length ? branchDates : cachedBranchDates,
      defaultBranch: repoData?.default_branch || previous.defaultBranch || project.repo.defaultBranch,
      pullRequests,
      errors,
    });
    return { projectRegistryId: project.id, repository, accepted: appended.accepted.length, duplicates: appended.duplicateCount, errors };
  }

  async collect({ businessKey, projects = [], force = false, nowMs = Date.now() } = {}) {
    const output = [];
    for (const project of projects) output.push(await this.collectProject({ businessKey, project, force, nowMs }));
    return output;
  }
}
