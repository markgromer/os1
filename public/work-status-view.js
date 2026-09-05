const esc = (value) => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
const pathKey = (value) => String(value || '').replaceAll('\\', '/').replace(/\/+$/, '').toLowerCase();
export function safeWorkUrl(value) {
  try { const url = new URL(value); return ['https:', 'http:'].includes(url.protocol) && !url.username && !url.password ? url.href : ''; } catch { return ''; }
}
export function matchWorkProject(project, overview) {
  if (!overview?.ok) return null;
  const raw = project.raw || {};
  // Never infer a registry binding from a display name or a fuzzy awareness match.
  const exactId = raw.projectRegistryId || raw.project?.projectRegistryId;
  if (exactId) return overview.projects.find((row) => row.id === exactId) || null;
  const workspace = pathKey(project.workspacePath || raw.workspacePath);
  if (!workspace) return null;
  const matches = overview.projects.filter((row) => pathKey(row.workspacePath) === workspace);
  return matches.length === 1 ? matches[0] : null;
}
export function reportState(rawStatus, handoffStatus = '') {
  const raw = `${rawStatus || ''} ${handoffStatus}`.toLowerCase();
  if (/archived|known_history|history/.test(raw)) return 'archived';
  if (/blocked|failed|error|recovery/.test(raw)) return 'blocked';
  if (/needs|approval|ready_for_mark|waiting_for_mark/.test(raw)) return 'needs_mark';
  if (/verify|audit|test|review/.test(raw)) return 'verifying';
  if (/complete|completed|done|accepted|ready_to_ship|ship/.test(raw)) return 'complete';
  if (/run|build|codex|moving|handoff|active|queued|progress/.test(raw)) return 'moving';
  return 'monitoring';
}
export function evidenceStages(project, linked) {
  // Separate evidence dimensions: progress in one never paints earlier stages green.
  const latest = linked?.operations?.[0];
  const deployment = linked?.deployment;
  const recordedSuccess = deployment && ['production_published', 'deployment_completed'].includes(deployment.type)
    && !/fail|cancel|error|deactivat/i.test(deployment.status || '');
  return [
    { label: 'Codex report', value: project.response && project.response !== 'No Codex handoff summary yet.' ? 'Received · unverified' : 'Not received', ok: false },
    { label: 'Latest execution', value: latest ? (latest.verified ? 'Required checks passed' : latest.status.replaceAll('_', ' ')) : 'Not linked', ok: latest?.verified === true },
    { label: 'Recorded deploy', value: deployment ? `Recorded ${deployment.status || deployment.type} · ${String(deployment.commit || 'commit unknown').slice(0, 8)}` : 'Not verified', ok: recordedSuccess === true },
    { label: 'Your acceptance', value: 'Not established here', ok: false },
  ];
}
export function workStatusText(linked) {
  if (!linked) return 'No exact project binding. Work and execution status are unknown.';
  const blocked = linked.items.filter((row) => row.readiness.blockers.length);
  return [`${linked.name}: ${linked.workCount} tracked work item(s), ${linked.operationCount} execution record(s).`,
    ...blocked.map((row) => `${row.objective}: ${row.readiness.blockers.map((entry) => entry.message).join(' ')}`),
    ...linked.items.filter((row) => row.readiness.runnable).map((row) => `${row.objective}: ready${linked.engineering.autoAdvance ? ' under the saved project policy' : ', not automatically authorized to advance'}.`),
    !linked.workCount ? 'No work-graph items are tracked. A Codex handoff is a report, not verified completion.' : '',
    `Engineering: ${linked.engineering.lifecycle}; automatic advancement ${linked.engineering.autoAdvance ? 'enabled' : 'off'}.`].filter(Boolean).join('\n');
}
export function projectBrief(project, overview) {
  const linked = matchWorkProject(project, overview);
  const deployment = linked?.deployment;
  const deployed = !!deployment && ['production_published', 'deployment_completed'].includes(deployment.type)
    && ['success', 'live', 'ready', 'active', 'completed'].includes(deployment.status)
    && overview?.evidenceAvailable !== false && !linked?.release?.refreshErrors?.some((row) => /^deployment/.test(row.endpoint))
    && !linked?.release?.providerRefreshSkipped?.some((row) => row.provider === deployment.source);
  const needs = linked?.needsYouCount || 0;
  const failed = deployment?.type === 'deployment_failed';
  const headline = !overview?.ok ? 'Connecting your project records…'
    : needs ? `${needs} recorded decision${needs === 1 ? '' : 's'} need${needs === 1 ? 's' : ''} you.`
    : failed ? 'The latest deployment needs attention.'
    : project.state === 'blocked' ? 'The session reports a blocker.'
    : deployed ? 'Your latest release is deployed.'
    : linked?.release?.mergedChanges?.length ? 'Changes are merged. Deployment evidence is incomplete.'
    : 'Your existing work, in one place.';
  const next = needs ? 'Review the specific decision below. No additional authority is implied.'
    : failed ? 'Inspect the deployment receipt before retrying. An older success does not clear this failure.'
    : project.state === 'blocked' ? 'Investigate the reported blocker; it is not an approval request by itself.'
    : deployed ? 'Try the live result and tell me what to change. Deployment does not accept the current request.'
    : 'Keep working in the existing session. There is no need to create a duplicate task.';
  return { linked, deployed, needs, headline, next };
}
function recordLink(url, label) {
  const safe = safeWorkUrl(url);
  return safe ? `<a href="${esc(safe)}" target="_blank" rel="noopener noreferrer">${esc(label)} ↗</a>` : '';
}
function stamp(value) {
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) : 'Not recorded';
}
export function workOverviewHtml(project, overview) {
  const brief = projectBrief(project, overview);
  const linked = brief.linked;
  const release = linked?.release;
  const deployment = linked?.deployment;
  const items = linked?.items || [];
  const report = project.response || project.truth || '';
  const objective = linked?.objective || project.request || project.current || 'Select an existing project to see its current work.';
  return `<section class="work-overview connected-brief" data-work-overview>
    <header class="work-hero"><span class="work-kicker">CONNECTED BRIEF · DISPLAY V3</span>
      <h2>${esc(brief.headline)}</h2>
      <p>${esc(linked ? linked.repository || linked.name : 'Session not yet bound to a registered project')} · ${esc(stamp(release?.refreshedAt || overview?.observedAt))}</p>
      <div class="work-actions">${brief.deployed ? recordLink(deployment.url, 'Open live result') : ''}<button class="btn" data-work-refresh>Refresh evidence</button>${linked ? '<button class="btn" data-work-question>What needs me?</button><button class="btn" data-work-draft>Add a follow-up</button>' : ''}</div>
    </header>
    ${!overview?.ok ? '<p class="work-coverage">Work data is unavailable. No success or absence of blockers can be inferred.</p>' : ''}
    ${!linked && overview?.ok ? `<label class="work-link">Inspect registered project <select data-work-project><option value="">Choose exact project…</option>${overview.projects.map((row) => `<option value="${esc(row.id)}">${esc(row.name)} — ${esc(row.repository || row.workspacePath || row.id)}</option>`).join('')}</select></label>` : ''}
    <section class="brief-next"><span>YOUR NEXT STEP</span><p>${esc(brief.next)}</p>${linked && !brief.needs ? '<small>No approval is requested by the linked work records. This is not a claim that every external blocker is known.</small>' : ''}</section>
    <div class="release-cards">
      <section class="release-card ${brief.deployed ? 'verified' : ''}"><span>PRODUCTION RELEASE</span>
        <h3>${esc(deployment ? deployment.status : 'Not connected yet')}</h3>
        <p>${esc(deployment ? `Commit ${String(deployment.commit || 'unknown').slice(0, 8)} · ${stamp(deployment.timestamp)}` : 'A chat link alone is not deployment evidence.')}</p>
        ${recordLink(deployment?.receiptUrl, 'Deployment receipt')}
        <small>${deployment ? 'Provider observation—not a fresh health check or acceptance.' : 'Requires a registered production URL and provider receipt. Refresh reads the connected repository.'}</small>
      </section>
      <section class="release-card ${release?.checks.allRecordedPassed ? 'verified' : ''}"><span>CI FOR THE SAME COMMIT</span>
        <h3>${esc(release?.checks.count ? `${release.checks.passed}/${release.checks.count} recorded runs passed` : 'No matching CI yet')}</h3>
        <p>${esc(release?.checks.count ? `Commit ${release.commit.slice(0, 8)}. This is recorded CI, not every acceptance criterion.` : 'Checks from another commit do not verify this release.')}</p>
        ${recordLink(release?.checks.runs[0]?.url, 'View CI results')}
      </section>
    </div>
    ${release?.refreshErrors.length ? `<p class="work-coverage">Some provider reads failed; saved records may be stale. ${esc(release.refreshErrors.map((row) => row.endpoint).join(', '))}. Refresh evidence to retry.</p>` : ''}
    ${release?.providerRefreshSkipped?.some((row) => row.provider === deployment?.source) ? '<p class="work-coverage">The provider for this historical receipt could not be refreshed. Its current release status is unknown.</p>' : ''}
    ${!overview?.evidenceAvailable ? '<p class="work-coverage">Provider evidence is unavailable; the release summary is incomplete.</p>' : ''}
    ${release?.mergedChanges.length ? `<section class="work-section merged-changes"><h3>Recently merged in this repository</h3><ul class="brief-changes">${release.mergedChanges.map((row) => `<li>${recordLink(row.url, row.title) || esc(row.title)} <small>${esc(stamp(row.timestamp))}</small></li>`).join('')}</ul></section>` : ''}
    <section class="work-section current-work"><h3>Current work</h3><p>${esc(objective)}</p>
      <p class="session-note">Existing ${project.source === 'codex' ? 'Codex session' : 'project context'} · ${esc(project.name)} · ${esc(stamp(project.updatedAt))}</p>
      ${report ? `<details data-brief-detail="report"><summary>Latest session report · not independent verification</summary><p class="work-report">${esc(report)}</p></details>` : ''}
      <small>Your existing session is shown directly. It has not been duplicated or marked complete.</small>
    </section>
    ${(linked?.attention || []).length ? `<section class="work-section"><h3>Recorded decisions needing you</h3>${linked.attention.map((row) => `<article class="work-item"><strong>${esc(row.title)}</strong><p>${esc(row.reason)}</p><small>${esc(row.source)} · ${esc(row.id)}</small></article>`).join('')}</section>` : ''}
    ${items.length ? `<section class="work-section"><h3>Follow-ups &amp; dependencies</h3>${items.map((row) => `<article class="work-item"><strong>${esc(row.objective)}</strong><span class="work-tag">${esc(row.status)}</span><p>${esc(row.readiness.blockers.map((entry) => entry.message).join(' ') || 'Current durable state.')}</p><details data-brief-detail="${esc(row.id)}"><summary>Acceptance criteria</summary><ul>${row.acceptanceCriteria.map((criterion) => `<li>${esc(criterion)}</li>`).join('')}</ul></details>${row.readiness.runnable ? `<button class="btn" data-work-start="${esc(row.id)}">Start this work…</button>` : ''}</article>`).join('')}</section>` : ''}
    <details class="work-section" data-brief-detail="records"><summary>Execution records, decisions &amp; policy</summary>
      <p>${esc(linked ? `${linked.workCount} explicit follow-ups; ${linked.operationCount} MARCUS execution records. Direct Codex work above is a separate supported path.` : 'Exact project binding is required for these records.')}</p>
      ${(linked?.operations || []).map((row) => `<article class="work-item"><strong>${esc(row.title || row.id)}</strong><p>${esc(row.status)} · ${esc(row.verified ? 'Required checks passed for this execution.' : 'Required verification is not established.')}</p><p>${esc(row.id)}</p></article>`).join('')}
      <p>${esc(linked ? `Engineering: ${linked.engineering.lifecycle}. Automatic advancement: ${linked.engineering.autoAdvance ? 'enabled' : 'off'}.` : 'Execution policy unknown.')}</p>
      <ul>${(linked?.decisions || []).map((row) => `<li>${esc(row.content)}</li>`).join('')}</ul>
      <p>Main model and voice are unchanged by this display. Reading and saving a follow-up start nothing. Owner acceptance is not established here.</p>
    </details>
  </section>`;
}
