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
export function workOverviewHtml(project, overview) {
  const linked = matchWorkProject(project, overview);
  const items = linked?.items || [];
  const needs = linked?.needsYouCount ?? items.filter((row) => row.readiness.needsMark).length;
  const ready = linked?.readyCount ?? items.filter((row) => row.readiness.runnable).length;
  const running = linked?.runningCount ?? items.filter((row) => row.status === 'running').length;
  const stages = evidenceStages(project, linked);
  const latest = linked?.operations?.[0];
  const deployment = linked?.deployment;
  const deploymentUrl = safeWorkUrl(deployment?.url);
  const coverage = !overview?.ok ? 'Work data is unavailable. No success or absence of blockers can be inferred.'
    : !linked ? 'This session is not linked to an exact registered workspace. Choose a registered project to inspect its work; matching names alone are not enough.'
    : !linked.workCount ? 'No work-graph items yet. Existing execution records appear below; Codex sessions and legacy tasks are not automatically imported.'
    : `Showing ${items.length} of ${linked.workCount} work items. Readiness is calculated from dependencies, current decisions and execution evidence.`;
  const heading = needs ? `${needs} recorded decision${needs === 1 ? '' : 's'} need${needs === 1 ? 's' : ''} you.` : linked?.workCount ? (running ? `${running} execution${running === 1 ? '' : 's'} running.` : `${ready} tracked item${ready === 1 ? '' : 's'} ready. Review the blockers and next steps below.`) : project.state === 'blocked' ? 'A blocker needs investigation.' : project.state === 'needs_mark' ? 'A handoff needs your review—not automatic acceptance.'
    : project.state === 'complete' ? 'Codex reported a result. Check what is verified.' : project.state === 'moving' ? 'Work is reported in progress.' : 'Here is what the records actually show.';
  return `<section class="work-overview" data-work-overview>
    <header class="work-hero"><span class="work-kicker">WORK &amp; EVIDENCE · DISPLAY V2</span><h2>${esc(heading)}</h2>
      <p>${esc(project.name)} · ${linked ? 'Exact registered workspace' : 'Unlinked session'} · Read ${esc(overview?.observedAt || 'unavailable')}</p>
      <div class="work-actions"><button class="btn" data-work-refresh>Refresh evidence</button>${linked ? '<button class="btn primary" data-work-draft>Track a follow-up</button><button class="btn" data-work-question>What needs me?</button>' : ''}</div>
    </header>
    <div class="work-stages">${stages.map((stage) => `<div class="work-stage ${stage.ok ? 'verified' : ''}"><span>${esc(stage.label)}</span><strong>${esc(stage.value)}</strong></div>`).join('')}</div>
    <p class="work-coverage">${esc(coverage)}</p>
    ${!overview?.evidenceAvailable ? '<p class="work-coverage">Provider evidence is unavailable. Deployment and recent-change records may be incomplete.</p>' : ''}
    ${!linked && overview?.ok ? `<label class="work-link">Inspect registered project <select data-work-project><option value="">Choose exact project…</option>${overview.projects.map((row) => `<option value="${esc(row.id)}">${esc(row.name)} — ${esc(row.repository || row.workspacePath || row.id)}</option>`).join('')}</select></label>` : ''}
    <div class="work-counts"><span><b>${linked ? needs : '—'}</b> Needs you</span><span><b>${linked ? running : '—'}</b> Running</span><span><b>${linked ? ready : '—'}</b> Ready</span><span><b>${linked?.operationCount ?? '—'}</b> Execution records</span></div>
    ${(linked?.attention || []).length ? `<section class="work-section"><h3>Needs your decision</h3>${linked.attention.map((row) => `<article class="work-item"><strong>${esc(row.title)}</strong><p>${esc(row.reason)}</p><small>${esc(row.source)} · ${esc(row.id)}</small></article>`).join('')}</section>` : ''}
    <section class="work-section"><h3>Tracked work</h3>${items.length ? items.map((row) => `<article class="work-item"><strong>${esc(row.objective)}</strong><span class="work-tag">${esc(row.status)}</span><p>${esc(row.readiness.blockers.map((entry) => entry.message).join(' ') || (row.readiness.runnable ? 'Ready. Starting requires an explicit action below.' : 'Current durable state.'))}</p><details><summary>Acceptance criteria &amp; evidence binding</summary><ul>${row.acceptanceCriteria.map((criterion) => `<li>${esc(criterion)}</li>`).join('')}</ul><p>${esc(row.operationId ? `Operation ${row.operationId}` : 'No execution bound yet.')}</p></details>${row.readiness.runnable ? `<button class="btn" data-work-start="${esc(row.id)}">Start this work…</button>` : ''}</article>`).join('') : '<p>Track a follow-up with an objective and acceptance criteria. Saving it does not launch Codex, grant permissions or enable automation.</p>'}</section>
    <section class="work-section"><h3>Existing execution records</h3><p>Historical records for this exact project—not proof that the current Codex handoff is complete.</p>${linked?.operations?.length ? linked.operations.map((row) => `<article class="work-item"><strong>${esc(row.title || row.id)}</strong><span class="work-tag">${esc(row.status)}</span><p>${esc(row.verified ? 'Required verification passed for this execution.' : 'Completion has not passed required verification.')} · ${esc(row.updatedAt)}</p><p>${esc(row.blockers.map((entry) => entry.message).join(' '))}</p><details><summary>Checks (${row.verification.length})</summary><ul>${row.verification.map((check) => `<li>${esc(check.type)}: ${esc(check.status)}${check.waived ? ' (waived)' : ''}</li>`).join('')}</ul><p>${esc(row.id)}</p></details></article>`).join('') : '<p>No linked execution record is available. A prose handoff does not replace one.</p>'}</section>
    <section class="work-section"><h3>Deployment evidence</h3>${deployment ? `<p>Latest recorded production event: ${esc(deployment.status)} · ${esc(deployment.timestamp)} · ${esc(deployment.source)}</p><p>Commit ${esc(deployment.commit || 'unknown')}. This is a saved observation, not a fresh health check or acceptance of the current request.</p>${deploymentUrl ? `<a href="${esc(deploymentUrl)}" target="_blank" rel="noopener noreferrer">Open recorded live URL ↗</a>` : ''}` : '<p>No trusted production deployment receipt is linked here. A deployment link in a chat message is not a verified receipt.</p>'}</section>
    <section class="work-section"><h3>Decisions &amp; execution policy</h3><p>${esc(linked ? `Engineering: ${linked.engineering.lifecycle}. Project grant: ${linked.engineering.granted ? 'present' : 'absent'}. Automatic advancement: ${linked.engineering.autoAdvance ? 'enabled' : 'off'}.` : 'Policy unknown until the project is linked.')}</p><ul>${(linked?.decisions || []).map((row) => `<li>${esc(row.content)} <small>revision ${esc(row.revision)}</small></li>`).join('')}</ul>${!linked?.decisions?.length ? '<p>No scoped active decisions returned.</p>' : ''}${overview?.decisionsMayBeTruncated ? '<p>Decision results are bounded and may be incomplete.</p>' : ''}<p>Main model and voice settings are unchanged by this display. GPT-6 preview qualification does not mean the main conversation uses it.</p></section>
    <details class="work-section"><summary>Codex report · not independent verification</summary><p class="work-report">${esc(project.response || project.truth || 'No handoff received.')}</p></details>
    <details class="work-section"><summary>Recent recorded changes</summary><ul>${(linked?.recentChanges || []).map((row) => `<li>${esc(row.timestamp)} · ${esc(row.source)} · ${esc(row.summary)}</li>`).join('')}</ul></details>
    <footer class="work-foot">Reading this screen starts nothing. “Reported”, “verified”, “deployed” and “accepted” are separate states.${latest ? ` Latest execution: ${esc(latest.id)}.` : ''}</footer>
  </section>`;
}
