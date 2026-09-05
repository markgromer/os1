import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { spawnSync } from 'node:child_process';

import { AwarenessService } from '../marcus/awareness/awareness_service.js';
import { AwarenessStore } from '../marcus/awareness/awareness_store.js';
import { ProjectMemoryIndexer } from '../marcus/awareness/project_memory_index.js';

async function temporaryRoot() {
  return fs.mkdtemp(path.join(os.tmpdir(), 'marcus-awareness-'));
}

function project(id, name, status = 'Active', workspace = '') {
  return {
    id,
    businessKey: 'personal',
    projectId: `legacy-${id}`,
    canonicalName: name,
    aliases: [`${name} alias`],
    status,
    description: `${name} durable description.`,
    currentObjective: { desiredOutcome: `Complete ${name} with verified evidence.` },
    repo: { fullName: `markgromer/${name.toLowerCase().replace(/\s+/g, '-')}` },
    localWorkspace: workspace ? { path: workspace, canonicalPath: workspace, trustStatus: 'approved' } : {},
    createdAt: '2026-08-01T00:00:00.000Z',
    updatedAt: '2026-08-14T00:00:00.000Z',
  };
}

test('awareness store preserves terminal history while synchronizing registry identity', async () => {
  const root = await temporaryRoot();
  try {
    const store = new AwarenessStore({ dataDir: root });
    const records = await store.synchronize('personal', [project('active', 'Active Work'), project('done', 'Finished Work', 'Done')]);
    assert.equal(records.length, 2);
    assert.equal(records.find((item) => item.projectRegistryId === 'done').lifecycle, 'completed');
    const firstDocument = await store.read('personal');
    await store.synchronize('personal', [project('active', 'Active Work'), project('done', 'Finished Work', 'Done')]);
    const unchangedDocument = await store.read('personal');
    assert.equal(unchangedDocument.revision, firstDocument.revision);
    assert.equal(unchangedDocument.updatedAt, firstDocument.updatedAt);
    assert.deepEqual(unchangedDocument.projects.map((item) => item.updatedAt), firstDocument.projects.map((item) => item.updatedAt));

    const active = records.find((item) => item.projectRegistryId === 'active');
    const archived = await store.setLifecycle('personal', active.id, 'archived', { actor: 'mark', reason: 'Accepted and filed.' });
    assert.equal(archived.lifecycle, 'archived');
    assert.equal(archived.lifecycleHistory[0].from, 'active');
    assert.equal(archived.lifecycleHistory[0].to, 'archived');
    assert.equal((await store.appendWorkEvent('personal', active.id, { eventId: 'op-1:completed', summary: 'Verified the project.', status: 'completed' })).created, true);
    assert.equal((await store.appendWorkEvent('personal', active.id, { eventId: 'op-1:completed', summary: 'Duplicate.', status: 'completed' })).created, false);

    const restarted = new AwarenessStore({ dataDir: root });
    assert.equal((await restarted.get('personal', active.id)).lifecycle, 'archived');
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('project memory indexing creates marcus.txt, indexes project notes, and inventories the repo without secrets or dependencies', async () => {
  const root = await temporaryRoot();
  try {
    const workspace = path.join(root, 'Example Project');
    const vault = path.join(root, 'docs', 'marcus');
    await fs.mkdir(path.join(workspace, 'src'), { recursive: true });
    await fs.mkdir(path.join(workspace, 'node_modules', 'ignored'), { recursive: true });
    await fs.mkdir(path.join(vault, 'projects'), { recursive: true });
    await fs.writeFile(path.join(workspace, 'README.md'), '# Example Project\nBuild the useful thing.\n', 'utf8');
    await fs.writeFile(path.join(workspace, 'src', 'index.js'), 'export const ready = true;\n', 'utf8');
    await fs.writeFile(path.join(workspace, '.env'), 'SECRET_TOKEN=do-not-index\n', 'utf8');
    await fs.writeFile(path.join(workspace, 'node_modules', 'ignored', 'index.js'), 'ignored\n', 'utf8');
    await fs.writeFile(path.join(vault, 'projects', 'example-project.md'), '# Example Project\nStatus: active\nThe durable decision is retained.\n', 'utf8');

    const indexer = new ProjectMemoryIndexer({ vaultDir: vault, currentWorkspace: workspace });
    const memory = await indexer.indexProject(project('example', 'Example Project', 'Active', workspace));
    assert.equal(memory.status, 'fresh');
    assert.ok(memory.sources.some((item) => item.type === 'marcus_root'));
    assert.ok(memory.sources.some((item) => item.type === 'obsidian_project'));
    assert.ok(memory.repositoryManifest.keyFiles.some((item) => item.path === 'marcus.txt'));
    assert.equal(memory.repositoryManifest.fileCount, 3);
    assert.equal(JSON.stringify(memory).includes('do-not-index'), false);
    assert.equal(await fs.readFile(path.join(workspace, 'marcus.txt'), 'utf8').then((value) => value.includes('MARCUS PROJECT NOTE')), true);
    assert.equal((await indexer.appendWorkNote(project('example', 'Example Project', 'Active', workspace), {
      eventId: 'operation:example:completed', summary: 'Completed the indexed project work.', status: 'completed', verification: '3/3 checks passed.',
    })).appended, true);
    const appended = await fs.readFile(path.join(workspace, 'marcus.txt'), 'utf8');
    assert.match(appended, /Completed the indexed project work/);
    assert.match(appended, /MARCUS event: operation:example:completed/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('awareness service keeps archived projects searchable but out of the default attention feed', async () => {
  const root = await temporaryRoot();
  try {
    let projects = [project('active', 'Current Build'), project('history', 'Historical Build', 'Archived')];
    const store = new AwarenessStore({ dataDir: root });
    const service = new AwarenessService({
      store,
      listProjects: async () => projects,
      updateProject: async (_businessKey, id, patch) => {
        projects = projects.map((item) => item.id === id ? { ...item, ...patch, updatedAt: new Date().toISOString() } : item);
        return projects.find((item) => item.id === id);
      },
      listOperations: async () => [{
        id: 'op-active', projectRegistryId: 'active', projectId: 'legacy-active', projectName: 'Current Build',
        title: 'Implement awareness', status: 'running', updatedAt: '2026-08-14T01:00:00.000Z', currentStep: { title: 'Build service' },
      }],
      evidenceService: { getActivity: async () => ({ snapshots: [] }), getProjectEvidence: async () => [] },
    });

    const defaultFeed = await service.feed('personal');
    assert.deepEqual(defaultFeed.projects.map((item) => item.canonicalName), ['Current Build']);
    assert.equal(defaultFeed.projects[0].attentionState, 'moving');
    assert.equal(defaultFeed.counts.archived, 1);

    const search = await service.search('personal', 'Historical Build');
    assert.equal(search[0].archived, true);

    const current = defaultFeed.projects[0];
    await service.setLifecycle('personal', current.id, 'archived', { reason: 'Accepted.' });
    assert.equal((await service.feed('personal')).projects.length, 0);
    assert.equal((await service.feed('personal', { includeArchived: true })).projects.length, 2);
    await service.setLifecycle('personal', current.id, 'active', { reason: 'Work resumed.' });
    assert.equal((await service.feed('personal')).projects[0].canonicalName, 'Current Build');
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('desktop visualizer uses canonical awareness instead of browser-local project dismissal', async () => {
  const html = await fs.readFile(new URL('../public/visualizer.html', import.meta.url), 'utf8');
  assert.match(html, /\/api\/marcus\/awareness\?includeArchived=true/);
  assert.match(html, /awarenessProjectId/);
  assert.doesNotMatch(html, /opsDismissedProjects/);
  assert.match(html, /data-tab="browser"/);
  assert.doesNotMatch(html, /<option value="__browser__">Browser<\/option>/);
  assert.match(html, /context: browserContext \? "browser"/);
  assert.match(html, /\.composer select option \{ background: #fffdfa; color: #17191d; \}/);
  assert.match(html, /data-tab="approvals"/);
  assert.match(html, /\/api\/marcus\/browser\/status/);
  assert.match(html, /\/api\/marcus\/browser\/actions/);
  assert.match(html, /\/api\/marcus\/browser\/publications/);
  assert.match(html, /grid-template-rows: 90px minmax\(0, 1fr\) 34px/);
  assert.match(html, /\.workspace-body \{ grid-row: 2;/);
  assert.match(html, /\.workspace-status \{ grid-row: 3; \}/);
  assert.match(html, /authenticatedFetch\(`\/api\/marcus\/browser\/frame\?v=/);
  assert.match(html, /URL\.createObjectURL\(blob\)/);
  assert.doesNotMatch(html, /frame\.src = `\/api\/marcus\/browser\/frame/);
  assert.match(html, /data-publication-approve/);
  assert.match(html, /data-publication-deny/);
  assert.match(html, /\["pending_approval", "failed"\]\.includes\(item\.status\)/);
  assert.match(html, /action required/);
  assert.doesNotMatch(html, /direct browser control is bound/);
  assert.match(html, /ACTIVE CODEX PROJECTS/);
  assert.match(html, /const projects = snapshot\.codexProjects/);
  assert.match(html, /project\.source !== "codex" && \(matched\.state === "archived"/);
  assert.match(html, /class="attention-banner"/);
  assert.doesNotMatch(html, /Accept &amp; archive/);
  assert.match(html, /projectBrief\(project, workOverview\)\.needs/);
  assert.match(html, /function pipelineStages\(project\)/);
  assert.match(html, /messageSeenAt/);
});

test('desktop visualizer retains the last confirmed Codex ledger during relay gaps', async () => {
  const html = await fs.readFile(new URL('../public/visualizer.html', import.meta.url), 'utf8');
  assert.match(html, /function retainCodexWorkspaces\(desktop\)/);
  assert.match(html, /lastConfirmedCodexWorkspaces/);
  assert.match(html, /codexWorkspacesStale: true/);
  assert.ok(html.includes('Relay reconnecting · projects retained'));
  const inlineScript = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)]
    .map((match) => match[1])
    .find((script) => script.includes('function retainCodexWorkspaces'));
  assert.ok(inlineScript);
  const syntax = spawnSync(process.execPath, ['--check', '--input-type=module'], { input: inlineScript, encoding: 'utf8', windowsHide: true });
  assert.equal(syntax.status, 0, syntax.stderr);
});
