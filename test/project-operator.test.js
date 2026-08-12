import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { ProjectEvidenceService } from '../marcus/evidence/project_evidence_service.js';
import { createOperationsEngine } from '../marcus/operations/operation_engine.js';
import { ProjectOperatorService } from '../marcus/operators/project_operator_service.js';

async function withProjectOperator(callback, { directCodexAdapter = null } = {}) {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-project-operator-'));
  const legacyByBusiness = {
    personal: {
      projects: [{ id: 'royal-doody', name: 'Royal Doody', status: 'active' }],
      tasks: [{ id: 'task-1', projectId: 'royal-doody', project: 'Royal Doody', title: 'Fix mobile booking flow', status: 'open', dueDate: '2026-08-20' }],
      inboxItems: [{ id: 'msg-1', projectId: 'royal-doody', project: 'Royal Doody', contactName: 'Client', text: 'The mobile booking form is hard to use.' }],
    },
  };
  const engine = createOperationsEngine({
    dataDir,
    getLegacyProjects: async (businessKey) => legacyByBusiness[businessKey]?.projects || [],
    directCodexAdapter,
  });
  const evidence = new ProjectEvidenceService({
    dataDir,
    listProjects: (businessKey) => engine.listProjectRegistry(businessKey),
    listOperations: (businessKey, filters) => engine.listOperations(businessKey, filters),
    getLegacyStore: async (businessKey) => legacyByBusiness[businessKey] || {},
    getSettings: async () => ({}),
  });
  engine.setCodexLifecycleRecorder((event) => evidence.recordCodexLifecycle(event));
  const service = new ProjectOperatorService({
    operationsEngine: engine,
    projectEvidenceService: evidence,
    getLegacyStore: async (businessKey) => legacyByBusiness[businessKey] || {},
    getDesktopContext: async () => ({
      workspace: {
        folderName: 'royal-doody',
        workspacePath: path.join(dataDir, 'royal-doody'),
        gitBranch: 'main',
        gitStatus: [{ status: 'M', file: 'src/booking.tsx' }],
      },
    }),
    githubApi: async (pathPart) => {
      if (pathPart.startsWith('/user/repos')) return [
        { full_name: 'markgromer/reggie', name: 'reggie', description: 'New Reggie agent runtime', default_branch: 'main' },
        { full_name: 'markgromer/reggie-hub', name: 'reggie-hub', description: 'Reggie hub control plane', default_branch: 'main' },
      ];
      if (pathPart.toLowerCase() === '/repos/markgromer/reggie') return {
        full_name: 'markgromer/Reggie', name: 'Reggie', description: 'New Reggie agent runtime', default_branch: 'main', html_url: 'https://github.com/markgromer/Reggie',
      };
      if (pathPart.includes('/reggie/contents/README.md')) return { content: Buffer.from('# Reggie\nNew agent runtime.').toString('base64') };
      if (pathPart.includes('/reggie-hub/contents/README.md')) return { content: Buffer.from('# Reggie Hub\nControl plane for Reggie.').toString('base64') };
      if (pathPart.endsWith('/README.md')) return { content: Buffer.from('# Royal Doody\nBooking app.').toString('base64') };
      if (pathPart.endsWith('/package.json')) return { content: Buffer.from(JSON.stringify({ scripts: { test: 'node --test', build: 'vite build' } })).toString('base64') };
      throw new Error('not found');
    },
  });
  try {
    return await callback({ engine, evidence, service, dataDir });
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
}

test('project operator audits context and creates a durable Codex handoff', async () => {
  await withProjectOperator(async ({ engine, evidence, service }) => {
    const project = await engine.createProjectRegistryRecord('personal', {
      canonicalName: 'Royal Doody',
      projectId: 'royal-doody',
      aliases: ['Royal Doody booking'],
      repo: { fullName: 'markgromer/royal-doody-demo', defaultBranch: 'main' },
      deployments: { productionUrl: 'https://royal.example.com', cloudflareProject: 'royal-doody' },
      commands: { test: 'node --test', build: 'npm run build' },
      stack: ['vite', 'react'],
    });
    await evidence.ingestManual('personal', {
      projectRegistryId: project.id,
      source: 'manual',
      type: 'manual_note',
      event: 'operator_note',
      summary: 'Previous audit found mobile layout risk in booking.',
      actor: 'mark',
      provenance: { method: 'test' },
    });

    const result = await service.prepareCodexOperation('personal', {
      message: 'Marcus, audit the Royal Doody mobile booking flow and get Codex fixing it.',
    });

    assert.equal(result.ok, true);
    assert.equal(result.status, 'codex_prepared');
    assert.equal(result.project.name, 'Royal Doody');
    assert.match(result.auditBrief, /Fix mobile booking flow/);
    assert.match(result.auditBrief, /Previous audit found mobile layout risk/);
    assert.match(result.auditBrief, /Active desktop workspace/);
    assert.match(result.codexPrompt, /Do not deploy, publish, merge, change DNS/);
    assert.match(result.codexPrompt, /Verification/);
    assert.equal(result.operation.status, 'blocked');
    assert.ok(result.handoff?.content.includes('M.A.R.C.U.S. Durable Operation Handoff'));
    assert.ok(result.operation.blockers.some((blocker) => blocker.type === 'external_codex_required'));
  });
});

test('project operator starts a direct Codex job when a direct adapter is configured', async () => {
  const starts = [];
  await withProjectOperator(async ({ engine, service }) => {
    const project = await engine.createProjectRegistryRecord('personal', {
      canonicalName: 'Royal Doody',
      projectId: 'royal-doody',
      aliases: ['Royal Doody booking'],
      repo: { fullName: 'markgromer/royal-doody-demo', defaultBranch: 'main' },
      commands: { test: 'node --test' },
    });

    const readiness = await engine.readiness('personal');
    assert.equal(readiness.codex.directAdapterConfigured, true);

    const result = await service.prepareCodexOperation('personal', {
      message: 'Marcus, audit the Royal Doody repository and start Codex fixing it.',
      projectId: project.id,
    });

    assert.equal(starts.length, 1);
    assert.equal(result.ok, true);
    assert.equal(result.status, 'codex_prepared');
    assert.equal(result.operation.status, 'awaiting_provider');
    assert.equal(result.handoff, null);
    assert.equal(result.operation.steps.some((step) => step.type === 'codex' && step.status === 'running'), true);
    assert.ok(Object.values(result.operation.metadata.codexJobs).some((job) => job.provider === 'test_codex' && job.jobId === 'codex-job-1'));
  }, {
    directCodexAdapter: {
      providerName: 'test_codex',
      async startJob(job) {
        starts.push(job);
        return { provider: 'test_codex', jobId: 'codex-job-1', status: 'started', branch: job.branch };
      },
      async getJobStatus(job) { return { ...job, provider: 'test_codex', status: 'running' }; },
      async sendFollowup(job) { return { ...job, provider: 'test_codex', status: 'running' }; },
      async getArtifacts() { return []; },
      async getDiff() { return { summary: '' }; },
      async cancelJob(job) { return { ...job, provider: 'test_codex', status: 'cancelled' }; },
    },
  });
});

test('project operator can audit and prepare without starting Codex', async () => {
  const starts = [];
  await withProjectOperator(async ({ engine, service }) => {
    const project = await engine.createProjectRegistryRecord('personal', {
      canonicalName: 'Royal Doody',
      projectId: 'royal-doody',
      repo: { fullName: 'markgromer/royal-doody-demo', defaultBranch: 'main' },
    });
    const result = await service.prepareCodexOperation('personal', {
      message: 'Audit the Royal Doody repository and prepare the Codex prompt, but do not start Codex.',
      projectId: project.id,
      autoStart: false,
    });
    assert.equal(starts.length, 0);
    assert.equal(result.operation.status, 'planned');
    assert.match(result.codexPrompt, /Royal Doody/);
  }, {
    directCodexAdapter: {
      providerName: 'test_codex',
      async startJob(job) { starts.push(job); return { provider: 'test_codex', jobId: 'unexpected', status: 'started' }; },
      async getJobStatus(job) { return job; },
      async sendFollowup(job) { return job; },
      async getArtifacts() { return []; },
      async getDiff() { return { summary: '' }; },
      async cancelJob(job) { return job; },
    },
  });
});

test('project operator asks for project clarification when confidence is low', async () => {
  await withProjectOperator(async ({ service }) => {
    const result = await service.prepareCodexOperation('personal', {
      message: 'Audit the unknown moonbase repo and start Codex.',
    });
    assert.equal(result.ok, true);
    assert.equal(result.status, 'needs_project');
    assert.match(result.reply, /project clarified/i);
  });
});

test('project operator detects website install and replace requests', async () => {
  await withProjectOperator(async ({ service }) => {
    assert.equal(
      service.shouldHandle('The Freedom Scoopers website needs the new Reggie and Reggie hub installed and replace the legacy Reggie.'),
      true,
    );
    assert.equal(
      service.shouldHandle('For a read-only continuity check, use markgromer/Reggie. Tell me what you retained. Do not audit or start Codex.'),
      false,
    );
    assert.equal(service.shouldHandle('Audit the Reggie repository, but do not start Codex.'), true);
  });
});

test('project operator auto-registers an explicit GitHub repo and reuses it as conversation context', async () => {
  await withProjectOperator(async ({ engine, service }) => {
    assert.equal(service.shouldHandle('Can we discuss a settings popup for Sweep and Go?'), false);

    const context = await service.resolveProjectContext('personal', {
      message: 'Reggie is my GitHub project at markgromer/Reggie.git. Sweep and Go needs a settings popup for its API token and slug.',
    });
    assert.equal(context.registered, true);
    assert.equal(context.project.name, 'Reggie');
    assert.equal(context.project.repo, 'markgromer/Reggie');

    const registered = await engine.listProjectRegistry('personal');
    const reggie = registered.find((project) => project.repo.fullName === 'markgromer/Reggie');
    assert.ok(reggie);

    const result = await service.prepareCodexOperation('personal', {
      message: 'Reggie is my GitHub project at markgromer/Reggie.git. Sweep and Go needs a settings popup for its API token and slug. Check the git repo and set up the implementation plan.',
      projectRegistryId: reggie.id,
    });
    assert.equal(result.status, 'codex_prepared');
    assert.equal(result.project.name, 'Reggie');
    assert.match(result.codexPrompt, /settings popup/i);
    assert.match(result.codexPrompt, /API token and slug/i);
  });
});

test('project operator inspects related GitHub repos before composing Codex prompt', async () => {
  await withProjectOperator(async ({ engine, service }) => {
    await engine.createProjectRegistryRecord('personal', {
      canonicalName: 'Freedom Scoopers',
      projectId: 'freedom-scoopers',
      aliases: ['Freedom Scoopers website'],
      repo: { fullName: 'markgromer/freedom-scoopers', defaultBranch: 'main' },
    });

    const result = await service.prepareCodexOperation('personal', {
      message: 'The Freedom Scoopers website needs the new Reggie and Reggie hub installed and replace the legacy Reggie system. You can find both projects in GitHub.',
    });

    assert.equal(result.ok, true);
    assert.equal(result.status, 'codex_prepared');
    assert.match(result.reply, /Inspected: /);
    assert.match(result.auditBrief, /## GitHub Audit/);
    assert.match(result.auditBrief, /markgromer\/reggie/);
    assert.match(result.auditBrief, /markgromer\/reggie-hub/);
    assert.match(result.codexPrompt, /Reggie Hub/);
    assert.deepEqual(
      result.operation.metadata.extra.projectOperator.githubAudit.repos.some((repo) => repo.fullName === 'markgromer/reggie-hub'),
      true,
    );
    assert.ok(result.operation.metadata.extra.projectOperator.githubAudit.files.length >= 2);
  });
});
