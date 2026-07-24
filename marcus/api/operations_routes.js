import express from 'express';

function errorStatus(error) {
  const code = error?.code || '';
  if (['OPERATION_NOT_FOUND', 'PROJECT_REGISTRY_NOT_FOUND', 'APPROVAL_NOT_FOUND', 'VERIFICATION_NOT_FOUND'].includes(code)) return 404;
  if (['REVISION_MISMATCH', 'STORE_REVISION_MISMATCH', 'STEP_REVISION_CHANGED'].includes(code)) return 409;
  if (['INVALID_TRANSITION', 'OPERATION_NOT_PLANNED', 'PROJECT_UNRESOLVED', 'OPERATION_STILL_BLOCKED', 'RETRY_LIMIT_REACHED', 'STEP_NOT_RETRYABLE', 'STRONG_CONFIRMATION_REQUIRED', 'WAIVER_APPROVAL_REQUIRED'].includes(code)) return 409;
  if (['CORRUPT_OPERATIONS_STORE', 'CORRUPT_PROJECT_REGISTRY'].includes(code)) return 503;
  return 400;
}

function asyncRoute(handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (error) {
      res.status(errorStatus(error)).json({ ok: false, error: error?.message || 'Operation request failed.', code: error?.code || 'OPERATION_ERROR', currentRevision: error?.currentRevision });
    }
  };
}

export function registerOperationsRoutes(app, { engine, getBusinessKey }) {
  if (!app || !engine || typeof getBusinessKey !== 'function') throw new Error('registerOperationsRoutes requires app, engine, and getBusinessKey.');
  const router = express.Router();
  const business = (req) => getBusinessKey(req);

  router.get('/operations', asyncRoute(async (req, res) => {
    const operations = await engine.listOperations(business(req), {
      status: req.query.status,
      projectId: req.query.projectId,
      projectRegistryId: req.query.projectRegistryId,
      limit: req.query.limit,
    });
    res.json({ ok: true, businessKey: business(req), operations });
  }));

  router.post('/operations', asyncRoute(async (req, res) => {
    const result = await engine.createFromRequest(business(req), req.body || {});
    res.status(201).json({ ok: true, ...result });
  }));

  router.get('/operations/:id', asyncRoute(async (req, res) => {
    const operation = await engine.getOperation(business(req), req.params.id);
    if (!operation) throw Object.assign(new Error('Operation not found.'), { code: 'OPERATION_NOT_FOUND' });
    res.json({ ok: true, operation });
  }));

  router.patch('/operations/:id', asyncRoute(async (req, res) => {
    const operation = await engine.updateOperation(business(req), req.params.id, req.body?.patch || req.body || {}, {
      expectedRevision: req.body?.revision,
      actor: req.body?.updatedBy || 'mark',
    });
    res.json({ ok: true, operation });
  }));

  for (const [path, method] of [
    ['plan', 'planOperation'], ['start', 'startOperation'], ['pause', 'pauseOperation'], ['resume', 'resumeOperation'],
    ['cancel', 'cancelOperation'], ['retry', 'retryOperation'],
  ]) {
    router.post(`/operations/:id/${path}`, asyncRoute(async (req, res) => {
      const operation = await engine[method](business(req), req.params.id, req.body || {});
      res.json({ ok: true, operation });
    }));
  }

  router.post('/operations/:id/tick', asyncRoute(async (req, res) => {
    const operation = await engine.tick(business(req), req.params.id);
    res.json({ ok: true, operation });
  }));

  router.post('/operations/:id/approvals/:approvalId/approve', asyncRoute(async (req, res) => {
    const operation = await engine.approveOperationStep(business(req), req.params.id, req.params.approvalId, req.body || {});
    res.json({ ok: true, operation });
  }));

  router.post('/operations/:id/approvals/:approvalId/reject', asyncRoute(async (req, res) => {
    const operation = await engine.rejectOperationStep(business(req), req.params.id, req.params.approvalId, req.body || {});
    res.json({ ok: true, operation });
  }));

  router.post('/operations/:id/external-job', asyncRoute(async (req, res) => {
    const operation = await engine.registerExternalCodexJob(business(req), req.params.id, req.body || {});
    res.json({ ok: true, operation });
  }));

  router.post('/operations/:id/verification-results', asyncRoute(async (req, res) => {
    const operation = await engine.registerVerificationResults(business(req), req.params.id, req.body?.results || [], { actor: req.body?.actor || 'mark' });
    res.json({ ok: true, operation });
  }));

  router.post('/operations/:id/verification/:verificationId/waive', asyncRoute(async (req, res) => {
    const operation = await engine.waiveVerification(business(req), req.params.id, req.params.verificationId, req.body || {});
    res.json({ ok: true, operation });
  }));

  router.get('/project-registry', asyncRoute(async (req, res) => {
    const projects = await engine.listProjectRegistry(business(req));
    res.json({ ok: true, businessKey: business(req), projects });
  }));

  router.post('/project-registry', asyncRoute(async (req, res) => {
    const project = await engine.createProjectRegistryRecord(business(req), req.body?.project || req.body || {});
    res.status(201).json({ ok: true, project });
  }));

  router.patch('/project-registry/:id', asyncRoute(async (req, res) => {
    const project = await engine.updateProjectRegistryRecord(business(req), req.params.id, req.body?.patch || req.body || {});
    res.json({ ok: true, project });
  }));

  router.post('/project-registry/resolve', asyncRoute(async (req, res) => {
    const request = String(req.body?.request || req.body?.query || '').trim();
    if (!request) return res.status(400).json({ ok: false, error: 'request is required.' });
    const resolution = await engine.resolveProject(business(req), request, req.body?.context || {});
    res.json({ ok: true, resolution });
  }));

  app.use('/api', router);
  return router;
}
