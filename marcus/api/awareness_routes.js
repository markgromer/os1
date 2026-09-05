import express from 'express';

function errorStatus(error) {
  if (['AWARENESS_PROJECT_NOT_FOUND', 'PROJECT_REGISTRY_NOT_FOUND'].includes(error?.code)) return 404;
  if (error?.code === 'CORRUPT_AWARENESS_STORE') return 503;
  return 400;
}

function asyncRoute(handler) {
  return async (req, res) => {
    try { await handler(req, res); }
    catch (error) {
      res.status(errorStatus(error)).json({
        ok: false,
        error: error?.message || 'Marcus awareness request failed.',
        code: error?.code || 'AWARENESS_ERROR',
      });
    }
  };
}

export function registerAwarenessRoutes(app, { service, getBusinessKey }) {
  if (!app || !service || typeof getBusinessKey !== 'function') throw new Error('registerAwarenessRoutes requires app, service, and getBusinessKey.');
  const router = express.Router();
  const business = (req) => getBusinessKey(req);

  router.post('/marcus/awareness/worklist', asyncRoute(async (req, res) => {
    const preference = await service.store.setWorklistPreference(business(req), req.body || {});
    res.json({ ok: true, preference });
  }));

  router.get('/marcus/awareness', asyncRoute(async (req, res) => {
    const result = await service.feed(business(req), {
      includeArchived: req.query.includeArchived === 'true',
      query: req.query.q,
    });
    res.json({ ok: true, ...result });
  }));

  router.get('/marcus/awareness/search', asyncRoute(async (req, res) => {
    const projects = await service.search(business(req), req.query.q, { limit: req.query.limit });
    res.json({ ok: true, businessKey: business(req), projects });
  }));

  router.get('/marcus/awareness/projects/:id', asyncRoute(async (req, res) => {
    const project = await service.detail(business(req), req.params.id);
    res.json({ ok: true, businessKey: business(req), project });
  }));

  router.post('/marcus/awareness/projects/:id/lifecycle', asyncRoute(async (req, res) => {
    const project = await service.setLifecycle(business(req), req.params.id, req.body?.lifecycle, {
      actor: 'mark',
      reason: req.body?.reason,
      source: 'authenticated_awareness_api',
    });
    res.json({ ok: true, businessKey: business(req), project });
  }));

  router.post('/marcus/awareness/projects/:id/refresh', asyncRoute(async (req, res) => {
    const project = await service.refreshMemory(business(req), req.params.id, { createRootNote: req.body?.createRootNote !== false });
    res.json({ ok: true, businessKey: business(req), project });
  }));

  router.get('/marcus/awareness/projects/:id/context', asyncRoute(async (req, res) => {
    const context = await service.projectContext(business(req), req.params.id, { maxChars: req.query.maxChars });
    res.json({ ok: true, businessKey: business(req), ...context });
  }));

  app.use('/api', router);
  return router;
}
