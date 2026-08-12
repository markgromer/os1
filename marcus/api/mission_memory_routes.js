import express from 'express';

function errorStatus(error) {
  if (error?.code === 'MISSION_MEMORY_NOT_FOUND') return 404;
  if (error?.code === 'CORRUPT_MISSION_MEMORY_STORE') return 503;
  return 400;
}

function asyncRoute(handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (error) {
      res.status(errorStatus(error)).json({
        ok: false,
        error: error?.message || 'Mission memory request failed.',
        code: error?.code || 'MISSION_MEMORY_ERROR',
      });
    }
  };
}

export function registerMissionMemoryRoutes(app, { store, getBusinessKey }) {
  if (!app || !store || typeof getBusinessKey !== 'function') {
    throw new Error('registerMissionMemoryRoutes requires app, store, and getBusinessKey.');
  }
  const router = express.Router();
  const business = (req) => getBusinessKey(req);

  router.get('/marcus/memory', asyncRoute(async (req, res) => {
    const result = await store.list(business(req), {
      status: req.query.status,
      kind: req.query.kind,
      query: req.query.q,
      limit: req.query.limit,
    });
    res.json({ ok: true, ...result });
  }));

  router.get('/marcus/memory/relevant', asyncRoute(async (req, res) => {
    const memories = await store.relevant(business(req), String(req.query.q || ''), { limit: req.query.limit });
    res.json({ ok: true, businessKey: business(req), memories });
  }));

  router.post('/marcus/memory', asyncRoute(async (req, res) => {
    const result = await store.add(business(req), req.body || {}, {
      actor: 'mark',
      source: 'authenticated_memory_api',
    });
    res.status(result.created ? 201 : 200).json({ ok: true, ...result });
  }));

  router.patch('/marcus/memory/:id', asyncRoute(async (req, res) => {
    const memory = await store.update(business(req), req.params.id, req.body?.patch || req.body || {}, {
      source: 'authenticated_memory_api',
    });
    res.json({ ok: true, memory });
  }));

  app.use('/api', router);
  return router;
}
