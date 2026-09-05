import express from 'express';

export function workRoute(handler) {
  return async (req, res) => {
    try { await handler(req, res); }
    catch (error) {
      const code = error.code || 'WORK_ERROR';
      const status = /NOT_FOUND/.test(code) ? 404 : /FORBIDDEN|SCOPE|AUTH/.test(code) ? 403 : /CORRUPT|BUSY|CAPACITY/.test(code) ? 503 : 409;
      res.status(status).json({ ok: false, code, error: error.message });
    }
  };
}

export function registerWorkRoutes(app, { graph, context, memory, director, identities, execution, operator, getBusinessKey }) {
  const router = express.Router();
  router.get('/', workRoute(async (req, res) => res.json({ ok: true, ...await graph.snapshot(getBusinessKey(req), String(req.query.projectId || '')) })));
  router.post('/', workRoute(async (req, res) => res.status(201).json({ ok: true, item: await graph.create(getBusinessKey(req), req.body || {}, 'mark') })));
  router.post('/dependencies', workRoute(async (req, res) => res.json({ ok: true, dependency: await graph.addDependency(getBusinessKey(req), req.body || {}) })));
  router.post('/reconcile', workRoute(async (req, res) => res.json({ ok: true, changed: await graph.reconcile(getBusinessKey(req)) })));
  if (director) {
    router.get('/engineering', workRoute(async (req, res) => res.json({ ok: true, agent: await director.store.read(getBusinessKey(req)) })));
    router.post('/engineering/configure', workRoute(async (req, res) => res.json({ ok: true, agent: await director.configure(getBusinessKey(req), req.body || {}) })));
    router.post('/:id/supervise', workRoute(async (req, res) => res.json({ ok: true, assignment: await director.supervise(getBusinessKey(req), req.params.id, { start: req.body?.start !== false }) })));
  }
  if (identities) {
    router.post('/identities/issue', workRoute(async (req, res) => { res.set('Cache-Control', 'no-store'); res.status(201).json({ ok: true, ...await identities.issue(getBusinessKey(req), req.body || {}) }); }));
    router.post('/identities/:id/revoke', workRoute(async (req, res) => res.json({ ok: true, grant: await identities.revoke(getBusinessKey(req), req.params.id) })));
    router.post('/:id/assign', workRoute(async (req, res) => res.json({ ok: true, item: await identities.assign(getBusinessKey(req), req.params.id, req.body?.identityId) })));
    router.post('/:id/accept', workRoute(async (req, res) => res.json({ ok: true, item: await identities.accept(getBusinessKey(req), req.params.id, req.body || {}) })));
  }
  if (execution) {
    router.get('/execution', workRoute(async (req, res) => res.json({ ok: true, execution: await execution.store.read(getBusinessKey(req)) })));
    router.post('/execution/policy', workRoute(async (req, res) => res.json({ ok: true, policy: await execution.setPolicy(getBusinessKey(req), req.body?.projectId, req.body?.autoAdvance) })));
    router.post('/execution/schedules', workRoute(async (req, res) => res.json({ ok: true, schedule: await execution.schedule(getBusinessKey(req), req.body || {}) })));
  }
  if (operator) {
    router.get('/operator/summary', workRoute(async (req, res) => res.json({ ok: true, summary: await operator.summary(getBusinessKey(req), { since: String(req.query.since || '') }) })));
    router.post('/operator/presence', workRoute(async (req, res) => res.json({ ok: true, presence: await operator.setPresence(getBusinessKey(req), req.body || {}) })));
    router.get('/operator/digests', workRoute(async (req, res) => { const doc = await operator.store.read(getBusinessKey(req)); res.json({ ok: true, digests: doc.digests.slice(-30) }); }));
  }
  if (context && memory) {
    router.post('/:id/context', workRoute(async (req, res) => res.json({ ok: true, packet: await context.prepare(getBusinessKey(req), req.params.id, req.body || {}) })));
    router.post('/decisions/:id/supersede', workRoute(async (req, res) => {
      const key = getBusinessKey(req);
      const result = await memory.supersedeDecision(key, req.params.id, req.body?.replacement || {}, { actor: 'mark', expectedRevision: req.body?.revision });
      const affected = await context.invalidate(key);
      res.json({ ok: true, ...result, affected });
    }));
  }
  router.get('/:id/impact', workRoute(async (req, res) => res.json({ ok: true, affected: await graph.impact(getBusinessKey(req), req.params.id) })));
  router.post('/:id/launch', workRoute(async (req, res) => res.json({ ok: true, item: await graph.launch(getBusinessKey(req), req.params.id, { start: req.body?.start === true }) })));
  router.post('/:id/cancel', workRoute(async (req, res) => res.json({ ok: true, item: await graph.cancel(getBusinessKey(req), req.params.id, req.body?.reason) })));
  app.use('/api/work', router);
}
