import express from 'express';

function asyncRoute(handler) {
  return async (req, res) => {
    try { await handler(req, res); }
    catch (error) {
      const status = ['ATTENTION_NOT_FOUND', 'OUTCOME_NOT_FOUND'].includes(error?.code) ? 404 : 400;
      res.status(status).json({ ok: false, error: error?.message || 'Nervous-system request failed.', code: error?.code || 'NERVOUS_SYSTEM_ERROR' });
    }
  };
}

export function registerNervousSystemRoutes(app, { attentionStore, outcomeLedger, signalJournal, getBusinessKey, getLoopHealth, triggerLoop } = {}) {
  if (!app || !attentionStore || !outcomeLedger || !signalJournal || typeof getBusinessKey !== 'function') throw new Error('registerNervousSystemRoutes requires stores and getBusinessKey.');
  const router = express.Router();
  const business = (req) => getBusinessKey(req);

  router.get('/marcus/nervous-system/status', asyncRoute(async (req, res) => {
    const businessKey = business(req);
    const [attention, signals, outcomes] = await Promise.all([
      attentionStore.list(businessKey, { status: 'open', limit: 25 }),
      signalJournal.recent(businessKey, { limit: 25 }),
      outcomeLedger.list(businessKey, { limit: 25 }),
    ]);
    res.json({ ok: true, businessKey, loop: typeof getLoopHealth === 'function' ? getLoopHealth() : null, attention, signals, outcomes });
  }));

  router.get('/marcus/attention', asyncRoute(async (req, res) => res.json({ ok: true, businessKey: business(req), items: await attentionStore.list(business(req), { status: req.query.status, owner: req.query.owner, limit: req.query.limit }) })));
  router.patch('/marcus/attention/:id', asyncRoute(async (req, res) => res.json({ ok: true, businessKey: business(req), item: await attentionStore.transition(business(req), req.params.id, req.body?.status, { resolution: req.body?.resolution, deferUntil: req.body?.deferUntil }) })));
  router.get('/marcus/outcomes', asyncRoute(async (req, res) => res.json({ ok: true, businessKey: business(req), entries: await outcomeLedger.list(business(req), { traceId: req.query.traceId, status: req.query.status, limit: req.query.limit }) })));
  router.patch('/marcus/outcomes/:id/correction', asyncRoute(async (req, res) => res.json({ ok: true, businessKey: business(req), outcome: await outcomeLedger.correct(business(req), req.params.id, req.body?.correction, { reusable: req.body?.reusable === true }) })));
  router.post('/marcus/nervous-system/tick', asyncRoute(async (_req, res) => res.json({ ok: true, result: typeof triggerLoop === 'function' ? await triggerLoop() : null })));

  app.use('/api', router);
  return router;
}
