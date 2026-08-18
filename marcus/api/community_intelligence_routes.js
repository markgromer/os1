import express from 'express';

function errorStatus(error) {
  if (['COMMUNITY_MEMBER_NOT_FOUND', 'COMMUNITY_THREAD_NOT_FOUND', 'COMMUNITY_NOTIFICATION_NOT_FOUND'].includes(error?.code)) return 404;
  if (error?.code === 'CORRUPT_COMMUNITY_INTELLIGENCE_STORE') return 503;
  return 400;
}

function asyncRoute(handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (error) {
      res.status(errorStatus(error)).json({
        ok: false,
        error: error?.message || 'Community intelligence request failed.',
        code: error?.code || 'COMMUNITY_INTELLIGENCE_ERROR',
      });
    }
  };
}

export function registerCommunityIntelligenceRoutes(app, {
  store, getBusinessKey, queueProfileProjection,
}) {
  if (!app || !store || typeof getBusinessKey !== 'function') {
    throw new Error('registerCommunityIntelligenceRoutes requires app, store, and getBusinessKey.');
  }
  const router = express.Router();
  const business = (req) => getBusinessKey(req);

  router.get('/marcus/community/members', asyncRoute(async (req, res) => {
    const result = await store.listMembers(business(req), {
      query: req.query.q,
      platform: req.query.platform,
      community: req.query.community,
      limit: req.query.limit,
    });
    res.json({ ok: true, ...result });
  }));

  router.get('/marcus/community/members/:id', asyncRoute(async (req, res) => {
    const result = await store.getMember(business(req), req.params.id);
    res.json({ ok: true, businessKey: business(req), ...result });
  }));

  router.post('/marcus/community/observations', asyncRoute(async (req, res) => {
    const values = req.body?.observations || req.body?.observation || req.body;
    const result = await store.ingestObservations(business(req), values);
    const projections = [];
    if (typeof queueProfileProjection === 'function') {
      for (const member of result.members.slice(0, 50)) {
        projections.push(await queueProfileProjection(business(req), member.id));
      }
    }
    res.status(result.created ? 201 : 200).json({ ok: true, ...result, projections });
  }));

  router.get('/marcus/community/context', asyncRoute(async (req, res) => {
    const result = await store.getCommunityContext(business(req), {
      platform: req.query.platform,
      community: req.query.community,
      memberLimit: req.query.memberLimit,
      threadLimit: req.query.threadLimit,
    });
    res.json({ ok: true, ...result });
  }));

  router.post('/marcus/community/knowledge', asyncRoute(async (req, res) => {
    const result = await store.rememberKnowledge(business(req), req.body || {});
    res.status(result.created ? 201 : 200).json({ ok: true, ...result });
  }));

  router.get('/marcus/community/notifications', asyncRoute(async (req, res) => {
    const result = await store.listNotifications(business(req), {
      state: req.query.state,
      limit: req.query.limit,
    });
    res.json({ ok: true, ...result });
  }));

  router.post('/marcus/community/notifications', asyncRoute(async (req, res) => {
    const values = req.body?.notifications || req.body?.notification || req.body;
    const result = await store.ingestNotifications(business(req), values);
    res.status(result.created ? 201 : 200).json({ ok: true, ...result });
  }));

  router.post('/marcus/community/notifications/:id/transition', asyncRoute(async (req, res) => {
    const notification = await store.transitionNotification(business(req), req.params.id, req.body || {});
    res.json({ ok: true, notification });
  }));

  app.use('/api', router);
  return router;
}
