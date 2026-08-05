import express from 'express';

function errorStatus(error) {
  if (['PROJECT_REGISTRY_NOT_FOUND', 'EVIDENCE_NOT_FOUND'].includes(error?.code)) return 404;
  if (['CORRUPT_PROJECT_EVIDENCE_STORE'].includes(error?.code)) return 503;
  if (['EVIDENCE_SOURCE_IMPERSONATION', 'EVIDENCE_TYPE_IMPERSONATION'].includes(error?.code)) return 403;
  return 400;
}
function asyncRoute(handler) {
  return async (req, res) => {
    try { await handler(req, res); }
    catch (error) {
      res.status(errorStatus(error)).json({ ok: false, error: error?.message || 'Project evidence request failed.', code: error?.code || 'PROJECT_EVIDENCE_ERROR' });
    }
  };
}

export function registerProjectEvidenceRoutes(app, { service, getBusinessKey }) {
  if (!app || !service || typeof getBusinessKey !== 'function') throw new Error('registerProjectEvidenceRoutes requires app, service, and getBusinessKey.');
  const router = express.Router();
  const business = (req) => getBusinessKey(req);

  router.get('/project-evidence', asyncRoute(async (req, res) => {
    const evidence = await service.listEvidence(business(req), {
      projectRegistryId: req.query.projectRegistryId,
      source: req.query.source,
      type: req.query.type,
      since: req.query.since,
      limit: req.query.limit,
    });
    res.json({ ok: true, businessKey: business(req), evidence });
  }));

  router.post('/project-evidence/refresh', asyncRoute(async (req, res) => {
    const result = await service.refresh(business(req), {
      force: req.body?.force === true,
      sources: Array.isArray(req.body?.sources) ? req.body.sources : null,
    });
    res.json({ ok: true, businessKey: business(req), result });
  }));

  router.post('/project-evidence/ingest', asyncRoute(async (req, res) => {
    const result = await service.ingestManual(business(req), req.body || {});
    res.status(201).json({ ok: true, businessKey: business(req), evidence: result.accepted, duplicateCount: result.duplicateCount });
  }));

  router.post('/project-evidence/browser-verification', asyncRoute(async (req, res) => {
    const result = await service.ingestBrowserResult(business(req), req.body || {});
    res.status(201).json({ ok: true, businessKey: business(req), evidence: result.accepted, duplicateCount: result.duplicateCount, mode: 'external_manual' });
  }));

  router.get('/project-evidence/:projectRegistryId', asyncRoute(async (req, res) => {
    const evidence = await service.getProjectEvidence(business(req), req.params.projectRegistryId, {
      source: req.query.source, type: req.query.type, since: req.query.since, limit: req.query.limit,
    });
    res.json({ ok: true, businessKey: business(req), projectRegistryId: req.params.projectRegistryId, evidence });
  }));

  router.get('/project-activity', asyncRoute(async (req, res) => {
    const activity = await service.getActivity(business(req), { recalculate: req.query.recalculate === 'true' });
    res.json({ ok: true, businessKey: business(req), ...activity });
  }));

  router.get('/project-activity/current-focus', asyncRoute(async (req, res) => {
    const activity = await service.getActivity(business(req));
    res.json({ ok: true, businessKey: business(req), currentFocus: activity.currentFocus || null });
  }));

  router.get('/project-activity/stale', asyncRoute(async (req, res) => {
    const activity = await service.getActivity(business(req));
    res.json({ ok: true, businessKey: business(req), projects: activity.stale || [] });
  }));

  router.get('/project-activity/bottlenecks', asyncRoute(async (req, res) => {
    const activity = await service.getActivity(business(req));
    res.json({ ok: true, businessKey: business(req), projects: activity.bottlenecks || [] });
  }));

  router.post('/project-activity/recalculate', asyncRoute(async (req, res) => {
    const activity = await service.recalculate(business(req));
    res.json({ ok: true, businessKey: business(req), ...activity });
  }));

  router.get('/project-activity/:projectRegistryId', asyncRoute(async (req, res) => {
    const activity = await service.getProjectActivity(business(req), req.params.projectRegistryId, { recalculate: req.query.recalculate === 'true' });
    res.json({ ok: true, businessKey: business(req), activity });
  }));

  app.use('/api', router);
  return router;
}
