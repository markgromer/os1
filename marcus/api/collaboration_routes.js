import express from 'express';
import { workRoute } from './work_routes.js';

// Mount before the owner's admin gate. This closed router never calls next()
// for an unmatched collaboration URL, so a project token cannot reach owner APIs.
export function registerCollaborationRoutes(app, { identities, getBusinessKey }) {
  const router = express.Router();
  router.use(async (req, res, next) => {
    res.set('Cache-Control', 'no-store');
    try { req.collaborator = await identities.authenticate(getBusinessKey(req), String(req.headers.authorization || '').replace(/^Bearer /i, '')); next(); }
    catch { res.status(401).json({ ok: false, code: 'AUTH_REQUIRED', error: 'A current project credential is required.' }); }
  });
  router.get('/work', workRoute(async (req, res) => res.json({ ok: true, ...await identities.projectView(getBusinessKey(req), req.collaborator) })));
  router.post('/work/:id/submit', workRoute(async (req, res) => res.json({ ok: true, submission: await identities.submit(getBusinessKey(req), req.collaborator, req.params.id, req.body || {}) })));
  router.use((req, res) => res.status(403).json({ ok: false, code: 'IDENTITY_FORBIDDEN', error: 'Project credentials cannot perform owner actions.' }));
  app.use('/api/collaboration', router);
}
