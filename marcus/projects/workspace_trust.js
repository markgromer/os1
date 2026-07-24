import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function canonicalExistingPath(value) {
  const candidate = String(value || '').trim();
  if (!candidate) throw Object.assign(new Error('Workspace path is required.'), { code: 'WORKSPACE_PATH_REQUIRED' });
  let canonical;
  try { canonical = fs.realpathSync.native(path.resolve(candidate)); }
  catch { throw Object.assign(new Error('Workspace path does not exist.'), { code: 'WORKSPACE_NOT_FOUND' }); }
  if (!fs.statSync(canonical).isDirectory()) throw Object.assign(new Error('Workspace path is not a directory.'), { code: 'WORKSPACE_NOT_DIRECTORY' });
  return canonical;
}

function normalizedForCompare(value) {
  const resolved = path.resolve(value);
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

function isWithin(root, candidate) {
  const relative = path.relative(normalizedForCompare(root), normalizedForCompare(candidate));
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function broadRoots() {
  const home = path.resolve(os.homedir());
  return new Set([
    path.parse(home).root,
    home,
    path.join(home, 'Documents'),
    path.join(home, 'OneDrive'),
    path.join(home, 'OneDrive', 'Documents'),
  ].map(normalizedForCompare));
}

export function validateAllowedWorkspaceRoots(values) {
  const roots = [];
  for (const value of Array.isArray(values) ? values : []) {
    const canonical = canonicalExistingPath(value);
    if (broadRoots().has(normalizedForCompare(canonical))) {
      throw Object.assign(new Error(`Workspace root is too broad: ${canonical}`), { code: 'WORKSPACE_ROOT_TOO_BROAD' });
    }
    if (!roots.some((root) => normalizedForCompare(root) === normalizedForCompare(canonical))) roots.push(canonical);
  }
  if (!roots.length) throw Object.assign(new Error('At least one safe allowed workspace root is required.'), { code: 'WORKSPACE_ROOTS_REQUIRED' });
  return roots;
}

export function validateTrustedWorkspace({ workspacePath, allowedRoots = [], registeredPath = '' } = {}) {
  const roots = validateAllowedWorkspaceRoots(allowedRoots);
  const canonical = canonicalExistingPath(workspacePath);
  if (!roots.some((root) => isWithin(root, canonical))) {
    throw Object.assign(new Error('Workspace is outside every allowed root.'), { code: 'WORKSPACE_OUTSIDE_ALLOWED_ROOT' });
  }
  if (registeredPath) {
    const registered = canonicalExistingPath(registeredPath);
    if (normalizedForCompare(registered) !== normalizedForCompare(canonical)) {
      throw Object.assign(new Error('Workspace does not match the registered project path.'), { code: 'WORKSPACE_REGISTRY_MISMATCH' });
    }
  }
  return canonical;
}

export { canonicalExistingPath, isWithin };
