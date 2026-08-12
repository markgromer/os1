import { safeInteger, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';

export const GITHUB_READ_ACTIONS = new Set([
  'repository_metadata', 'default_branch', 'branch_metadata', 'commit_metadata', 'repository_file',
  'compare_refs', 'pull_request_metadata', 'workflow_status',
]);

export const GITHUB_WRITE_ACTIONS = new Set(['merge_pull_request']);

function safeRef(value) {
  const ref = safeString(value, 240);
  return ref && /^[A-Za-z0-9._/-]+$/.test(ref) && !ref.includes('..') && !ref.startsWith('/') ? ref : '';
}

function safeRepoPath(value) {
  const candidate = safeString(value, 1_000).replaceAll('\\', '/');
  if (!candidate || candidate.startsWith('/') || candidate.includes('\0')) return '';
  const segments = candidate.split('/');
  return segments.every((segment) => segment && segment !== '.' && segment !== '..') ? candidate : '';
}

export class GitHubReadProvider {
  constructor({ readAdapter = null } = {}) {
    this.readAdapter = typeof readAdapter === 'function' ? readAdapter : null;
  }

  async execute({ operation, step, registryRecord, idempotencyKey }) {
    if (!this.readAdapter) return { status: 'failed', error: 'GitHub read integration is not configured.' };
    const action = safeString(step.toolName, 100);
    if (!GITHUB_READ_ACTIONS.has(action)) return { status: 'failed', error: `GitHub read action is not allowlisted: ${action || '(missing)'}.` };
    const repository = safeString(registryRecord?.repo?.fullName, 500);
    if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository)) return { status: 'failed', error: 'No valid GitHub repository is registered for this project.' };
    const raw = safeObject(step.input);
    const input = { limit: safeInteger(raw.limit, 50, 1, 100) };
    if (['branch_metadata', 'commit_metadata', 'repository_file', 'workflow_status'].includes(action)) input.ref = safeRef(raw.ref);
    if (action === 'repository_file') {
      input.path = safeRepoPath(raw.path);
      if (!input.path) return { status: 'failed', error: 'A safe repository-relative file path is required.' };
    }
    if (action === 'compare_refs') {
      input.base = safeRef(raw.base); input.head = safeRef(raw.head);
      if (!input.base || !input.head) return { status: 'failed', error: 'Safe base and head refs are required.' };
    }
    if (action === 'pull_request_metadata') {
      input.pullNumber = safeInteger(raw.pullNumber || raw.id, 0, 1, 1_000_000_000);
      if (!input.pullNumber) return { status: 'failed', error: 'A valid pull request number is required.' };
    }
    const result = await this.readAdapter({
      repository, action, input, businessKey: operation.businessKey, projectRegistryId: registryRecord.id,
      operationId: operation.id, idempotencyKey: safeString(idempotencyKey, 240),
    });
    return { status: 'completed', output: sanitizeStructured(result, 40_000) };
  }
}

export class GitHubWriteProvider {
  constructor({ writeAdapter = null } = {}) {
    this.writeAdapter = typeof writeAdapter === 'function' ? writeAdapter : null;
  }

  async execute({ operation, step, registryRecord, idempotencyKey }) {
    if (!this.writeAdapter) return { status: 'failed', error: 'GitHub write integration is not configured.' };
    const action = safeString(step.toolName, 100);
    if (!GITHUB_WRITE_ACTIONS.has(action)) return { status: 'failed', error: `GitHub write action is not allowlisted: ${action || '(missing)'}.` };
    const repository = safeString(registryRecord?.repo?.fullName, 500);
    if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository)) return { status: 'failed', error: 'No valid GitHub repository is registered for this project.' };
    const raw = safeObject(step.input);
    const input = {
      pullNumber: safeInteger(raw.pullNumber, 0, 1, 1_000_000_000),
      expectedHeadSha: safeString(raw.expectedHeadSha, 40).toLowerCase(),
      mergeMethod: ['merge', 'squash', 'rebase'].includes(safeString(raw.mergeMethod, 20).toLowerCase())
        ? safeString(raw.mergeMethod, 20).toLowerCase()
        : 'squash',
      commitTitle: safeString(raw.commitTitle, 240),
      commitMessage: safeString(raw.commitMessage, 2_000),
    };
    if (!input.pullNumber) return { status: 'failed', error: 'A valid pull request number is required.' };
    if (!/^[a-f0-9]{40}$/.test(input.expectedHeadSha)) return { status: 'failed', error: 'The exact expected 40-character pull request head SHA is required.' };
    const result = await this.writeAdapter({
      repository, action, input, businessKey: operation.businessKey, projectRegistryId: registryRecord.id,
      operationId: operation.id, idempotencyKey: safeString(idempotencyKey, 240),
    });
    if (result?.verified !== true) return { status: 'failed', error: 'GitHub did not return authoritative post-action verification.' };
    return { status: 'completed', output: sanitizeStructured(result, 40_000) };
  }
}

export { safeRef as validateGitHubRef, safeRepoPath as validateRepositoryPath };
