export class GitHubReadProvider {
  constructor({ readAdapter = null } = {}) {
    this.readAdapter = typeof readAdapter === 'function' ? readAdapter : null;
  }

  async execute({ operation, step, registryRecord }) {
    if (!this.readAdapter) return { status: 'failed', error: 'GitHub read integration is not configured.' };
    const repository = registryRecord?.repo?.fullName || '';
    if (!repository) return { status: 'failed', error: 'No GitHub repository is registered for this project.' };
    const result = await this.readAdapter({ repository, action: step.toolName, input: step.input, operationId: operation.id });
    return { status: 'completed', output: result };
  }
}
