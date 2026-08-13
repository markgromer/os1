function requiresDesktop(job = {}) {
  return job.providerMode === 'desktop_codex'
    || job.provider === 'desktop_codex'
    || (Boolean(job.workspacePath) && Boolean(job.desktopAgentId));
}

export class RoutedCodexAdapter {
  constructor({ desktopAdapter = null, fallbackAdapter = null } = {}) {
    if (!desktopAdapter && !fallbackAdapter) throw new Error('RoutedCodexAdapter requires at least one adapter.');
    this.providerName = desktopAdapter && fallbackAdapter
      ? 'desktop_codex_with_fallback'
      : (desktopAdapter?.providerName || fallbackAdapter?.providerName || 'direct_codex');
    this.desktopAdapter = desktopAdapter;
    this.fallbackAdapter = fallbackAdapter;
    if (typeof desktopAdapter?.collectTargetEvidence === 'function' || typeof fallbackAdapter?.collectTargetEvidence === 'function') {
      this.collectTargetEvidence = (job) => {
        const adapter = this.adapterFor(job);
        return typeof adapter?.collectTargetEvidence === 'function' ? adapter.collectTargetEvidence(job) : null;
      };
    }
  }

  adapterFor(job, { launch = false } = {}) {
    if (requiresDesktop(job)) {
      if (!this.desktopAdapter) throw new Error('This operation requires local Codex, but the desktop adapter is disabled.');
      return this.desktopAdapter;
    }
    if (this.fallbackAdapter) return this.fallbackAdapter;
    if (launch) throw new Error('No direct Codex adapter can execute a project without an attested desktop workspace.');
    return this.desktopAdapter;
  }

  startJob(job, options) {
    return this.adapterFor(job, { launch: true }).startJob(job, options);
  }

  getJobStatus(job) {
    return this.adapterFor(job).getJobStatus(job);
  }

  getArtifacts(job) {
    return this.adapterFor(job).getArtifacts(job);
  }

  getDiff(job) {
    return this.adapterFor(job).getDiff(job);
  }

  sendFollowup(job, message) {
    return this.adapterFor(job).sendFollowup(job, message);
  }

  cancelJob(job) {
    return this.adapterFor(job).cancelJob(job);
  }
}
