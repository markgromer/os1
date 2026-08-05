export class BrowserVerificationProvider {
  constructor({ adapter = null, mode = 'external_manual' } = {}) {
    this.adapter = adapter;
    this.mode = adapter ? 'direct' : mode;
  }

  async invoke(method, ...args) {
    if (!this.adapter || typeof this.adapter[method] !== 'function') {
      return { status: 'external_required', mode: 'external_manual', verified: false };
    }
    return this.adapter[method](...args);
  }

  startVerification(input) { return this.invoke('startVerification', input); }
  getVerificationStatus(input) { return this.invoke('getVerificationStatus', input); }
  getScreenshots(input) { return this.invoke('getScreenshots', input); }
  getConsoleErrors(input) { return this.invoke('getConsoleErrors', input); }
  getNetworkErrors(input) { return this.invoke('getNetworkErrors', input); }
  getAccessibilityResults(input) { return this.invoke('getAccessibilityResults', input); }
  getInteractionResults(input) { return this.invoke('getInteractionResults', input); }
  cancelVerification(input) { return this.invoke('cancelVerification', input); }
}
