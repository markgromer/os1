import { safeInteger, safeObject, safeString, sanitizeStructured } from '../operations/operation_types.js';

export const CLOUDFLARE_WRITE_ACTIONS = new Set([
  'upsert_dns_record', 'delete_dns_record', 'deploy_worker_version',
]);

const DNS_TYPES = new Set(['A', 'AAAA', 'CAA', 'CNAME', 'MX', 'NS', 'SRV', 'TXT']);

function safeCloudflareId(value) {
  const id = safeString(value, 64);
  return /^[A-Za-z0-9_-]{16,64}$/.test(id) ? id : '';
}

function safeHostname(value) {
  const name = safeString(value, 255).toLowerCase().replace(/\.$/, '');
  return name && name.length <= 253 && name.split('.').every((label) => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label)) ? name : '';
}

function safeDnsName(value) {
  const name = safeString(value, 255).toLowerCase().replace(/\.$/, '');
  return name && name.length <= 253 && name.split('.').every((label) => /^[a-z0-9_](?:[a-z0-9_-]{0,61}[a-z0-9_])?$/.test(label)) ? name : '';
}

function workerMatchesRegisteredDeployment(scriptName, deployments) {
  if (safeString(deployments.cloudflareProject, 300).toLowerCase() === scriptName.toLowerCase()) return true;
  try {
    const hostname = new URL(deployments.productionUrl).hostname.toLowerCase();
    return hostname.endsWith('.workers.dev') && hostname.split('.')[0] === scriptName.toLowerCase();
  } catch {
    return false;
  }
}

export class CloudflareWriteProvider {
  constructor({ writeAdapter = null } = {}) {
    this.writeAdapter = typeof writeAdapter === 'function' ? writeAdapter : null;
  }

  async execute({ operation, step, registryRecord, idempotencyKey }) {
    if (!this.writeAdapter) return { status: 'failed', error: 'Cloudflare write integration is not configured.' };
    const action = safeString(step.toolName, 100);
    if (!CLOUDFLARE_WRITE_ACTIONS.has(action)) return { status: 'failed', error: `Cloudflare write action is not allowlisted: ${action || '(missing)'}.` };
    const raw = safeObject(step.input);
    const deployments = safeObject(registryRecord?.deployments);
    let input;

    if (action === 'deploy_worker_version') {
      const scriptName = safeString(raw.scriptName, 200).toLowerCase();
      input = {
        accountId: safeCloudflareId(raw.accountId || deployments.cloudflareAccountId),
        scriptName: /^[a-z0-9][a-z0-9_-]{0,62}$/.test(scriptName) ? scriptName : '',
        versionId: safeString(raw.versionId, 64).toLowerCase(),
        expectedCurrentDeploymentId: safeString(raw.expectedCurrentDeploymentId, 64).toLowerCase(),
        message: safeString(raw.message, 1_000),
      };
      if (!input.accountId || !input.scriptName || !/^[a-f0-9-]{32,64}$/.test(input.versionId) || !/^[a-f0-9-]{32,64}$/.test(input.expectedCurrentDeploymentId)) {
        return { status: 'failed', error: 'A registered Cloudflare account, valid Worker script, target version, and exact current deployment ID are required.' };
      }
      if (!workerMatchesRegisteredDeployment(input.scriptName, deployments)) {
        return { status: 'failed', error: 'The Worker script is not bound to this project registry record.' };
      }
    } else {
      const type = safeString(raw.recordType || raw.type, 16).toUpperCase();
      input = {
        zoneId: safeCloudflareId(raw.zoneId || deployments.cloudflareZoneId),
        zoneName: safeHostname(deployments.cloudflareZoneName),
        productionUrl: safeString(deployments.productionUrl, 2_000),
        recordId: safeCloudflareId(raw.recordId),
        recordType: DNS_TYPES.has(type) ? type : '',
        name: safeDnsName(raw.name),
        content: safeString(raw.content, 4_000),
        ttl: safeInteger(raw.ttl, 1, 1, 86_400),
        proxied: raw.proxied === true,
        priority: safeInteger(raw.priority, 0, 0, 65_535),
        comment: safeString(raw.comment, 500),
      };
      if (!input.zoneId || !input.recordType || !input.name || !input.content) {
        return { status: 'failed', error: 'A registered zone plus the exact DNS type, name, and content are required.' };
      }
      if (action === 'delete_dns_record' && !input.recordId) {
        return { status: 'failed', error: 'Deleting DNS requires the exact record ID and expected record values.' };
      }
    }

    const result = await this.writeAdapter({
      action, input, businessKey: operation.businessKey, projectRegistryId: registryRecord.id,
      operationId: operation.id, idempotencyKey: safeString(idempotencyKey, 240),
      registryTarget: {
        deployments: sanitizeStructured(deployments, 8_000),
        projectName: safeString(registryRecord?.canonicalName, 300),
      },
    });
    if (result?.verified !== true) return { status: 'failed', error: 'Cloudflare did not return authoritative post-action verification.' };
    return { status: 'completed', output: sanitizeStructured(result, 40_000) };
  }
}

export { safeCloudflareId, safeDnsName, safeHostname, workerMatchesRegisteredDeployment };
