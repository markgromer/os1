import { spawn } from 'node:child_process';

const DEFAULT_PROTOCOL_VERSION = '2024-11-05';

function encodeJsonRpc(message) {
  const json = JSON.stringify(message);
  const payload = Buffer.from(json, 'utf8');
  const header = Buffer.from(`Content-Length: ${payload.length}\r\n\r\n`, 'utf8');
  return Buffer.concat([header, payload]);
}

function parseHeader(headerText) {
  const headers = {};
  for (const line of headerText.split(/\r\n/)) {
    const idx = line.indexOf(':');
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim().toLowerCase();
    const value = line.slice(idx + 1).trim();
    headers[key] = value;
  }
  const lenRaw = headers['content-length'];
  const len = lenRaw ? Number(lenRaw) : NaN;
  return Number.isFinite(len) ? len : null;
}

function splitArgs(args) {
  if (Array.isArray(args)) return args.map((v) => String(v));
  const s = String(args || '').trim();
  if (!s) return [];

  // Minimal quoted-string arg parser: supports "..." and '...'.
  const out = [];
  let cur = '';
  let quote = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (quote) {
      if (ch === quote) {
        quote = '';
      } else if (ch === '\\' && i + 1 < s.length) {
        cur += s[i + 1];
        i++;
      } else {
        cur += ch;
      }
      continue;
    }

    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }

    if (/\s/.test(ch)) {
      if (cur) {
        out.push(cur);
        cur = '';
      }
      continue;
    }

    cur += ch;
  }
  if (cur) out.push(cur);
  return out;
}

class McpStdioClient {
  constructor(proc) {
    this.proc = proc;
    this.buffer = Buffer.alloc(0);
    this.pending = new Map();
    this.nextId = 1;

    proc.stdout.on('data', (chunk) => this.onData(chunk));
    proc.stderr.on('data', (chunk) => {
      // Best-effort: surface stderr to any pending request if everything fails.
      const msg = String(chunk || '');
      this.lastStderr = (this.lastStderr || '') + msg;
      if (this.lastStderr.length > 4000) this.lastStderr = this.lastStderr.slice(-4000);
    });

    proc.on('exit', (code, signal) => {
      const err = new Error(`MCP server exited (code=${code}, signal=${signal || ''})`);
      for (const { reject, timeoutId } of this.pending.values()) {
        clearTimeout(timeoutId);
        reject(err);
      }
      this.pending.clear();
    });
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    // MCP stdio uses Content-Length framing.
    while (true) {
      const headerEnd = this.buffer.indexOf('\r\n\r\n');
      if (headerEnd === -1) return;
      const header = this.buffer.slice(0, headerEnd).toString('utf8');
      const contentLength = parseHeader(header);
      if (!contentLength) {
        // Can't parse; drop up to headerEnd and continue.
        this.buffer = this.buffer.slice(headerEnd + 4);
        continue;
      }
      const messageStart = headerEnd + 4;
      const messageEnd = messageStart + contentLength;
      if (this.buffer.length < messageEnd) return;

      const payload = this.buffer.slice(messageStart, messageEnd).toString('utf8');
      this.buffer = this.buffer.slice(messageEnd);

      let msg;
      try {
        msg = JSON.parse(payload);
      } catch {
        continue;
      }

      if (msg && typeof msg === 'object' && msg.id != null) {
        const key = String(msg.id);
        const pending = this.pending.get(key);
        if (pending) {
          this.pending.delete(key);
          clearTimeout(pending.timeoutId);
          if (msg.error) pending.reject(Object.assign(new Error(msg.error.message || 'MCP error'), { data: msg.error }));
          else pending.resolve(msg.result);
        }
      }
    }
  }

  sendNotification(method, params) {
    const message = { jsonrpc: '2.0', method, params };
    this.proc.stdin.write(encodeJsonRpc(message));
  }

  sendRequest(method, params, { timeoutMs = 8000 } = {}) {
    const id = String(this.nextId++);
    const message = { jsonrpc: '2.0', id, method, params };

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.pending.delete(id);
        const extra = this.lastStderr ? `\nSTDERR: ${this.lastStderr}` : '';
        reject(new Error(`MCP request timeout (${method}).${extra}`));
      }, timeoutMs);

      this.pending.set(id, { resolve, reject, timeoutId });
      this.proc.stdin.write(encodeJsonRpc(message));
    });
  }

  async close() {
    try {
      this.proc.kill();
    } catch {}
  }
}

export async function withMcpClient(config, fn) {
  const command = String(config?.command || '').trim();
  if (!command) throw new Error('MCP command is not configured');
  const args = splitArgs(config?.args);
  const cwd = typeof config?.cwd === 'string' && config.cwd.trim() ? config.cwd.trim() : process.cwd();

  const proc = spawn(command, args, {
    cwd,
    stdio: ['pipe', 'pipe', 'pipe'],
    windowsHide: true,
    env: process.env,
  });

  const client = new McpStdioClient(proc);

  try {
    const initResult = await client.sendRequest(
      'initialize',
      {
        protocolVersion: DEFAULT_PROTOCOL_VERSION,
        capabilities: {},
        clientInfo: { name: 'OS.1', version: '1.0' },
      },
      { timeoutMs: 8000 }
    );

    client.sendNotification('notifications/initialized', {});

    return await fn({ client, initResult });
  } finally {
    await client.close();
  }
}

export async function mcpListTools(config) {
  return await withMcpClient(config, async ({ client }) => {
    const result = await client.sendRequest('tools/list', {}, { timeoutMs: 8000 });
    const tools = Array.isArray(result?.tools) ? result.tools : [];
    return { tools };
  });
}

export async function mcpCallTool(config, name, args) {
  const toolName = String(name || '').trim();
  if (!toolName) throw new Error('Tool name is required');
  const toolArgs = args && typeof args === 'object' && !Array.isArray(args) ? args : {};

  return await withMcpClient(config, async ({ client }) => {
    const result = await client.sendRequest('tools/call', { name: toolName, arguments: toolArgs }, { timeoutMs: 20000 });
    return result;
  });
}
