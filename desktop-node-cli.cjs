const fs = require('fs');
const path = require('path');

function pathWithin(root, candidate) {
  const relative = path.relative(path.resolve(root), path.resolve(candidate));
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function npmCliInvocation(args = [], { execPath = process.execPath } = {}) {
  const cliPath = path.join(path.dirname(execPath), 'node_modules', 'npm', 'bin', 'npm-cli.js');
  if (!fs.existsSync(cliPath)) return { ok: false, error: 'The npm JavaScript entry point was not found beside node.exe' };
  return { ok: true, command: execPath, args: [cliPath, ...args.map(String)] };
}

function localPackageBinInvocation(cwd, packageName, binName, args = [], { execPath = process.execPath } = {}) {
  if (!/^[A-Za-z0-9._-]+$/.test(String(packageName || '')) || !/^[A-Za-z0-9._-]+$/.test(String(binName || ''))) {
    return { ok: false, error: 'Invalid local package binary name' };
  }
  try {
    const packageRoot = fs.realpathSync.native(path.join(path.resolve(cwd), 'node_modules', packageName));
    const manifest = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8'));
    const relativeBin = typeof manifest.bin === 'string' ? manifest.bin : manifest.bin?.[binName];
    if (typeof relativeBin !== 'string' || !relativeBin.trim()) return { ok: false, error: `Package ${packageName} has no ${binName} binary` };
    const binaryPath = fs.realpathSync.native(path.resolve(packageRoot, relativeBin));
    if (!pathWithin(packageRoot, binaryPath) || !fs.statSync(binaryPath).isFile()) {
      return { ok: false, error: `Package ${packageName} binary escapes its package directory` };
    }
    return { ok: true, command: execPath, args: [binaryPath, ...args.map(String)] };
  } catch {
    return { ok: false, error: `Local package ${packageName} is not installed correctly` };
  }
}

module.exports = { localPackageBinInvocation, npmCliInvocation };
