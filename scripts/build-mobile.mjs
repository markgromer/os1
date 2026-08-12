import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

const root = path.resolve(fileURLToPath(new URL('..', import.meta.url)));
const outfile = path.join(root, 'public', 'marcus-realtime.js');
const trackerOutfile = path.join(root, 'public', 'marcus-operation-tracker.js');

await build({
  entryPoints: [path.join(root, 'client', 'marcus-realtime.js')],
  bundle: true,
  minify: true,
  format: 'iife',
  platform: 'browser',
  target: ['chrome110'],
  outfile,
  legalComments: 'none',
});

await build({
  entryPoints: [path.join(root, 'client', 'mobile-operation-tracker.js')],
  bundle: true,
  minify: true,
  format: 'iife',
  platform: 'browser',
  target: ['chrome110'],
  outfile: trackerOutfile,
  legalComments: 'none',
});

const output = await fs.readFile(outfile, 'utf8');
await fs.writeFile(outfile, output.replace(/[ \t]+$/gm, ''), 'utf8');
const trackerOutput = await fs.readFile(trackerOutfile, 'utf8');
await fs.writeFile(trackerOutfile, trackerOutput.replace(/[ \t]+$/gm, ''), 'utf8');
