const fs = require('fs');
const path = require('path');

const COMMUNITY_PROFILE_NOTE_NAME = /^community-[a-z0-9][a-z0-9-]{5,120}\.md$/;

function writeBoundedMarkdown(payload, { root, directoryName, maxBytes }) {
  const filename = String(payload.filename || '').trim().toLowerCase();
  const content = String(payload.content || '');
  if (!COMMUNITY_PROFILE_NOTE_NAME.test(filename)) {
    return { ok: false, error: 'Invalid MARCUS community-note filename.' };
  }
  if (!content.trim() || Buffer.byteLength(content, 'utf8') > maxBytes) {
    return { ok: false, error: `MARCUS community-note content must be between 1 and ${maxBytes.toLocaleString('en-US')} bytes.` };
  }
  const directory = path.join(path.resolve(root), 'docs', 'marcus', directoryName);
  const destination = path.join(directory, filename);
  if (path.dirname(destination) !== directory) {
    return { ok: false, error: 'Community-note destination escaped its vault directory.' };
  }
  try {
    fs.mkdirSync(directory, { recursive: true });
    const temporary = `${destination}.${process.pid}.tmp`;
    fs.writeFileSync(temporary, content, 'utf8');
    fs.renameSync(temporary, destination);
    const stat = fs.statSync(destination);
    return { ok: true, path: destination, filename, bytesWritten: stat.size, modifiedAt: stat.mtime.toISOString() };
  } catch (error) {
    return { ok: false, error: String(error?.message || error) };
  }
}

function writeMarcusCommunityProfile(payload = {}, { root = __dirname } = {}) {
  return writeBoundedMarkdown(payload, { root, directoryName: 'people', maxBytes: 120_000 });
}

function writeMarcusCommunityBrief(payload = {}, { root = __dirname } = {}) {
  return writeBoundedMarkdown(payload, { root, directoryName: 'community', maxBytes: 240_000 });
}

module.exports = { COMMUNITY_PROFILE_NOTE_NAME, writeMarcusCommunityBrief, writeMarcusCommunityProfile };
