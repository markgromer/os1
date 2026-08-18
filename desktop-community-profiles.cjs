const fs = require('fs');
const path = require('path');

const COMMUNITY_PROFILE_NOTE_NAME = /^community-[a-z0-9][a-z0-9-]{5,120}\.md$/;

function writeMarcusCommunityProfile(payload = {}, { root = __dirname } = {}) {
  const filename = String(payload.filename || '').trim().toLowerCase();
  const content = String(payload.content || '');
  if (!COMMUNITY_PROFILE_NOTE_NAME.test(filename)) {
    return { ok: false, error: 'Invalid MARCUS community-profile filename.' };
  }
  if (!content.trim() || Buffer.byteLength(content, 'utf8') > 120_000) {
    return { ok: false, error: 'MARCUS community-profile content must be between 1 and 120,000 bytes.' };
  }
  const directory = path.join(path.resolve(root), 'docs', 'marcus', 'people');
  const destination = path.join(directory, filename);
  if (path.dirname(destination) !== directory) {
    return { ok: false, error: 'Community-profile destination escaped the people vault.' };
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

module.exports = { COMMUNITY_PROFILE_NOTE_NAME, writeMarcusCommunityProfile };
