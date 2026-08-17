const fs = require('fs');
const path = require('path');

const MEETING_NOTE_NAME = /^\d{4}-\d{2}-\d{2}-[a-z0-9][a-z0-9-]{5,100}\.md$/;

function writeMarcusMeetingNote(payload = {}, { root = __dirname } = {}) {
  const filename = String(payload.filename || '').trim().toLowerCase();
  const content = String(payload.content || '');
  if (!MEETING_NOTE_NAME.test(filename)) return { ok: false, error: 'Invalid MARCUS meeting-note filename.' };
  if (!content.trim() || Buffer.byteLength(content, 'utf8') > 80_000) return { ok: false, error: 'MARCUS meeting-note content must be between 1 and 80,000 bytes.' };
  const directory = path.join(path.resolve(root), 'docs', 'marcus', 'conversations');
  const destination = path.join(directory, filename);
  if (path.dirname(destination) !== directory) return { ok: false, error: 'Meeting-note destination escaped the conversation vault.' };
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

module.exports = { MEETING_NOTE_NAME, writeMarcusMeetingNote };
