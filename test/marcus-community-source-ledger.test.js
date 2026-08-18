import test from 'node:test';
import assert from 'node:assert/strict';

import { buildCommunitySourceLedger } from '../marcus/social/community_source_ledger.js';

test('community source ledger gives the writer compact verified observation IDs', () => {
  const ledger = buildCommunitySourceLedger([{
    id: 'obs_verified_123',
    member: { displayName: 'Jeremy Example' },
    sourceTitle: 'Speed is not the hard part',
    contentSummary: 'A specific field report about testing an AI lead-response system with live traffic.',
    sourceUrl: 'https://www.skool.com/localgiants/speed-is-not-the-hard-part',
  }]);

  assert.deepEqual(ledger, [{
    id: 'obs_verified_123',
    member: 'Jeremy Example',
    title: 'Speed is not the hard part',
    summary: 'A specific field report about testing an AI lead-response system with live traffic.',
    sourceUrl: 'https://www.skool.com/localgiants/speed-is-not-the-hard-part',
  }]);
});

test('community source ledger excludes records without a stable id or source', () => {
  const ledger = buildCommunitySourceLedger([
    { id: '', sourceUrl: 'https://www.skool.com/localgiants/a' },
    { id: 'obs_missing_source', sourceUrl: '' },
  ]);

  assert.deepEqual(ledger, []);
});

