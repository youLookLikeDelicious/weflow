import assert from 'node:assert/strict'
import test from 'node:test'

import {
  hasReachedExportMessageLimit,
  normalizeExportMessageLimit,
  shouldExportMediaForMessage
} from './exportPolicy.ts'

test('normalizes a per-session message limit while treating zero as unlimited', () => {
  assert.equal(normalizeExportMessageLimit(1000), 1000)
  assert.equal(normalizeExportMessageLimit(1.8), 1)
  assert.equal(normalizeExportMessageLimit(0), null)
  assert.equal(normalizeExportMessageLimit(-10), null)
  assert.equal(normalizeExportMessageLimit('invalid'), null)
  assert.equal(hasReachedExportMessageLimit(999, 1000), false)
  assert.equal(hasReachedExportMessageLimit(1000, 1000), true)
  assert.equal(hasReachedExportMessageLimit(10000, 0), false)
})

test('skips old media while preserving media with unknown timestamps', () => {
  const nowSeconds = 1_800_000_000
  const oneYearSeconds = 365 * 24 * 60 * 60

  assert.equal(shouldExportMediaForMessage(nowSeconds - oneYearSeconds, 365, nowSeconds), true)
  assert.equal(shouldExportMediaForMessage(nowSeconds - oneYearSeconds - 1, 365, nowSeconds), false)
  assert.equal(shouldExportMediaForMessage(0, 365, nowSeconds), true)
  assert.equal(shouldExportMediaForMessage(nowSeconds, 0, nowSeconds), true)
  assert.equal(shouldExportMediaForMessage(nowSeconds - 1, null, nowSeconds), true)
})
