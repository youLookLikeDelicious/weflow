import assert from 'node:assert/strict'
import test from 'node:test'
import { paginateApiItems } from './apiPagination.ts'

test('clamps API pages to 200 items and returns the next offset', () => {
  const items = Array.from({ length: 450 }, (_, index) => index)

  const first = paginateApiItems(items, 0, 10_000)
  const last = paginateApiItems(items, 400, 10_000)

  assert.equal(first.limit, 200)
  assert.equal(first.items.length, 200)
  assert.equal(first.hasMore, true)
  assert.equal(first.nextOffset, 200)
  assert.deepEqual(last.items, items.slice(400))
  assert.equal(last.hasMore, false)
  assert.equal(last.nextOffset, undefined)
})

test('normalizes invalid offsets without changing item order', () => {
  const items = ['a', 'b', 'c']
  const page = paginateApiItems(items, -5, 2)

  assert.equal(page.offset, 0)
  assert.deepEqual(page.items, ['a', 'b'])
  assert.equal(page.nextOffset, 2)
})
