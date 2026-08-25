import assert from 'node:assert/strict'
import { mkdtemp, mkdir, rm, stat, utimes, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'

import {
  cleanupApiMediaStorage,
  clearApiMediaStorage,
  ensureApiMediaStorageRoot,
  inspectApiMediaStorage
} from './apiMediaStorageService.ts'

const DAY_MS = 24 * 60 * 60 * 1000

async function createFile(rootPath: string, relativePath: string, size: number, mtimeMs: number) {
  const filePath = path.join(rootPath, relativePath)
  await mkdir(path.dirname(filePath), { recursive: true })
  await writeFile(filePath, Buffer.alloc(size, 1))
  const date = new Date(mtimeMs)
  await utimes(filePath, date, date)
  return filePath
}

test('removes expired media while preserving recent and excluded files', async () => {
  const rootPath = await mkdtemp(path.join(os.tmpdir(), 'weflow-api-media-'))
  const nowMs = Date.UTC(2026, 7, 25)

  try {
    const expiredPath = await createFile(rootPath, 'wxid-a/image/expired.jpg', 4, nowMs - 31 * DAY_MS)
    const recentPath = await createFile(rootPath, 'wxid-a/image/recent.jpg', 4, nowMs - 2 * DAY_MS)
    const processingPath = await createFile(rootPath, 'wxid-a/video/processing.mp4', 4, nowMs - 60 * DAY_MS)

    const result = await cleanupApiMediaStorage(rootPath, {
      maxBytes: 100,
      targetBytes: 90,
      retentionMs: 30 * DAY_MS,
      nowMs,
      excludedRelativePaths: new Set(['wxid-a/video/processing.mp4'])
    })

    assert.deepEqual(result.removedRelativePaths, ['wxid-a/image/expired.jpg'])
    assert.equal(result.removedFiles, 1)
    assert.equal(result.beforeBytes, 12)
    assert.equal(result.afterBytes, 8)
    await assert.rejects(stat(expiredPath), { code: 'ENOENT' })
    assert.equal((await stat(recentPath)).size, 4)
    assert.equal((await stat(processingPath)).size, 4)
  } finally {
    await rm(rootPath, { recursive: true, force: true })
  }
})

test('removes oldest media until usage reaches the target below the hard limit', async () => {
  const rootPath = await mkdtemp(path.join(os.tmpdir(), 'weflow-api-media-'))
  const nowMs = Date.UTC(2026, 7, 25)

  try {
    const oldestPath = await createFile(rootPath, 'oldest.bin', 4, nowMs - 3 * DAY_MS)
    const middlePath = await createFile(rootPath, 'middle.bin', 4, nowMs - 2 * DAY_MS)
    const newestPath = await createFile(rootPath, 'newest.bin', 4, nowMs - DAY_MS)

    const result = await cleanupApiMediaStorage(rootPath, {
      maxBytes: 10,
      targetBytes: 9,
      retentionMs: 30 * DAY_MS,
      nowMs
    })

    assert.deepEqual(result.removedRelativePaths, ['oldest.bin'])
    assert.equal(result.beforeBytes, 12)
    assert.equal(result.afterBytes, 8)
    await assert.rejects(stat(oldestPath), { code: 'ENOENT' })
    assert.equal((await stat(middlePath)).size, 4)
    assert.equal((await stat(newestPath)).size, 4)
  } finally {
    await rm(rootPath, { recursive: true, force: true })
  }
})

test('reports real disk usage and clears all API media files', async () => {
  const rootPath = await mkdtemp(path.join(os.tmpdir(), 'weflow-api-media-'))
  const nowMs = Date.UTC(2026, 7, 25)

  try {
    await createFile(rootPath, 'one/image.jpg', 5, nowMs)
    await createFile(rootPath, 'two/video.mp4', 7, nowMs)

    const usage = await inspectApiMediaStorage(rootPath)
    assert.equal(usage.files, 2)
    assert.equal(usage.bytes, 12)

    const cleared = await clearApiMediaStorage(rootPath)
    assert.equal(cleared.removedFiles, 2)
    assert.equal(cleared.beforeBytes, 12)
    assert.equal(cleared.afterBytes, 0)
    assert.deepEqual(cleared.removedRelativePaths.sort(), ['one/image.jpg', 'two/video.mp4'])

    const emptyUsage = await inspectApiMediaStorage(rootPath)
    assert.deepEqual(emptyUsage, { files: 0, bytes: 0, errors: [] })
  } finally {
    await rm(rootPath, { recursive: true, force: true })
  }
})

test('refuses to adopt a non-empty unmarked directory', async () => {
  const rootPath = await mkdtemp(path.join(os.tmpdir(), 'weflow-api-media-'))

  try {
    await writeFile(path.join(rootPath, 'personal-file.txt'), 'keep me')
    await assert.rejects(
      ensureApiMediaStorageRoot(rootPath),
      /不是空目录/
    )
    assert.equal((await stat(path.join(rootPath, 'personal-file.txt'))).size, 7)
  } finally {
    await rm(rootPath, { recursive: true, force: true })
  }
})
