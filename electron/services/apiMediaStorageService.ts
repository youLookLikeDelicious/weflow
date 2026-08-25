import { mkdir, readdir, rm, stat, writeFile } from 'node:fs/promises'
import path from 'node:path'

const API_MEDIA_STORAGE_MARKER = '.weflow-api-media-cache'

function toPortableRelativePath(rootPath: string, targetPath: string): string {
  return path.relative(rootPath, targetPath).split(path.sep).join('/')
}

export interface ApiMediaCleanupOptions {
  maxBytes: number
  targetBytes: number
  retentionMs: number
  nowMs?: number
  excludedRelativePaths?: ReadonlySet<string>
}

export interface ApiMediaCleanupResult {
  scannedFiles: number
  removedFiles: number
  beforeBytes: number
  afterBytes: number
  removedRelativePaths: string[]
  errors: Array<{ relativePath: string; message: string }>
}

interface MediaFileEntry {
  absolutePath: string
  relativePath: string
  size: number
  mtimeMs: number
  excluded: boolean
}

export interface ApiMediaStorageUsage {
  files: number
  bytes: number
  errors: Array<{ relativePath: string; message: string }>
}

async function listMediaFiles(
  rootPath: string,
  currentPath: string,
  excludedRelativePaths: ReadonlySet<string>,
  result: MediaFileEntry[],
  errors: ApiMediaCleanupResult['errors']
): Promise<void> {
  let entries
  try {
    entries = await readdir(currentPath, { withFileTypes: true })
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      errors.push({
        relativePath: toPortableRelativePath(rootPath, currentPath),
        message: error instanceof Error ? error.message : String(error)
      })
    }
    return
  }

  for (const entry of entries) {
    if (currentPath === rootPath && entry.name === API_MEDIA_STORAGE_MARKER) continue
    const absolutePath = path.join(currentPath, entry.name)
    if (entry.isDirectory()) {
      await listMediaFiles(rootPath, absolutePath, excludedRelativePaths, result, errors)
      continue
    }
    if (!entry.isFile()) continue

    const relativePath = toPortableRelativePath(rootPath, absolutePath)
    try {
      const fileStat = await stat(absolutePath)
      result.push({
        absolutePath,
        relativePath,
        size: fileStat.size,
        mtimeMs: fileStat.mtimeMs,
        excluded: excludedRelativePaths.has(relativePath)
      })
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        errors.push({
          relativePath,
          message: error instanceof Error ? error.message : String(error)
        })
      }
    }
  }
}

async function removeFile(
  entry: MediaFileEntry,
  result: ApiMediaCleanupResult
): Promise<boolean> {
  try {
    await rm(entry.absolutePath, { force: true })
    result.removedFiles += 1
    result.afterBytes -= entry.size
    result.removedRelativePaths.push(entry.relativePath)
    return true
  } catch (error) {
    result.errors.push({
      relativePath: entry.relativePath,
      message: error instanceof Error ? error.message : String(error)
    })
    return false
  }
}

export async function cleanupApiMediaStorage(
  rootPath: string,
  options: ApiMediaCleanupOptions
): Promise<ApiMediaCleanupResult> {
  const result: ApiMediaCleanupResult = {
    scannedFiles: 0,
    removedFiles: 0,
    beforeBytes: 0,
    afterBytes: 0,
    removedRelativePaths: [],
    errors: []
  }
  const files: MediaFileEntry[] = []
  const excludedRelativePaths = options.excludedRelativePaths ?? new Set<string>()
  await mkdir(rootPath, { recursive: true })
  await listMediaFiles(rootPath, rootPath, excludedRelativePaths, files, result.errors)

  result.scannedFiles = files.length
  result.beforeBytes = files.reduce((total, file) => total + file.size, 0)
  result.afterBytes = result.beforeBytes

  const nowMs = options.nowMs ?? Date.now()
  const expired = files
    .filter(file => !file.excluded && nowMs - file.mtimeMs > options.retentionMs)
    .sort((left, right) => left.mtimeMs - right.mtimeMs || left.relativePath.localeCompare(right.relativePath))
  const removed = new Set<string>()

  for (const file of expired) {
    if (await removeFile(file, result)) removed.add(file.relativePath)
  }

  if (result.afterBytes > options.maxBytes) {
    const oldestFirst = files
      .filter(file => !file.excluded && !removed.has(file.relativePath))
      .sort((left, right) => left.mtimeMs - right.mtimeMs || left.relativePath.localeCompare(right.relativePath))

    for (const file of oldestFirst) {
      if (result.afterBytes <= options.targetBytes) break
      await removeFile(file, result)
    }
  }

  return result
}

export async function inspectApiMediaStorage(rootPath: string): Promise<ApiMediaStorageUsage> {
  const files: MediaFileEntry[] = []
  const errors: ApiMediaStorageUsage['errors'] = []
  await mkdir(rootPath, { recursive: true })
  await listMediaFiles(rootPath, rootPath, new Set<string>(), files, errors)
  return {
    files: files.length,
    bytes: files.reduce((total, file) => total + file.size, 0),
    errors
  }
}

export async function ensureApiMediaStorageRoot(
  rootPath: string,
  options: { allowExistingUnmarked?: boolean } = {}
): Promise<void> {
  await mkdir(rootPath, { recursive: true })
  const entries = await readdir(rootPath)
  if (entries.includes(API_MEDIA_STORAGE_MARKER)) return
  if (entries.length > 0 && options.allowExistingUnmarked !== true) {
    throw new Error('所选目录不是空目录，也不是 WeFlow 管理的 API 媒体目录')
  }
  await writeFile(
    path.join(rootPath, API_MEDIA_STORAGE_MARKER),
    'Managed by WeFlow. Files in this directory may be removed automatically.\n',
    { flag: 'wx' }
  ).catch((error: NodeJS.ErrnoException) => {
    if (error.code !== 'EEXIST') throw error
  })
}

export async function clearApiMediaStorage(rootPath: string): Promise<ApiMediaCleanupResult> {
  const files: MediaFileEntry[] = []
  const errors: ApiMediaCleanupResult['errors'] = []
  await mkdir(rootPath, { recursive: true })
  await listMediaFiles(rootPath, rootPath, new Set<string>(), files, errors)

  const result: ApiMediaCleanupResult = {
    scannedFiles: files.length,
    removedFiles: 0,
    beforeBytes: files.reduce((total, file) => total + file.size, 0),
    afterBytes: files.reduce((total, file) => total + file.size, 0),
    removedRelativePaths: [],
    errors
  }

  for (const file of files) {
    await removeFile(file, result)
  }
  return result
}
