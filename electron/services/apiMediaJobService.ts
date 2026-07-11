import * as path from 'path'
import { mkdirSync } from 'fs'
import { createHash } from 'crypto'
import { ConfigService } from './config'
import type { Message } from './chatService'

export type ApiMediaJobStatus = 'queued' | 'processing' | 'ready' | 'missing' | 'failed'
export type ApiMediaKind = 'image' | 'voice' | 'video' | 'emoji'

export interface ApiMediaJobRecord {
  id: string
  sourceId: string
  talker: string
  localId: number
  serverId: string
  localType: number
  createTime: number
  mediaKind: ApiMediaKind
  status: ApiMediaJobStatus
  attempts: number
  payloadJson: string
  outputRelativePath: string
  missingReason: string
  lastError: string
  createdAt: number
  updatedAt: number
}

export interface ApiMediaJobInput {
  sourceId: string
  talker: string
  message: Message
  mediaKind: ApiMediaKind
}

type DatabaseSync = any

class ApiMediaJobService {
  private db: DatabaseSync | null = null
  private configService = ConfigService.getInstance()

  init(): boolean {
    if (this.db) return true

    try {
      // Electron 41 ships a modern Node runtime. Use the built-in sqlite module
      // to avoid adding another native dependency to the app bundle.
      const sqlite = require('node:sqlite')
      const Database = sqlite.DatabaseSync
      if (typeof Database !== 'function') {
        throw new Error('node:sqlite DatabaseSync is unavailable')
      }

      const basePath = this.configService.getCacheBasePath()
      mkdirSync(basePath, { recursive: true })
      this.db = new Database(path.join(basePath, 'api-media-jobs.sqlite'))
      this.db.exec(`
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        CREATE TABLE IF NOT EXISTS media_jobs (
          id TEXT PRIMARY KEY,
          source_id TEXT NOT NULL,
          talker TEXT NOT NULL,
          local_id INTEGER NOT NULL,
          server_id TEXT NOT NULL DEFAULT '',
          local_type INTEGER NOT NULL,
          create_time INTEGER NOT NULL DEFAULT 0,
          media_kind TEXT NOT NULL,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          payload_json TEXT NOT NULL,
          output_relative_path TEXT NOT NULL DEFAULT '',
          missing_reason TEXT NOT NULL DEFAULT '',
          last_error TEXT NOT NULL DEFAULT '',
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_media_jobs_status_updated ON media_jobs(status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_media_jobs_talker_status ON media_jobs(talker, status);
      `)
      return true
    } catch (error) {
      console.warn('[ApiMediaJobService] sqlite unavailable:', error)
      this.db = null
      return false
    }
  }

  close(): void {
    try {
      this.db?.close?.()
    } catch {}
    this.db = null
  }

  buildJobId(sourceId: string, talker: string, localId: number, localType: number): string {
    return createHash('sha1')
      .update(`${sourceId}\u0000${talker}\u0000${localId}\u0000${localType}`)
      .digest('hex')
  }

  getJob(id: string): ApiMediaJobRecord | null {
    if (!this.init() || !this.db) return null
    const row = this.db.prepare('SELECT * FROM media_jobs WHERE id = ?').get(id)
    return row ? this.mapRow(row) : null
  }

  getOrCreateJob(input: ApiMediaJobInput): ApiMediaJobRecord | null {
    if (!this.init() || !this.db) return null
    const localId = Number(input.message.localId || 0)
    const localType = Number(input.message.localType || 0)
    const id = this.buildJobId(input.sourceId, input.talker, localId, localType)
    const existing = this.getJob(id)
    if (existing) return existing

    const now = Math.floor(Date.now() / 1000)
    const payloadJson = JSON.stringify(input.message)
    this.db.prepare(`
      INSERT INTO media_jobs (
        id, source_id, talker, local_id, server_id, local_type, create_time,
        media_kind, status, attempts, payload_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?)
    `).run(
      id,
      input.sourceId,
      input.talker,
      localId,
      String(input.message.serverIdRaw || input.message.serverId || ''),
      localType,
      Number(input.message.createTime || 0),
      input.mediaKind,
      payloadJson,
      now,
      now
    )

    return this.getJob(id)
  }

  listJobs(status: string, since: number, limit: number): ApiMediaJobRecord[] {
    if (!this.init() || !this.db) return []
    const rows = this.db.prepare(`
      SELECT * FROM media_jobs
      WHERE status = ? AND updated_at >= ?
      ORDER BY updated_at ASC
      LIMIT ?
    `).all(status, since, limit)
    return rows.map((row: any) => this.mapRow(row))
  }

  claimNextJobs(limit: number, excludedTalkers: Set<string>, maxAttempts: number): ApiMediaJobRecord[] {
    if (!this.init() || !this.db) return []
    const candidates = this.db.prepare(`
      SELECT * FROM media_jobs
      WHERE status IN ('queued', 'failed') AND attempts < ?
      ORDER BY updated_at ASC
      LIMIT 50
    `).all(maxAttempts)

    const claimed: ApiMediaJobRecord[] = []
    const now = Math.floor(Date.now() / 1000)
    for (const row of candidates) {
      const job = this.mapRow(row)
      if (excludedTalkers.has(job.talker)) continue
      this.db.prepare(`
        UPDATE media_jobs
        SET status = 'processing', attempts = attempts + 1, updated_at = ?
        WHERE id = ? AND status IN ('queued', 'failed')
      `).run(now, job.id)
      const updated = this.getJob(job.id)
      if (updated) {
        claimed.push(updated)
        excludedTalkers.add(updated.talker)
      }
      if (claimed.length >= limit) break
    }
    return claimed
  }

  markReady(id: string, outputRelativePath: string): void {
    this.updateStatus(id, 'ready', outputRelativePath, '', '')
  }

  markMissing(id: string, reason: string): void {
    this.updateStatus(id, 'missing', '', reason, '')
  }

  markFailed(id: string, error: string, maxAttempts: number): void {
    const job = this.getJob(id)
    const nextStatus: ApiMediaJobStatus = job && job.attempts >= maxAttempts ? 'failed' : 'queued'
    this.updateStatus(id, nextStatus, '', '', error)
  }

  recoverProcessingJobs(): void {
    if (!this.init() || !this.db) return
    const now = Math.floor(Date.now() / 1000)
    this.db.prepare(`
      UPDATE media_jobs
      SET status = 'queued', updated_at = ?
      WHERE status = 'processing'
    `).run(now)
  }

  private updateStatus(
    id: string,
    status: ApiMediaJobStatus,
    outputRelativePath: string,
    missingReason: string,
    lastError: string
  ): void {
    if (!this.init() || !this.db) return
    const now = Math.floor(Date.now() / 1000)
    this.db.prepare(`
      UPDATE media_jobs
      SET status = ?, output_relative_path = ?, missing_reason = ?, last_error = ?, updated_at = ?
      WHERE id = ?
    `).run(status, outputRelativePath, missingReason, lastError, now, id)
  }

  private mapRow(row: any): ApiMediaJobRecord {
    return {
      id: String(row.id || ''),
      sourceId: String(row.source_id || ''),
      talker: String(row.talker || ''),
      localId: Number(row.local_id || 0),
      serverId: String(row.server_id || ''),
      localType: Number(row.local_type || 0),
      createTime: Number(row.create_time || 0),
      mediaKind: String(row.media_kind || 'image') as ApiMediaKind,
      status: String(row.status || 'queued') as ApiMediaJobStatus,
      attempts: Number(row.attempts || 0),
      payloadJson: String(row.payload_json || '{}'),
      outputRelativePath: String(row.output_relative_path || ''),
      missingReason: String(row.missing_reason || ''),
      lastError: String(row.last_error || ''),
      createdAt: Number(row.created_at || 0),
      updatedAt: Number(row.updated_at || 0)
    }
  }
}

export const apiMediaJobService = new ApiMediaJobService()
