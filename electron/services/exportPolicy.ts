const MAX_EXPORT_MESSAGE_LIMIT = 1_000_000

export function normalizeExportMessageLimit(value: unknown): number | null {
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric <= 0) return null
  return Math.min(MAX_EXPORT_MESSAGE_LIMIT, Math.floor(numeric))
}

export function normalizeExportMediaMaxAgeDays(value: unknown): number | null {
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric <= 0) return null
  return Math.min(3650, Math.floor(numeric))
}

export function hasReachedExportMessageLimit(messageCount: number, limit: unknown): boolean {
  const normalizedLimit = normalizeExportMessageLimit(limit)
  return normalizedLimit !== null && messageCount >= normalizedLimit
}

export function shouldExportMediaForMessage(
  createTime: unknown,
  maxAgeDays: unknown,
  nowSeconds = Math.floor(Date.now() / 1000)
): boolean {
  const normalizedMaxAgeDays = normalizeExportMediaMaxAgeDays(maxAgeDays)
  if (normalizedMaxAgeDays === null) return true

  const rawTimestamp = Number(createTime)
  if (!Number.isFinite(rawTimestamp) || rawTimestamp <= 0) return true
  const timestampSeconds = rawTimestamp > 100_000_000_000
    ? Math.floor(rawTimestamp / 1000)
    : Math.floor(rawTimestamp)
  const normalizedNowSeconds = Number.isFinite(nowSeconds) && nowSeconds > 0
    ? Math.floor(nowSeconds)
    : Math.floor(Date.now() / 1000)
  const cutoffSeconds = normalizedNowSeconds - normalizedMaxAgeDays * 24 * 60 * 60
  return timestampSeconds >= cutoffSeconds
}
