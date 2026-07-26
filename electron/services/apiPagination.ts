export const API_ENTITY_PAGE_MAX_LIMIT = 200

export interface ApiPage<T> {
  items: T[]
  offset: number
  limit: number
  total: number
  hasMore: boolean
  nextOffset?: number
}

export function paginateApiItems<T>(items: T[], requestedOffset: number, requestedLimit: number): ApiPage<T> {
  const offset = Math.max(0, Math.floor(Number(requestedOffset) || 0))
  const limit = Math.min(
    API_ENTITY_PAGE_MAX_LIMIT,
    Math.max(1, Math.floor(Number(requestedLimit) || API_ENTITY_PAGE_MAX_LIMIT))
  )
  const pageItems = items.slice(offset, offset + limit)
  const hasMore = offset + pageItems.length < items.length

  return {
    items: pageItems,
    offset,
    limit,
    total: items.length,
    hasMore,
    nextOffset: hasMore ? offset + pageItems.length : undefined
  }
}
