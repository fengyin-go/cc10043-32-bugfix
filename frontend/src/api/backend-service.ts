import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type FieldDetail = {
  field: string
  path: string
  status: 'resolved' | 'null' | 'missing' | 'type_mismatch' | 'conflict_candidate' | 'payload_error'
  value: unknown
  expected_type?: string | null
  actual_type?: string
  chosen?: boolean
}

export type FieldError = {
  field: string | null
  path: string | null
  code: string
  error: string
  expected?: string | null
  actual?: string | null
  value?: unknown
  blocking?: boolean
}

export type ResolvedField = {
  field: string
  value: unknown
  status: string
  chosen_path: string
  paths: string[]
  resolution: null | string | { type: string; path: string; operator: string }
}

export type ParseConflict = {
  id: number
  template_id: number
  parse_id: number
  target: string
  paths: string[]
  candidates: { path: string; value: unknown; type: string }[]
  status: string
  reason: string
  raw_payload: string
  mapping: string
  created_at: string
  resolved_path?: string
  resolved_by?: string
  resolution_remark?: string
  superseded?: boolean
  superseded_by_parse_ids?: number[]
}

export type Transformation = {
  field: string
  source_path: string | null
  value: unknown
  rule_id: number
  rule_name: string
  target_field: string
  transform: string
  ok: boolean
  transformed_value: unknown
  error: string | null
  skipped?: string
  error_policy: string
}

export type ParseDelivery = {
  status: '已交付' | '暂停交付'
  delivered: boolean
  undelivered_reason: string
  reason: string
  reasons: string[]
  skipped_field_errors?: string[]
  parse_id: number
  field_count: number
  message: string
}

export type ParseResult = {
  ok: boolean
  message: string
  parse_id: number
  revision: number
  payload_sha256: string
  template: { id: number; name: string; format: string }
  basis: {
    payload_sha256: string
    mapping: string
    field_paths: Record<string, string[]>
    revision: number
  }
  fields: Record<string, unknown>
  resolved_fields: ResolvedField[]
  field_details: FieldDetail[]
  field_errors: FieldError[]
  mapping_errors: string[]
  conflicts: ParseConflict[]
  transformations: Transformation[]
  delivery: ParseDelivery
}

export type ParseSnapshot = {
  id: number
  revision: number
  created_at: string
  source: string
  payload_text: string
  payload_sha256: string
  mapping: string
  basis: ParseResult['basis']
  fields: Record<string, unknown>
  resolved_fields: ResolvedField[]
  field_details: FieldDetail[]
  field_errors: FieldError[]
  conflicts: ParseConflict[]
  transformations: Transformation[]
  delivery: ParseDelivery
}

async function readJson<T>(response: Response, fallback: string): Promise<T> {
  if (!response.ok) {
    const detail = await response.json().catch(() => null)
    throw new Error(detail?.detail ?? fallback)
  }
  return (await response.json()) as T
}

export async function parsePayload(
  templateId: number,
  payload: string,
  messageId = '',
): Promise<ParseResult> {
  const body = { payload, message_id: messageId || null }
  return readJson(
    await request(`/api/message_parser/${templateId}/parse`, {
      method: 'POST',
      body: JSON.stringify(body),
    }),
    '解析接口调用失败',
  )
}

export async function ingestPayload(
  connectorId: number,
  payload: string,
  messageId = '',
): Promise<ParseResult> {
  return readJson(
    await request(`/api/api_connector/${connectorId}/ingest`, {
      method: 'POST',
      body: JSON.stringify({ payload, message_id: messageId || null }),
    }),
    '接入接口调用失败',
  )
}

export async function listParseSnapshots(templateId: number): Promise<ParseSnapshot[]> {
  const data = await readJson<{ items: ParseSnapshot[] }>(
    await request(`/api/message_parser/${templateId}/snapshots`),
    '解析依据读取失败',
  )
  return data.items
}

export async function listParseConflicts(
  templateId: number,
  status = '',
): Promise<ParseConflict[]> {
  const query = status ? `?status=${encodeURIComponent(status)}` : ''
  const data = await readJson<{ items: ParseConflict[] }>(
    await request(`/api/message_parser/${templateId}/conflicts${query}`),
    '冲突记录读取失败',
  )
  return data.items
}

export async function resolveParseConflict(
  templateId: number,
  conflictId: number,
  path: string,
  operator: string,
  remark: string,
): Promise<ParseResult> {
  return readJson(
    await request(`/api/message_parser/${templateId}/conflicts/${conflictId}/resolve`, {
      method: 'POST',
      body: JSON.stringify({ path, operator, remark }),
    }),
    '冲突裁决失败',
  )
}
