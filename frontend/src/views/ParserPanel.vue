<template>
  <section class="parser-panel" data-testid="parser-panel">
    <header class="panel-head">
      <div>
        <h3>解析工作台</h3>
        <p class="page-desc">
          载荷按模板映射解析，字段值来自载荷；路径缺失、类型不符或双路径冲突会暂停交付并留下依据。
        </p>
      </div>
      <div class="panel-head-actions">
        <label class="inline-field">
          <span>解析模板</span>
          <select v-model.number="templateId" data-testid="parser-template">
            <option v-for="template in templates" :key="template.id" :value="template.id">
              #{{ template.id }} {{ template['模板名称'] }}（{{ template.status }}）
            </option>
          </select>
        </label>
        <label class="inline-field">
          <span>连接器 ID</span>
          <input v-model.number="connectorId" data-testid="parser-connector" type="number" min="1" />
        </label>
      </div>
    </header>

    <form class="payload-form" @submit.prevent="runParse">
      <label class="payload-field">
        <span>原始消息载荷（JSON）</span>
        <textarea
          v-model="payloadText"
          data-testid="parser-payload"
          rows="6"
          spellcheck="false"
        ></textarea>
      </label>
      <label class="inline-field">
        <span>消息 ID</span>
        <input v-model="messageId" data-testid="parser-message-id" placeholder="可选" />
      </label>
      <div class="panel-actions">
        <button class="btn primary" data-testid="parser-run" type="submit" :disabled="busy">
          {{ busy ? '解析中…' : '解析载荷' }}
        </button>
        <button class="btn" data-testid="ingest-run" type="button" :disabled="busy" @click="runIngest">
          从连接器接入
        </button>
        <button class="btn ghost" data-testid="parser-refresh" type="button" @click="refreshTrail">
          刷新依据
        </button>
      </div>
      <p v-if="errorMessage" class="error-text" data-testid="parser-error">{{ errorMessage }}</p>
    </form>

    <template v-if="result">
      <article
        class="delivery-banner"
        :class="result.delivery.status === '已交付' ? 'delivery-ok' : 'delivery-paused'"
        data-testid="parser-delivery"
      >
        <strong>{{ result.delivery.status }}</strong>
        <span v-if="result.delivery.status === '暂停交付'" data-testid="parser-pause-reason">
          未交付理由：{{ result.delivery.undelivered_reason }}
        </span>
        <span v-else>
          解析 #{{ result.parse_id }}（修订 {{ result.revision }}）已交付 {{ result.delivery.field_count }} 个字段
        </span>
        <code class="basis-code" data-testid="parser-basis">
          依据 sha256:{{ result.payload_sha256.slice(0, 16) }} · 修订 {{ result.revision }}
        </code>
      </article>

      <div class="parser-grid">
        <section>
          <h4>结构化字段</h4>
          <table class="data-table" data-testid="parser-fields">
            <thead>
              <tr><th>目标字段</th><th>解析值</th><th>采用路径</th><th>其他候选路径</th><th>取值方式</th></tr>
            </thead>
            <tbody>
              <tr v-for="item in result.resolved_fields" :key="item.field">
                <td>{{ item.field }}</td>
                <td>{{ formatValue(item.value) }}</td>
                <td><code>{{ item.chosen_path }}</code></td>
                <td>{{ item.paths.filter((path) => path !== item.chosen_path).join('、') || '—' }}</td>
                <td>{{ describeResolution(item) }}</td>
              </tr>
              <tr v-if="!result.resolved_fields.length">
                <td colspan="5" class="empty-state">没有可交付的结构化字段</td>
              </tr>
            </tbody>
          </table>
        </section>

        <section>
          <h4>字段明细与错误</h4>
          <table class="data-table" data-testid="parser-details">
            <thead>
              <tr><th>字段</th><th>路径</th><th>状态</th><th>值</th></tr>
            </thead>
            <tbody>
              <tr v-for="(detail, index) in result.field_details" :key="`${detail.field}-${detail.path}-${index}`">
                <td>{{ detail.field }}</td>
                <td><code>{{ detail.path }}</code></td>
                <td>{{ detailLabel(detail.status) }}</td>
                <td>{{ detail.status === 'missing' ? '路径不存在' : formatValue(detail.value) }}</td>
              </tr>
            </tbody>
          </table>
          <ul v-if="result.field_errors.length" class="error-list" data-testid="parser-field-errors">
            <li v-for="(item, index) in result.field_errors" :key="index">
              {{ item.field || '载荷' }}：{{ item.error }}
              <template v-if="item.code === 'type_mismatch'">（期望 {{ item.expected }}，实际 {{ item.actual }}）</template>
              <span v-if="item.blocking === false" class="tag">未阻断</span>
            </li>
          </ul>
          <ul v-if="result.mapping_errors.length" class="error-list">
            <li v-for="(item, index) in result.mapping_errors" :key="index">{{ item }}</li>
          </ul>
        </section>
      </div>

      <section v-if="result.conflicts.length || openConflicts.length" class="conflict-block">
        <h4>字段映射冲突复核</h4>
        <p class="page-desc">
          同一目标字段被多条路径写入且值不一致时不会静默择一：未裁决前该字段不交付，原始载荷与映射保留待复核。
        </p>
        <table class="data-table" data-testid="parser-conflicts">
          <thead>
            <tr><th>冲突</th><th>目标字段</th><th>候选来源</th><th>状态</th><th>裁决</th></tr>
          </thead>
          <tbody>
            <tr v-for="conflict in conflictsToReview" :key="conflict.id">
              <td>#{{ conflict.id }} · 解析 #{{ conflict.parse_id }}</td>
              <td>{{ conflict.target }}</td>
              <td>
                <label v-for="candidate in conflict.candidates" :key="candidate.path" class="candidate">
                  <input
                    v-model="choices[conflict.id]"
                    type="radio"
                    :name="`conflict-${conflict.id}`"
                    :value="candidate.path"
                    :data-testid="`conflict-${conflict.id}-${candidate.path}`"
                  />
                  <code>{{ candidate.path }}</code> = {{ formatValue(candidate.value) }}（{{ candidate.type }}）
                </label>
              </td>
              <td>
                {{ conflict.status }}
                <span v-if="conflict.superseded" class="tag">原文已更正</span>
              </td>
              <td>
                <button
                  v-if="conflict.status === '待复核' && !conflict.superseded"
                  class="link"
                  type="button"
                  :data-testid="`resolve-${conflict.id}`"
                  :disabled="!choices[conflict.id]"
                  @click="resolve(conflict)"
                >
                  按所选来源重新解析
                </button>
                <span v-else-if="conflict.resolved_path">已采用 <code>{{ conflict.resolved_path }}</code></span>
                <span v-else>—</span>
              </td>
            </tr>
            <tr v-if="!conflictsToReview.length">
              <td colspan="5" class="empty-state">没有待复核冲突</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section>
        <h4>字段转换列表</h4>
        <table class="data-table" data-testid="parser-transforms">
          <thead>
            <tr><th>解析字段</th><th>采用路径</th><th>原值</th><th>转换规则</th><th>目标字段</th><th>实际转换值</th></tr>
          </thead>
          <tbody>
            <tr v-for="item in result.transformations" :key="`${item.field}-${item.rule_id}`">
              <td>{{ item.field }}</td>
              <td><code>{{ item.source_path || '—' }}</code></td>
              <td>{{ formatValue(item.value) }}</td>
              <td>{{ item.rule_name }}（{{ item.transform }}）</td>
              <td>{{ item.target_field }}</td>
              <td>
                {{ formatValue(item.transformed_value) }}
                <span v-if="item.skipped" class="tag">{{ item.skipped }}</span>
                <span v-if="!item.ok" class="error-text">{{ item.error }}</span>
              </td>
            </tr>
            <tr v-if="!result.transformations.length">
              <td colspan="6" class="empty-state">没有匹配的转换规则</td>
            </tr>
          </tbody>
        </table>
      </section>
    </template>

    <section class="trail-block">
      <h4>解析依据（按修订）</h4>
      <table class="data-table" data-testid="parser-snapshots">
        <thead>
          <tr><th>解析号</th><th>修订</th><th>载荷摘要</th><th>交付</th><th>数量字段路径</th><th>时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="snapshot in snapshots" :key="snapshot.id">
            <td>#{{ snapshot.id }}</td>
            <td>{{ snapshot.revision }}</td>
            <td><code>{{ snapshot.payload_sha256.slice(0, 16) }}</code></td>
            <td>{{ snapshot.delivery.status }}</td>
            <td>{{ (snapshot.basis.field_paths.qty || []).join(' / ') || '—' }}</td>
            <td>{{ snapshot.created_at }}</td>
          </tr>
          <tr v-if="!snapshots.length">
            <td colspan="6" class="empty-state">还没有解析记录</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  ingestPayload,
  listEntries,
  listParseConflicts,
  listParseSnapshots,
  parsePayload,
  resolveParseConflict,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'
import type {
  ParseConflict,
  ParseResult,
  ParseSnapshot,
  ResolvedField,
} from '@/api/backend-service'

const templates = ref<EntryRow[]>([])
const templateId = ref(0)
const connectorId = ref(2)
const payloadText = ref(
  JSON.stringify(
    { order: { id: 'A-1001', qty: 12, amount: 99.5 }, items: { count: 10 } },
    null,
    2,
  ),
)
const messageId = ref('')
const result = ref<ParseResult | null>(null)
const snapshots = ref<ParseSnapshot[]>([])
const openConflicts = ref<ParseConflict[]>([])
const choices = ref<Record<number, string>>({})
const errorMessage = ref('')
const busy = ref(false)

const conflictsToReview = computed(() => {
  const merged = new Map<number, ParseConflict>()
  for (const conflict of [...openConflicts.value, ...(result.value?.conflicts ?? [])]) {
    merged.set(conflict.id, conflict)
  }
  return [...merged.values()]
})

function formatValue(value: unknown): string {
  if (value === null) return 'null'
  if (value === undefined) return '—'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function detailLabel(status: string): string {
  return {
    resolved: '已取到值',
    null: '值为 null',
    missing: '路径不存在',
    type_mismatch: '类型不符',
    conflict_candidate: '冲突候选',
    payload_error: '载荷错误',
  }[status] ?? status
}

function describeResolution(item: ResolvedField): string {
  if (item.resolution === null || item.resolution === undefined) return '唯一路径'
  if (typeof item.resolution === 'string') return item.resolution
  return `已确认来源${item.resolution.operator ? `（${item.resolution.operator}）` : ''}`
}

async function loadTemplates() {
  try {
    const page = await listEntries('message_parser', {})
    templates.value = page.items
    const enabled = page.items.find((item) => item.status === '已启用')
    templateId.value = enabled ? Number(enabled.id) : Number(page.items[0]?.id ?? 0)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '模板读取失败'
  }
}

async function refreshTrail() {
  if (!templateId.value) return
  try {
    snapshots.value = await listParseSnapshots(templateId.value)
    openConflicts.value = await listParseConflicts(templateId.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '解析依据读取失败'
  }
}

async function runParse() {
  if (!templateId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const parsed = await parsePayload(templateId.value, payloadText.value, messageId.value)
    result.value = parsed
    choices.value = {}
    await refreshTrail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '解析失败'
  } finally {
    busy.value = false
  }
}

async function runIngest() {
  if (!connectorId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const parsed = await ingestPayload(connectorId.value, payloadText.value, messageId.value)
    result.value = parsed
    choices.value = {}
    await refreshTrail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接入失败'
  } finally {
    busy.value = false
  }
}

async function resolve(conflict: ParseConflict) {
  const path = choices.value[conflict.id]
  if (!path) return
  busy.value = true
  errorMessage.value = ''
  try {
    const parsed = await resolveParseConflict(
      templateId.value,
      conflict.id,
      path,
      '数据工程组',
      '复核确认来源',
    )
    result.value = parsed
    await refreshTrail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '冲突裁决失败'
  } finally {
    busy.value = false
  }
}

watch(templateId, () => {
  result.value = null
  void refreshTrail()
})

onMounted(async () => {
  await loadTemplates()
  await refreshTrail()
})
</script>

<style scoped>
.parser-panel { margin-top: 20px; padding-top: 16px; border-top: 1px solid var(--border); }
.panel-head { display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; }
.panel-head h3, .parser-panel h4 { margin: 0 0 8px; }
.panel-head-actions { display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end; }
.inline-field { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.inline-field select, .inline-field input { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; }
.payload-form { margin: 12px 0; display: flex; flex-direction: column; gap: 10px; }
.payload-field span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
.payload-field textarea { width: 100%; font-family: ui-monospace, monospace; font-size: 12px; padding: 8px; border: 1px solid var(--border); border-radius: 6px; }
.panel-actions { display: flex; gap: 8px; flex-wrap: wrap; }
.panel-actions button:disabled { opacity: 0.6; cursor: not-allowed; }
.delivery-banner { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; padding: 10px 12px; border-radius: 8px; font-size: 13px; margin: 12px 0; }
.delivery-ok { background: #ecfdf3; border: 1px solid #abefc6; color: #067647; }
.delivery-paused { background: #fef3f2; border: 1px solid #fecdca; color: #b42318; }
.basis-code { font-size: 11px; color: var(--muted); }
.parser-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }
.parser-panel section { margin-bottom: 16px; }
.error-list { margin: 8px 0 0; padding-left: 18px; color: #b42318; font-size: 12px; }
.tag { display: inline-block; margin-left: 6px; padding: 0 6px; border-radius: 4px; background: #eef2f6; color: var(--muted); font-size: 11px; }
.candidate { display: block; font-size: 12px; margin: 2px 0; }
.candidate code { margin-right: 4px; }
.conflict-block { background: #fffdf5; border: 1px solid #f2e3b3; border-radius: 8px; padding: 12px; }
.trail-block { margin-top: 20px; }
</style>
