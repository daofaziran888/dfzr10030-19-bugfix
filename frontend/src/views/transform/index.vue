<template>
  <section class="page" data-module="transform">
    <header class="page-head">
      <div>
        <h2>采集转换管道</h2>
        <p class="page-desc">只读消费流转 DAG 快照：仅有效路径上的转换节点接收数据并按边匹配转换表达式，停用隔离节点不生成可执行边。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记转换节点</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>

    <div class="snapshot-bar" data-testid="snapshot-bar">
      <div class="snapshot-meta">
        <strong>只读输入：流转 DAG 快照 v{{ graph.snapshot_version ?? ledgerVersion }}</strong>
        <span>{{ graph.summary }}</span>
        <em class="readonly-tag">转换服务不修改快照，只回传核对结果</em>
      </div>
      <div class="snapshot-tools">
        <button class="btn" type="button" data-testid="sync-ledger" @click="syncLedger">同步快照并刷新待确认台账</button>
      </div>
    </div>
    <p v-if="errorMessage" class="error-text" data-testid="error-text">{{ errorMessage }}</p>

    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead-t" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#16a34a"/></marker></defs>
        <!-- 只渲染快照可执行边（有效路径） -->
        <g v-for="edge in graph.edges" :key="`${edge.from}-${edge.to}-${edge.pipe_id}`">
          <line class="dag-edge edge-active"
                :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0"
                :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"
                marker-end="url(#arrowhead-t)"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6"
                font-size="10" fill="#15803d" text-anchor="middle">{{ edge.管道编号 }}·有效路径</text>
        </g>
        <!-- 节点：有效路径节点按状态着色，隔离节点灰显虚线 -->
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id, isolated: !node.on_valid_path}]"
           @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 64" :y="nodePos[node.id]?.y - 22" width="128" height="44" rx="6"
                :fill="node.on_valid_path ? nodeColor(node.status) : '#f1f5f9'"
                :stroke="node.on_valid_path ? 'transparent' : '#94a3b8'"
                :stroke-dasharray="node.on_valid_path ? '0' : '4 3'" :stroke-width="node.on_valid_path ? 0 : 1.5"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y - 2" text-anchor="middle">{{ node.name || node.label }}</text>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 13" font-size="9"
                :fill="node.on_valid_path ? '#475569' : '#94a3b8'" text-anchor="middle">
            {{ node.on_valid_path ? `${node.输入来源}→${node.输出目标}` : '停用隔离·不接收数据' }}
          </text>
        </g>
      </svg>
    </div>

    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>节点编号</dt><dd>{{ selectedNode.节点编号 }}</dd></dl>
      <dl><dt>节点状态</dt><dd>{{ selectedNode.status }}</dd></dl>
      <dl><dt>输入来源</dt><dd>{{ selectedNode.输入来源 }}</dd></dl>
      <dl><dt>输出目标</dt><dd>{{ selectedNode.输出目标 }}</dd></dl>
      <dl class="full-line"><dt>转换表达式</dt><dd>{{ selectedNode.转换表达式 || '—' }}</dd></dl>
      <dl class="full-line"><dt>有效路径</dt>
        <dd>{{ selectedNode.on_valid_path ? '命中快照可执行边，接收数据' : '未命中可执行边（停用或未通过校验），不生成可执行边' }}</dd>
      </dl>
      <div class="full-line detail-close"><button class="btn ghost" type="button" @click="selectedNode=null">关闭详情</button></div>
    </div>

    <!-- 待确认台账：状态随快照版本变化回到待核对 -->
    <article class="pipe-panel" data-testid="ledger-panel">
      <h4>待确认台账（快照 v{{ ledgerVersion }}）—— 一批提交仅一条确认，其余拒绝</h4>
      <table class="data-table">
        <thead><tr><th>台账编号</th><th>转换节点</th><th>匹配流转边</th><th>转换表达式</th><th>核对状态</th><th>说明/原因</th><th>本批核对</th></tr></thead>
        <tbody>
          <tr v-for="item in ledger" :key="item.transform_id" :data-state="item.state"
              :class="{ 'row-isolated': item.state === '隔离' }">
            <td>{{ item.ledger_id }}</td>
            <td>{{ item.节点编号 }}</td>
            <td>{{ item.matched_edge ? `${item.matched_edge.from} → ${item.matched_edge.to}（${item.matched_edge.管道编号}）` : '—' }}</td>
            <td class="expr-cell">{{ item.转换表达式 }}</td>
            <td><span :class="['status-badge', ledgerStatusClass(item.state)]">{{ item.state }}</span>
              <span class="snap-tag">v{{ item.snapshot_version }}</span></td>
            <td class="iso-reason">{{ (item.reasons || []).join('；') || '—' }}</td>
            <td>
              <template v-if="item.state === '待确认'">
                <label class="review-pick"><input type="radio" :name="`review-${item.transform_id}`" value="确认"
                  :checked="reviewMap[item.transform_id] === '确认'" @change="reviewMap[item.transform_id]='确认'" /> 确认</label>
                <label class="review-pick"><input type="radio" :name="`review-${item.transform_id}`" value="拒绝"
                  :checked="reviewMap[item.transform_id] === '拒绝'" @change="reviewMap[item.transform_id]='拒绝'" /> 拒绝</label>
              </template>
              <span v-else class="muted">已闭环</span>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="review-bar">
        <input v-model="reviewRemark" class="reason-input" data-testid="review-remark" placeholder="核对备注（确认原因）" />
        <button class="btn primary" type="button" data-testid="submit-review" @click="submitReview">提交本批核对</button>
        <span class="muted">规则：恰好一条「确认」才受理；0 条或多条确认整批拒绝，同批其余项自动拒绝。</span>
      </div>
      <p v-if="reviewMessage" :class="reviewOk ? 'ok-text' : 'error-text'" data-testid="review-message">{{ reviewMessage }}</p>
    </article>

    <!-- 回传给数据流转编排的核对结果 -->
    <article class="pipe-panel" data-testid="reconciliation-panel">
      <h4>回传数据流转编排的核对结果</h4>
      <p v-if="!reconciliation.length" class="empty-state">暂无核对结果</p>
      <ul class="topo-list">
        <li v-for="(r, idx) in reconciliation" :key="idx">
          <span class="topo-text">v{{ r.snapshot_version }}：确认 {{ r.confirmed.节点编号 }}（{{ r.confirmed.reason }}）；
            拒绝 {{ (r.rejected || []).map((x: any) => x.节点编号).join('、') || '无' }}</span>
          <span class="iso-reason">{{ r.reviewed_at }}</span>
        </li>
      </ul>
    </article>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记转换节点</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field">
          <span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required />
        </label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot">
      <span>{{ graph.nodes?.length || 0 }} 个转换节点（{{ validCount }} 个在有效路径）· {{ graph.edges?.length || 0 }} 条有效路径边</span>
      <span>快照版本 v{{ graph.snapshot_version ?? ledgerVersion }}</span>
    </footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'

const ENDPOINT = '/api/transform'
const columns = ['节点编号', '输入来源', '输出目标', '转换表达式', '执行顺序', '依赖节点', '失败策略', '节点状态']
const STATUS_COLORS: Record<string, string> = { '草稿': '#e2e8f0', '待校验': '#fef3c7', '已启用': '#d1fae5', '已停用': '#fee2e2' }

const graph = reactive<{ nodes: any[]; edges: any[]; snapshot_version?: number; summary?: string }>({ nodes: [], edges: [] })
const ledger = ref<any[]>([])
const ledgerVersion = ref<number | undefined>()
const reconciliation = ref<any[]>([])
const nodePos = ref<Record<number, { x: number; y: number }>>({})
const selectedNode = ref<any>(null)
const svgW = ref(900); const svgH = ref(420)
const errorMessage = ref('')
const showCreate = ref(false)
const createValues = ref<Record<string, string>>({})
const reviewMap = reactive<Record<number, string>>({})
const reviewRemark = ref('')
const reviewMessage = ref('')
const reviewOk = ref(false)
let dragNode: any = null
const dagRef = ref<HTMLElement | null>(null)

const validCount = computed(() => graph.nodes.filter(n => n.on_valid_path).length)

function nodeColor(s: string) { return STATUS_COLORS[s] || '#e2e8f0' }
function ledgerStatusClass(state: string) {
  if (state === '已确认') return 'st-已启用'
  if (state === '已拒绝' || state === '隔离') return 'st-已停用'
  return 'st-待校验'
}

/** 按有效路径边的拓扑深度分层布局，隔离节点单独沉底。 */
function layout(nodes: any[], edges: any[]) {
  const depth: Record<number, number> = {}
  const adjacency: Record<number, number[]> = {}
  edges.forEach(e => { (adjacency[e.from] ||= []).push(e.to) })
  const visit = (id: number, seen = new Set<number>()): number => {
    if (depth[id] !== undefined) return depth[id]
    if (seen.has(id)) return 0
    seen.add(id)
    const next = (adjacency[id] || []).map(t => visit(t, seen) + 1)
    depth[id] = Math.max(0, ...next)
    return depth[id]
  }
  nodes.forEach(n => visit(n.id))
  const columns: Record<number, any[]> = {}
  nodes.forEach(n => { (columns[depth[n.id]] ||= []).push(n) })
  const colKeys = Object.keys(columns).map(Number).sort((a, b) => b - a)
  const w = 900
  const h = Math.max(420, (Math.max(...colKeys.map(k => columns[k].length)) + 1) * 120)
  svgW.value = w; svgH.value = h
  const pos: Record<number, { x: number; y: number }> = {}
  colKeys.forEach(k => {
    columns[k].forEach((n, i) => { pos[n.id] = { x: 100 + k * 180, y: 70 + i * 110 } })
  })
  nodePos.value = pos
}

function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) {
  dragNode = node
  const onMove = (ev: MouseEvent) => {
    if (!dragNode || !dagRef.value) return
    const rect = dagRef.value.getBoundingClientRect()
    nodePos.value[node.id] = {
      x: (ev.clientX - rect.left) * (svgW.value / rect.width),
      y: (ev.clientY - rect.top) * (svgH.value / rect.height),
    }
  }
  const onUp = () => {
    dragNode = null
    document.removeEventListener('mousemove', onMove)
    document.removeEventListener('mouseup', onUp)
  }
  document.addEventListener('mousemove', onMove)
  document.addEventListener('mouseup', onUp)
}

async function reload() {
  try {
    const [graphRes, ledgerRes, reconRes] = await Promise.all([
      request(ENDPOINT), request(`${ENDPOINT}/ledger`), request(`${ENDPOINT}/reconciliation`),
    ])
    const graphData = await graphRes.json()
    graph.nodes = graphData.nodes || []
    graph.edges = graphData.edges || []
    graph.snapshot_version = graphData.snapshot_version
    graph.summary = graphData.summary
    const ledgerData = await ledgerRes.json()
    ledger.value = ledgerData.items || []
    ledgerVersion.value = ledgerData.snapshot_version
    reconciliation.value = (await reconRes.json()).items || []
    if (selectedNode.value && !graph.nodes.some(n => n.id === selectedNode.value.id)) selectedNode.value = null
    ledger.value.forEach(item => { if (item.state === '待确认' && !reviewMap[item.transform_id]) reviewMap[item.transform_id] = '拒绝' })
    layout(graph.nodes, graph.edges)
  } catch (e) {
    errorMessage.value = '加载失败'
  }
}

async function syncLedger() {
  errorMessage.value = ''
  const res = await request(`${ENDPOINT}/ledger/sync`, { method: 'POST' })
  const data = await res.json()
  if (!data.ok) { errorMessage.value = data.message; return }
  await reload()
}

async function submitReview() {
  reviewMessage.value = ''
  const reviews = ledger.value
    .filter(item => item.state === '待确认' && reviewMap[item.transform_id])
    .map(item => ({ transform_id: item.transform_id, decision: reviewMap[item.transform_id] }))
  const res = await request(`${ENDPOINT}/ledger/review`, {
    method: 'POST',
    body: JSON.stringify({ reviews, remark: reviewRemark.value || null }),
  })
  const data = await res.json()
  reviewOk.value = data.ok
  reviewMessage.value = data.message
  if (data.ok) {
    reviewRemark.value = ''
    await reload()
  }
}

function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try {
    const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) })
    const data = await res.json()
    if (!data.ok) throw new Error(data.message)
    showCreate.value = false
    await reload()
  } catch (e: any) {
    errorMessage.value = e.message
  }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

onMounted(reload)
</script>
