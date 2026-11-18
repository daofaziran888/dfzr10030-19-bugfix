<template>
  <section class="page" data-module="flow">
    <header class="page-head">
      <div>
        <h2>数据流转编排</h2>
        <p class="page-desc">按管道声明的起始/中间/终止节点构建流转 DAG 快照，每条边都符合管道声明；停用或未通过校验的管道不产生可执行边。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记流转管道</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>

    <!-- 快照版本条：两个入口共享同一份版本，避免残留与不一致 -->
    <div class="snapshot-bar" data-testid="snapshot-bar">
      <div class="snapshot-meta">
        <strong>流转 DAG 快照 v{{ graph.snapshot_version ?? snapshot.version }}</strong>
        <span>{{ graph.summary || snapshot.summary }}</span>
        <em v-if="snapshot.previous_summary" class="prev-summary">上一版：{{ snapshot.previous_summary }}</em>
      </div>
      <div class="snapshot-tools">
        <label class="policy-pick">
          <span>共享节点冲突取舍</span>
          <select v-model="policyForm.policy" data-testid="conflict-policy">
            <option value="edge">边级启停（保留已启用管道的边，标记共享节点）</option>
            <option value="node">节点级冲突（共享节点整组暂停至停用管道清理）</option>
          </select>
        </label>
        <input v-model="policyForm.reason" class="reason-input" data-testid="policy-reason" placeholder="切换/重算必须填写原因" />
        <button class="btn" type="button" data-testid="apply-policy" @click="applyPolicy">应用取舍</button>
        <button class="btn" type="button" data-testid="rebuild-snapshot" @click="rebuildSnapshot">按旧版本重算</button>
      </div>
    </div>
    <p v-if="errorMessage" class="error-text" data-testid="error-text">{{ errorMessage }}</p>

    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker>
        <marker id="arrowhead-active" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#16a34a"/></marker></defs>
        <g v-for="edge in graph.edges" :key="`${edge.from}-${edge.to}-${edge.pipe_id}`">
          <line :class="['dag-edge', { 'edge-isolated': !edge.executable, 'edge-active': edge.executable }]"
                :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0"
                :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"
                :marker-end="edge.executable ? 'url(#arrowhead-active)' : 'url(#arrowhead)'"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6"
                font-size="10" :fill="edge.executable ? '#15803d' : '#94a3b8'" text-anchor="middle">
            {{ edge.管道编号 }}·{{ edge.label }}
          </text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]"
           @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6"
                :fill="nodeColor(node)" :stroke="node.conflict || node.paused ? '#dc2626' : (node.shared ? '#d97706' : 'transparent')"
                :stroke-width="node.shared ? 2 : 0"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
          <text v-if="node.shared" :x="nodePos[node.id]?.x + 48" :y="nodePos[node.id]?.y - 12" font-size="9" fill="#b45309" text-anchor="middle">共享</text>
        </g>
      </svg>
    </div>

    <!-- 节点详情：声明边、共享/冲突状态 -->
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>节点</dt><dd>{{ selectedNode.name }}</dd></dl>
      <dl><dt>共享节点</dt><dd>{{ selectedNode.shared ? `是（管道 ${selectedNode.pipes?.join('、')}）` : '否' }}</dd></dl>
      <dl><dt>边界冲突</dt><dd>{{ selectedNode.conflict ? '已启用与停用管道共享，存在冲突' : '无' }}</dd></dl>
      <dl><dt>暂停状态</dt><dd>{{ selectedNode.paused ? '节点级冲突整组暂停中' : '正常' }}</dd></dl>
      <dl class="full-line"><dt>关联声明边</dt>
        <dd><span v-for="e in edgesOf(selectedNode.name)" :key="`${e.pipe_id}-${e.from}-${e.to}`"
                 :class="['edge-chip', e.executable ? 'chip-on' : 'chip-off']">
          {{ e.管道编号 }}：{{ e.source_name }} → {{ e.target_name }}（{{ e.executable ? '可执行' : '已隔离' }}）
        </span></dd>
      </dl>
      <div class="full-line detail-close"><button class="btn ghost" type="button" @click="selectedNode=null">关闭详情</button></div>
    </div>

    <!-- 拓扑三清单：声明边 / 共享节点 / 停用隔离 -->
    <div class="topo-grid">
      <article class="topo-card" data-testid="declared-panel">
        <h4>声明边（{{ topology.declared_edges?.length || 0 }}）</h4>
        <ul class="topo-list">
          <li v-for="e in topology.declared_edges || []" :key="`${e.pipe_id}-${e.from}-${e.to}-${e.seq}`">
            <span :class="['edge-dot', e.executable ? 'dot-on' : 'dot-off']"></span>
            <span class="topo-text">{{ e.管道编号 }}：{{ e.from }} → {{ e.to }}</span>
            <span :class="['edge-tag', e.executable ? 'tag-on' : 'tag-off']">{{ e.executable ? '可执行' : '隔离' }}</span>
          </li>
        </ul>
      </article>
      <article class="topo-card" data-testid="shared-panel">
        <h4>共享节点（{{ topology.shared_nodes?.length || 0 }}）</h4>
        <ul class="topo-list">
          <li v-if="!(topology.shared_nodes || []).length" class="empty-state">暂无共享节点</li>
          <li v-for="n in topology.shared_nodes || []" :key="n.id">
            <span class="topo-text"><strong>{{ n.name }}</strong>（管道 {{ n.pipes.join('、') }}）</span>
            <span :class="['edge-tag', n.conflict ? 'tag-off' : 'tag-on']">{{ n.conflict ? '边界冲突' : '无冲突' }}</span>
            <span v-if="n.paused" class="edge-tag tag-off">已暂停</span>
          </li>
        </ul>
      </article>
      <article class="topo-card" data-testid="isolation-panel">
        <h4>停用隔离清单（{{ topology.isolation?.length || 0 }}）</h4>
        <ul class="topo-list">
          <li v-if="!(topology.isolation || []).length" class="empty-state">无隔离管道</li>
          <li v-for="item in topology.isolation || []" :key="item.pipe_id">
            <span class="topo-text"><strong>{{ item.管道编号 }}</strong>（{{ item.status }}）</span>
            <span class="iso-reason">{{ item.reasons.join('；') }}</span>
          </li>
        </ul>
      </article>
    </div>

    <!-- 管道台账：状态只能顺业务顺序推进，切换/变更/回退必须带原因 -->
    <article class="pipe-panel" data-testid="pipeline-panel">
      <h4>流转管道声明与状态履历</h4>
      <table class="data-table">
        <thead><tr><th>管道编号</th><th>管道名称</th><th>声明链路</th><th>状态</th><th>状态操作（回退/变更需填原因）</th><th>履历</th></tr></thead>
        <tbody>
          <tr v-for="pipe in pipelines" :key="pipe.id" :data-status="pipe.status">
            <td>{{ pipe.管道编号 }}</td>
            <td>{{ pipe.管道名称 }}</td>
            <td class="chain-cell">{{ chainOf(pipe) }}</td>
            <td><span :class="['status-badge', `st-${pipe.status}`]">{{ pipe.status }}</span></td>
            <td>
              <div class="action-row">
                <input v-model="reasonMap[pipe.id]" class="reason-input" :data-testid="`reason-${pipe.id}`" placeholder="原因（回退/变更必填）" />
                <button v-for="action in actions" :key="action" class="btn"
                        :data-testid="`action-${pipe.id}-${action}`"
                        @click="runAction(action, pipe)">{{ action }}</button>
              </div>
              <p v-if="actionErrors[pipe.id]" class="error-text">{{ actionErrors[pipe.id] }}</p>
            </td>
            <td>
              <button class="link" type="button" :data-testid="`history-${pipe.id}`" @click="toggleHistory(pipe)">
                {{ historyOpen[pipe.id] ? '收起履历' : `查看履历（${pipe.status_history?.length || 0}）` }}
              </button>
              <ul v-if="historyOpen[pipe.id]" class="history-list" :data-testid="`history-list-${pipe.id}`">
                <li v-for="(h, idx) in pipe.status_history || []" :key="idx">
                  {{ h.time }} {{ h.from || '—' }} → {{ h.to }} <em>{{ h.kind }}</em>：{{ h.reason }}
                </li>
              </ul>
            </td>
          </tr>
        </tbody>
      </table>
    </article>

    <!-- 转换服务回传的核对结果 -->
    <article class="pipe-panel" data-testid="reconciliation-panel">
      <h4>转换管道服务回传核对结果（只读）</h4>
      <p v-if="!reconciliation.length" class="empty-state">转换页尚未提交核对</p>
      <ul class="topo-list">
        <li v-for="(r, idx) in reconciliation" :key="idx">
          <span class="topo-text">快照 v{{ r.snapshot_version }}：确认 {{ r.confirmed.节点编号 }}（{{ r.confirmed.reason }}）；
            拒绝 {{ (r.rejected || []).map((x: any) => x.节点编号).join('、') || '无' }}</span>
          <span class="iso-reason">{{ r.reviewed_at }}</span>
        </li>
      </ul>
    </article>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记流转管道</h3>
        <label v-for="field in createFields" :key="field" class="create-field">
          <span>{{ field }}<em v-if="requiredFields.includes(field)">*</em><i v-else>（可选，补全声明才可生成可执行边）</i></span>
          <input v-model="createValues[field]" :placeholder="`请输入${field}`" :required="requiredFields.includes(field)" />
        </label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ graph.nodes?.length || 0 }} 个声明节点 · {{ graph.edges?.length || 0 }} 条声明边</span><span>快照版本 v{{ graph.snapshot_version ?? snapshot.version }}</span></footer>
  </section>
</template>
<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'

const ENDPOINT = '/api/flow'
const actions = ['提交校验', '启用管道', '停用管道']
const STATUS_COLORS: Record<string, string> = { '草稿': '#e2e8f0', '待校验': '#fef3c7', '已启用': '#d1fae5', '已停用': '#fee2e2' }
const createFields = ['管道编号', '管道名称', '起始节点', '终止节点', '中间节点']
const requiredFields = ['管道编号', '管道名称', '起始节点']

const graph = reactive<{ nodes: any[]; edges: any[]; snapshot_version?: number; summary?: string }>({ nodes: [], edges: [] })
const snapshot = ref<Record<string, any>>({})
const topology = ref<Record<string, any>>({})
const pipelines = ref<any[]>([])
const reconciliation = ref<any[]>([])
const nodePos = ref<Record<number, { x: number; y: number }>>({})
const selectedNode = ref<any>(null)
const svgW = ref(900); const svgH = ref(420)
const errorMessage = ref('')
const showCreate = ref(false)
const createValues = ref<Record<string, string>>({})
const reasonMap = reactive<Record<number, string>>({})
const actionErrors = reactive<Record<number, string>>({})
const historyOpen = reactive<Record<number, boolean>>({})
const policyForm = reactive<{ policy: string; reason: string }>({ policy: 'edge', reason: '' })
let dragNode: any = null
const dagRef = ref<HTMLElement | null>(null)

function nodeColor(node: any) {
  if (node.paused) return '#fee2e2'
  return STATUS_COLORS[node.status] || '#e2e8f0'
}

/** 按声明边的拓扑深度分层布局，不再使用列表顺序。 */
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
  const h = Math.max(420, (Math.max(...colKeys.map(k => columns[k].length)) + 1) * 110)
  svgW.value = w; svgH.value = h
  const pos: Record<number, { x: number; y: number }> = {}
  colKeys.forEach(k => {
    columns[k].forEach((n, i) => {
      pos[n.id] = { x: 90 + k * 170, y: 70 + i * 110 }
    })
  })
  nodePos.value = pos
}

function edgesOf(name: string) {
  return (topology.value.declared_edges || []).filter((e: any) => e.from === name || e.to === name)
}
function chainOf(pipe: any) {
  const view = (snapshot.value.pipelines || []).find((p: any) => p.pipe_id === pipe.id)
  return (view?.chain || []).join(' → ')
}
function selectNode(node: any) {
  selectedNode.value = selectedNode.value?.id === node.id ? null : node
}
function startDrag(e: MouseEvent, node: any) {
  dragNode = node
  const onMove = (ev: MouseEvent) => {
    if (!dragNode || !dagRef.value) return
    const rect = dagRef.value.getBoundingClientRect()
    const scaleX = svgW.value / rect.width
    const scaleY = svgH.value / rect.height
    nodePos.value[node.id] = { x: (ev.clientX - rect.left) * scaleX, y: (ev.clientY - rect.top) * scaleY }
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
    const [graphRes, snapRes, topoRes, pipeRes, reconRes] = await Promise.all([
      request(ENDPOINT), request(`${ENDPOINT}/snapshot`), request(`${ENDPOINT}/topology`),
      request(`${ENDPOINT}/pipelines`), request(`${ENDPOINT}/reconciliation`),
    ])
    const graphData = await graphRes.json()
    graph.nodes = graphData.nodes || []
    graph.edges = graphData.edges || []
    graph.snapshot_version = graphData.snapshot_version
    graph.summary = graphData.summary
    snapshot.value = await snapRes.json()
    topology.value = await topoRes.json()
    const pipeData = await pipeRes.json()
    pipelines.value = pipeData.items || []
    reconciliation.value = (await reconRes.json()).items || []
    policyForm.policy = snapshot.value.conflict_policy || 'edge'
    // 选中节点若已不在快照中则清理，避免工作台面板残留
    if (selectedNode.value && !graph.nodes.some(n => n.id === selectedNode.value.id)) selectedNode.value = null
    layout(graph.nodes, graph.edges)
  } catch (e) {
    errorMessage.value = '加载失败'
  }
}

async function runAction(action: string, pipe: any) {
  actionErrors[pipe.id] = ''
  try {
    const res = await request(`${ENDPOINT}/${pipe.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action, remark: reasonMap[pipe.id] || null }),
    })
    const data = await res.json()
    if (!data.ok) {
      actionErrors[pipe.id] = data.message
      return
    }
    reasonMap[pipe.id] = ''
    await reload()
  } catch (e: any) {
    actionErrors[pipe.id] = e.message
  }
}

async function toggleHistory(pipe: any) {
  historyOpen[pipe.id] = !historyOpen[pipe.id]
  if (historyOpen[pipe.id] && !(pipe.status_history || []).length) {
    const res = await request(`${ENDPOINT}/${pipe.id}/history`)
    const data = await res.json()
    pipe.status_history = data.items || []
  }
}

async function applyPolicy() {
  errorMessage.value = ''
  const res = await request(`${ENDPOINT}/conflict-policy`, {
    method: 'POST',
    body: JSON.stringify({ policy: policyForm.policy, reason: policyForm.reason }),
  })
  const data = await res.json()
  if (!data.ok) { errorMessage.value = data.message; return }
  policyForm.reason = ''
  await reload()
}

async function rebuildSnapshot() {
  errorMessage.value = ''
  if (!policyForm.reason.trim()) { errorMessage.value = '重算快照必须填写原因'; return }
  const res = await request(`${ENDPOINT}/snapshot/rebuild`, {
    method: 'POST',
    body: JSON.stringify({ action: '重算快照', remark: policyForm.reason }),
  })
  const data = await res.json()
  if (!data.ok) { errorMessage.value = data.message; return }
  policyForm.reason = ''
  await reload()
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
