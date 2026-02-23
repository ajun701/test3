<template>
  <!-- ========== 登录/注册页 ========== -->
  <div v-if="!isLoggedIn" class="login-page">
    <div class="login-card">
      <h2>退运费智能审核中台</h2>
      <el-tabs v-model="authTab" stretch>
        <el-tab-pane label="登录" name="login">
          <el-form @submit.prevent="doLogin" label-width="0">
            <el-form-item><el-input v-model="authForm.username" placeholder="用户名" prefix-icon="User" clearable /></el-form-item>
            <el-form-item><el-input v-model="authForm.password" placeholder="密码" type="password" prefix-icon="Lock" show-password clearable /></el-form-item>
            <el-button type="primary" :loading="authLoading" @click="doLogin" style="width:100%">登录</el-button>
          </el-form>
        </el-tab-pane>
        <el-tab-pane label="注册" name="register">
          <el-form @submit.prevent="doRegister" label-width="0">
            <el-form-item><el-input v-model="authForm.username" placeholder="用户名" prefix-icon="User" clearable /></el-form-item>
            <el-form-item><el-input v-model="authForm.password" placeholder="密码" type="password" prefix-icon="Lock" show-password clearable /></el-form-item>
            <el-form-item><el-input v-model="authForm.registerKey" placeholder="注册密钥" prefix-icon="Key" show-password clearable /></el-form-item>
            <el-button type="success" :loading="authLoading" @click="doRegister" style="width:100%">注册</el-button>
          </el-form>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>

  <!-- ========== 主应用 ========== -->
  <div v-else class="app">
    <header class="hero">
      <div class="hero-inner">
        <div><h1>退运费智能审核中台</h1><p>Vue3 + FastAPI + Celery</p></div>
        <div class="user-bar">
          <span>{{ currentUser }}</span>
          <el-button size="small" type="info" plain @click="doLogout">退出</el-button>
        </div>
      </div>
    </header>

    <el-tabs v-model="tab" type="border-card">
      <el-tab-pane label="1. 清洗" name="clean">
        <el-upload action="#" :auto-upload="false" :on-change="onCleanFile" :limit="1" drag>
          <div class="el-upload__text">上传退运费登记表</div>
        </el-upload>
        <div class="bar">
          <el-input-number v-model="cleanPreviewRows" :min="20" :max="1000" :step="20" />
          <el-button type="primary" :loading="cleanLoading" @click="runClean">执行清洗</el-button>
        </div>
        <TableView title="上传预览" :preview="cleanSourcePreview" v-model:displayRows="cleanSourceShow" />
        <div v-if="cleanRes" class="panel">
          <el-row :gutter="12">
            <el-col :span="8"><el-statistic title="总行" :value="cleanRes.total_rows" /></el-col>
            <el-col :span="8"><el-statistic title="正常" :value="cleanRes.normal_rows" /></el-col>
            <el-col :span="8"><el-statistic title="异常" :value="cleanRes.abnormal_rows" /></el-col>
          </el-row>
          <div class="bar">
            <el-button type="success" @click="download(cleanRes.normal_file_url)">下载正常表</el-button>
            <el-button type="warning" @click="download(cleanRes.abnormal_file_url)">下载异常表</el-button>
          </div>
          <div class="grid">
            <TableView title="正常预览" :preview="cleanRes.normal_preview" v-model:displayRows="cleanNormalShow" />
            <TableView title="异常预览" :preview="cleanRes.abnormal_preview" v-model:displayRows="cleanAbnormalShow" />
          </div>
        </div>
      </el-tab-pane>

      <el-tab-pane label="2. 入库匹配" name="match">
        <el-form label-width="170px">
          <el-form-item label="使用步骤一正常表">
            <el-switch v-model="matchUseStep1" :disabled="!cleanRes?.normal_file_url" />
          </el-form-item>
          <el-form-item v-if="!matchUseStep1" label="步骤一正常表">
            <el-upload action="#" :auto-upload="false" :on-change="onMatchSourceFile" :limit="1">
              <el-button>选择文件</el-button>
            </el-upload>
          </el-form-item>
          <el-form-item label="已入库单号表">
            <el-upload action="#" :auto-upload="false" :on-change="onMatchInboundFile" :limit="1">
              <el-button>选择文件</el-button>
            </el-upload>
          </el-form-item>
        </el-form>
        <div class="bar">
          <el-input-number v-model="matchPreviewRows" :min="20" :max="1000" :step="20" />
          <el-button type="primary" :loading="matchLoading" @click="runMatch">执行匹配</el-button>
        </div>
        <div class="grid">
          <TableView title="源表预览" :preview="matchSourcePreview" v-model:displayRows="matchSourceShow" />
          <TableView title="入库表预览" :preview="matchInboundPreview" v-model:displayRows="matchInboundShow" />
        </div>
        <div v-if="matchRes" class="panel">
          <el-row :gutter="12">
            <el-col :span="8"><el-statistic title="总行" :value="matchRes.total_rows" /></el-col>
            <el-col :span="8"><el-statistic title="已入库" :value="matchRes.inbound_rows" /></el-col>
            <el-col :span="8"><el-statistic title="未入库" :value="matchRes.pending_rows" /></el-col>
          </el-row>
          <div class="bar">
            <el-button type="success" @click="download(matchRes.inbound_file_url)">下载已入库</el-button>
            <el-button type="warning" @click="download(matchRes.pending_file_url)">下载未入库</el-button>
          </div>
          <div class="grid">
            <TableView title="已入库预览" :preview="matchRes.inbound_preview" v-model:displayRows="matchInboundResShow" />
            <TableView title="未入库预览" :preview="matchRes.pending_preview" v-model:displayRows="matchPendingResShow" />
          </div>
        </div>
      </el-tab-pane>

      <el-tab-pane label="3. AI复核" name="ai">
        <el-form label-width="190px">
          <el-form-item label="使用步骤二已入库表">
            <el-switch v-model="aiUseStep2" :disabled="!matchRes?.inbound_file_url" />
          </el-form-item>
          <el-form-item v-if="!aiUseStep2" label="上传待复核表">
            <el-upload action="#" :auto-upload="false" :on-change="onAiFile" :limit="1" drag>
              <div class="el-upload__text">上传已入库表</div>
            </el-upload>
          </el-form-item>
          <el-form-item label="DashScope API Key"><el-input v-model="aiApiKey" show-password clearable /></el-form-item>
          <el-form-item label="模型"><el-input v-model="aiModel" /></el-form-item>
          <el-form-item label="最大图片数"><el-input-number v-model="aiMaxImages" :min="1" :max="10" /></el-form-item>
          <el-form-item label="最大处理行数"><el-input-number v-model="aiMaxRows" :min="1" :max="10000" :step="50" /></el-form-item>
          <el-form-item label="处理速度(几秒/几条)">
            <div class="bar">
              <el-input-number v-model="aiRateSeconds" :min="0.1" :step="0.1" :precision="1" />
              <span>/</span>
              <el-input-number v-model="aiRateRows" :min="1" :step="1" />
              <span>≈ {{ aiMinIntervalSec.toFixed(3) }} 秒/条</span>
            </div>
          </el-form-item>
        </el-form>
        <div class="bar">
          <el-button type="primary" :loading="aiStarting" @click="startAi" :disabled="aiStatus==='running'">启动任务</el-button>
          <el-button type="warning" @click="pauseAi" :disabled="aiStatus!=='running'">暂停</el-button>
          <el-button type="success" @click="resumeAi" :disabled="!['paused','error','pending'].includes(aiStatus)">继续</el-button>
          <el-button @click="refreshAi" :disabled="!taskId">刷新</el-button>
          <el-button :loading="snapshotLoading" @click="downloadTaskSnapshot" :disabled="!taskId">生成已处理/未处理快照</el-button>
        </div>
        <TableView title="待复核预览" :preview="aiSourcePreview" v-model:displayRows="aiSourceShow" />
        <div v-if="aiTask" class="panel">
          <el-row :gutter="12">
            <el-col :span="6"><el-statistic title="计划" :value="aiTask.total" /></el-col>
            <el-col :span="6"><el-statistic title="已处理" :value="aiTask.processed" /></el-col>
            <el-col :span="6"><el-statistic title="正常" :value="aiTask.ok_rows" /></el-col>
            <el-col :span="6"><el-statistic title="异常" :value="aiTask.bad_rows" /></el-col>
          </el-row>
          <el-progress :percentage="Math.round((aiTask.progress_ratio||0)*100)" :text-inside="true" :stroke-width="22" />
          <el-alert :title="`状态：${aiTask.status}`" type="info" :closable="false" />
          <el-alert v-if="aiTask.min_interval_sec !== undefined && aiTask.min_interval_sec !== null" :title="`当前速度：${Number(aiTask.min_interval_sec).toFixed(3)} 秒/条`" type="success" :closable="false" />
          <el-alert v-if="aiTask.error_message" :title="aiTask.error_message" type="error" :closable="false" />
          <el-alert
            v-if="aiTask.alignment_report?.can_compare"
            :title="`一致性校验：${aiTask.alignment_report.ok ? '通过' : '存在差异'}（缺失 ${aiTask.alignment_report.missing_rows || 0}，新增 ${aiTask.alignment_report.extra_rows || 0}）`"
            type="warning"
            :closable="false"
          />
          <div class="bar" v-if="aiTask.artifacts?.length">
            <el-button v-for="(u,i) in aiTask.artifacts" :key="`${u}${i}`" @click="download('/'+u)">{{ artifactLabel(u, i) }}</el-button>
          </div>
          <div class="bar" v-if="snapshotRes">
            <el-button type="primary" plain @click="download(snapshotRes.processed_file_url)">下载已处理部分</el-button>
            <el-button type="warning" plain @click="download(snapshotRes.unprocessed_file_url)">下载未处理部分</el-button>
            <el-button v-if="snapshotRes.ok_file_url" type="success" plain @click="download(snapshotRes.ok_file_url)">下载可打款</el-button>
            <el-button v-if="snapshotRes.bad_file_url" type="danger" plain @click="download(snapshotRes.bad_file_url)">下载需回访</el-button>
          </div>
        </div>
        <div v-if="taskId" class="panel">
          <div class="bar">
            <el-select v-model="aiRowsScope" @change="onAiRowsQueryChange">
              <el-option label="全部" value="all" />
              <el-option label="已处理" value="processed" />
              <el-option label="未处理" value="pending" />
            </el-select>
            <el-select v-model="aiRowsSize" @change="onAiRowsQueryChange">
              <el-option :value="20" label="20" /><el-option :value="50" label="50" /><el-option :value="100" label="100" />
            </el-select>
          </div>
          <el-table :data="aiRows.rows" border stripe height="340" v-loading="aiRowsLoading">
            <el-table-column v-for="c in aiRows.columns" :key="`r-${c}`" :prop="c" :label="c" min-width="130" show-overflow-tooltip />
          </el-table>
          <el-pagination class="pager" layout="total, prev, pager, next" :total="aiRows.total_rows" :page-size="aiRows.page_size" :current-page="aiRows.page" @current-change="onAiRowsPageChange" />
        </div>
      </el-tab-pane>

      <el-tab-pane label="4. 历史记录" name="history">
        <div class="bar">
          <el-date-picker
            v-model="historyTimeRange"
            type="datetimerange"
            range-separator="至"
            start-placeholder="开始时间"
            end-placeholder="结束时间"
            value-format="YYYY-MM-DD HH:mm:ss"
          />
          <el-input v-model="historyStage" clearable placeholder="阶段过滤" style="max-width: 200px" />
          <el-input v-model="historyAction" clearable placeholder="动作过滤" style="max-width: 200px" />
          <el-input v-model="historyOperator" clearable placeholder="操作人过滤" style="max-width: 160px" />
          <el-button type="primary" @click="loadHistory(true)">查询</el-button>
          <el-button @click="loadHistory(false)">刷新</el-button>
          <el-button :loading="historyExporting" @click="downloadHistoryCsv">下载CSV</el-button>
        </div>
        <el-table :data="historyItems" border stripe height="420" v-loading="historyLoading">
          <el-table-column prop="timestamp" label="时间" width="170" />
          <el-table-column prop="operator" label="操作人" width="110" />
          <el-table-column prop="stage" label="阶段" width="130" />
          <el-table-column prop="action" label="动作" width="130" />
          <el-table-column prop="input_rows" label="输入" width="80" />
          <el-table-column prop="output_rows" label="输出" width="80" />
          <el-table-column label="文件下载" width="200">
            <template #default="s">
              <template v-if="getArtifacts(s.row).length">
                <el-button v-for="(u,i) in getArtifacts(s.row)" :key="i" size="small" type="primary" link @click="download(u)">文件{{ i+1 }}</el-button>
              </template>
              <span v-else style="color:#999">-</span>
            </template>
          </el-table-column>
          <el-table-column label="详情" min-width="260" show-overflow-tooltip><template #default="s">{{ fmtDetail(s.row.detail) }}</template></el-table-column>
        </el-table>
        <el-pagination class="pager" layout="total, sizes, prev, pager, next" :page-sizes="[20,50,100]" :total="historyTotal" :page-size="historySize" :current-page="historyPage" @size-change="onHistorySizeChange" @current-change="onHistoryPageChange" />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup>
import { computed, defineComponent, h, onMounted, onUnmounted, reactive, ref, resolveComponent, watch } from 'vue'
import axios from 'axios'
import { ElMessage } from 'element-plus'

const runtimeHost = typeof window !== 'undefined' && window.location?.hostname ? window.location.hostname : 'localhost'
const runtimeBackendPort = import.meta.env.VITE_BACKEND_PORT || '8000'
const API_BASE = import.meta.env.VITE_API_BASE || `http://${runtimeHost}:${runtimeBackendPort}/api/v1`
const BASE_URL = import.meta.env.VITE_BASE_URL || `http://${runtimeHost}:${runtimeBackendPort}`
const AI_TASK_ID_KEY = 'refund_audit_active_task_id'
const AI_SPEED_SETTINGS_KEY = 'refund_audit_ai_speed_settings'
const TOKEN_KEY = 'refund_audit_token'
const USERNAME_KEY = 'refund_audit_username'

// ========== 认证状态 ==========
const authTab = ref('login')
const authForm = reactive({ username: '', password: '', registerKey: '' })
const authLoading = ref(false)
const currentUser = ref(localStorage.getItem(USERNAME_KEY) || '')
const isLoggedIn = ref(!!localStorage.getItem(TOKEN_KEY))

const getToken = () => localStorage.getItem(TOKEN_KEY) || ''
const setAuth = (token, username) => {
  localStorage.setItem(TOKEN_KEY, token)
  localStorage.setItem(USERNAME_KEY, username)
  currentUser.value = username
  isLoggedIn.value = true
}
const clearAuth = () => {
  localStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(USERNAME_KEY)
  currentUser.value = ''
  isLoggedIn.value = false
}

const http = axios.create({ timeout: 30000 })
// 请求拦截器：自动附加 Bearer token
http.interceptors.request.use((config) => {
  const token = getToken()
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})
// 响应拦截器：401 自动退出到登录页
http.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err?.response?.status === 401) {
      clearAuth()
      ElMessage.warning('登录已过期，请重新登录')
    }
    return Promise.reject(err)
  }
)

const doLogin = async () => {
  if (!authForm.username || !authForm.password) return ElMessage.warning('请填写用户名和密码')
  authLoading.value = true
  try {
    const r = (await axios.post(`${API_BASE}/auth/login`, { username: authForm.username, password: authForm.password })).data
    setAuth(r.access_token, r.username)
    ElMessage.success('登录成功')
  } catch (e) {
    ElMessage.error(typeof e?.response?.data?.detail === 'string' ? e.response.data.detail : '登录失败')
  } finally { authLoading.value = false }
}
const doRegister = async () => {
  if (!authForm.username || !authForm.password || !authForm.registerKey) return ElMessage.warning('请填写所有字段')
  authLoading.value = true
  try {
    const r = (await axios.post(`${API_BASE}/auth/register`, { username: authForm.username, password: authForm.password, register_key: authForm.registerKey })).data
    setAuth(r.access_token, r.username)
    ElMessage.success('注册成功')
  } catch (e) {
    ElMessage.error(typeof e?.response?.data?.detail === 'string' ? e.response.data.detail : '注册失败')
  } finally { authLoading.value = false }
}
const doLogout = () => { clearAuth(); ElMessage.info('已退出') }
const tab = ref('clean')
const opts = [{l:'20',v:20},{l:'50',v:50},{l:'100',v:100},{l:'200',v:200},{l:'500',v:500},{l:'全部',v:0}]
const errMsg = (e, d='请求失败') => (typeof e?.response?.data?.detail === 'string' ? e.response.data.detail : d)
const abs = (u) => (!u ? '' : (String(u).startsWith('http') ? u : `${BASE_URL}/${String(u).replace(/^\/+/, '')}`))
const download = (u) => { const x = abs(u); if (!x) return ElMessage.warning('下载链接无效'); window.open(x, '_blank', 'noopener') }
const artifactLabel = (u, i) => {
  const v = decodeURIComponent(String(u || '')).toLowerCase()
  if (v.includes('ai复核正常') || v.includes('ai可打款')) return '下载可打款'
  if (v.includes('ai复核异常') || v.includes('ai需回访')) return '下载需回访'
  if (v.includes('ai未处理')) return '下载未处理'
  return `下载产物${i + 1}`
}
const normPreview = (d) => ({ total_rows: Number(d?.total_rows||0), columns: Array.isArray(d?.columns)?d.columns:[], rows: Array.isArray(d?.rows)?d.rows:[] })
const uploadPreview = async (f, n=1000) => { const fd = new FormData(); fd.append('file', f); fd.append('sample_rows', String(n)); return normPreview((await http.post(`${API_BASE}/preview-table`, fd)).data) }
const artifactPreview = async (u, n=1000) => normPreview((await http.get(`${API_BASE}/artifact/preview`, { params: { file_url: u, sample_rows: n } })).data)

const TableView = defineComponent({
  props: { title: String, preview: Object, displayRows: Number },
  emits: ['update:displayRows'],
  setup(p, { emit }) {
    const rows = () => (!p.preview?.rows ? [] : (p.displayRows === 0 ? p.preview.rows : p.preview.rows.slice(0, p.displayRows || 50)))
    return () => h('div', { class: 'panel' }, [
      h('div', { class: 'bar' }, [h('strong', {}, p.title || ''), h('span', {}, `显示 ${rows().length}/${p.preview?.total_rows || 0}`),
        h(resolveComponent('el-select'), { modelValue: p.displayRows ?? 50, 'onUpdate:modelValue': (v) => emit('update:displayRows', v), style: 'width:110px' },
          () => opts.map((o) => h(resolveComponent('el-option'), { key: String(o.v), label: o.l, value: o.v })))
      ]),
      p.preview?.columns?.length
        ? h(resolveComponent('el-table'), { data: rows(), border: true, stripe: true, height: 300 },
            () => p.preview.columns.map((c) => h(resolveComponent('el-table-column'), { key: `c-${c}`, prop: c, label: c, minWidth: 130, showOverflowTooltip: true })))
        : h(resolveComponent('el-empty'), { description: '暂无数据' }),
    ])
  },
})

const cleanFile = ref(null), cleanLoading = ref(false), cleanRes = ref(null), cleanPreviewRows = ref(200), cleanSourcePreview = ref(null), cleanSourceShow = ref(50), cleanNormalShow = ref(50), cleanAbnormalShow = ref(50)
const onCleanFile = async (f) => {
  cleanFile.value = f?.raw || null
  cleanSourcePreview.value = null
  if (!cleanFile.value) return
  try { cleanSourcePreview.value = await uploadPreview(cleanFile.value) } catch (e) { ElMessage.error(errMsg(e, '文件预览失败')) }
}
const runClean = async () => { if (!cleanFile.value) return ElMessage.warning('请先上传文件'); cleanLoading.value = true; const fd = new FormData(); fd.append('file', cleanFile.value); fd.append('preview_rows', String(cleanPreviewRows.value)); try { cleanRes.value = (await http.post(`${API_BASE}/clean`, fd)).data; ElMessage.success('步骤一完成') } catch (e) { ElMessage.error(errMsg(e, '清洗失败')) } finally { cleanLoading.value = false } }

const matchUseStep1 = ref(true), matchSourceFile = ref(null), matchInboundFile = ref(null), matchLoading = ref(false), matchRes = ref(null), matchPreviewRows = ref(200), matchSourcePreview = ref(null), matchInboundPreview = ref(null), matchSourceShow = ref(50), matchInboundShow = ref(50), matchInboundResShow = ref(50), matchPendingResShow = ref(50)
const syncMatchSource = async () => { if (matchUseStep1.value) { matchSourcePreview.value = cleanRes.value?.normal_file_url ? await artifactPreview(cleanRes.value.normal_file_url) : null } else { matchSourcePreview.value = matchSourceFile.value ? await uploadPreview(matchSourceFile.value) : null } }
watch(matchUseStep1, async () => { try { await syncMatchSource() } catch (e) { ElMessage.error(errMsg(e, '加载源表预览失败')) } })
watch(() => cleanRes.value?.normal_file_url, async () => { if (matchUseStep1.value) { try { await syncMatchSource() } catch (e) { ElMessage.error(errMsg(e, '加载步骤一结果失败')) } } })
const onMatchSourceFile = async (f) => { matchSourceFile.value = f?.raw || null; if (!matchUseStep1.value) { try { await syncMatchSource() } catch (e) { ElMessage.error(errMsg(e, '源表预览失败')) } } }
const onMatchInboundFile = async (f) => {
  matchInboundFile.value = f?.raw || null
  matchInboundPreview.value = null
  if (!matchInboundFile.value) return
  try { matchInboundPreview.value = await uploadPreview(matchInboundFile.value) } catch (e) { ElMessage.error(errMsg(e, '入库表预览失败')) }
}
const runMatch = async () => {
  const fd = new FormData()
  fd.append('preview_rows', String(matchPreviewRows.value))
  if (matchUseStep1.value) {
    if (!cleanRes.value?.normal_file_url) return ElMessage.warning('请先完成步骤一')
    fd.append('source_file_url', cleanRes.value.normal_file_url)
  } else if (matchSourceFile.value) {
    fd.append('source_file', matchSourceFile.value)
  } else return ElMessage.warning('请上传源表')
  if (!matchInboundFile.value) return ElMessage.warning('请上传入库表')
  fd.append('inbound_file', matchInboundFile.value)
  matchLoading.value = true
  try {
    matchRes.value = (await http.post(`${API_BASE}/match`, fd)).data
    ElMessage.success('步骤二完成')
  } catch (e) {
    ElMessage.error(errMsg(e, '匹配失败'))
  } finally {
    matchLoading.value = false
  }
}

const aiUseStep2 = ref(true), aiFile = ref(null), aiSourcePreview = ref(null), aiSourceShow = ref(50), aiApiKey = ref(''), aiModel = ref('qwen3-vl-flash'), aiMaxImages = ref(4), aiMaxRows = ref(300), aiRateSeconds = ref(2.0), aiRateRows = ref(1), aiStarting = ref(false), taskId = ref(''), aiTask = ref(null), aiStatus = ref(''), aiRowsScope = ref('all'), aiRowsSize = ref(50), aiRowsLoading = ref(false)
const aiMinIntervalSec = computed(() => {
  const seconds = Number(aiRateSeconds.value || 0)
  const rows = Number(aiRateRows.value || 0)
  if (!Number.isFinite(seconds) || !Number.isFinite(rows) || seconds <= 0 || rows <= 0) return 0.8
  return Math.max(0.0, seconds / rows)
})
const aiRows = reactive({ rows: [], columns: [], total_rows: 0, page: 1, page_size: 50 })
const snapshotLoading = ref(false), snapshotRes = ref(null)
const getStoredTaskId = () => {
  if (typeof window === 'undefined') return ''
  return String(window.localStorage.getItem(AI_TASK_ID_KEY) || '').trim()
}
const setStoredTaskId = (id) => {
  if (typeof window === 'undefined') return
  const v = String(id || '').trim()
  if (v) window.localStorage.setItem(AI_TASK_ID_KEY, v)
  else window.localStorage.removeItem(AI_TASK_ID_KEY)
}
const loadAiSpeedSettings = () => {
  if (typeof window === 'undefined') return
  try {
    const parsed = JSON.parse(window.localStorage.getItem(AI_SPEED_SETTINGS_KEY) || '{}')
    if (Number.isFinite(Number(parsed.seconds)) && Number(parsed.seconds) > 0) aiRateSeconds.value = Number(parsed.seconds)
    if (Number.isFinite(Number(parsed.rows)) && Number(parsed.rows) > 0) aiRateRows.value = Number(parsed.rows)
  } catch (_) {}
}
const saveAiSpeedSettings = () => {
  if (typeof window === 'undefined') return
  const payload = JSON.stringify({ seconds: Number(aiRateSeconds.value || 0), rows: Number(aiRateRows.value || 0) })
  window.localStorage.setItem(AI_SPEED_SETTINGS_KEY, payload)
}
loadAiSpeedSettings()
watch([aiRateSeconds, aiRateRows], saveAiSpeedSettings)
const syncAiSource = async () => { if (aiUseStep2.value) { aiSourcePreview.value = matchRes.value?.inbound_file_url ? await artifactPreview(matchRes.value.inbound_file_url) : null } else { aiSourcePreview.value = aiFile.value ? await uploadPreview(aiFile.value) : null } }
watch(aiUseStep2, async () => { try { await syncAiSource() } catch (e) { ElMessage.error(errMsg(e, '加载步骤三预览失败')) } })
watch(() => matchRes.value?.inbound_file_url, async () => { if (aiUseStep2.value) { try { await syncAiSource() } catch (e) { ElMessage.error(errMsg(e, '加载步骤二结果失败')) } } })
const onAiFile = async (f) => {
  aiFile.value = f?.raw || null
  if (!aiUseStep2.value) { try { await syncAiSource() } catch (e) { ElMessage.error(errMsg(e, '文件预览失败')) } }
}
const fetchAiStatus = async () => {
  if (!taskId.value) return
  aiTask.value = (await http.get(`${API_BASE}/ai-task/${taskId.value}/status`)).data
  aiStatus.value = aiTask.value.status
  const interval = Number(aiTask.value?.min_interval_sec)
  if (Number.isFinite(interval) && interval > 0) {
    aiRateSeconds.value = Number(interval.toFixed(3))
    aiRateRows.value = 1
  }
  setStoredTaskId(taskId.value)
}
const fetchAiRows = async () => { if (!taskId.value) return; aiRowsLoading.value = true; try { const d = (await http.get(`${API_BASE}/ai-task/${taskId.value}/rows`, { params: { scope: aiRowsScope.value, page: aiRows.page || 1, page_size: aiRowsSize.value } })).data; aiRows.rows = d.rows || []; aiRows.columns = d.columns || []; aiRows.total_rows = d.total_rows || 0; aiRows.page = d.page || 1; aiRows.page_size = d.page_size || aiRowsSize.value } finally { aiRowsLoading.value = false } }
const onAiRowsQueryChange = async () => { aiRows.page = 1; await fetchAiRows() }
const onAiRowsPageChange = async (p) => { aiRows.page = p; await fetchAiRows() }
let timer = null, errCount = 0
const stopPoll = () => { if (timer) { clearInterval(timer); timer = null } }
const startPoll = () => { stopPoll(); errCount = 0; timer = setInterval(async () => { try { await fetchAiStatus(); await fetchAiRows(); errCount = 0; if (['completed','error','paused'].includes(aiStatus.value)) stopPoll() } catch (e) { errCount += 1; if (errCount >= 3) { stopPoll(); ElMessage.error(errMsg(e, '任务轮询失败')) } } }, 1500) }
const startAi = async () => {
  const fd = new FormData()
  if (aiUseStep2.value) {
    if (!matchRes.value?.inbound_file_url) return ElMessage.warning('请先完成步骤二')
    fd.append('file_url', matchRes.value.inbound_file_url)
  } else if (aiFile.value) {
    fd.append('file', aiFile.value)
  } else return ElMessage.warning('请上传待复核表')
  fd.append('api_key', aiApiKey.value || '')
  fd.append('model_name', aiModel.value)
  fd.append('max_images', String(aiMaxImages.value))
  fd.append('max_ai_rows', String(aiMaxRows.value))
  fd.append('min_interval_sec', String(aiMinIntervalSec.value))
  aiStarting.value = true
  try {
    const r = (await http.post(`${API_BASE}/ai-task/start`, fd)).data
    taskId.value = r.task_id
    setStoredTaskId(taskId.value)
    aiStatus.value = r.status
    snapshotRes.value = null
    aiRows.page = 1
    await fetchAiStatus()
    await fetchAiRows()
    startPoll()
    ElMessage.success('AI任务已启动')
  } catch (e) {
    ElMessage.error(errMsg(e, '任务启动失败'))
  } finally {
    aiStarting.value = false
  }
}
const pauseAi = async () => { if (!taskId.value) return; try { await http.post(`${API_BASE}/ai-task/${taskId.value}/pause`); stopPoll(); await fetchAiStatus(); ElMessage.info('任务已暂停') } catch (e) { ElMessage.error(errMsg(e, '暂停失败')) } }
const resumeAi = async () => { if (!taskId.value) return; try { const fd = new FormData(); fd.append('api_key', aiApiKey.value || ''); fd.append('min_interval_sec', String(aiMinIntervalSec.value)); await http.post(`${API_BASE}/ai-task/${taskId.value}/resume`, fd); await fetchAiStatus(); startPoll(); ElMessage.success('任务已恢复') } catch (e) { ElMessage.error(errMsg(e, '恢复失败')) } }
const refreshAi = async () => { try { await fetchAiStatus(); await fetchAiRows(); ElMessage.success('已刷新') } catch (e) { ElMessage.error(errMsg(e, '刷新失败')) } }
const downloadTaskSnapshot = async () => {
  if (!taskId.value) return
  snapshotLoading.value = true
  try {
    const r = (await http.post(`${API_BASE}/ai-task/${taskId.value}/snapshot`)).data
    snapshotRes.value = r
    ElMessage.success('已生成快照，可下载已处理/未处理')
  } catch (e) {
    ElMessage.error(errMsg(e, '快照生成失败'))
  } finally {
    snapshotLoading.value = false
  }
}

const historyLoading = ref(false), historyItems = ref([]), historyTotal = ref(0), historyPage = ref(1), historySize = ref(50), historyStage = ref(''), historyAction = ref(''), historyOperator = ref(''), historyTimeRange = ref([]), historyExporting = ref(false)
const historyParams = () => {
  const start = Array.isArray(historyTimeRange.value) ? historyTimeRange.value[0] : ''
  const end = Array.isArray(historyTimeRange.value) ? historyTimeRange.value[1] : ''
  const params = {}
  if (historyStage.value) params.stage = historyStage.value
  if (historyAction.value) params.action = historyAction.value
  if (historyOperator.value) params.operator = historyOperator.value
  if (start) params.start_time = String(start).replace(' ', 'T')
  if (end) params.end_time = String(end).replace(' ', 'T')
  return params
}
const getArtifacts = (row) => {
  try {
    const d = row?.detail || {}
    const a = d.artifacts
    return Array.isArray(a) ? a : []
  } catch { return [] }
}
const loadHistory = async (reset) => {
  if (reset) historyPage.value = 1
  historyLoading.value = true
  try {
    const offset = (historyPage.value - 1) * historySize.value
    const d = (await http.get(`${API_BASE}/history`, { params: { limit: historySize.value, offset, ...historyParams() } })).data
    historyItems.value = d.items || []
    historyTotal.value = d.total || 0
  } catch (e) {
    ElMessage.error(errMsg(e, '历史加载失败'))
  } finally {
    historyLoading.value = false
  }
}
const onHistoryPageChange = async (p) => { historyPage.value = p; await loadHistory(false) }
const onHistorySizeChange = async (s) => { historySize.value = s; historyPage.value = 1; await loadHistory(false) }
const downloadHistoryCsv = async () => {
  historyExporting.value = true
  try {
    const res = await http.get(`${API_BASE}/history/export`, { params: historyParams(), responseType: 'blob' })
    const blob = new Blob([res.data], { type: 'text/csv;charset=utf-8;' })
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `operation_history_${Date.now()}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(url)
    ElMessage.success('历史记录已下载')
  } catch (e) {
    ElMessage.error(errMsg(e, '历史记录下载失败'))
  } finally {
    historyExporting.value = false
  }
}
const fmtDetail = (d) => { try { const t = JSON.stringify(d || {}); return t.length > 160 ? `${t.slice(0,160)}...` : t } catch { return String(d || '') } }

const restoreAiTaskAfterRefresh = async () => {
  let latestTaskId = getStoredTaskId()
  if (!latestTaskId) {
    try {
      const latest = (await http.get(`${API_BASE}/ai-task/latest`, { params: { active_only: true } })).data
      latestTaskId = String(latest?.task_id || '').trim()
    } catch (_) {
      latestTaskId = ''
    }
  }
  if (!latestTaskId) return

  taskId.value = latestTaskId
  aiRows.page = 1
  try {
    await fetchAiStatus()
    await fetchAiRows()
    if (['running', 'pending'].includes(aiStatus.value)) startPoll()
    if (['running', 'paused', 'pending', 'error'].includes(aiStatus.value)) tab.value = 'ai'
  } catch (e) {
    if (e?.response?.status === 404) setStoredTaskId('')
  }
}

onMounted(async () => { if (isLoggedIn.value) { await loadHistory(false); await restoreAiTaskAfterRefresh() } })
onUnmounted(() => stopPoll())

// 登录成功后自动加载数据
watch(isLoggedIn, async (v) => { if (v) { await loadHistory(false); await restoreAiTaskAfterRefresh() } })
</script>

<style scoped>
.login-page { display: flex; justify-content: center; align-items: center; min-height: 100vh; background: linear-gradient(135deg, #0f5ea8 0%, #0f766e 100%); }
.login-card { background: #fff; border-radius: 12px; padding: 32px 36px; width: 400px; box-shadow: 0 8px 32px rgba(0,0,0,0.18); }
.login-card h2 { text-align: center; margin-bottom: 20px; color: #0f5ea8; }
.app { max-width: 1320px; margin: 0 auto; padding: 20px; font-family: 'Microsoft YaHei', Arial, sans-serif; }
.hero { background: linear-gradient(120deg,#0f5ea8,#0f766e); color: #fff; border-radius: 10px; padding: 18px 24px; margin-bottom: 16px; }
.hero-inner { display: flex; justify-content: space-between; align-items: center; }
.user-bar { display: flex; align-items: center; gap: 12px; color: #fff; font-size: 14px; }
.bar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin: 12px 0; }
.panel { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 12px; margin-top: 12px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.pager { margin-top: 10px; display: flex; justify-content: flex-end; }
@media (max-width: 980px) { .grid { grid-template-columns: 1fr; } }
</style>

