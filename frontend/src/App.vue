<template>
  <div class="app">
    <div v-if="!isAuthenticated" class="auth-wrap">
      <header class="hero">
        <h1>退运费智能审核中台</h1>
        <p>Vue3 + FastAPI + Celery</p>
      </header>
      <section class="auth-card">
        <el-tabs v-model="authMode" stretch>
          <el-tab-pane label="登录" name="login">
            <el-form label-width="90px" class="auth-form">
              <el-form-item label="用户名">
                <el-input v-model="loginForm.username" clearable placeholder="请输入用户名" />
              </el-form-item>
              <el-form-item label="密码">
                <el-input v-model="loginForm.password" type="password" show-password clearable placeholder="请输入密码" @keyup.enter="runLogin" />
              </el-form-item>
              <div class="bar">
                <el-button type="primary" :loading="authLoading" @click="runLogin">登录</el-button>
              </div>
            </el-form>
          </el-tab-pane>
          <el-tab-pane label="注册" name="register">
            <el-form label-width="90px" class="auth-form">
              <el-form-item label="用户名">
                <el-input v-model="registerForm.username" clearable placeholder="字母/数字/下划线" />
              </el-form-item>
              <el-form-item label="密码">
                <el-input v-model="registerForm.password" type="password" show-password clearable placeholder="至少6位" />
              </el-form-item>
              <el-form-item label="注册密钥">
                <el-input v-model="registerForm.register_key" show-password clearable placeholder="请输入密钥" />
              </el-form-item>
              <div class="bar">
                <el-button type="primary" :loading="authLoading" @click="runRegister">注册并登录</el-button>
              </div>
            </el-form>
          </el-tab-pane>
        </el-tabs>
      </section>
    </div>

    <template v-else>
      <header class="hero hero-main">
        <div>
          <h1>退运费智能审核中台</h1>
          <p>Vue3 + FastAPI + Celery</p>
        </div>
        <div class="hero-user">
          <span>当前用户：{{ me?.username }}</span>
          <el-button type="info" plain @click="logout">退出登录</el-button>
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
            <el-col :span="6"><el-statistic title="总行" :value="cleanRes.total_rows" /></el-col>
            <el-col :span="6"><el-statistic title="正常" :value="cleanRes.normal_rows" /></el-col>
            <el-col :span="6"><el-statistic title="异常" :value="cleanRes.abnormal_rows" /></el-col>
            <el-col :span="6"><el-statistic title="超12其余正常" :value="cleanRes.over_limit_rows || 0" /></el-col>
          </el-row>
          <div class="bar">
            <el-button type="success" @click="download(cleanRes.normal_file_url)">下载正常表</el-button>
            <el-button type="warning" @click="download(cleanRes.abnormal_file_url)">下载异常表</el-button>
            <el-button v-if="cleanRes.over_limit_file_url" type="info" plain @click="download(cleanRes.over_limit_file_url)">下载超12单独表</el-button>
          </div>
          <div class="grid">
            <TableView title="正常预览" :preview="cleanRes.normal_preview" v-model:displayRows="cleanNormalShow" />
            <TableView title="异常预览" :preview="cleanRes.abnormal_preview" v-model:displayRows="cleanAbnormalShow" />
          </div>
          <TableView v-if="cleanRes.over_limit_rows > 0" title="超12其余正常预览" :preview="cleanRes.over_limit_preview" v-model:displayRows="cleanOverLimitShow" />
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
            <el-switch v-model="aiUseStep2" />
            <span class="hint" v-if="!matchRes?.inbound_file_url">未检测到步骤二结果，关闭开关即可单独上传待复核文件</span>
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
          <el-button :loading="alignmentChecking" @click="runAiAlignmentCheck" :disabled="!taskId">主动校验一致性</el-button>
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
          <div v-if="aiAlignment.can_compare" class="panel alignment-wrap">
            <el-alert
              :title="`一致性校验：${aiAlignment.ok ? '通过' : '存在差异'}（缺失 ${aiAlignment.missing_rows || 0}，新增 ${aiAlignment.extra_rows || 0}）`"
              :type="aiAlignment.ok ? 'success' : 'warning'"
              :closable="false"
            />
            <el-row :gutter="12" class="align-stats">
              <el-col :span="6"><el-statistic title="源数据行" :value="aiAlignment.source_rows || 0" /></el-col>
              <el-col :span="6"><el-statistic title="处理后行" :value="aiAlignment.processed_rows || 0" /></el-col>
              <el-col :span="6"><el-statistic title="源表重复物流单号" :value="aiAlignment.source_duplicate_logistics || 0" /></el-col>
              <el-col :span="6"><el-statistic title="处理后重复物流单号" :value="aiAlignment.processed_duplicate_logistics || 0" /></el-col>
            </el-row>
            <div class="grid">
              <div class="panel align-table">
                <div class="bar"><strong>缺失样例（源表有、处理后无）</strong></div>
                <el-table :data="aiAlignmentMissingSamples" border stripe height="220">
                  <el-table-column prop="id_key" label="ID" min-width="120" show-overflow-tooltip />
                  <el-table-column prop="order_key" label="订单号" min-width="120" show-overflow-tooltip />
                  <el-table-column prop="logistics_key" label="物流单号" min-width="140" show-overflow-tooltip />
                  <el-table-column prop="count" label="数量" width="90" />
                </el-table>
              </div>
              <div class="panel align-table">
                <div class="bar"><strong>新增样例（处理后有、源表无）</strong></div>
                <el-table :data="aiAlignmentExtraSamples" border stripe height="220">
                  <el-table-column prop="id_key" label="ID" min-width="120" show-overflow-tooltip />
                  <el-table-column prop="order_key" label="订单号" min-width="120" show-overflow-tooltip />
                  <el-table-column prop="logistics_key" label="物流单号" min-width="140" show-overflow-tooltip />
                  <el-table-column prop="count" label="数量" width="90" />
                </el-table>
              </div>
            </div>
          </div>
          <el-alert
            v-else-if="aiTask.alignment_report && Object.keys(aiTask.alignment_report).length"
            :title="`一致性校验暂不可比对：${aiTask.alignment_report.message || '缺少关键对比字段'}`"
            type="info"
            :closable="false"
          />
          <div class="bar" v-if="aiTask.artifacts?.length">
            <el-button v-for="(u,i) in aiTask.artifacts" :key="`${u}${i}`" @click="download(artifactUrl(u))">{{ artifactLabel(u, i) }}</el-button>
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
          <el-input v-model="historyOperator" clearable placeholder="用户过滤" style="max-width: 180px" />
          <el-button type="primary" @click="onHistoryQuery">查询</el-button>
          <el-button @click="onHistoryRefresh">刷新</el-button>
          <el-button :loading="historyExporting" @click="downloadHistoryCsv">下载CSV</el-button>
        </div>

        <div class="panel">
          <div class="bar"><strong>操作记录</strong></div>
          <el-table :data="historyItems" border stripe height="320" v-loading="historyLoading">
            <el-table-column prop="timestamp" label="时间" width="170" />
            <el-table-column prop="operator" label="操作人" width="120" />
            <el-table-column prop="stage" label="阶段" width="130" />
            <el-table-column prop="action" label="动作" width="130" />
            <el-table-column prop="input_rows" label="输入" width="90" />
            <el-table-column prop="output_rows" label="输出" width="90" />
            <el-table-column label="详情" min-width="300" show-overflow-tooltip><template #default="s">{{ fmtDetail(s.row.detail) }}</template></el-table-column>
          </el-table>
          <el-pagination class="pager" layout="total, sizes, prev, pager, next" :page-sizes="[20,50,100]" :total="historyTotal" :page-size="historySize" :current-page="historyPage" @size-change="onHistorySizeChange" @current-change="onHistoryPageChange" />
        </div>

        <div class="panel">
          <div class="bar">
            <strong>历史处理文件</strong>
            <el-button @click="loadArtifactHistory(false)" :loading="artifactLoading">刷新文件列表</el-button>
          </div>
          <el-table :data="artifactItems" border stripe height="320" v-loading="artifactLoading">
            <el-table-column prop="created_at" label="时间" width="170" />
            <el-table-column prop="operator" label="操作人" width="120" />
            <el-table-column prop="stage" label="阶段" width="130" />
            <el-table-column prop="action" label="动作" width="130" />
            <el-table-column prop="file_name" label="文件名" min-width="250" show-overflow-tooltip />
            <el-table-column prop="task_id" label="任务ID" min-width="180" show-overflow-tooltip />
            <el-table-column label="下载" width="120">
              <template #default="s">
                <el-button type="primary" plain size="small" @click="download(s.row.file_url)">下载</el-button>
              </template>
            </el-table-column>
          </el-table>
          <el-pagination class="pager" layout="total, sizes, prev, pager, next" :page-sizes="[20,50,100]" :total="artifactTotal" :page-size="artifactSize" :current-page="artifactPage" @size-change="onArtifactSizeChange" @current-change="onArtifactPageChange" />
        </div>
      </el-tab-pane>
    </el-tabs>
    </template>
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
const AUTH_TOKEN_KEY = 'refund_audit_auth_token'
const AI_TASK_ID_KEY = 'refund_audit_active_task_id'
const AI_SPEED_SETTINGS_KEY = 'refund_audit_ai_speed_settings'
const http = axios.create({ timeout: 30000 })
const tab = ref('clean')
const opts = [{l:'20',v:20},{l:'50',v:50},{l:'100',v:100},{l:'200',v:200},{l:'500',v:500},{l:'全部',v:0}]
const errMsg = (e, d='请求失败') => (typeof e?.response?.data?.detail === 'string' ? e.response.data.detail : d)
const abs = (u) => (!u ? '' : (String(u).startsWith('http') ? u : `${BASE_URL}/${String(u).replace(/^\/+/, '')}`))
const download = (u) => { const x = abs(u); if (!x) return ElMessage.warning('下载链接无效'); window.open(x, '_blank', 'noopener') }
const artifactUrl = (u) => {
  const raw = String(u || '').trim()
  if (!raw) return ''
  if (raw.startsWith('/artifacts/')) return raw
  if (raw.startsWith('artifacts/')) return `/${raw}`
  return `/${raw.replace(/^\/+/, '')}`
}
const artifactLabel = (u, i) => {
  const v = decodeURIComponent(String(u || '')).toLowerCase()
  if (v.includes('ai复核正常') || v.includes('ai可打款') || v.includes('ok')) return '下载可打款'
  if (v.includes('ai复核异常') || v.includes('ai需回访') || v.includes('bad')) return '下载需回访'
  if (v.includes('ai未处理') || v.includes('pending')) return '下载未处理'
  return `下载产物${i + 1}`
}

const getStoredToken = () => {
  if (typeof window === 'undefined') return ''
  return String(window.localStorage.getItem(AUTH_TOKEN_KEY) || '').trim()
}
const setStoredToken = (token) => {
  if (typeof window === 'undefined') return
  const v = String(token || '').trim()
  if (v) window.localStorage.setItem(AUTH_TOKEN_KEY, v)
  else window.localStorage.removeItem(AUTH_TOKEN_KEY)
}
const applyAuthHeader = (token) => {
  const v = String(token || '').trim()
  if (v) http.defaults.headers.common.Authorization = `Bearer ${v}`
  else delete http.defaults.headers.common.Authorization
}

const authToken = ref(getStoredToken())
applyAuthHeader(authToken.value)
const me = ref(null)
const isAuthenticated = computed(() => Boolean(authToken.value && me.value?.username))
const authMode = ref('login')
const authLoading = ref(false)
const loginForm = reactive({ username: '', password: '' })
const registerForm = reactive({ username: '', password: '', register_key: '' })
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

const clearAuthState = () => {
  setStoredToken('')
  authToken.value = ''
  applyAuthHeader('')
  me.value = null
  setStoredTaskId('')
  stopPoll()
  taskId.value = ''
  aiTask.value = null
  aiStatus.value = ''
  tab.value = 'clean'
}
const setAuth = (token, user) => {
  const t = String(token || '').trim()
  if (!t) return
  setStoredToken(t)
  authToken.value = t
  applyAuthHeader(t)
  me.value = user || null
}
const handleAuthExpired = () => {
  if (!authToken.value) return
  clearAuthState()
  ElMessage.warning('登录状态已失效，请重新登录')
}
http.interceptors.response.use((resp) => resp, (error) => {
  const reqUrl = String(error?.config?.url || '')
  const isLoginApi = reqUrl.includes('/auth/login') || reqUrl.includes('/auth/register')
  if (error?.response?.status === 401 && !isLoginApi) handleAuthExpired()
  return Promise.reject(error)
})
const loadCurrentUser = async () => {
  const user = (await http.get(`${API_BASE}/auth/me`)).data
  me.value = user
  return user
}
const runLogin = async () => {
  const username = String(loginForm.username || '').trim()
  const password = String(loginForm.password || '')
  if (!username || !password) return ElMessage.warning('请输入用户名和密码')
  authLoading.value = true
  try {
    const res = (await http.post(`${API_BASE}/auth/login`, { username, password })).data
    setAuth(res.access_token, res.user)
    await initAfterLogin()
    ElMessage.success('登录成功')
  } catch (e) {
    ElMessage.error(errMsg(e, '登录失败'))
  } finally {
    authLoading.value = false
  }
}
const runRegister = async () => {
  const payload = {
    username: String(registerForm.username || '').trim(),
    password: String(registerForm.password || ''),
    register_key: String(registerForm.register_key || '').trim(),
  }
  if (!payload.username || !payload.password || !payload.register_key) return ElMessage.warning('请完整填写注册信息')
  authLoading.value = true
  try {
    const res = (await http.post(`${API_BASE}/auth/register`, payload)).data
    setAuth(res.access_token, res.user)
    await initAfterLogin()
    ElMessage.success('注册成功')
  } catch (e) {
    ElMessage.error(errMsg(e, '注册失败'))
  } finally {
    authLoading.value = false
  }
}
const logout = () => {
  clearAuthState()
  ElMessage.success('已退出登录')
}

const cleanFile = ref(null), cleanLoading = ref(false), cleanRes = ref(null), cleanPreviewRows = ref(200), cleanSourcePreview = ref(null), cleanSourceShow = ref(50), cleanNormalShow = ref(50), cleanAbnormalShow = ref(50), cleanOverLimitShow = ref(50)
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

const aiUseStep2 = ref(false), aiFile = ref(null), aiSourcePreview = ref(null), aiSourceShow = ref(50), aiApiKey = ref(''), aiModel = ref('qwen3-vl-flash'), aiMaxImages = ref(4), aiMaxRows = ref(300), aiRateSeconds = ref(2.0), aiRateRows = ref(1), aiStarting = ref(false), taskId = ref(''), aiTask = ref(null), aiStatus = ref(''), aiRowsScope = ref('all'), aiRowsSize = ref(50), aiRowsLoading = ref(false)
const aiMinIntervalSec = computed(() => {
  const seconds = Number(aiRateSeconds.value || 0)
  const rows = Number(aiRateRows.value || 0)
  if (!Number.isFinite(seconds) || !Number.isFinite(rows) || seconds <= 0 || rows <= 0) return 0.8
  return Math.max(0.0, seconds / rows)
})
const aiRows = reactive({ rows: [], columns: [], total_rows: 0, page: 1, page_size: 50 })
const snapshotLoading = ref(false), snapshotRes = ref(null), alignmentChecking = ref(false)
const aiAlignment = computed(() => aiTask.value?.alignment_report || {})
const aiAlignmentMissingSamples = computed(() => (Array.isArray(aiAlignment.value?.missing_samples) ? aiAlignment.value.missing_samples : []))
const aiAlignmentExtraSamples = computed(() => (Array.isArray(aiAlignment.value?.extra_samples) ? aiAlignment.value.extra_samples : []))
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
watch(aiUseStep2, async (enabled) => {
  if (enabled && !matchRes.value?.inbound_file_url) ElMessage.warning('当前没有步骤二结果，请关闭开关后单独上传文件')
  try { await syncAiSource() } catch (e) { ElMessage.error(errMsg(e, '加载步骤三预览失败')) }
})
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
const runAiAlignmentCheck = async () => {
  if (!taskId.value) return
  alignmentChecking.value = true
  try {
    const d = (await http.post(`${API_BASE}/ai-task/${taskId.value}/alignment-check`)).data
    if (aiTask.value) aiTask.value.alignment_report = d?.alignment_report || {}
    await fetchAiStatus()
    ElMessage.success('一致性校验完成')
  } catch (e) {
    ElMessage.error(errMsg(e, '一致性校验失败'))
  } finally {
    alignmentChecking.value = false
  }
}
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
const artifactLoading = ref(false), artifactItems = ref([]), artifactTotal = ref(0), artifactPage = ref(1), artifactSize = ref(50)
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
const loadArtifactHistory = async (reset) => {
  if (reset) artifactPage.value = 1
  artifactLoading.value = true
  try {
    const offset = (artifactPage.value - 1) * artifactSize.value
    const d = (await http.get(`${API_BASE}/history/files`, { params: { limit: artifactSize.value, offset, ...historyParams() } })).data
    artifactItems.value = d.items || []
    artifactTotal.value = d.total || 0
  } catch (e) {
    ElMessage.error(errMsg(e, '历史文件加载失败'))
  } finally {
    artifactLoading.value = false
  }
}
const onHistoryQuery = async () => { await loadHistory(true); await loadArtifactHistory(true) }
const onHistoryRefresh = async () => { await loadHistory(false); await loadArtifactHistory(false) }
const onHistoryPageChange = async (p) => { historyPage.value = p; await loadHistory(false) }
const onHistorySizeChange = async (s) => { historySize.value = s; historyPage.value = 1; await loadHistory(false) }
const onArtifactPageChange = async (p) => { artifactPage.value = p; await loadArtifactHistory(false) }
const onArtifactSizeChange = async (s) => { artifactSize.value = s; artifactPage.value = 1; await loadArtifactHistory(false) }
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

const initAfterLogin = async () => {
  await loadHistory(false)
  await loadArtifactHistory(false)
  await restoreAiTaskAfterRefresh()
}

onMounted(async () => {
  if (!authToken.value) return
  try {
    await loadCurrentUser()
    await initAfterLogin()
  } catch (_) {
    clearAuthState()
  }
})
onUnmounted(() => stopPoll())
</script>

<style scoped>
.app {
  --ios-bg: #eef4ff;
  --ios-surface: rgba(255, 255, 255, 0.7);
  --ios-surface-strong: rgba(255, 255, 255, 0.9);
  --ios-border: rgba(255, 255, 255, 0.78);
  --ios-text: #0f172a;
  --ios-subtext: #475569;
  --ios-primary: #1476ff;
  --ios-primary-2: #25a3ff;
  --ios-shadow: 0 12px 45px rgba(21, 65, 136, 0.12);
  position: relative;
  overflow: hidden;
  max-width: 1320px;
  margin: 0 auto;
  padding: 24px;
  background: linear-gradient(180deg, var(--ios-bg), #f8fbff 42%, #edf5ff);
  border-radius: 28px;
  font-family: "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei UI", sans-serif;
  color: var(--ios-text);
  animation: page-rise 420ms ease-out;
}

.app::before,
.app::after {
  content: "";
  position: absolute;
  border-radius: 999px;
  filter: blur(24px);
  pointer-events: none;
  z-index: 0;
}

.app::before {
  width: 440px;
  height: 440px;
  left: -140px;
  top: -180px;
  background: radial-gradient(circle, rgba(86, 175, 255, 0.38) 0%, rgba(86, 175, 255, 0) 70%);
}

.app::after {
  width: 520px;
  height: 520px;
  right: -190px;
  bottom: -210px;
  background: radial-gradient(circle, rgba(25, 125, 255, 0.26) 0%, rgba(25, 125, 255, 0) 72%);
}

.app > * {
  position: relative;
  z-index: 1;
}

.hero {
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.45), rgba(255, 255, 255, 0.15)),
    linear-gradient(122deg, #1476ff, #24a4ff 58%, #17b5be);
  color: #f8fbff;
  border: 1px solid rgba(255, 255, 255, 0.35);
  border-radius: 24px;
  padding: 22px 26px;
  margin-bottom: 18px;
  box-shadow: var(--ios-shadow);
  backdrop-filter: blur(16px);
}

.hero h1 {
  margin: 0 0 6px;
  font-size: 30px;
  font-weight: 650;
  letter-spacing: 0.02em;
}

.hero p {
  margin: 0;
  opacity: 0.88;
}

.hero-main {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
}

.hero-user {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 14px;
  padding: 8px 12px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.2);
  border: 1px solid rgba(255, 255, 255, 0.28);
}

.auth-wrap {
  max-width: 580px;
  margin: 48px auto;
}

.auth-card,
.panel {
  background: var(--ios-surface);
  border: 1px solid var(--ios-border);
  border-radius: 20px;
  box-shadow: var(--ios-shadow);
  backdrop-filter: blur(18px);
}

.auth-card {
  padding: 18px;
}

.auth-form {
  padding-top: 8px;
}

.bar {
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
  margin: 12px 0;
}

.hint {
  color: var(--ios-subtext);
  font-size: 12px;
}

.panel {
  padding: 14px;
  margin-top: 12px;
}

.grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}

.alignment-wrap {
  margin-top: 10px;
}

.align-stats {
  margin-top: 10px;
}

.align-table {
  margin-top: 0;
}

.pager {
  margin-top: 10px;
  display: flex;
  justify-content: flex-end;
}

.app :deep(.el-tabs--border-card) {
  border: 1px solid var(--ios-border);
  border-radius: 22px;
  background: var(--ios-surface);
  box-shadow: var(--ios-shadow);
  overflow: hidden;
}

.app :deep(.el-tabs--border-card > .el-tabs__content) {
  background: transparent;
}

.app :deep(.el-tabs--border-card > .el-tabs__header) {
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.62), rgba(255, 255, 255, 0.3));
  border-bottom: 1px solid rgba(255, 255, 255, 0.66);
}

.app :deep(.el-tabs__item) {
  color: var(--ios-subtext);
  font-weight: 520;
}

.app :deep(.el-tabs__item.is-active) {
  color: var(--ios-text);
}

.app :deep(.el-upload-dragger) {
  border-radius: 16px;
  border: 1px dashed rgba(20, 118, 255, 0.38);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.76), rgba(255, 255, 255, 0.52));
}

.app :deep(.el-input__wrapper),
.app :deep(.el-textarea__inner),
.app :deep(.el-select__wrapper) {
  border-radius: 12px;
  box-shadow: 0 0 0 1px rgba(255, 255, 255, 0.72) inset, 0 8px 18px rgba(30, 70, 120, 0.08);
  background: var(--ios-surface-strong);
}

.app :deep(.el-button) {
  border-radius: 12px;
  font-weight: 520;
  border-color: rgba(16, 24, 40, 0.08);
  transition: transform 0.18s ease, box-shadow 0.18s ease;
}

.app :deep(.el-button:hover) {
  transform: translateY(-1px);
  box-shadow: 0 10px 20px rgba(31, 78, 145, 0.14);
}

.app :deep(.el-button--primary) {
  background: linear-gradient(135deg, var(--ios-primary), var(--ios-primary-2));
  border-color: rgba(20, 118, 255, 0.55);
}

.app :deep(.el-table) {
  --el-table-border-color: rgba(184, 205, 236, 0.54);
  --el-table-header-bg-color: rgba(234, 244, 255, 0.72);
  --el-table-row-hover-bg-color: rgba(218, 236, 255, 0.52);
  border-radius: 14px;
  overflow: hidden;
}

.app :deep(.el-alert) {
  border-radius: 12px;
}

@keyframes page-rise {
  from {
    opacity: 0;
    transform: translateY(8px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@media (max-width: 980px) {
  .app {
    padding: 14px;
  }
  .grid {
    grid-template-columns: 1fr;
  }
  .hero-main {
    align-items: flex-start;
  }
  .hero h1 {
    font-size: 24px;
  }
}
</style>
