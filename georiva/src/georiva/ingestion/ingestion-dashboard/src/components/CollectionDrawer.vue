<template>
  <Drawer
      v-model:visible="visibleProxy"
      :header="collection?.name"
      position="right"
      style="width: 52rem; max-width: 95vw"
  >
    <template #header>
      <div class="drawer-header">
        <div class="drawer-title">{{ collection?.name }}</div>
        <div class="drawer-meta">
          <span class="meta-badge">
            <i :class="collection?.type === 'automated' ? 'pi pi-bolt' : 'pi pi-upload'"/>
            {{ collection?.type === 'automated' ? 'Automated' : 'Manual' }}
          </span>
          <span class="meta-badge">
            <i class="pi pi-folder"/>
            {{ collection?.catalog_name }}
          </span>
          <span class="meta-badge">
            <i class="pi pi-database"/>
            {{ collection?.item_count?.toLocaleString() }} items
          </span>
        </div>
      </div>
    </template>

    <div v-if="collection" class="drawer-body">

      <div class="tab-bar">
        <!-- Acquisition tab — always visible, content varies by type -->
        <button
            :class="['tab-btn', {active: activeTab === 'acquisition'}]"
            @click="activeTab = 'acquisition'"
        >
          <i class="pi pi-cloud-download"/> Acquisition
          <span v-if="acquisitionCount" class="tab-count">{{ acquisitionCount }}</span>
        </button>
        <button
            :class="['tab-btn', {active: activeTab === 'file-history'}]"
            @click="activeTab = 'file-history'"
        >
          <i class="pi pi-inbox"/> File History
          <span v-if="logs.length" class="tab-count">{{ logs.length }}</span>
        </button>
        <button
            :class="['tab-btn', {active: activeTab === 'active-jobs'}]"
            @click="activeTab = 'active-jobs'"
        >
          <i class="pi pi-cog"/> Active Jobs
          <span v-if="activeJobCount" :class="['tab-count', 'tab-count--active']">{{ activeJobCount }}</span>
          <span v-else-if="jobs.length" class="tab-count">{{ jobs.length }}</span>
        </button>
      </div>

      <div v-if="loading" class="loading-state">
        <i class="pi pi-spin pi-spinner"/> Loading...
      </div>

      <div v-else-if="error" class="error-state">
        <i class="pi pi-exclamation-triangle"/> {{ error }}
      </div>

      <!-- Acquisition tab -->
      <div v-else-if="activeTab === 'acquisition'" class="tab-content">

        <!-- Automated: FetchRuns -->
        <template v-if="collection.type === 'automated'">
          <div v-if="!runs.length" class="empty-state">No fetch runs found.</div>
          <div v-for="run in runs" :key="run.id" class="run-row">
            <div class="run-row__top">
              <div class="run-row__left">
                <span :class="['status-pill', loaderStatusClass(run.status)]">
                  <i :class="loaderStatusIcon(run.status)"/>
                  {{ run.status }}
                </span>
                <span class="run-time" :title="formatDateTime(run.started_at)">
                  {{ timeAgo(run.started_at) }}
                </span>
                <span class="run-date">{{ formatShortDate(run.started_at) }}</span>
              </div>
              <div class="run-row__right">
                <span v-if="run.duration_seconds != null" class="run-duration">
                  <i class="pi pi-clock"/> {{ formatDuration(run.duration_seconds) }}
                </span>
                <span class="run-feed">{{ run.data_feed_name }}</span>
              </div>
            </div>
            <div class="run-row__counts">
              <span class="count-chip fetched">
                <i class="pi pi-download"/> {{ run.files_fetched }} fetched
              </span>
              <span class="count-chip skipped">
                <i class="pi pi-forward"/> {{ run.files_skipped }} skipped
              </span>
              <span class="count-chip failed" v-if="run.files_failed > 0">
                <i class="pi pi-times"/> {{ run.files_failed }} failed
              </span>
              <span class="count-chip bytes" v-if="run.bytes_transferred > 0">
                <i class="pi pi-arrow-right-arrow-left"/> {{ formatBytes(run.bytes_transferred) }}
              </span>
            </div>
            <div v-if="run.errors?.length" class="run-errors">
              <button class="error-toggle" @click="toggleRunErrors(run.id)">
                <i class="pi pi-exclamation-triangle"/>
                {{ run.errors.length }} error{{ run.errors.length > 1 ? 's' : '' }}
                <i :class="expandedRunErrors.has(run.id) ? 'pi pi-chevron-up' : 'pi pi-chevron-down'"/>
              </button>
              <div v-if="expandedRunErrors.has(run.id)" class="error-list">
                <div v-for="(err, i) in run.errors" :key="i" class="error-item">{{ err }}</div>
              </div>
            </div>
          </div>
        </template>

        <!-- Manual: UploadSessions -->
        <template v-else>
          <div v-if="!uploadSessions.length" class="empty-state">No upload sessions found.</div>
          <div v-for="session in uploadSessions" :key="session.id" class="run-row">
            <div class="run-row__top">
              <div class="run-row__left">
                <span :class="['status-pill', uploadSessionStatusClass(session.status)]">
                  <i :class="uploadSessionStatusIcon(session.status)"/>
                  {{ session.status }}
                </span>
                <span class="run-time" :title="formatDateTime(session.started_at)">
                  {{ timeAgo(session.started_at) }}
                </span>
                <span class="run-date">{{ formatShortDate(session.started_at) }}</span>
              </div>
              <div class="run-row__right">
                <span v-if="session.duration_seconds != null" class="run-duration">
                  <i class="pi pi-clock"/> {{ formatDuration(session.duration_seconds) }}
                </span>
                <span class="run-feed">{{ session.uploaded_by ?? 'Manual upload' }}</span>
              </div>
            </div>
            <div class="run-row__counts">
              <span class="count-chip fetched">
                <i class="pi pi-upload"/> {{ session.files_stored }}/{{ session.files_count }} stored
              </span>
              <span class="count-chip failed" v-if="session.files_failed > 0">
                <i class="pi pi-times"/> {{ session.files_failed }} failed
              </span>
            </div>
          </div>
        </template>
      </div>

      <!-- File History tab -->
      <div v-else-if="activeTab === 'file-history'" class="tab-content">
        <div v-if="!logs.length" class="empty-state">No file history found.</div>

        <div v-for="log in logs" :key="log.id" class="log-row">
          <div class="log-row__top">
            <span :class="['status-pill', ingestionStatusClass(log.status)]">
              <i :class="ingestionStatusIcon(log.status)"/>
              {{ log.status }}
            </span>
            <span class="run-time" :title="formatDateTime(log.created_at)">
              {{ timeAgo(log.created_at) }}
            </span>
            <span class="run-date">{{ formatShortDate(log.created_at) }}</span>
            <span v-if="log.retry_count > 0" class="retry-badge">
              {{ log.retry_count }} retr{{ log.retry_count > 1 ? 'ies' : 'y' }}
            </span>
          </div>
          <div class="log-filepath">
            <i class="pi pi-file"/> {{ fileName(log.file_path) }}
            <span class="log-filepath__dir">{{ fileDir(log.file_path) }}</span>
          </div>
          <div class="log-row__stats">
            <span v-if="log.reference_time" class="count-chip bytes">
              <i class="pi pi-calendar"/> {{ formatShortDate(log.reference_time) }}
            </span>
            <span v-if="log.status === 'completed'" class="count-chip fetched">
              <i class="pi pi-box"/> {{ log.items_created }} items
            </span>
            <span v-if="log.status === 'completed'" class="count-chip fetched">
              <i class="pi pi-images"/> {{ log.assets_created }} assets
            </span>
          </div>
          <div v-if="log.error" class="run-errors">
            <button class="error-toggle" @click="toggleLogError(log.id)">
              <i class="pi pi-exclamation-triangle"/>
              Error
              <i :class="expandedLogErrors.has(log.id) ? 'pi pi-chevron-up' : 'pi pi-chevron-down'"/>
            </button>
            <div v-if="expandedLogErrors.has(log.id)" class="error-list">
              <div class="error-item">{{ log.error }}</div>
            </div>
          </div>
        </div>
      </div>

      <!-- Active Jobs tab -->
      <div v-else-if="activeTab === 'active-jobs'" class="tab-content">

        <!-- Live queue -->
        <template v-if="liveJobs.length">
          <div class="section-label">Live</div>
          <div v-for="job in liveJobs" :key="job.id" class="log-row log-row--active">
            <div class="log-row__top">
              <span :class="['status-pill', jobStatusClass(job.state)]">
                <i :class="jobStatusIcon(job.state)"/>
                {{ job.state }}
              </span>
              <span class="run-time" :title="formatDateTime(job.created_at)">
                {{ timeAgo(job.created_at) }}
              </span>
              <span class="run-date">{{ formatShortDate(job.created_at) }}</span>
            </div>
            <div class="log-filepath">
              <i class="pi pi-file"/> {{ fileName(job.file_path) }}
              <span class="log-filepath__dir">{{ fileDir(job.file_path) }}</span>
            </div>
            <div v-if="job.state === 'started'" class="job-progress">
              <div class="progress-bar-wrap">
                <div class="progress-bar-fill" :style="{width: job.progress_percentage + '%'}"/>
              </div>
              <div class="progress-label">
                {{ job.progress_percentage }}%
                <span v-if="job.progress_state" class="progress-state">— {{ job.progress_state }}</span>
              </div>
            </div>
            <div v-if="job.error" class="run-errors">
              <button class="error-toggle" @click="toggleJobError(job.id)">
                <i class="pi pi-exclamation-triangle"/> Error
                <i :class="expandedJobErrors.has(job.id) ? 'pi pi-chevron-up' : 'pi pi-chevron-down'"/>
              </button>
              <div v-if="expandedJobErrors.has(job.id)" class="error-list">
                <div class="error-item">{{ job.error }}</div>
              </div>
            </div>
          </div>
        </template>

        <!-- Recent section -->
        <template v-if="recentJobs.length">
          <div class="section-label" :class="{'section-label--mt': liveJobs.length}">Recent</div>
          <div v-for="job in recentJobs" :key="job.id" class="log-row">
            <div class="log-row__top">
              <span :class="['status-pill', jobStatusClass(job.state)]">
                <i :class="jobStatusIcon(job.state)"/>
                {{ job.state }}
              </span>
              <span class="run-time" :title="formatDateTime(job.created_at)">
                {{ timeAgo(job.created_at) }}
              </span>
              <span class="run-date">{{ formatShortDate(job.created_at) }}</span>
            </div>
            <div class="log-filepath">
              <i class="pi pi-file"/> {{ fileName(job.file_path) }}
              <span class="log-filepath__dir">{{ fileDir(job.file_path) }}</span>
            </div>
            <div v-if="job.state === 'finished'" class="log-row__stats">
              <span class="count-chip fetched"><i class="pi pi-box"/> {{ job.items_created }} items</span>
              <span class="count-chip fetched"><i class="pi pi-images"/> {{ job.assets_created }} assets</span>
            </div>
            <div v-if="job.error" class="run-errors">
              <button class="error-toggle" @click="toggleJobError(job.id)">
                <i class="pi pi-exclamation-triangle"/> Error
                <i :class="expandedJobErrors.has(job.id) ? 'pi pi-chevron-up' : 'pi pi-chevron-down'"/>
              </button>
              <div v-if="expandedJobErrors.has(job.id)" class="error-list">
                <div class="error-item">{{ job.error }}</div>
              </div>
            </div>
          </div>
        </template>

        <div v-if="!liveJobs.length && !recentJobs.length" class="empty-state">
          No jobs found.
        </div>
      </div>

    </div>
  </Drawer>
</template>

<script setup>
import {ref, watch, computed} from "vue";
import Drawer from "primevue/drawer";

const props = defineProps({
  modelValue: {type: Boolean, default: false},
  collection: {type: Object, default: null},
});

const emit = defineEmits(["update:modelValue"]);

const visibleProxy = computed({
  get: () => props.modelValue,
  set: (value) => emit("update:modelValue", value),
});

const activeTab = ref("file-history");
const runs = ref([]);
const uploadSessions = ref([]);
const logs = ref([]);
const jobs = ref([]);
const loading = ref(false);
const error = ref(null);
const expandedRunErrors = ref(new Set());
const expandedLogErrors = ref(new Set());
const expandedJobErrors = ref(new Set());
const pollTimer = ref(null);

const activeJobCount = computed(() =>
    jobs.value.filter(j => j.state === "pending" || j.state === "started").length
);

const liveJobs = computed(() =>
    jobs.value.filter(j => j.state === "pending" || j.state === "started")
);

const recentJobs = computed(() =>
    jobs.value.filter(j => j.state === "finished" || j.state === "failed" || j.state === "cancelled")
);

const acquisitionCount = computed(() =>
    props.collection?.type === "automated" ? runs.value.length : uploadSessions.value.length
);

watch(
    () => [props.modelValue, props.collection?.id],
    async ([visible, collectionId]) => {
      stopJobPolling();
      if (!visible || !collectionId || !props.collection) {
        jobs.value = [];
        return;
      }

      activeTab.value = "file-history";
      runs.value = [];
      uploadSessions.value = [];
      logs.value = [];
      jobs.value = [];
      expandedRunErrors.value = new Set();
      expandedLogErrors.value = new Set();
      expandedJobErrors.value = new Set();

      await fetchAll(props.collection);
    }
);

watch(activeTab, async (tab) => {
  if (!props.collection) return;
  if (tab === "acquisition") {
    if (props.collection.type === "automated" && !runs.value.length) {
      await fetchRuns(props.collection.id);
    } else if (props.collection.type === "manual" && !uploadSessions.value.length) {
      await fetchUploadSessions(props.collection.id);
    }
  }
  if (tab === "file-history" && !logs.value.length) await fetchLogs(props.collection.id);
  if (tab === "active-jobs") {
    await fetchJobs(props.collection.id);
    startJobPolling(props.collection.id);
  } else {
    stopJobPolling();
  }
});

async function fetchAll(collection) {
  loading.value = true;
  error.value = null;
  try {
    const promises = [fetchLogs(collection.id)];
    if (collection.type === "automated") promises.push(fetchRuns(collection.id));
    else promises.push(fetchUploadSessions(collection.id));
    await Promise.all(promises);
  } catch (e) {
    error.value = e.message;
  } finally {
    loading.value = false;
  }
}

async function fetchRuns(id) {
  const res = await fetch(`/admin/api/ingestion/collections/${id}/fetch-runs/`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  runs.value = data.fetch_runs;
}

async function fetchUploadSessions(id) {
  const res = await fetch(`/admin/api/ingestion/collections/${id}/upload-sessions/`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  uploadSessions.value = data.upload_sessions;
}

async function fetchLogs(id) {
  const res = await fetch(`/admin/api/ingestion/collections/${id}/ingestion-logs/`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  logs.value = data.logs;
}

async function fetchJobs(id) {
  const res = await fetch(`/admin/api/ingestion/collections/${id}/ingestion-jobs/`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  jobs.value = data.jobs;
  return data.has_active;
}

function startJobPolling(id) {
  stopJobPolling();
  pollTimer.value = setInterval(async () => {
    const hasActive = await fetchJobs(id);
    if (!hasActive) stopJobPolling();
  }, 3000);
}

function stopJobPolling() {
  if (pollTimer.value) {
    clearInterval(pollTimer.value);
    pollTimer.value = null;
  }
}

function toggleRunErrors(id) {
  const s = new Set(expandedRunErrors.value);
  s.has(id) ? s.delete(id) : s.add(id);
  expandedRunErrors.value = s;
}

function toggleLogError(id) {
  const s = new Set(expandedLogErrors.value);
  s.has(id) ? s.delete(id) : s.add(id);
  expandedLogErrors.value = s;
}

function toggleJobError(id) {
  const s = new Set(expandedJobErrors.value);
  s.has(id) ? s.delete(id) : s.add(id);
  expandedJobErrors.value = s;
}

function loaderStatusClass(s) {
  return {completed: "success", partial: "warning", failed: "danger", running: "info", empty: "neutral", queued: "neutral"}[s] ?? "neutral";
}

function loaderStatusIcon(s) {
  return {
    completed: "pi pi-check-circle", partial: "pi pi-exclamation-triangle",
    failed: "pi pi-times-circle", running: "pi pi-spin pi-spinner",
    empty: "pi pi-minus-circle", queued: "pi pi-clock",
  }[s] ?? "pi pi-circle";
}

function uploadSessionStatusClass(s) {
  return {completed: "success", failed: "danger", cancelled: "neutral", active: "info"}[s] ?? "neutral";
}

function uploadSessionStatusIcon(s) {
  return {
    completed: "pi pi-check-circle", failed: "pi pi-times-circle",
    cancelled: "pi pi-ban", active: "pi pi-spin pi-spinner",
  }[s] ?? "pi pi-circle";
}

function ingestionStatusClass(s) {
  return {completed: "success", failed: "danger", processing: "info", pending: "neutral"}[s] ?? "neutral";
}

function ingestionStatusIcon(s) {
  return {
    completed: "pi pi-check-circle", failed: "pi pi-times-circle",
    processing: "pi pi-spin pi-spinner", pending: "pi pi-clock",
  }[s] ?? "pi pi-circle";
}

function jobStatusClass(s) {
  return {pending: "neutral", started: "info", finished: "success", failed: "danger", cancelled: "neutral"}[s] ?? "neutral";
}

function jobStatusIcon(s) {
  return {
    pending: "pi pi-clock", started: "pi pi-spin pi-spinner",
    finished: "pi pi-check-circle", failed: "pi pi-times-circle", cancelled: "pi pi-ban",
  }[s] ?? "pi pi-circle";
}

function timeAgo(iso) {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 2) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

function formatDateTime(iso) {
  return new Date(iso).toLocaleString();
}

function formatShortDate(iso) {
  return new Date(iso).toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });
}

function formatDuration(secs) {
  if (secs < 60) return `${Math.round(secs)}s`;
  const m = Math.floor(secs / 60);
  const s = Math.round(secs % 60);
  return `${m}m ${s}s`;
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

function fileName(path) {
  return path?.split("/").pop() ?? path;
}

function fileDir(path) {
  const parts = path?.split("/") ?? [];
  return parts.length > 1 ? parts.slice(0, -1).join("/") : "";
}
</script>

<style scoped>

.pi {
  font-size: 10px;
}

.drawer-header {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.drawer-title {
  font-size: 18px;
  font-weight: 700;
  color: #0f172a;
}

.drawer-meta {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.meta-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 12px;
  font-weight: 500;
  color: #64748b;
  background: #f1f5f9;
  padding: 3px 8px;
  border-radius: 4px;
  border: 1px solid #e2e8f0;
}

.drawer-body {
  font-family: 'Inter', -apple-system, sans-serif;
  color: #334155;
}

.tab-bar {
  display: flex;
  border-bottom: 2px solid #e2e8f0;
  margin-bottom: 16px;
}

.tab-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 10px 16px;
  background: none;
  border: none;
  border-bottom: 2px solid transparent;
  margin-bottom: -2px;
  font-size: 13px;
  font-weight: 500;
  color: #64748b;
  cursor: pointer;
  transition: color 0.15s, border-color 0.15s;
}

.tab-btn:hover {
  color: #0f172a;
}

.tab-btn.active {
  color: #0f172a;
  border-bottom-color: #0f172a;
  font-weight: 600;
}

.tab-count {
  background: #e2e8f0;
  color: #475569;
  font-size: 11px;
  font-weight: 600;
  padding: 1px 6px;
  border-radius: 10px;
}

.tab-btn.active .tab-count {
  background: #0f172a;
  color: #fff;
}

.tab-count--active {
  background: #dbeafe !important;
  color: #1d4ed8 !important;
}

.tab-content {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.loading-state, .error-state, .empty-state {
  padding: 32px;
  text-align: center;
  color: #94a3b8;
  font-size: 14px;
}

.error-state {
  color: #dc2626;
}

.section-label {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: #94a3b8;
  padding: 4px 0 2px;
}

.section-label--mt {
  margin-top: 12px;
}

.run-row, .log-row {
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  padding: 12px 14px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.log-row--active {
  border-color: #bfdbfe;
  background: #eff6ff;
}

.run-row:hover, .log-row:hover {
  border-color: #cbd5e1;
}

.run-row__top, .log-row__top {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.run-row__right {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 10px;
}

.run-row__left {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 11px;
  font-weight: 600;
  padding: 3px 8px;
  border-radius: 4px;
  text-transform: capitalize;
}

.status-pill.success { background: #dcfce7; color: #15803d; }
.status-pill.warning { background: #fef3c7; color: #b45309; }
.status-pill.danger  { background: #fee2e2; color: #b91c1c; }
.status-pill.info    { background: #dbeafe; color: #1d4ed8; }
.status-pill.neutral { background: #f1f5f9; color: #64748b; }

.run-time {
  font-size: 13px;
  font-weight: 500;
  color: #0f172a;
}

.run-date {
  font-size: 11px;
  color: #94a3b8;
  font-family: 'SF Mono', 'Roboto Mono', monospace;
}

.run-duration, .run-feed {
  font-size: 12px;
  color: #64748b;
  display: flex;
  align-items: center;
  gap: 4px;
}

.retry-badge {
  font-size: 11px;
  font-weight: 600;
  background: #fef3c7;
  color: #b45309;
  padding: 2px 6px;
  border-radius: 4px;
}

.run-row__counts, .log-row__stats {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.count-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 11px;
  font-weight: 500;
  padding: 2px 8px;
  border-radius: 4px;
}

.count-chip.fetched { background: #dcfce7; color: #166534; }
.count-chip.skipped { background: #f1f5f9; color: #475569; }
.count-chip.failed  { background: #fee2e2; color: #b91c1c; }
.count-chip.bytes   { background: #ede9fe; color: #6d28d9; }

.log-filepath {
  font-size: 12px;
  color: #334155;
  font-family: 'SF Mono', 'Roboto Mono', monospace;
  display: flex;
  align-items: center;
  gap: 6px;
  background: #fff;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  padding: 4px 8px;
  overflow: hidden;
}

.log-filepath__dir {
  color: #94a3b8;
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.run-errors {
  margin-top: 2px;
}

.error-toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  font-weight: 500;
  color: #b91c1c;
  background: #fee2e2;
  border: none;
  border-radius: 4px;
  padding: 3px 8px;
  cursor: pointer;
  transition: background 0.1s;
}

.error-toggle:hover { background: #fecaca; }

.error-list {
  margin-top: 6px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.error-item {
  font-size: 12px;
  font-family: 'SF Mono', 'Roboto Mono', monospace;
  background: #fff;
  border: 1px solid #fecaca;
  border-left: 3px solid #dc2626;
  border-radius: 4px;
  padding: 6px 10px;
  color: #7f1d1d;
  white-space: pre-wrap;
  word-break: break-all;
}

.job-progress {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.progress-bar-wrap {
  width: 100%;
  height: 6px;
  background: #e2e8f0;
  border-radius: 3px;
  overflow: hidden;
}

.progress-bar-fill {
  height: 100%;
  background: #3b82f6;
  border-radius: 3px;
  transition: width 0.4s ease;
}

.progress-label {
  font-size: 11px;
  color: #64748b;
  display: flex;
  align-items: center;
  gap: 4px;
}

.progress-state { color: #94a3b8; }
</style>
