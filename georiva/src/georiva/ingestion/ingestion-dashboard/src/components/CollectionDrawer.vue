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

      <!-- Tabs: automated gets two, manual gets one -->
      <div class="tab-bar">
        <button
            v-if="collection.type === 'automated'"
            :class="['tab-btn', {active: activeTab === 'loader'}]"
            @click="activeTab = 'loader'"
        >
          <i class="pi pi-sync"/> Loader Runs
          <span v-if="runs.length" class="tab-count">{{ runs.length }}</span>
        </button>
        <button
            :class="['tab-btn', {active: activeTab === 'ingestion'}]"
            @click="activeTab = 'ingestion'"
        >
          <i class="pi pi-inbox"/> Ingestion Logs
          <span v-if="logs.length" class="tab-count">{{ logs.length }}</span>
        </button>
      </div>

      <!-- Loading state -->
      <div v-if="loading" class="loading-state">
        <i class="pi pi-spin pi-spinner"/> Loading...
      </div>

      <div v-else-if="error" class="error-state">
        <i class="pi pi-exclamation-triangle"/> {{ error }}
      </div>

      <!-- Loader Runs tab -->
      <div v-else-if="activeTab === 'loader'" class="tab-content">
        <div v-if="!runs.length" class="empty-state">No loader runs found.</div>

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
              <span v-if="run.run_time" class="run-ref">
                <i class="pi pi-calendar"/> {{ formatShortDate(run.run_time) }}
              </span>
            </div>
          </div>

          <!-- File counts -->
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

          <!-- Errors (collapsible) -->
          <div v-if="run.errors?.length" class="run-errors">
            <button class="error-toggle" @click="toggleRunErrors(run.id)">
              <i class="pi pi-exclamation-triangle"/>
              {{ run.errors.length }} error{{ run.errors.length > 1 ? 's' : '' }}
              <i :class="expandedRunErrors.has(run.id) ? 'pi pi-chevron-up' : 'pi pi-chevron-down'"/>
            </button>
            <div v-if="expandedRunErrors.has(run.id)" class="error-list">
              <div v-for="(err, i) in run.errors" :key="i" class="error-item">
                {{ err }}
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Ingestion Logs tab -->
      <div v-else-if="activeTab === 'ingestion'" class="tab-content">
        <div v-if="!logs.length" class="empty-state">No ingestion logs found.</div>

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

          <!-- File path -->
          <div class="log-filepath">
            <i class="pi pi-file"/> {{ fileName(log.file_path) }}
            <span class="log-filepath__dir">{{ fileDir(log.file_path) }}</span>
          </div>

          <!-- Stats row -->
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

          <!-- Error (collapsible) -->
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

const emit = defineEmits(["update:modelValue"])

const visibleProxy = computed({
  get: () => props.modelValue,
  set: (value) => emit("update:modelValue", value),
});

const activeTab = ref("loader");
const runs = ref([]);
const logs = ref([]);
const loading = ref(false);
const error = ref(null);
const expandedRunErrors = ref(new Set());
const expandedLogErrors = ref(new Set());

function closeDrawer() {
  emit("update:modelValue", false);
}

// When collection changes or drawer opens, fetch data
watch(
    () => [props.modelValue, props.collection?.id],
    async ([visible, collectionId]) => {
      if (!visible || !collectionId || !props.collection) return;

      activeTab.value = props.collection.type === "automated" ? "loader" : "ingestion";
      runs.value = [];
      logs.value = [];
      expandedRunErrors.value = new Set();
      expandedLogErrors.value = new Set();

      await fetchAll(props.collection);
    }
);

// Also fetch when tab switches if data not yet loaded
watch(activeTab, async (tab) => {
  if (!props.collection) return;
  if (tab === "loader" && !runs.value.length) await fetchRuns(props.collection.id);
  if (tab === "ingestion" && !logs.value.length) await fetchLogs(props.collection.id);
});

async function fetchAll(collection) {
  loading.value = true;
  error.value = null;
  try {
    const promises = [fetchLogs(collection.id)];
    if (collection.type === "automated") promises.push(fetchRuns(collection.id));
    await Promise.all(promises);
  } catch (e) {
    error.value = e.message;
  } finally {
    loading.value = false;
  }
}

async function fetchRuns(id) {
  const res = await fetch(`/admin/api/ingestion/collections/${id}/loader-runs/`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  runs.value = data.runs;
}

async function fetchLogs(id) {
  const res = await fetch(`/admin/api/ingestion/collections/${id}/ingestion-logs/`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  logs.value = data.logs;
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

// --- Status helpers ---
function loaderStatusClass(s) {
  return {
    success: "success",
    partial: "warning",
    failed: "danger",
    running: "info",
    empty: "neutral",
    queued: "neutral"
  }[s] ?? "neutral";
}

function loaderStatusIcon(s) {
  return {
    success: "pi pi-check-circle",
    partial: "pi pi-exclamation-triangle",
    failed: "pi pi-times-circle",
    running: "pi pi-spin pi-spinner",
    empty: "pi pi-minus-circle",
    queued: "pi pi-clock"
  }[s] ?? "pi pi-circle";
}

function ingestionStatusClass(s) {
  return {completed: "success", failed: "danger", processing: "info", pending: "neutral"}[s] ?? "neutral";
}

function ingestionStatusIcon(s) {
  return {
    completed: "pi pi-check-circle",
    failed: "pi pi-times-circle",
    processing: "pi pi-spin pi-spinner",
    pending: "pi pi-clock"
  }[s] ?? "pi pi-circle";
}

// --- Formatters ---
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

/* Tabs */
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

/* Content area */
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

/* Run row */
.run-row, .log-row {
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  padding: 12px 14px;
  display: flex;
  flex-direction: column;
  gap: 8px;
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

/* Status pill */
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

.status-pill.success {
  background: #dcfce7;
  color: #15803d;
}

.status-pill.warning {
  background: #fef3c7;
  color: #b45309;
}

.status-pill.danger {
  background: #fee2e2;
  color: #b91c1c;
}

.status-pill.info {
  background: #dbeafe;
  color: #1d4ed8;
}

.status-pill.neutral {
  background: #f1f5f9;
  color: #64748b;
}

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

.run-duration, .run-ref {
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

/* Count chips */
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

.count-chip.fetched {
  background: #dcfce7;
  color: #166534;
}

.count-chip.skipped {
  background: #f1f5f9;
  color: #475569;
}

.count-chip.failed {
  background: #fee2e2;
  color: #b91c1c;
}

.count-chip.bytes {
  background: #ede9fe;
  color: #6d28d9;
}

/* File path */
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

/* Error toggle */
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

.error-toggle:hover {
  background: #fecaca;
}

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
</style>