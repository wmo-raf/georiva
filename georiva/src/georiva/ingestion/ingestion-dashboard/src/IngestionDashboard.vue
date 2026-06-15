<template>
  <div class="ingestion-dashboard">

    <!-- Header -->
    <div class="header-section">
      <div class="header-top-row">
        <div class="header-title-group">
          <h1 class="dashboard-title">Ingestion Dashboard</h1>
          <div class="header-badges">
            <span class="meta-badge">
              <i class="pi pi-database"></i>
              {{ allCollections.length }} Collections
            </span>
            <span class="meta-badge">
              <i class="pi pi-bolt"></i>
              {{ allCollections.filter(c => c.type === 'automated').length }} Automated
            </span>
            <span class="meta-badge">
              <i class="pi pi-upload"></i>
              {{ allCollections.filter(c => c.type === 'manual').length }} Manual
            </span>
          </div>
        </div>

        <div class="status-summary-group">
          <div class="summary-card success" @click="setFilter('ok')" :class="{active: activeFilter === 'ok'}">
            <span class="summary-label">Active</span>
            <span class="summary-count">{{ globalSummary.ok }}</span>
          </div>
          <div class="summary-card danger" @click="setFilter('failed')" :class="{active: activeFilter === 'failed'}">
            <span class="summary-label">Failed</span>
            <span class="summary-count">{{ globalSummary.failed }}</span>
          </div>
          <div class="summary-card info" @click="setFilter('empty')" :class="{active: activeFilter === 'empty'}">
            <span class="summary-label">No Data</span>
            <span class="summary-count">{{ globalSummary.empty }}</span>
          </div>
        </div>
      </div>
    </div>

    <!-- Controls -->
    <div class="content-section">
      <div class="controls-bar">
        <div class="controls-left">
          <div class="search-wrapper">
            <i class="pi pi-search search-icon"/>
            <InputText
                v-model="searchQuery"
                placeholder="Search collections..."
                class="search-input"
            />
          </div>

          <div class="filter-group">
            <button
                v-for="opt in filterOptions"
                :key="opt.value"
                :class="['filter-btn', opt.cls, {active: activeFilter === opt.value}]"
                @click="setFilter(opt.value)"
            >
              {{ opt.label }}
            </button>
          </div>
        </div>

        <Button
            icon="pi pi-refresh"
            label="Refresh"
            class="refresh-btn"
            :loading="loading"
            @click="fetchCatalogs"
        />
      </div>

      <Message v-if="error" severity="error" class="mb-3">{{ error }}</Message>

      <!-- Catalog hierarchy -->
      <div class="catalog-list" v-if="!loading || catalogs.length">

        <div v-if="!visibleCatalogs.length" class="empty-state">No collections found.</div>

        <div
            v-for="cat in visibleCatalogs"
            :key="cat.id"
            class="catalog-group"
        >
          <!-- Catalog header row -->
          <div class="catalog-row" @click="toggleCatalog(cat.id)">
            <i :class="['expand-icon', 'pi', expandedCatalogs.has(cat.id) ? 'pi-chevron-down' : 'pi-chevron-right']"/>
            <div class="catalog-name">{{ cat.name }}</div>
            <div :class="['status-indicator', statusClass(cat.status)]">
              <i :class="statusIcon(cat.status)"/>
              {{ statusLabel(cat.status) }}
            </div>
            <div class="catalog-summary">
              <span v-if="cat.summary.ok" class="summary-chip ok">{{ cat.summary.ok }} ok</span>
              <span v-if="cat.summary.failed" class="summary-chip failed">{{ cat.summary.failed }} failed</span>
              <span v-if="cat.summary.empty" class="summary-chip empty">{{ cat.summary.empty }} empty</span>
            </div>
            <span class="catalog-col-count">{{ cat.collections.length }} collection{{ cat.collections.length !== 1 ? 's' : '' }}</span>
          </div>

          <!-- Collection rows -->
          <div v-if="expandedCatalogs.has(cat.id)" class="collection-list">
            <div
                v-for="col in cat.collections"
                :key="col.id"
                class="collection-row"
                @click="openDrawer(col)"
            >
              <!-- Name + icon -->
              <div class="collection-cell">
                <div class="collection-icon-box">
                  <i :class="col.type === 'automated' ? 'pi pi-bolt' : 'pi pi-upload'"/>
                </div>
                <div class="cell-title">{{ col.name }}</div>
              </div>

              <!-- Status -->
              <div :class="['status-indicator', statusClass(col.status)]">
                <i :class="statusIcon(col.status)"/>
                {{ statusLabel(col.status) }}
              </div>

              <!-- Type badge -->
              <span class="type-badge" :class="col.type">
                {{ col.type === 'automated' ? 'Automated' : 'Manual' }}
              </span>

              <!-- Last run -->
              <div class="time-cell" v-if="col.last_run_at">
                <span class="time-main" :class="{'text-danger': isStale(col.last_run_at)}">
                  {{ timeAgo(col.last_run_at) }}
                </span>
                <span class="time-sub">{{ formatShortDate(col.last_run_at) }}</span>
              </div>
              <span v-else class="time-main never">Never</span>

              <!-- Items -->
              <span class="items-count">{{ col.item_count.toLocaleString() }}</span>

              <!-- Sparkline -->
              <div class="sparkline">
                <div
                    v-for="(day, i) in col.sparkline"
                    :key="i"
                    :class="['spark-bar', `spark-bar--${day.status}`]"
                    :title="`${day.date}: ${sparklineLabel(day.status)}`"
                />
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-if="loading && !catalogs.length" class="loading-state">
        <i class="pi pi-spin pi-spinner"/> Loading...
      </div>
    </div>

    <CollectionDrawer
        v-model="drawerVisible"
        :collection="selectedCollection"
    />
  </div>
</template>

<script setup>
import {computed, onMounted, onUnmounted, ref, watch} from "vue";
import Button from "primevue/button";
import Message from "primevue/message";
import InputText from "primevue/inputtext";
import CollectionDrawer from "@/components/CollectionDrawer.vue";

const props = defineProps({
  apiUrl: {type: String, required: true},
});

const catalogs = ref([]);
const loading = ref(false);
const error = ref(null);
const searchQuery = ref("");
const activeFilter = ref("all");
const drawerVisible = ref(false);
const selectedCollection = ref(null);
const expandedCatalogs = ref(new Set());
let sseSource = null;

const filterOptions = [
  {label: "All", value: "all", cls: ""},
  {label: "Automated", value: "automated", cls: ""},
  {label: "Manual", value: "manual", cls: ""},
  {label: "Active", value: "ok", cls: "success"},
  {label: "Failed", value: "failed", cls: "danger"},
  {label: "No Data", value: "empty", cls: ""},
];

const allCollections = computed(() =>
    catalogs.value.flatMap(cat => cat.collections)
);

const globalSummary = computed(() => ({
  ok: allCollections.value.filter(c => c.status === "ok").length,
  failed: allCollections.value.filter(c => c.status === "failed").length,
  empty: allCollections.value.filter(c => c.status === "empty").length,
}));

const visibleCatalogs = computed(() => {
  const q = searchQuery.value.toLowerCase();
  const filter = activeFilter.value;
  const isFiltering = q || filter !== "all";

  return catalogs.value
      .map(cat => {
        const collections = cat.collections.filter(col => {
          const matchesSearch = !q || col.name.toLowerCase().includes(q) || col.catalog.toLowerCase().includes(q);
          let matchesFilter = true;
          if (filter === "automated") matchesFilter = col.type === "automated";
          else if (filter === "manual") matchesFilter = col.type === "manual";
          else if (["ok", "failed", "empty"].includes(filter)) matchesFilter = col.status === filter;
          return matchesSearch && matchesFilter;
        });
        return {...cat, collections};
      })
      .filter(cat => cat.collections.length > 0);
});

// When a filter/search is active: auto-expand matching catalogs.
// When cleared: restore default (only failing expanded).
watch(visibleCatalogs, (cats) => {
  const isFiltering = searchQuery.value || activeFilter.value !== "all";
  if (isFiltering) {
    const next = new Set(expandedCatalogs.value);
    cats.forEach(cat => next.add(cat.id));
    expandedCatalogs.value = next;
  } else {
    applyDefaultExpansion();
  }
});

function setFilter(value) {
  activeFilter.value = activeFilter.value === value ? "all" : value;
}

function toggleCatalog(id) {
  const next = new Set(expandedCatalogs.value);
  next.has(id) ? next.delete(id) : next.add(id);
  expandedCatalogs.value = next;
}

function applyDefaultExpansion() {
  const next = new Set();
  catalogs.value.forEach(cat => {
    if (cat.status === "failed") next.add(cat.id);
  });
  expandedCatalogs.value = next;
}

async function fetchCatalogs() {
  loading.value = true;
  error.value = null;
  try {
    const res = await fetch(props.apiUrl);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    catalogs.value = data.catalogs;
    applyDefaultExpansion();
  } catch (e) {
    error.value = `Failed to load dashboard: ${e.message}`;
  } finally {
    loading.value = false;
  }
}

// Silent background re-fetch on SSE events: patches status + last_run_at only,
// leaves sparklines intact to avoid visual noise during active ingestion.
async function refreshStatuses() {
  try {
    const res = await fetch(props.apiUrl);
    if (!res.ok) return;
    const data = await res.json();

    const colIndex = {};
    for (const cat of catalogs.value) {
      for (const col of cat.collections) {
        colIndex[col.id] = col;
      }
    }

    for (const newCat of data.catalogs) {
      for (const newCol of newCat.collections) {
        const existing = colIndex[newCol.id];
        if (existing) {
          existing.status = newCol.status;
          existing.last_run_at = newCol.last_run_at;
          existing.last_run_status = newCol.last_run_status;
          existing.item_count = newCol.item_count;
        }
      }
    }

    // Recalculate catalog badges from updated collection statuses.
    for (const cat of catalogs.value) {
      const statuses = cat.collections.map(c => c.status);
      cat.status = statuses.includes("failed") ? "failed"
          : statuses.some(s => s === "ok") ? "ok"
              : "empty";
      cat.summary = {
        ok: statuses.filter(s => s === "ok").length,
        failed: statuses.filter(s => s === "failed").length,
        empty: statuses.filter(s => s === "empty").length,
      };
    }
  } catch (_) {
    // Silent — SSE-triggered refresh is best-effort.
  }
}

function connectSSE() {
  if (sseSource) return;
  sseSource = new EventSource("/admin/api/ingestion/events/");
  sseSource.addEventListener("file_ingestion.status_changed", refreshStatuses);
  sseSource.onerror = () => {
    sseSource?.close();
    sseSource = null;
    // Reconnect after 5s on error.
    setTimeout(connectSSE, 5000);
  };
}

function openDrawer(col) {
  selectedCollection.value = col;
  drawerVisible.value = true;
}

onMounted(() => {
  fetchCatalogs();
  connectSSE();
});

onUnmounted(() => {
  sseSource?.close();
  sseSource = null;
});

function statusLabel(s) {
  return {ok: "Active", failed: "Failed", empty: "No Data"}[s] ?? s;
}

function statusClass(s) {
  return {ok: "success", failed: "danger", empty: "info"}[s] ?? "info";
}

function statusIcon(s) {
  return {
    ok: "pi pi-check-circle",
    failed: "pi pi-times-circle",
    empty: "pi pi-minus-circle",
  }[s] ?? "pi pi-circle";
}

function sparklineLabel(s) {
  return {success: "OK", failed: "Failed", empty: "No data"}[s] ?? s;
}

function timeAgo(iso) {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 2) return "just now";
  if (mins < 60) return `${mins} minutes ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)} days ago`;
}

function formatShortDate(iso) {
  return new Date(iso).toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });
}

function isStale(iso) {
  return Date.now() - new Date(iso).getTime() > 1000 * 60 * 60 * 24 * 3;
}
</script>

<style scoped>
.ingestion-dashboard {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: #ffffff;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  color: #334155;
  width: 100%;
  overflow: hidden;
}

/* Header */
.header-section {
  padding: 24px 32px;
  background-color: #f8fafc;
  border-bottom: 1px solid #e2e8f0;
}

.header-top-row {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  flex-wrap: wrap;
  gap: 24px;
}

.dashboard-title {
  font-size: 24px;
  font-weight: 700;
  color: #0f172a;
  margin: 0 0 12px 0;
  line-height: 1.2;
}

.header-badges {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}

.meta-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  font-weight: 500;
  color: #64748b;
  background: #ffffff;
  padding: 4px 10px;
  border-radius: 4px;
  border: 1px solid #e2e8f0;
}

.status-summary-group {
  display: flex;
  gap: 12px;
}

.summary-card {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-width: 90px;
  padding: 8px 16px;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  background: #fff;
  cursor: pointer;
  transition: box-shadow 0.15s;
  user-select: none;
}

.summary-card:hover { box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1); }
.summary-card.active { box-shadow: 0 0 0 2px currentColor; }

.summary-label {
  font-size: 11px;
  text-transform: uppercase;
  font-weight: 600;
  color: #64748b;
  letter-spacing: 0.5px;
  margin-bottom: 4px;
}

.summary-count {
  font-size: 20px;
  font-weight: 700;
  line-height: 1;
}

.summary-card.success { border-bottom: 3px solid #15803d; }
.summary-card.success .summary-count { color: #15803d; }
.summary-card.danger  { border-bottom: 3px solid #b91c1c; }
.summary-card.danger .summary-count  { color: #b91c1c; }
.summary-card.info    { border-bottom: 3px solid #64748b; }
.summary-card.info .summary-count    { color: #64748b; }

/* Content */
.content-section {
  padding: 24px 32px;
  background: #ffffff;
}

.controls-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  flex-wrap: wrap;
  gap: 16px;
}

.controls-left {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  align-items: center;
}

.search-wrapper { position: relative; }

.search-icon {
  position: absolute;
  left: 12px;
  top: 50%;
  transform: translateY(-50%);
  color: #64748b;
  pointer-events: none;
  z-index: 1;
}

.search-input {
  padding-left: 36px !important;
  border: 1px solid #cbd5e1 !important;
  border-radius: 4px !important;
  height: 40px;
  width: 260px;
  color: #0f172a;
  font-size: 14px;
  background: #fff;
}

.filter-group {
  display: flex;
  background: #fff;
  border: 1px solid #cbd5e1;
  border-radius: 4px;
  overflow: hidden;
}

.filter-btn {
  background: #fff;
  border: none;
  padding: 0 16px;
  height: 38px;
  font-size: 13px;
  font-weight: 500;
  color: #64748b;
  border-right: 1px solid #e2e8f0;
  cursor: pointer;
  transition: all 0.1s;
}

.filter-btn:last-child { border-right: none; }
.filter-btn:hover { background: #f8fafc; color: #0f172a; }
.filter-btn.active { background: #0f172a; color: #fff; }
.filter-btn.success.active { background: #166534; color: #fff; }
.filter-btn.danger.active  { background: #dc2626; color: #fff; }

.refresh-btn {
  border: 1px solid #cbd5e1 !important;
  font-weight: 600 !important;
  font-size: 13px !important;
}

/* Catalog hierarchy */
.catalog-list {
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  overflow: hidden;
}

.catalog-group + .catalog-group {
  border-top: 1px solid #e2e8f0;
}

.catalog-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 20px;
  background: #f1f5f9;
  cursor: pointer;
  user-select: none;
  transition: background 0.1s;
}

.catalog-row:hover { background: #e9eef5; }

.expand-icon {
  color: #64748b;
  font-size: 11px;
  width: 14px;
  flex-shrink: 0;
}

.catalog-name {
  font-weight: 700;
  font-size: 14px;
  color: #0f172a;
  flex: 1;
}

.catalog-summary {
  display: flex;
  gap: 6px;
}

.summary-chip {
  font-size: 11px;
  font-weight: 600;
  padding: 2px 7px;
  border-radius: 10px;
}

.summary-chip.ok     { background: #dcfce7; color: #15803d; }
.summary-chip.failed { background: #fee2e2; color: #b91c1c; }
.summary-chip.empty  { background: #f1f5f9; color: #64748b; }

.catalog-col-count {
  font-size: 12px;
  color: #94a3b8;
  white-space: nowrap;
}

/* Collection rows */
.collection-list {
  border-top: 1px solid #e2e8f0;
}

.collection-row {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 12px 20px 12px 46px;
  border-bottom: 1px solid #f1f5f9;
  cursor: pointer;
  transition: background 0.1s;
}

.collection-row:last-child { border-bottom: none; }
.collection-row:hover { background: #f8fafc; }

.collection-cell {
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 14rem;
  flex: 1;
}

.collection-icon-box {
  width: 30px;
  height: 30px;
  background: #f1f5f9;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #64748b;
  flex-shrink: 0;
  font-size: 11px;
}

.cell-title {
  font-weight: 600;
  font-size: 13px;
  color: #0f172a;
}

.status-indicator {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 12px;
  font-weight: 600;
  padding: 3px 8px;
  border-radius: 4px;
  white-space: nowrap;
}

.status-indicator.success { background: #dcfce7; color: #15803d; }
.status-indicator.danger  { background: #fee2e2; color: #b91c1c; }
.status-indicator.info    { background: #f1f5f9; color: #64748b; }

.type-badge {
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 4px;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  white-space: nowrap;
}

.type-badge.automated { background: #dbeafe; color: #1d4ed8; }
.type-badge.manual    { background: #f1f5f9; color: #475569; }

.time-cell { display: flex; flex-direction: column; min-width: 9rem; }

.time-main {
  font-weight: 500;
  color: #0f172a;
  font-size: 13px;
}

.time-main.text-danger { color: #dc2626; }
.time-main.never { color: #94a3b8; }

.time-sub {
  font-size: 11px;
  color: #94a3b8;
  margin-top: 2px;
  font-family: 'SF Mono', 'Roboto Mono', monospace;
}

.items-count {
  font-size: 13px;
  font-family: 'SF Mono', 'Roboto Mono', monospace;
  color: #334155;
  min-width: 4rem;
  text-align: right;
}

/* Sparkline */
.sparkline {
  display: flex;
  align-items: flex-end;
  gap: 2px;
  height: 18px;
}

.spark-bar {
  width: 5px;
  border-radius: 2px;
  flex-shrink: 0;
  transition: opacity 0.15s;
}

.spark-bar--success { height: 14px; background: #15803d; }
.spark-bar--failed  { height: 14px; background: #dc2626; }
.spark-bar--empty   { height: 6px;  background: #e2e8f0; }
.spark-bar:hover { opacity: 0.75; }

.empty-state, .loading-state {
  padding: 40px;
  text-align: center;
  color: #64748b;
  font-size: 14px;
}

@media (max-width: 768px) {
  .header-top-row, .controls-left, .status-summary-group { flex-direction: column; }
  .search-input { width: 100%; }
  .collection-row { flex-wrap: wrap; padding-left: 20px; }
}
</style>
