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
                            {{ collections.length }} Collections
                        </span>
            <span class="meta-badge">
                            <i class="pi pi-bolt"></i>
                            {{ collections.filter(c => c.type === 'automated').length }} Automated
                        </span>
            <span class="meta-badge">
                            <i class="pi pi-upload"></i>
                            {{ collections.filter(c => c.type === 'manual').length }} Manual
                        </span>
          </div>
        </div>

        <div class="status-summary-group">
          <div class="summary-card success" @click="setFilter('ok')" :class="{active: activeFilter === 'ok'}">
            <span class="summary-label">Active</span>
            <span class="summary-count">{{ summary.ok }}</span>
          </div>
          <div class="summary-card warning" @click="setFilter('warning')" :class="{active: activeFilter === 'warning'}">
            <span class="summary-label">Warning</span>
            <span class="summary-count">{{ summary.warning }}</span>
          </div>
          <div class="summary-card danger" @click="setFilter('failed')" :class="{active: activeFilter === 'failed'}">
            <span class="summary-label">Failed</span>
            <span class="summary-count">{{ summary.failed }}</span>
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
            @click="fetchCollections"
        />
      </div>

      <!-- Error -->
      <Message v-if="error" severity="error" class="mb-3">{{ error }}</Message>

      <!-- Table -->
      <div class="table-wrapper">
        <DataTable
            :value="filteredCollections"
            :loading="loading"
            row-hover
            size="small"
            paginator
            :rows="50"
            @row-click="onRowClick"
            :row-class="() => 'cursor-pointer'"
        >
          <template #empty>
            <div class="empty-state">No collections found.</div>
          </template>

          <!-- Name + catalog -->
          <Column header="Collection" style="min-width: 16rem">
            <template #body="{data}">
              <div class="collection-cell">
                <div class="collection-icon-box">
                  <i :class="data.type === 'automated' ? 'pi pi-bolt' : 'pi pi-upload'"/>
                </div>
                <div>
                  <div class="cell-title">{{ data.name }}</div>
                  <div class="sub-text">{{ data.catalog_name }}</div>
                </div>
              </div>
            </template>
          </Column>

          <!-- Status -->
          <Column header="Status" style="width: 9rem">
            <template #body="{data}">
              <div :class="['status-indicator', statusClass(data.status)]">
                <i :class="statusIcon(data.status)"/>
                {{ statusLabel(data.status) }}
              </div>
            </template>
          </Column>

          <!-- Type -->
          <Column header="Type" style="width: 8rem">
            <template #body="{data}">
                            <span class="type-badge" :class="data.type">
                                {{ data.type === 'automated' ? 'Automated' : 'Manual' }}
                            </span>
            </template>
          </Column>

          <!-- Last run -->
          <Column header="Last Run" style="width: 12rem">
            <template #body="{data}">
              <div class="time-cell" v-if="data.last_run_at">
                                <span
                                    class="time-main"
                                    :class="{'text-danger': isStale(data.last_run_at)}"
                                >
                                    {{ timeAgo(data.last_run_at) }}
                                </span>
                <span class="time-sub">{{ formatShortDate(data.last_run_at) }}</span>
              </div>
              <span v-else class="time-main" style="color:#94a3b8">Never</span>
            </template>
          </Column>

          <!-- Items -->
          <Column header="Items" style="width: 6rem">
            <template #body="{data}">
                            <span class="font-mono" style="font-size:14px">
                                {{ data.item_count.toLocaleString() }}
                            </span>
            </template>
          </Column>

          <!-- Sparkline -->
          <Column header="Last 30 Days" style="width: 14rem">
            <template #body="{data}">
              <div class="sparkline">
                <div
                    v-for="(day, i) in data.sparkline"
                    :key="i"
                    :class="['spark-bar', `spark-bar--${day.status}`]"
                    :title="`${day.date}: ${sparklineLabel(day.status)}`"
                />
              </div>
            </template>
          </Column>
        </DataTable>
      </div>
    </div>

    <!-- Drawer -->
    <CollectionDrawer
        v-model="drawerVisible"
        :collection="selectedCollection"
    />
  </div>
</template>

<script setup>
import {computed, onMounted, ref} from "vue";
import DataTable from "primevue/datatable";
import Column from "primevue/column";
import Button from "primevue/button";
import Message from "primevue/message";
import InputText from "primevue/inputtext";
import CollectionDrawer from "@/components/CollectionDrawer.vue";

const props = defineProps({
  apiUrl: {type: String, required: true},
});

const collections = ref([]);
const loading = ref(false);
const error = ref(null);
const searchQuery = ref("");
const activeFilter = ref("all");
const drawerVisible = ref(false);
const selectedCollection = ref(null);

const filterOptions = [
  {label: "All", value: "all", cls: ""},
  {label: "Automated", value: "automated", cls: ""},
  {label: "Manual", value: "manual", cls: ""},
  {label: "Active", value: "ok", cls: "success"},
  {label: "Warning", value: "warning", cls: "warning"},
  {label: "Failed", value: "failed", cls: "danger"},
];

const summary = computed(() => ({
  ok: collections.value.filter(c => c.status === "ok").length,
  warning: collections.value.filter(c => c.status === "warning").length,
  failed: collections.value.filter(c => c.status === "failed").length,
}));

function setFilter(value) {
  activeFilter.value = activeFilter.value === value ? "all" : value;
}

const filteredCollections = computed(() => {
  const q = searchQuery.value.toLowerCase();
  return collections.value.filter((c) => {
    const matchesSearch = !q || c.name.toLowerCase().includes(q) || c.catalog.toLowerCase().includes(q);
    let matchesFilter = true;
    if (activeFilter.value === "automated") matchesFilter = c.type === "automated";
    else if (activeFilter.value === "manual") matchesFilter = c.type === "manual";
    else if (["ok", "warning", "failed", "empty"].includes(activeFilter.value))
      matchesFilter = c.status === activeFilter.value;
    return matchesSearch && matchesFilter;
  });
});

async function fetchCollections() {
  loading.value = true;
  error.value = null;
  try {
    const res = await fetch(props.apiUrl);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    collections.value = data.collections;
  } catch (e) {
    error.value = `Failed to load collections: ${e.message}`;
  } finally {
    loading.value = false;
  }
}

onMounted(fetchCollections);

function onRowClick({data}) {
  selectedCollection.value = data;
  drawerVisible.value = true;
}

function statusLabel(s) {
  return {ok: "Active", failed: "Failed", warning: "Warning", empty: "No Data"}[s] ?? s;
}

function statusClass(s) {
  return {ok: "success", failed: "danger", warning: "warning", empty: "info"}[s] ?? "info";
}

function statusIcon(s) {
  return {
    ok: "pi pi-check-circle",
    failed: "pi pi-times-circle",
    warning: "pi pi-exclamation-triangle",
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

/* Summary cards */
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

.summary-card:hover {
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
}

.summary-card.active {
  box-shadow: 0 0 0 2px currentColor;
}

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

.summary-card.success {
  border-bottom: 3px solid #15803d;
}

.summary-card.success .summary-count {
  color: #15803d;
}

.summary-card.warning {
  border-bottom: 3px solid #b45309;
}

.summary-card.warning .summary-count {
  color: #b45309;
}

.summary-card.danger {
  border-bottom: 3px solid #b91c1c;
}

.summary-card.danger .summary-count {
  color: #b91c1c;
}

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

.search-wrapper {
  position: relative;
}

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

.filter-btn:last-child {
  border-right: none;
}

.filter-btn:hover {
  background: #f8fafc;
  color: #0f172a;
}

.filter-btn.active {
  background: #0f172a;
  color: #fff;
}

.filter-btn.success.active {
  background: #166534;
  color: #fff;
}

.filter-btn.warning.active {
  background: #d97706;
  color: #fff;
}

.filter-btn.danger.active {
  background: #dc2626;
  color: #fff;
}

.refresh-btn {
  border: 1px solid #cbd5e1 !important;
  font-weight: 600 !important;
  font-size: 13px !important;
}

/* Table */
.table-wrapper {
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  overflow: hidden;
}

:deep(.p-datatable-thead > tr > th) {
  background: #f1f5f9 !important;
  color: #475569 !important;
  font-weight: 600 !important;
  text-transform: uppercase !important;
  font-size: 12px !important;
  letter-spacing: 0.05em !important;
  padding: 14px 20px !important;
  border-bottom: 1px solid #e2e8f0 !important;
  border-top: none !important;
}

:deep(.p-datatable-tbody > tr > td) {
  padding: 14px 20px !important;
  border-bottom: 1px solid #f1f5f9 !important;
  color: #334155;
  font-size: 14px;
}

:deep(.p-datatable-tbody > tr:last-child > td) {
  border-bottom: none !important;
}

:deep(.p-datatable-tbody > tr:hover > td) {
  background: #f8fafc !important;
  cursor: pointer;
}

/* Collection cell */
.collection-cell {
  display: flex;
  align-items: center;
  gap: 12px;
}

.collection-icon-box {
  width: 34px;
  height: 34px;
  background: #f1f5f9;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #64748b;
  flex-shrink: 0;
}

.cell-title {
  font-weight: 600;
  font-size: 14px;
  color: #0f172a;
}

.sub-text {
  font-size: 11px;
  color: #94a3b8;
  margin-top: 2px;
}

/* Status indicator */
.status-indicator {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 12px;
  font-weight: 600;
  padding: 3px 8px;
  border-radius: 4px;
}

.status-indicator.success {
  background: #dcfce7;
  color: #15803d;
}

.status-indicator.warning {
  background: #fef3c7;
  color: #b45309;
}

.status-indicator.danger {
  background: #fee2e2;
  color: #b91c1c;
}

.status-indicator.info {
  background: #f1f5f9;
  color: #64748b;
}

/* Type badge */
.type-badge {
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 4px;
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.type-badge.automated {
  background: #dbeafe;
  color: #1d4ed8;
}

.type-badge.manual {
  background: #f1f5f9;
  color: #475569;
}

/* Time cell */
.time-cell {
  display: flex;
  flex-direction: column;
}

.time-main {
  font-weight: 500;
  color: #0f172a;
  font-size: 13px;
}

.time-main.text-danger {
  color: #dc2626;
}

.time-sub {
  font-size: 11px;
  color: #94a3b8;
  margin-top: 2px;
  font-family: 'SF Mono', 'Roboto Mono', monospace;
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

.spark-bar--success {
  height: 14px;
  background: #15803d;
}

.spark-bar--failed {
  height: 14px;
  background: #dc2626;
}

.spark-bar--empty {
  height: 6px;
  background: #e2e8f0;
}

.spark-bar:hover {
  opacity: 0.75;
}

.empty-state {
  padding: 40px;
  text-align: center;
  color: #64748b;
}

@media (max-width: 768px) {
  .header-top-row, .controls-left, .status-summary-group {
    flex-direction: column;
  }

  .search-input {
    width: 100%;
  }
}
</style>