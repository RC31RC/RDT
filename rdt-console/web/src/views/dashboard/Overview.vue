<template>
  <div class="home-page" v-loading="initialLoading">
    <el-alert v-if="error" :title="error" type="error" :closable="false" />
    <el-alert
      v-if="overview?.partial"
      title="部分运行数据暂时不可用，当前只展示已读取的结果。"
      type="warning"
      :closable="false"
    />
    <el-card class="home-card" shadow="never" :body-style="cardBodyStyle">
      <div class="home-welcome">
        <img class="home-welcome__avatar" src="/rdt-mark.svg" alt="RDT" />
        <div class="home-welcome__content">
          <div class="home-welcome__greeting">欢迎回来，{{ auth.principal?.displayName }}</div>
          <div class="home-welcome__subtitle">
            查看消息消费与任务执行状态，当前环境
            <span class="home-welcome__name">{{ namespace.selected || "尚未连接" }}</span>
          </div>
        </div>
        <el-button :icon="Refresh" :disabled="loading" @click="load">刷新数据</el-button>
      </div>
    </el-card>

    <div class="home-stat-grid">
      <el-card
        v-for="metric in metrics"
        :key="metric.label"
        class="home-card home-stat-card"
        shadow="never"
        :body-style="cardBodyStyle"
      >
        <div class="home-stat-card__inner">
          <div>
            <div class="home-stat-card__label">{{ metric.label }}</div>
            <div class="home-stat-card__value">
              {{ overview ? metric.value.toLocaleString() : "—" }}
            </div>
          </div>
          <div class="home-stat-card__icon" :class="`tone-${metric.tone}`">
            <KoiGlobalIcon :name="metric.icon" :size="24" />
          </div>
        </div>
      </el-card>
    </div>

    <RuntimeCharts :samples="samples" :streams="chartStreams" :stream-total="chartStreamTotal" :instances="chartInstances" :stream-error="streamError" :instance-error="instanceError" :group-count="overview?.groupCount" />

    <div class="home-panel-grid">
      <el-card class="home-card" shadow="never" :body-style="cardBodyStyle">
        <div class="home-panel-head">
          <div class="home-panel-head__title">运行健康</div>
          <el-tag :type="!namespace.selected ? 'info' : alert?.level === 'OK' ? 'success' : 'warning'">
            {{ !namespace.selected ? "未连接" : alert?.level === "OK" ? "监控正常" : "请检查" }}
          </el-tag>
        </div>
        <el-alert
          v-if="alert && alert.level !== 'OK'"
          :title="alert.message || '死信监控暂不可用'"
          type="warning"
          :closable="false"
          show-icon
        />
        <div class="signal-row">
          <span>
            <i class="signal-dot warning" />
            任务失败记录
          </span>
          <strong>{{ overview?.taskFailureCount ?? "—" }}</strong>
        </div>
        <div class="signal-row">
          <span>
            <i class="signal-dot danger" />
            死信消息
          </span>
          <strong>{{ overview?.deadLetterCount ?? "—" }}</strong>
        </div>
        <div class="signal-row">
          <span>
            <i class="signal-dot" />
            待确认消息
          </span>
          <strong>{{ overview?.pendingCount ?? "—" }}</strong>
        </div>
        <p class="home-panel-desc">
          指标每 30 秒刷新。待确认包含正在执行和等待重试的消息，请结合任务记录与死信判断故障。
        </p>
      </el-card>
      <el-card class="home-card" shadow="never" :body-style="cardBodyStyle">
        <div class="home-panel-head">
          <div class="home-panel-head__title">快捷管理</div>
        </div>
        <div class="home-shortcuts">
          <router-link v-for="link in shortcuts" :key="link.path" :to="link.path">
            <KoiGlobalIcon :name="link.icon" :size="24" />
            <span>{{ link.title }}</span>
            <el-tag v-if="overview" size="small" effect="plain">{{ link.count }}</el-tag>
            <el-icon><ArrowRight /></el-icon>
          </router-link>
        </div>
        <p class="home-panel-desc">管理操作按当前账号权限开放。Redis 连接设置与本地账号管理相互独立。</p>
      </el-card>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import { ArrowRight, Refresh } from "@element-plus/icons-vue";
import { managementApi, type ManagementOverview, type DeadLetterAlert, type RuntimeInstance, type StreamView } from "@/api/management";
import { useNamespaceStore } from "@/stores/namespace";
import { useAuthStore } from "@/stores/auth";
import RuntimeCharts, { type Observation } from "./RuntimeCharts.vue";

const auth = useAuthStore();
const cardBodyStyle = { padding: "16px 20px" };
const overview = ref<ManagementOverview>();
const alert = ref<DeadLetterAlert>();
const samples = ref<Observation[]>([]);
const chartStreams = ref<StreamView[]>([]);
const chartStreamTotal = ref(0);
const chartInstances = ref<RuntimeInstance[]>([]);
const streamError = ref("");
const instanceError = ref("");
let refreshTimer: number | undefined;
let generation = 0;
const loading = ref(false);
const initialLoading = computed(() => loading.value && !overview.value);
const error = ref("");
const namespace = useNamespaceStore();

const shortcuts = computed(() => [
  { path: "/instances", title: "运行节点", icon: "koi-driver", count: chartInstances.value.length },
  { path: "/streams", title: "消息主题", icon: "koi-conversation-box", count: overview.value?.streamCount ?? 0 },
  { path: "/tasks", title: "定时任务", icon: "koi-money-time", count: overview.value?.taskCount ?? 0 },
  { path: "/dead-letters", title: "死信记录", icon: "koi-notes", count: overview.value?.deadLetterCount ?? 0 }
]);
const metrics = computed(() => [
  {
    label: "消息主题",
    value: overview.value?.streamCount ?? 0,
    icon: "koi-conversation-box",
    tone: "blue"
  },
  {
    label: "保留消息总数",
    value: overview.value?.streamLength ?? 0,
    icon: "koi-analysis",
    tone: "teal"
  },
  {
    label: "待确认消息",
    value: overview.value?.pendingCount ?? 0,
    icon: "koi-conversation-box",
    tone: "amber"
  },
  {
    label: "定时任务",
    value: overview.value?.taskCount ?? 0,
    icon: "koi-money-time",
    tone: "green"
  }
]);

async function load() {
  const selected = namespace.selected;
  const request = ++generation;
  if (!selected) {
    alert.value = undefined;
    overview.value = undefined;
    loading.value = false;
    error.value = "";
    streamError.value = "";
    instanceError.value = "";
    return;
  }
  loading.value = true;
  error.value = "";
  try {
    const [overviewResult, alertResult, streamResult, instanceResult] = await Promise.allSettled([
      managementApi.overview(selected),
      managementApi.deadLetterAlert(selected),
      managementApi.streams(selected, 0, 50),
      managementApi.instances(selected)
    ]);
    if (request !== generation || selected !== namespace.selected) return;
    if (overviewResult.status === "fulfilled") {
      overview.value = overviewResult.value.data;
      if (!overview.value.partial) samples.value = [...samples.value, { at: Date.now(), pending: overview.value.pendingCount, deadLetters: overview.value.deadLetterCount }].slice(-30);
    } else {
      error.value = overview.value ? "刷新失败，暂时保留上次成功读取的数据。" : "概览数据读取失败，请检查 Redis 连接。";
    }
    if (streamResult.status === "fulfilled") {
      chartStreams.value = streamResult.value.data.items;
      chartStreamTotal.value = streamResult.value.data.total;
    }
    if (instanceResult.status === "fulfilled") chartInstances.value = instanceResult.value.data;
    streamError.value = streamResult.status === "rejected" ? "主题数据读取失败" : "";
    instanceError.value = instanceResult.status === "rejected" ? "节点数据读取失败" : "";
    alert.value =
      alertResult.status === "fulfilled"
        ? alertResult.value.data
        : {
            namespace: selected,
            level: "UNKNOWN",
            retainedCount: -1,
            recentCount: -1,
            oldestAtMillis: 0,
            checkedAtMillis: Date.now(),
            message: "死信监控不可用，请检查管理服务。"
          };
  } finally {
    if (request === generation) loading.value = false;
  }
}

onMounted(() => {
  void load();
  refreshTimer = window.setInterval(() => {
    if (!loading.value && !document.hidden) void load();
  }, 30000);
});
onUnmounted(() => {
  generation++;
  if (refreshTimer) window.clearInterval(refreshTimer);
});
watch(
  () => namespace.selected,
  () => {
    alert.value = undefined;
    overview.value = undefined;
    samples.value = [];
    chartStreams.value = [];
    chartStreamTotal.value = 0;
    chartInstances.value = [];
    void load();
  }
);
</script>
<style scoped lang="scss">
.home-page {
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  gap: 20px;
  padding: 0;
}

.home-stat-grid,
.home-panel-grid {
  display: grid;
  gap: 20px;
  width: 100%;
}

.home-stat-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.home-panel-grid {
  grid-template-columns: 5fr 7fr;
}

.home-card {
  border-radius: 8px;
}

.home-welcome {
  display: flex;
  align-items: center;
  gap: 16px;
  min-height: 76px;
  padding: 4px 0;
}

.home-welcome__avatar {
  width: 56px;
  height: 56px;
  object-fit: cover;
  border: 2px solid color-mix(in srgb, var(--el-color-primary) 18%, transparent);
  border-radius: 50%;
  user-select: none;
}

.home-welcome__greeting {
  font-size: 20px;
  font-weight: 700;
  line-height: 1.3;
  color: var(--el-text-color-primary);
}

.home-welcome__subtitle {
  margin-top: 8px;
  font-size: 13px;
  line-height: 1.6;
  color: var(--el-text-color-secondary);
}

.home-welcome__name {
  margin-left: 4px;
  font-weight: 600;
  color: var(--el-color-primary);
}

.home-panel-card {
  height: 100%;
  min-height: 420px;
}

.home-panel-card--compact {
  min-height: 420px;
}

.home-panel-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 12px;
}

.home-panel-head__title {
  font-size: 16px;
  font-weight: 700;
  color: var(--el-text-color-primary);
}

.home-panel-head__trend {
  margin-top: 6px;
  font-size: 12px;

  &.is-up {
    color: var(--el-color-success);
  }

  &.is-down {
    color: var(--el-color-danger);
  }
}

.home-panel-desc {
  margin: 0 0 12px;
  font-size: 12px;
  line-height: 1.65;
  color: var(--el-text-color-secondary);
}

.home-user-metrics {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  padding-top: 16px;
  margin-top: 12px;
  border-top: 1px solid var(--el-border-color-lighter);
}

.home-user-metrics__value {
  font-size: 18px;
  font-weight: 700;
  color: var(--el-text-color-primary);
}

.home-user-metrics__label {
  margin-top: 4px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

@media (width <= 992px) {
  .home-page {
    gap: 16px;
    padding: 10px 12px 14px;
  }

  .home-stat-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 16px;
  }

  .home-panel-grid {
    grid-template-columns: 1fr;
    gap: 16px;
  }

  .home-user-metrics {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (width <= 576px) {
  .home-page {
    gap: 12px;
    padding: 8px 10px 12px;
  }

  .home-stat-grid,
  .home-panel-grid {
    grid-template-columns: 1fr;
    gap: 12px;
  }
}

.home-card {
  border-radius: 8px;
}

.home-stat-card {
  height: 100%;
}

.home-stat-card__inner {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  min-height: 88px;
}

.home-stat-card__label {
  font-size: 13px;
  color: var(--el-text-color-secondary);
}

.home-stat-card__value {
  margin-top: 8px;
  font-size: 28px;
  font-weight: 700;
  line-height: 1.1;
  color: var(--el-text-color-primary);
  letter-spacing: -0.02em;
}

.home-stat-card__trend {
  margin-top: 10px;
  font-size: 12px;

  &.is-up {
    color: var(--el-color-success);
  }

  &.is-down {
    color: var(--el-color-danger);
  }
}

.home-stat-card__icon {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  justify-content: center;
  width: 46px;
  height: 46px;
  border-radius: 12px;
}

.home-welcome__content {
  flex: 1;
  min-width: 0;
}
.home-welcome {
  flex-wrap: wrap;
}
.home-shortcuts {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin: 20px 0;
}
.home-shortcuts a {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 18px 12px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  transition:
    transform 0.2s,
    background 0.2s;
}
.home-shortcuts a span {
  flex: 1;
}
.home-shortcuts a:hover {
  color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
  transform: translateY(-2px);
}
.tone-blue,
.tone-teal,
.tone-violet,
.tone-amber,
.tone-green,
.tone-red {
  color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
}
.tone-amber {
  color: var(--el-color-warning);
  background: var(--el-color-warning-light-9);
}
.tone-red {
  color: var(--el-color-danger);
  background: var(--el-color-danger-light-9);
}
.tone-green,
.tone-teal {
  color: var(--el-color-success);
  background: var(--el-color-success-light-9);
}
@media (max-width: 576px) {
  .home-welcome__content {
    flex-basis: calc(100% - 80px);
  }
  .home-welcome > .el-button {
    margin-left: 72px;
  }
  .home-shortcuts {
    grid-template-columns: 1fr;
  }
}
</style>
