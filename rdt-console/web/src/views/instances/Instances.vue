<template>
  <div class="page-stack">
    <section class="page-intro">
      <div>
        <div class="eyebrow">RUNTIME REGISTRY</div>
        <h2>运行节点</h2>
        <p>实例信息由业务进程的短租约心跳维护；过期租约不会显示为在线。</p>
      </div>
      <KoiToolbar :searchable="false" :loading="loading" @refresh-table="load" />
    </section>
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <KoiCard class="table-panel">
      <RdtTable v-loading="loading" :data="instances" row-key="runId" empty-text="当前命名空间没有在线节点">
        <el-table-column type="expand">
          <template #default="{ row }">
            <div class="expand-content">
              <RdtTable :data="row.components" size="small">
                <el-table-column label="组件类型" width="120">
                  <template #default="{ row: component }">{{ component.kind === "listener" ? "消息消费者" : component.kind === "job" ? "定时任务" : component.kind }}</template>
                </el-table-column>
                <el-table-column prop="id" label="组件 ID" min-width="200" />
                <el-table-column prop="destination" label="消息主题（Topic）" min-width="140" />
                <el-table-column prop="group" label="消费组名称" min-width="140" />
                <el-table-column prop="definition" label="能力 / 策略" min-width="200" />
                <el-table-column prop="capacity" label="并发上限" width="90" />
                <el-table-column prop="active" label="执行中" width="110" />
                <el-table-column label="共享缓冲队列" width="150">
                  <template #default="{ row: component }">
                    {{ component.buffered }} / {{ component.bufferCapacity }}
                  </template>
                </el-table-column>
              </RdtTable>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="应用 / 节点 ID" min-width="190" show-overflow-tooltip>
          <template #default="{ row }">
            <strong>{{ row.application }}</strong>
            <div class="instance-secondary">{{ row.instanceId }}</div>
          </template>
        </el-table-column>
        <el-table-column label="进程运行 ID" min-width="230" show-overflow-tooltip>
          <template #default="{ row }">
            <code>{{ row.runId }}</code>
          </template>
        </el-table-column>
        <el-table-column label="启动时间" min-width="170">
          <template #default="{ row }">{{ formatTime(row.startedAtMillis) }}</template>
        </el-table-column>
        <el-table-column label="最近心跳" min-width="170">
          <template #default="{ row }">{{ formatTime(row.lastHeartbeatAtMillis) }}</template>
        </el-table-column>
        <el-table-column label="租约时长" width="100">
          <template #default="{ row }">{{ formatDuration(row.leaseMillis) }}</template>
        </el-table-column>
        <el-table-column prop="queueConcurrency" label="消费并发" width="110" />
        <el-table-column prop="scheduleConcurrency" label="任务并发" width="110" />
        <el-table-column label="压力保护" min-width="190">
          <template #default="{ row }">
            <el-tag :type="row.pressure.paused ? 'warning' : 'success'">
              {{ row.pressure.paused ? "暂停新工作" : "正常" }}
            </el-tag>
            <div class="instance-secondary">
              CPU {{ percent(row.pressure.cpuLoad) }} · DB
              {{ percent(row.pressure.databaseUsage) }}
            </div>
            <div v-if="row.pressure.paused" class="instance-secondary">
              {{ pressureReason(row.pressure.reasons) }}
            </div>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.draining ? 'warning' : 'success'" effect="light">
              {{ row.draining ? "停止接单中" : "在线" }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="120" fixed="right">
          <template #default="{ row }">
            <el-button v-permission="'runtime:operate'" link type="primary" @click="drain(row)">
              {{ row.draining ? "恢复接单" : "停止接单" }}
            </el-button>
          </template>
        </el-table-column>
      </RdtTable>
    </KoiCard>
  </div>
</template>

<script setup lang="ts">
import KoiCard from "@/components/KoiCard/index.vue";
import KoiToolbar from "@/components/KoiToolbar/index.vue";
import { onMounted, ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { managementApi, type RuntimeInstance } from "@/api/management";
import { useNamespaceStore } from "@/stores/namespace";

const namespace = useNamespaceStore();
const instances = ref<RuntimeInstance[]>([]);
const loading = ref(false);
const error = ref("");

function percent(value: number) {
  return value < 0 ? "未采集" : `${Math.round(value * 100)}%`;
}
function pressureReason(reasons: string[]) {
  const labels: Record<string, string> = {
    CPU_PRESSURE: "CPU 繁忙",
    DATABASE_PRESSURE: "数据库连接繁忙",
    RECOVERY_COOLDOWN: "等待负载稳定",
    PRESSURE_SENSOR_FAILURE: "压力采样失败"
  };
  return reasons.map((reason) => labels[reason] || reason).join("、");
}

function formatTime(value: number) {
  return value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
}

function formatDuration(value: number) {
  return `${Math.round(value / 1000)} s`;
}

async function drain(row: unknown) {
  const instance = row as RuntimeInstance;
  try {
    await ElMessageBox.confirm(
      instance.draining ? "恢复此实例接收新工作？" : "停止此实例接收新工作，正在执行的工作继续完成？",
      "实例接单状态"
    );
  } catch {
    return;
  }
  await managementApi.drainInstance(namespace.selected, instance.instanceId, !instance.draining);
  ElMessage.success("已保存，在下一次心跳生效");
}

async function load() {
  if (!namespace.selected) {
    instances.value = [];
    return;
  }
  loading.value = true;
  error.value = "";
  try {
    instances.value = (await managementApi.instances(namespace.selected)).data;
  } catch {
    error.value = "实例注册信息读取失败。";
  } finally {
    loading.value = false;
  }
}

onMounted(load);
watch(() => namespace.selected, load);
</script>

<style scoped>
.instance-secondary {
  margin-top: 4px;
  color: #8993a3;
  font-size: 12px;
}
</style>
