<template>
  <div class="page-stack">
    <section class="page-intro">
      <div>
        <div class="eyebrow">SCHEDULED TASKS</div>
        <h2>定时任务</h2>
        <p>任务定义描述调度规则；每次触发产生一个执行实例，失败重试会增加执行次数。展开任务可查看执行实例与每次执行的结果。</p>
      </div>
      <KoiToolbar :searchable="false" :loading="loading" @refresh-table="load" />
    </section>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <KoiCard class="table-panel">
      <RdtTable v-loading="loading" :data="tasks" @expand-change="expand" row-key="redisKey" empty-text="暂无定时任务">
        <el-table-column type="expand">
          <template #default="{ row }">
            <div class="expand-content">
              <div class="detail-caption">执行实例（每页 50 条）</div>
              <RdtTable :data="executions[row.taskId] || []" size="small" empty-text="暂无执行实例">
                <el-table-column prop="executionId" label="执行实例 ID" min-width="230" />
                <el-table-column label="执行状态" width="120">
                  <template #default="{ row: execution }"><el-tag :type="statusTone(execution.status)">{{ statusLabel(execution.status) }}</el-tag></template>
                </el-table-column>
                <el-table-column prop="attempt" label="执行次数" width="80" />
                <el-table-column prop="instanceId" label="执行节点" min-width="160" />
                <el-table-column prop="runId" label="进程运行 ID" min-width="220" />
                <el-table-column prop="detail" label="结果 / 等待原因" min-width="230" show-overflow-tooltip />
                <el-table-column label="操作" width="110">
                  <template #default="{ row: execution }">
                    <el-button
                      v-permission="'runtime:operate'"
                      v-if="execution.status === 'FAILED'"
                      link
                      type="primary"
                      @click="retry(execution)"
                    >
                      重试失败
                    </el-button>
                  </template>
                </el-table-column>
              </RdtTable>
              <el-button
                :disabled="!(executionOffsets[row.taskId] || 0)"
                @click="executionPage(row.taskId, (executionOffsets[row.taskId] || 0) - 50)"
              >
                上一页
              </el-button>
              <el-button
                :disabled="(executions[row.taskId] || []).length < 50"
                @click="executionPage(row.taskId, (executionOffsets[row.taskId] || 0) + 50)"
              >
                下一页
              </el-button>
              <div class="detail-caption">执行明细（含重试）</div>
              <RdtTable :data="attempts[row.taskId] || []" size="small" empty-text="暂无执行记录">
                <el-table-column prop="executionId" label="执行实例 ID" min-width="210" show-overflow-tooltip />
                <el-table-column prop="attempt" label="第几次执行" width="110" />
                <el-table-column label="计划时间" min-width="180">
                  <template #default="{ row: result }">{{ formatTimestamp(result.scheduledAt) }}</template>
                </el-table-column>
                <el-table-column label="开始时间" min-width="180">
                  <template #default="{ row: result }">{{ formatTimestamp(result.startedAt) }}</template>
                </el-table-column>
                <el-table-column label="完成时间" min-width="180">
                  <template #default="{ row: result }">{{ formatTimestamp(result.completedAt) }}</template>
                </el-table-column>
                <el-table-column prop="instanceId" label="执行节点" min-width="150" />
                <el-table-column label="结果" width="100">
                  <template #default="{ row: result }">
                    <el-tag :type="result.successful ? 'success' : 'danger'" size="small">
                      {{ result.successful ? "成功" : "失败" }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="detail" label="详情" min-width="220" show-overflow-tooltip />
              </RdtTable>
              <el-button
                aria-label="上一页执行明细"
                :disabled="!(attemptOffsets[row.taskId] || 0)"
                @click="attemptPage(row.taskId, (attemptOffsets[row.taskId] || 0) - 50)"
              >
                上一页
              </el-button>
              <el-button
                aria-label="下一页执行明细"
                :disabled="(attempts[row.taskId] || []).length < 50"
                @click="attemptPage(row.taskId, (attemptOffsets[row.taskId] || 0) + 50)"
              >
                下一页
              </el-button>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="任务名称 / ID" min-width="240" show-overflow-tooltip>
          <template #default="{ row }">
            <strong>{{ row.displayName || row.taskId }}</strong>
            <div class="instance-secondary"><code>{{ row.taskId }}</code></div>
          </template>
        </el-table-column>
        <el-table-column prop="definition" label="调度规则" min-width="210" show-overflow-tooltip />
        <el-table-column label="下次执行" min-width="180">
          <template #default="{ row }">{{ formatNext(row.nextAtMillis) }}</template>
        </el-table-column>
        <el-table-column label="节点选择策略" min-width="140">
          <template #default="{ row }">{{ routeLabels[row.route] || row.route || "—" }}</template>
        </el-table-column>
        <el-table-column label="执行状态" min-width="130">
          <template #default="{ row }">
            <el-tag :type="statusTone(row.executionStatus)">{{ statusLabel(row.executionStatus) }}</el-tag>
            <el-tag v-if="row.paused" type="warning">已暂停</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="230" fixed="right">
          <template #default="{ row }">
            <el-button v-permission="'runtime:operate'" link type="primary" @click="pause(row)">
              {{ row.paused ? "恢复" : "暂停" }}
            </el-button>
            <el-button
              v-permission="'runtime:operate'"
              link
              type="primary"
              :loading="triggering === row.taskId"
              @click="trigger(row as unknown as TaskView)"
            >
              立即触发
            </el-button>
            <el-button
              v-permission="'runtime:delete'"
              link
              type="danger"
              :loading="deleting === row.taskId"
              @click="deleteResults(row as unknown as TaskView)"
            >
              清理记录
            </el-button>
          </template>
        </el-table-column>
      </RdtTable>
      <el-pagination
        v-model:current-page="page"
        :page-size="50"
        :total="total"
        layout="total, prev, pager, next"
        @current-change="load"
      />
    </KoiCard>
  </div>
</template>

<script setup lang="ts">
import KoiCard from "@/components/KoiCard/index.vue";
import KoiToolbar from "@/components/KoiToolbar/index.vue";
import { onMounted, ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { managementApi, type TaskView, type JobExecution, type TaskResult } from "@/api/management";
import { useNamespaceStore } from "@/stores/namespace";

const routeLabels: Record<string, string> = { HEALTH_FIRST: "健康节点优先", RANDOM: "随机节点", SPECIFIED: "指定节点" };
const statusLabels: Record<string, string> = { READY: "待派发", WAITING: "等待可用节点", DISPATCHED: "已派发", RUNNING: "执行中", SUCCEEDED: "执行成功", FAILED: "执行失败", LEASE_LOST: "执行租约失效" };
const statusLabel = (status: string) => statusLabels[status] || status || "等待触发";
const statusTone = (status: string) => status === "SUCCEEDED" ? "success" : status === "FAILED" || status === "LEASE_LOST" ? "danger" : status === "WAITING" ? "warning" : "primary";
const tasks = ref<TaskView[]>([]);
const executions = ref<Record<string, JobExecution[]>>({});
const attempts = ref<Record<string, TaskResult[]>>({});
async function expand(row: unknown, expanded?: boolean | unknown[]) {
  const task = row as TaskView;
  if (
    expanded === false ||
    (Array.isArray(expanded) && !expanded.some((item) => (item as TaskView).taskId === task.taskId))
  )
    return;
  await Promise.all([executionPage(task.taskId, 0), attemptPage(task.taskId, 0)]);
}
const executionOffsets = ref<Record<string, number>>({});
const attemptOffsets = ref<Record<string, number>>({});
async function executionPage(taskId: string, offset: number) {
  try {
    executions.value[taskId] = (await managementApi.executions(namespace.selected, taskId, offset)).data;
    executionOffsets.value[taskId] = offset;
  } catch {
    ElMessage.error("执行记录读取失败。");
  }
}
async function attemptPage(taskId: string, offset: number) {
  try {
    attempts.value[taskId] = (await managementApi.attempts(namespace.selected, taskId, offset)).data;
    attemptOffsets.value[taskId] = offset;
  } catch {
    ElMessage.error("执行明细读取失败。");
  }
}
async function pause(row: unknown) {
  const task = row as TaskView;
  await managementApi.pauseTask(namespace.selected, task.taskId, !task.paused);
  await load();
}
async function retry(row: unknown) {
  const execution = row as JobExecution;
  try {
    await ElMessageBox.confirm("为这次失败执行创建新的手动重试？", "重试失败任务");
  } catch {
    return;
  }
  await managementApi.retryExecution(namespace.selected, execution.jobId, execution.executionId);
  await load();
}
const page = ref(1);
const total = ref(0);
const loading = ref(false);
const error = ref("");
const triggering = ref("");
const deleting = ref("");
const namespace = useNamespaceStore();

function formatTimestamp(value: string) {
  return value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
}
function formatNext(timestamp: number) {
  if (timestamp >= Number.MAX_SAFE_INTEGER) return "不再触发";
  if (!timestamp || timestamp < 1) return "-";
  return new Date(timestamp).toLocaleString("zh-CN", { hour12: false });
}

async function load() {
  if (!namespace.selected) {
    tasks.value = [];
    return;
  }
  loading.value = true;
  error.value = "";
  try {
    const { data } = await managementApi.tasks(namespace.selected, (page.value - 1) * 50, 50);
    tasks.value = data.items;
    executions.value = {};
    attempts.value = {};
    executionOffsets.value = {};
    attemptOffsets.value = {};
    total.value = data.total;
  } catch {
    error.value = "定时任务数据读取失败。";
  } finally {
    loading.value = false;
  }
}

async function trigger(task: unknown) {
  const taskView = task as TaskView;
  if (!namespace.selected) return;
  try {
    await ElMessageBox.confirm(
      `为 ${taskView.displayName || taskView.taskId}（ID：${taskView.taskId}）创建一次立即执行，正常周期保持原计划。任务将按注册的路由和容量派发到可用实例。`,
      "确认立即触发",
      {
        confirmButtonText: "立即触发",
        cancelButtonText: "取消",
        type: "warning"
      }
    );
  } catch {
    return;
  }
  triggering.value = taskView.taskId;
  try {
    const { data } = await managementApi.triggerTask(namespace.selected, taskView.taskId);
    if (data.changed) {
      ElMessage.success("已请求任务尽快执行。");
    } else {
      ElMessage.info("任务已到期或不存在，无需推进。");
    }
    await load();
  } catch {
    ElMessage.error("任务触发请求失败。");
  } finally {
    triggering.value = "";
  }
}

async function deleteResults(task: unknown) {
  const taskView = task as TaskView;
  if (!namespace.selected) return;
  try {
    await ElMessageBox.confirm(
      `只删除 ${taskView.displayName || taskView.taskId} 的执行记录，不会删除任务定义，也不会影响下一次调度。`,
      "确认清理执行记录",
      {
        confirmButtonText: "清理记录",
        cancelButtonText: "取消",
        type: "warning"
      }
    );
  } catch {
    return;
  }
  deleting.value = taskView.taskId;
  try {
    const { data } = await managementApi.deleteTaskResults(namespace.selected, taskView.taskId);
    if (data.changed) {
      ElMessage.success("执行记录已清理。");
    } else {
      ElMessage.info("没有可清理的执行记录。");
    }
    await load();
  } catch {
    ElMessage.error("执行记录清理失败。");
  } finally {
    deleting.value = "";
  }
}

onMounted(load);
watch(
  () => namespace.selected,
  () => {
    page.value = 1;
    void load();
  }
);
</script>
