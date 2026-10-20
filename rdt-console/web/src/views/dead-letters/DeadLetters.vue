<template>
  <div class="page-stack">
    <section class="page-intro">
      <div>
        <div class="eyebrow">DEAD LETTERS</div>
        <h2>死信记录</h2>
        <p>消息消费失败且无法继续重试时进入死信记录。可查看消息内容和失败原因，再选择重新投递或删除。记录按业务端配置的保留期限自动过期。</p>
      </div>
      <KoiToolbar :searchable="false" :loading="loading" @refresh-table="load" />
    </section>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <KoiCard class="table-panel">
      <RdtTable v-loading="loading" :data="deadLetters" empty-text="暂无死信记录">
        <el-table-column label="消息主题（Topic）" min-width="240" show-overflow-tooltip>
          <template #default="{ row }">
            <strong>{{ row.displayName || row.destination }}</strong>
            <div class="instance-secondary"><code>{{ row.destination }}</code></div>
          </template>
        </el-table-column>
        <el-table-column prop="group" label="消费组名称" min-width="150" />
        <el-table-column prop="messageId" label="消息 ID" min-width="190">
          <template #default="{ row }">
            <code>{{ row.messageId }}</code>
          </template>
        </el-table-column>
        <el-table-column prop="attempts" label="消费次数" width="110" />
        <el-table-column prop="error" label="错误摘要" min-width="240" show-overflow-tooltip />
        <el-table-column label="失败时间" min-width="180">
          <template #default="{ row }">{{ row.failedAt ? new Date(row.failedAt).toLocaleString("zh-CN", { hour12: false }) : "—" }}</template>
        </el-table-column>
        <el-table-column label="消息内容" width="90" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" @click="showPayload(row.payload)">查看</el-button>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="150" fixed="right">
          <template #default="{ row }">
            <el-button
              v-permission="'runtime:delete'"
              link
              type="primary"
              :loading="retrying === keyFor(row as unknown as DeadLetterView)"
              @click="retry(row as unknown as DeadLetterView)"
            >
              重新投递
            </el-button>
            <el-button
              v-permission="'runtime:delete'"
              link
              type="danger"
              :loading="deleting === keyFor(row as unknown as DeadLetterView)"
              @click="remove(row as unknown as DeadLetterView)"
            >
              删除
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

    <el-dialog v-model="payloadVisible" title="消息内容" width="min(680px, 92vw)">
      <pre class="payload-view">{{ formattedPayload }}</pre>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import KoiCard from "@/components/KoiCard/index.vue";
import KoiToolbar from "@/components/KoiToolbar/index.vue";
import { computed, onMounted, ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { managementApi, type DeadLetterView } from "@/api/management";
import { useNamespaceStore } from "@/stores/namespace";

const deadLetters = ref<DeadLetterView[]>([]);
const page = ref(1);
const total = ref(0);
const loading = ref(false);
const error = ref("");
const payloadVisible = ref(false);
const payload = ref("");
const deleting = ref("");
const retrying = ref("");
const namespace = useNamespaceStore();

const formattedPayload = computed(() => {
  try {
    return JSON.stringify(JSON.parse(payload.value), null, 2);
  } catch {
    return payload.value;
  }
});

function showPayload(value: string) {
  payload.value = value;
  payloadVisible.value = true;
}

function keyFor(row: unknown) {
  const deadLetter = row as DeadLetterView;
  return `${deadLetter.destination}:${deadLetter.group}:${deadLetter.messageId}`;
}

async function load() {
  if (!namespace.selected) {
    deadLetters.value = [];
    return;
  }
  loading.value = true;
  error.value = "";
  try {
    const { data } = await managementApi.deadLetters(namespace.selected, (page.value - 1) * 50, 50);
    deadLetters.value = data.items;
    total.value = data.total;
  } catch {
    error.value = "死信数据读取失败。";
  } finally {
    loading.value = false;
  }
}

async function remove(row: unknown) {
  const deadLetter = row as DeadLetterView;
  if (!namespace.selected) return;
  try {
    await ElMessageBox.confirm(
      `删除 ${deadLetter.displayName || deadLetter.destination}（ID：${deadLetter.destination}）的死信 ${deadLetter.messageId} 后无法恢复，原始 Stream 消息不会被重放。`,
      "确认删除死信",
      { confirmButtonText: "删除", cancelButtonText: "取消", type: "warning" }
    );
  } catch {
    return;
  }
  deleting.value = keyFor(row);
  try {
    const { data } = await managementApi.deleteDeadLetter(
      namespace.selected,
      deadLetter.destination,
      deadLetter.group,
      deadLetter.messageId,
      deadLetter.mode === "BROADCAST" ? deadLetter.instanceId : ""
    );
    if (data.changed) {
      ElMessage.success("死信已删除。");
    } else {
      ElMessage.info("死信不存在或已被删除。");
    }
    await load();
  } catch {
    ElMessage.error("死信删除失败。");
  } finally {
    deleting.value = "";
  }
}

async function retry(row: unknown) {
  const deadLetter = row as DeadLetterView;
  if (!namespace.selected) return;
  try {
    await ElMessageBox.confirm(
      `重新投递 ${deadLetter.displayName || deadLetter.destination} 的消息 ${deadLetter.messageId}？成功后原死信会删除，RDT 会按原模式生成新消息，不会复用原消息 ID。`,
      "确认重新投递",
      {
        confirmButtonText: "重新投递",
        cancelButtonText: "取消",
        type: "warning"
      }
    );
  } catch {
    return;
  }
  retrying.value = keyFor(row);
  try {
    const { data } = await managementApi.retryDeadLetter(
      namespace.selected,
      deadLetter.destination,
      deadLetter.group,
      deadLetter.messageId,
      deadLetter.mode === "BROADCAST" ? deadLetter.instanceId : ""
    );
    if (data.changed) {
      ElMessage.success("死信已重新投递。");
    } else {
      ElMessage.info("死信不存在或已被其他操作处理。");
    }
    await load();
  } catch {
    ElMessage.error("死信重新投递失败。");
  } finally {
    retrying.value = "";
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
