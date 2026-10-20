<template>
  <div class="page-stack">
    <section class="page-intro">
      <div>
        <div class="eyebrow">MESSAGE STREAMS</div>
        <h2>消息消费状态</h2>
        <p>
          按消息主题（Topic）查看消费组状态。保留消息数表示 Redis Stream 中仍存在的消息；待确认表示已投递但尚未确认，未投递表示尚未交给该消费组的消息。
        </p>
      </div>
      <KoiToolbar :searchable="false" :loading="loading" @refresh-table="load" />
    </section>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <KoiCard class="table-panel">
      <RdtTable
        v-loading="loading"
        :data="streams"
        row-key="redisKey"
        empty-text="暂无消息主题"
        @expand-change="handleExpand"
      >
        <el-table-column type="expand">
          <template #default="{ row }">
            <div class="expand-content">
              <div class="detail-caption">消费组与消费者</div>
              <RdtTable :data="row.groups" size="small" empty-text="暂无消费组">
                <el-table-column prop="name" label="消费组名称" min-width="150" />
                <el-table-column prop="consumers" label="消费者数" width="110" />
                <el-table-column prop="pending" label="待确认" width="110" />
                <el-table-column label="未投递" width="110">
                  <template #default="{ row: group }">{{ group.lag < 0 ? "未提供" : group.lag }}</template>
                </el-table-column>
                <el-table-column label="消费者空闲时间" min-width="180">
                  <template #default="{ row: group }">
                    <span v-if="group.consumerViews.length">
                      {{ formatIdle(group.consumerViews[0].idleTimeMillis) }}
                      <span v-if="group.consumerViews.length > 1" class="muted">
                        +{{ group.consumerViews.length - 1 }} 个
                      </span>
                    </span>
                    <span v-else class="muted">无消费者</span>
                  </template>
                </el-table-column>
              </RdtTable>
              <div class="detail-caption stream-message-caption">最近消息</div>
              <RdtTable
                v-loading="loadingMessages === row.redisKey"
                :data="messages[row.redisKey] || []"
                size="small"
                empty-text="暂无消息"
              >
                <el-table-column prop="id" label="消息 ID" min-width="180">
                  <template #default="{ row: message }">
                    <code>{{ message.id }}</code>
                  </template>
                </el-table-column>
                <el-table-column label="消息内容" min-width="320" show-overflow-tooltip>
                  <template #default="{ row: message }">
                    <code>{{ stringify(message.fields) }}</code>
                  </template>
                </el-table-column>
                <el-table-column label="操作" width="85" fixed="right">
                  <template #default="{ row: message }">
                    <el-button
                      v-permission="'runtime:delete'"
                      link
                      type="danger"
                      :loading="deleting === message.id"
                      @click="removeMessage(row, message as unknown as StreamMessageView)"
                    >
                      删除
                    </el-button>
                  </template>
                </el-table-column>
              </RdtTable>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="消息主题（Topic）" min-width="260" show-overflow-tooltip>
          <template #default="{ row }">
            <strong>{{ row.displayName || row.destination }}</strong>
            <div class="instance-secondary"><code>{{ row.destination }}</code></div>
          </template>
        </el-table-column>
        <el-table-column prop="length" label="保留消息数" width="140" sortable />
        <el-table-column label="消费组数" width="120">
          <template #default="{ row }">{{ row.groups.length }}</template>
        </el-table-column>
        <el-table-column label="待确认数" width="130">
          <template #default="{ row }">{{ pendingFor(row) }}</template>
        </el-table-column>
        <el-table-column prop="lastGeneratedId" label="最近消息 ID" min-width="210">
          <template #default="{ row }">
            <code>{{ row.lastGeneratedId || "-" }}</code>
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
import { managementApi, type StreamMessageView, type StreamView } from "@/api/management";
import { useNamespaceStore } from "@/stores/namespace";

const streams = ref<StreamView[]>([]);
const page = ref(1);
const total = ref(0);
const loading = ref(false);
const error = ref("");
const messages = ref<Record<string, StreamMessageView[]>>({});
const loadingMessages = ref("");
const deleting = ref("");
const namespace = useNamespaceStore();

function pendingFor(stream: unknown) {
  const groups = (stream as Partial<StreamView>).groups ?? [];
  return groups.reduce((total, group) => total + group.pending, 0);
}

function formatIdle(milliseconds: number) {
  if (milliseconds < 1000) return `${milliseconds} ms`;
  if (milliseconds < 60000) return `${Math.floor(milliseconds / 1000)} s`;
  return `${Math.floor(milliseconds / 60000)} min`;
}

function stringify(fields: Record<string, string>) {
  return JSON.stringify(fields);
}

async function load() {
  if (!namespace.selected) {
    streams.value = [];
    messages.value = {};
    return;
  }
  loading.value = true;
  error.value = "";
  try {
    const { data } = await managementApi.streams(namespace.selected, (page.value - 1) * 50, 50);
    streams.value = data.items;
    total.value = data.total;
    messages.value = {};
  } catch {
    error.value = "Stream 数据读取失败。";
    ElMessage.error(error.value);
  } finally {
    loading.value = false;
  }
}

async function handleExpand(row: unknown, expanded: boolean | unknown[]) {
  const stream = row as StreamView;
  const isExpanded = Array.isArray(expanded)
    ? expanded.some((item) => (item as StreamView).redisKey === stream.redisKey)
    : expanded;
  if (!namespace.selected || !isExpanded) return;
  loadingMessages.value = stream.redisKey;
  try {
    const [details, recent] = await Promise.all([
      managementApi.stream(namespace.selected, stream.destination),
      managementApi.streamMessages(namespace.selected, stream.destination)
    ]);
    stream.groups = details.data.groups;
    messages.value[stream.redisKey] = recent.data;
  } catch {
    ElMessage.error("Stream 消息读取失败。");
  } finally {
    loadingMessages.value = "";
  }
}

async function removeMessage(stream: unknown, message: StreamMessageView) {
  const streamView = stream as StreamView;
  if (!namespace.selected) return;
  try {
    await ElMessageBox.confirm(
      `删除 ${streamView.displayName || streamView.destination}（ID：${streamView.destination}）中的消息 ${message.id} 后无法恢复；已开始的业务消费回调不会被中断。`,
      "确认删除消息",
      { confirmButtonText: "删除", cancelButtonText: "取消", type: "warning" }
    );
  } catch {
    return;
  }
  deleting.value = message.id;
  try {
    const { data } = await managementApi.deleteStreamMessage(namespace.selected, streamView.destination, message.id);
    if (data.changed) {
      ElMessage.success("消息已删除。");
      messages.value[streamView.redisKey] = (messages.value[streamView.redisKey] || []).filter(
        (item) => item.id !== message.id
      );
    } else {
      ElMessage.info("消息不存在或已被删除。");
    }
  } catch {
    ElMessage.error("消息删除失败。");
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
