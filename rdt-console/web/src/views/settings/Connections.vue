<template>
  <div class="connection-settings">
    <KoiCard>
      <div class="settings-heading">
        <div>
          <h2>Redis 连接设置</h2>
          <p class="connection-hint">Console 先独立启动；登录后选择要管理的 Redis，不修改业务应用的客户端配置。</p>
        </div>
        <KoiToolbar :searchable="false" :loading="loading" @refresh-table="load" />
      </div>
      <el-alert
        title="消息、任务、锁、实例和死信只读取 runtime；缓存 Redis 是独立的可选连接，可在下方按命名空间清理可重算缓存。逻辑分库不隔离内存、淘汰策略或故障。"
        type="info"
        :closable="false"
        show-icon
      />
      <el-alert v-if="error" :title="error" type="error" :closable="false" class="status-message" />
      <el-form label-position="top" class="settings-form" @submit.prevent="save">
        <el-tabs v-model="activeRole">
          <el-tab-pane v-for="role in roles" :key="role.key" :name="role.key" :label="role.title">
            <el-checkbox v-model="form[role.key].enabled">配置 {{ role.key }} Redis</el-checkbox>
            <template v-if="form[role.key].enabled">
              <div class="settings-grid">
                <el-form-item label="连接模式">
                  <el-select v-model="form[role.key].topology" @change="form[role.key].database = 0">
                    <el-option label="单机" value="SINGLE" />
                    <el-option label="Cluster" value="CLUSTER" />
                    <el-option label="Sentinel" value="SENTINEL" />
                  </el-select>
                </el-form-item>
                <el-form-item :label="role.key + ' Database（Cluster 仅 DB0）'">
                  <el-input-number
                    v-model="form[role.key].database"
                    :min="0"
                    :max="65535"
                    :disabled="form[role.key].topology === 'CLUSTER'"
                    :aria-label="role.key + ' Database'"
                  />
                </el-form-item>
              </div>
              <el-form-item :label="role.key + ' 节点地址（每行一个，TLS 使用 rediss://）'">
                <el-input
                  v-model="form[role.key].addresses"
                  type="textarea"
                  :rows="3"
                  :aria-label="role.key + ' 节点地址'"
                  placeholder="redis://127.0.0.1:6379"
                />
              </el-form-item>
              <el-form-item v-if="form[role.key].topology === 'SENTINEL'" label="Sentinel master name">
                <el-input v-model="form[role.key].masterName" />
              </el-form-item>
              <div class="settings-grid">
                <el-form-item label="Redis ACL 用户名（可空）">
                  <el-input v-model="form[role.key].username" autocomplete="off" />
                </el-form-item>
                <el-form-item label="Redis 密码（留空保留；不会回显）">
                  <el-input
                    v-model="form[role.key].password"
                    type="password"
                    show-password
                    autocomplete="new-password"
                    :placeholder="form[role.key].passwordSet ? '已保存密码，留空保留' : '未设置密码'"
                    :aria-label="role.key + ' 密码'"
                  />
                  <el-checkbox v-model="form[role.key].clearPassword">清除已保存密码</el-checkbox>
                </el-form-item>
              </div>
              <div v-if="form[role.key].topology === 'SENTINEL'" class="settings-grid">
                <el-form-item label="Sentinel 用户名">
                  <el-input v-model="form[role.key].sentinelUsername" autocomplete="off" />
                </el-form-item>
                <el-form-item label="Sentinel 密码（留空保留）">
                  <el-input
                    v-model="form[role.key].sentinelPassword"
                    type="password"
                    show-password
                    autocomplete="new-password"
                  />
                  <el-checkbox v-model="form[role.key].clearSentinelPassword">清除 Sentinel 密码</el-checkbox>
                </el-form-item>
              </div>
              <el-button
                :loading="testing === role.key"
                :disabled="saving || testing !== ''"
                @click="testConnection(role.key)"
              >
                测试 {{ role.key }} 连接
              </el-button>
              <p class="connection-hint">
                测试不会保存；保存后按需连接。客户端只在 Console 内生效，runtime/cache 不互相回退。
              </p>
            </template>
          </el-tab-pane>
        </el-tabs>
        <el-form-item label="允许管理的 namespace（每行一个；留空自动发现 runtime 中的 RDT namespace）">
          <el-input v-model="namespaces" type="textarea" :rows="3" aria-label="允许管理的 namespace" />
        </el-form-item>
        <div class="settings-actions">
          <el-button type="primary" native-type="submit" :loading="saving" :disabled="loading || testing !== ''">
            保存并应用
          </el-button>
          <el-button :loading="loading" :disabled="saving" @click="load">重新读取</el-button>
          <el-button @click="router.push('/overview')">进入运行概览</el-button>
        </div>
      </el-form>
    </KoiCard>
    <KoiCard v-permission="'cache:clear'">
      <h3>清理命名空间缓存</h3>
      <p class="connection-hint">清理所选 Redis 中当前命名空间及其 region 的可重算缓存。请先保存并确认连接目标；共享状态、计数器和持久 Map 会保留。</p>
      <div class="settings-actions">
        <el-select v-model="clearRole" style="width: 200px" aria-label="缓存清理连接">
          <el-option label="cache Redis" value="CACHE" :disabled="!connections.settings?.cache" />
          <el-option label="runtime Redis" value="RUNTIME" :disabled="!connections.settings?.runtime" />
        </el-select>
        <span>命名空间：{{ namespace.selected || "尚未选择" }}</span>
        <el-button type="danger" plain :loading="clearing" :disabled="!namespace.selected || saving || loading" @click="clearCache">
          清理缓存
        </el-button>
      </div>
    </KoiCard>
    <KoiCard class="startup-settings">
      <h3>仍在启动配置中的项目</h3>
      <p>监听端口、管理账号与 Token 密钥、JVM/连接池预算、配置文件目录属于软件自身的安全或生命周期边界，修改后重启。</p>
      <p>
        当前告警阈值、归档目录/cron/保留期仍由 YAML
        管理，不提供伪热更新；连接切换期间的在途操作和归档会使用原连接完成，再释放旧客户端。
      </p>
      <p>
        连接资料仅保存在本机私有目录中并加密，不保存在浏览器 localStorage；备份时需同时保护 connections.enc 和
        connections.key。
      </p>
    </KoiCard>
  </div>
</template>
<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import KoiCard from "@/components/KoiCard/index.vue";
import KoiToolbar from "@/components/KoiToolbar/index.vue";
import { connectionsApi, type ProfileInput, type ProfileView, type Topology } from "@/api/connections";
import { useConnectionsStore } from "@/stores/connections";
import { useNamespaceStore } from "@/stores/namespace";

import { managementApi } from "@/api/management";

const router = useRouter();
const connections = useConnectionsStore();
const namespace = useNamespaceStore();
const clearing = ref(false);
const clearRole = ref<"RUNTIME" | "CACHE">("CACHE");
async function clearCache() {
  const selected = namespace.selected;
  const role = clearRole.value;
  const profile = role === "CACHE" ? connections.settings?.cache : connections.settings?.runtime;
  if (!selected || !profile) { ElMessage.warning("请先保存所选 Redis 连接"); return; }
  try {
    await ElMessageBox.confirm(`确认清理 ${role}（${profile.addresses.join(", ")}，DB ${profile.database}）中命名空间 ${selected} 的全部可重算缓存？`, "清理缓存", {
      type: "warning", confirmButtonText: "确认清理", cancelButtonText: "取消"
    });
  } catch { return; }
  clearing.value = true;
  try {
    const result = await managementApi.clearCache(selected, role);
    ElMessage.success(`已清理 ${result.data.deleted} 个缓存键`);
  } catch { ElMessage.error("缓存清理失败，请检查连接、权限和命名空间"); }
  finally { clearing.value = false; }
}
type Role = "runtime" | "cache";
const roles: { key: Role; title: string }[] = [
  { key: "runtime", title: "runtime：分布式组件" },
  { key: "cache", title: "cache：可选缓存" }
];
function initial(profile: ProfileView | null) {
  return {
    enabled: Boolean(profile),
    topology: profile?.topology || ("SINGLE" as Topology),
    addresses: profile?.addresses.join("\n") || "",
    database: profile?.database || 0,
    masterName: profile?.masterName || "",
    username: profile?.username || "",
    password: "",
    passwordSet: profile?.passwordSet || false,
    clearPassword: false,
    sentinelUsername: profile?.sentinelUsername || "",
    sentinelPassword: "",
    clearSentinelPassword: false
  };
}
const form = reactive({ runtime: initial(null), cache: initial(null) });
const namespaces = ref("");
const activeRole = ref<Role>("runtime");
const loading = ref(false);
const saving = ref(false);
const testing = ref<Role | "">("");
const error = ref("");

function message(failure: unknown) {
  const response = (failure as { response?: { data?: { message?: string } } }).response;
  return response?.data?.message || "操作失败，请检查连接参数和 Console 日志。";
}
async function load() {
  loading.value = true;
  error.value = "";
  try {
    await connections.load();
    const settings = connections.settings!;
    Object.assign(form.runtime, initial(settings.runtime));
    Object.assign(form.cache, initial(settings.cache));
    namespaces.value = settings.managedNamespaces.join("\n");
  } catch (failure) {
    error.value = message(failure);
  } finally {
    loading.value = false;
  }
}
function profile(role: Role): ProfileInput {
  const value = form[role];
  return {
    topology: value.topology,
    addresses: value.addresses
      .split("\n")
      .map((item) => item.trim())
      .filter(Boolean),
    database: value.database,
    masterName: value.masterName,
    username: value.username,
    password: value.clearPassword ? "" : value.password || null,
    sentinelUsername: value.sentinelUsername,
    sentinelPassword: value.clearSentinelPassword ? "" : value.sentinelPassword || null
  };
}
async function testConnection(role: Role) {
  testing.value = role;
  error.value = "";
  try {
    await connectionsApi.test(role.toUpperCase() as "RUNTIME" | "CACHE", profile(role));
    ElMessage.success(`${role} 连接可达`);
  } catch (failure) {
    error.value = message(failure);
  } finally {
    testing.value = "";
  }
}
async function save() {
  if (!connections.settings) return;
  // Applying a profile changes the destination of every subsequent management command.
  if (
    (await ElMessageBox.confirm(
      "将切换后续管理操作的 Redis 目标；在途请求完成后才释放旧连接。确认应用？",
      "确认连接设置",
      { type: "warning" }
    ).catch(() => false)) === false
  )
    return;
  saving.value = true;
  error.value = "";
  try {
    connections.settings = (
      await connectionsApi.save({
        revision: connections.settings.revision,
        runtime: form.runtime.enabled ? profile("runtime") : null,
        cache: form.cache.enabled ? profile("cache") : null,
        managedNamespaces: [
          ...new Set(
            namespaces.value
              .split("\n")
              .map((item) => item.trim())
              .filter(Boolean)
          )
        ]
      })
    ).data;
    namespace.reset();
    Object.assign(form.runtime, initial(connections.settings.runtime));
    Object.assign(form.cache, initial(connections.settings.cache));
    ElMessage.success("连接配置已保存并应用；Redis 将按需连接");
  } catch (failure) {
    error.value = message(failure);
  } finally {
    saving.value = false;
  }
}
onMounted(load);
</script>
<style scoped lang="scss">
.settings-heading {
  display: flex;
  justify-content: space-between;
  gap: 16px;
}
.connection-settings {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  min-width: 0;
  gap: 18px;
  overflow-wrap: anywhere;
}
.settings-form {
  min-width: 0;
  margin-top: 20px;
  max-width: 960px;
}
.settings-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 20px;
}
.connection-hint,
.startup-settings p {
  color: var(--el-text-color-secondary);
  line-height: 1.8;
}
.settings-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.status-message {
  margin-top: 16px;
}
@media (max-width: 768px) {
  .settings-grid {
    grid-template-columns: 1fr;
    gap: 0;
  }
}
</style>
