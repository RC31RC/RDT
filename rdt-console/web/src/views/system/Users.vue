<template>
  <div class="page-stack">
    <KoiCard>
      <div class="koi-page-heading">
        <div>
          <h2>用户与权限</h2>
          <p>账号、权限和会话保存在 Console 本机，不依赖 Redis 或外部数据库。</p>
        </div>
        <el-button type="primary" :icon="Plus" @click="open()">新增用户</el-button>
      </div>
      <el-alert v-if="error" :title="error" type="error" :closable="false" />
      <RdtTable v-loading="loading" :data="users" stripe>
        <el-table-column prop="username" label="账号" min-width="140" />
        <el-table-column prop="displayName" label="显示名称" min-width="140" />
        <el-table-column label="角色" min-width="160">
          <template #default="{ row }">
            <el-tag>{{ roleNames[row.role as Role] }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.enabled ? 'success' : 'info'">{{ row.enabled ? "启用" : "禁用" }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="160">
          <template #default="{ row }">
            <el-button link type="primary" @click="open(row as LocalUser)">编辑</el-button>
            <el-button link type="danger" :disabled="row.username === auth.username" @click="remove(row as LocalUser)">
              删除
            </el-button>
          </template>
        </el-table-column>
      </RdtTable>
    </KoiCard>
    <KoiCard>
      <h3>角色权限边界</h3>
      <RdtTable :data="permissions">
        <el-table-column prop="name" label="角色" width="140" />
        <el-table-column prop="description" label="允许操作" />
      </RdtTable>
      <p class="muted">
        后端逐请求校验权限；角色变更、禁用、重置密码会立即撤销该账号的已有会话。至少保留一位启用的管理员。
      </p>
    </KoiCard>
    <el-dialog v-model="visible" :title="editing ? '编辑用户' : '新增用户'" width="min(520px, 94vw)" destroy-on-close>
      <el-form ref="formRef" :model="form" :rules="rules" label-position="top">
        <el-form-item label="账号" prop="username">
          <el-input v-model="form.username" :disabled="editing" maxlength="40" />
        </el-form-item>
        <el-form-item label="显示名称" prop="displayName">
          <el-input v-model="form.displayName" maxlength="60" />
        </el-form-item>
        <el-form-item label="角色">
          <el-select v-model="form.role">
            <el-option v-for="role in roles" :key="role" :value="role" :label="roleNames[role]" />
          </el-select>
        </el-form-item>
        <el-form-item :label="editing ? '重置密码（留空保持原密码）' : '初始密码'" prop="password">
          <el-input v-model="form.password" type="password" show-password autocomplete="new-password" />
        </el-form-item>
        <el-form-item v-if="editing" label="启用"><el-switch v-model="form.enabled" /></el-form-item>
      </el-form>
      <el-alert v-if="dialogError" :title="dialogError" type="error" :closable="false" />
      <template #footer>
        <el-button @click="visible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="save">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>
<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { Plus } from "@element-plus/icons-vue";
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from "element-plus";
import KoiCard from "@/components/KoiCard/index.vue";
import { securityApi, apiError, type LocalUser, type Role } from "@/api/security";
import { useAuthStore } from "@/stores/auth";
const auth = useAuthStore();
const users = ref<LocalUser[]>([]);
const loading = ref(false);
const saving = ref(false);
const error = ref("");
const dialogError = ref("");
const visible = ref(false);
const editing = ref(false);
const formRef = ref<FormInstance>();
const roles: Role[] = ["ADMIN", "OPERATOR", "VIEWER"];
const roleNames: Record<Role, string> = {
  ADMIN: "管理员",
  OPERATOR: "运维操作员",
  VIEWER: "只读用户"
};
const permissions = [
  {
    name: "管理员",
    description: "连接与用户管理、读取、任务触发/暂停、实例排空、删除与死信重投"
  },
  {
    name: "运维操作员",
    description: "读取、任务触发/暂停、实例排空；不能改连接/用户，不能删除或重投死信"
  },
  {
    name: "只读用户",
    description: "读取当前 runtime 的概览、实例、消息、任务、死信；不能修改 Redis"
  }
];
const form = reactive({
  username: "",
  displayName: "",
  role: "VIEWER" as Role,
  enabled: true,
  password: ""
});
const rules: FormRules = {
  username: [
    {
      required: true,
      pattern: /^[A-Za-z0-9_-]{3,40}$/,
      message: "3–40 位字母、数字、下划线或横线",
      trigger: "blur"
    }
  ],
  displayName: [{ required: true, message: "请输入显示名称", trigger: "blur" }],
  password: [
    {
      validator: (_rule, value, done) => {
        if ((!editing.value || value) && (value.length < 12 || value.length > 128))
          done(new Error("密码需为 12–128 位"));
        else done();
      },
      trigger: "blur"
    }
  ]
};
async function load() {
  loading.value = true;
  error.value = "";
  try {
    users.value = (await securityApi.users()).data;
  } catch (failure) {
    error.value = apiError(failure);
  } finally {
    loading.value = false;
  }
}
function open(user?: LocalUser) {
  editing.value = Boolean(user);
  dialogError.value = "";
  Object.assign(form, user || { username: "", displayName: "", role: "VIEWER", enabled: true });
  form.password = "";
  visible.value = true;
}
async function save() {
  if (!(await formRef.value?.validate())) return;
  saving.value = true;
  dialogError.value = "";
  try {
    if (editing.value)
      await securityApi.update(form.username, {
        ...form,
        password: form.password || null
      });
    else await securityApi.create(form);
    visible.value = false;
    ElMessage.success("用户已保存，原会话已按需撤销");
    if (form.username === auth.username) {
      await auth.logout();
      return;
    }
    await load();
  } catch (failure) {
    dialogError.value = apiError(failure);
  } finally {
    saving.value = false;
  }
}
async function remove(user: LocalUser) {
  if (
    (await ElMessageBox.confirm(`删除账号 ${user.username} 并撤销其全部会话？`, "确认删除", { type: "warning" }).catch(
      () => false
    )) === false
  )
    return;
  try {
    await securityApi.remove(user.username);
    await load();
  } catch (failure) {
    error.value = apiError(failure);
  }
}
onMounted(load);
</script>
