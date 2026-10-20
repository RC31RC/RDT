<template>
  <div class="page-stack">
    <KoiCard>
      <div class="koi-page-heading">
        <div>
          <h2>个人中心</h2>
          <p>管理本地账号信息与登录凭据。</p>
        </div>
        <el-avatar :size="56">{{ auth.principal?.displayName.slice(0, 1) }}</el-avatar>
      </div>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="账号">{{ auth.username }}</el-descriptions-item>
        <el-descriptions-item label="显示名称">{{ auth.principal?.displayName }}</el-descriptions-item>
        <el-descriptions-item label="角色">{{ auth.principal?.role }}</el-descriptions-item>
        <el-descriptions-item label="权限">
          <el-tag v-for="permission in auth.principal?.permissions" :key="permission" class="m-r-8px">
            {{ permission }}
          </el-tag>
        </el-descriptions-item>
      </el-descriptions>
    </KoiCard>
    <KoiCard>
      <h3>修改密码</h3>
      <p class="muted">成功后立即撤销此账号的全部会话，需要重新登录。</p>
      <el-form ref="formRef" :model="form" :rules="rules" label-position="top" class="password-form">
        <el-form-item label="当前密码" prop="currentPassword">
          <el-input v-model="form.currentPassword" type="password" show-password autocomplete="current-password" />
        </el-form-item>
        <el-form-item label="新密码" prop="newPassword">
          <el-input v-model="form.newPassword" type="password" show-password autocomplete="new-password" />
        </el-form-item>
        <el-form-item label="确认新密码" prop="confirm">
          <el-input v-model="form.confirm" type="password" show-password autocomplete="new-password" />
        </el-form-item>
        <el-alert v-if="error" :title="error" type="error" :closable="false" />
        <el-button type="primary" :loading="saving" @click="change">更新密码并重新登录</el-button>
      </el-form>
    </KoiCard>
  </div>
</template>
<script setup lang="ts">
import { reactive, ref } from "vue";
import { type FormInstance, type FormRules } from "element-plus";
import KoiCard from "@/components/KoiCard/index.vue";
import { useAuthStore } from "@/stores/auth";
import { securityApi, apiError } from "@/api/security";
const auth = useAuthStore();
const formRef = ref<FormInstance>();
const saving = ref(false);
const error = ref("");
const form = reactive({ currentPassword: "", newPassword: "", confirm: "" });
const rules: FormRules = {
  currentPassword: [{ required: true, message: "请输入当前密码" }],
  newPassword: [{ required: true, min: 12, max: 128, message: "密码需为 12–128 位" }],
  confirm: [
    {
      validator: (_rule, value, done) => (value === form.newPassword ? done() : done(new Error("两次密码不一致")))
    }
  ]
};
async function change() {
  if (!(await formRef.value?.validate())) return;
  saving.value = true;
  try {
    await securityApi.password(form);
    auth.clear();
    window.location.assign("/login");
  } catch (failure) {
    error.value = apiError(failure);
  } finally {
    saving.value = false;
  }
}
</script>
<style scoped>
.password-form {
  max-width: 480px;
}
.password-form .el-button {
  margin-top: 16px;
}
</style>
