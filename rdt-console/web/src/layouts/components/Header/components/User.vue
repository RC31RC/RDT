<template>
  <el-popover
    placement="bottom-end"
    :width="240"
    trigger="click"
    :show-arrow="false"
    :offset="8"
    popper-class="koi-user-popover"
  >
    <template #reference>
      <button class="koi-header-action" aria-label="用户菜单">
        <el-avatar :size="30" class="user-avatar">{{ auth.principal?.displayName.slice(0, 1) }}</el-avatar>
      </button>
    </template>
    <div class="user-card-content">
      <div class="user-card-header">
        <el-avatar :size="40">{{ auth.principal?.displayName.slice(0, 1) }}</el-avatar>
        <div>
          <div class="user-name">{{ auth.principal?.displayName }}</div>
          <div class="user-phone">{{ auth.principal?.username }} · {{ auth.principal?.role }}</div>
        </div>
      </div>
      <button class="user-menu-item" @click="router.push('/profile')">
        <el-icon><User /></el-icon>
        个人中心
      </button>
      <button v-if="auth.can('users:manage')" class="user-menu-item" @click="router.push('/users')">
        <el-icon><UserFilled /></el-icon>
        用户与权限
      </button>
      <button v-if="auth.can('connections:manage')" class="user-menu-item" @click="router.push('/settings')">
        <el-icon><Connection /></el-icon>
        连接设置
      </button>
      <div class="user-card-footer">
        <el-button :icon="SwitchButton" plain @click="auth.logout()">退出登录</el-button>
      </div>
    </div>
  </el-popover>
</template>
<script setup lang="ts">
import { useRouter } from "vue-router";
import { User, UserFilled, Connection, SwitchButton } from "@element-plus/icons-vue";
import { useAuthStore } from "@/stores/auth";
const auth = useAuthStore();
const router = useRouter();
</script>
<style scoped lang="scss">
.user-card-header {
  display: flex;
  gap: 12px;
  align-items: center;
  padding: 10px 4px 16px;
  border-bottom: 1px solid var(--el-border-color-lighter);
}
.user-name {
  font-weight: 650;
}
.user-phone {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-top: 5px;
}
.user-menu-item {
  display: flex;
  align-items: center;
  gap: 10px;
  width: 100%;
  padding: 12px 8px;
  background: transparent;
  border: 0;
  border-radius: 6px;
  color: var(--el-text-color-primary);
  cursor: pointer;
}
.user-menu-item:hover {
  background: var(--el-fill-color-light);
}
.user-card-footer {
  padding: 12px 0 2px;
  border-top: 1px solid var(--el-border-color-lighter);
}
.user-card-footer .el-button {
  width: 100%;
}
</style>
