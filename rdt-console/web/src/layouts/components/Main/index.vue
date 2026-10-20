<template>
  <div class="main-wrapper">
    <Tabs v-if="showTabs" />
    <el-button v-if="globalStore.maximize" class="restore-page" type="primary" @click="globalStore.maximize = false">
      退出页面全屏
    </el-button>
    <div class="runtime-context-bar">
      <el-select
        v-if="namespace.namespaces.length"
        v-model="namespace.selected"
        aria-label="Namespace"
        placeholder="选择命名空间"
        @change="namespace.select"
      >
        <el-option v-for="name in namespace.namespaces" :key="name" :label="name" :value="name" />
      </el-select>
      <span v-else>未选择运行环境</span>
      <el-button v-if="auth.can('connections:manage')" class="manage-connections" type="primary" @click="$router.push('/settings')">
        <KoiGlobalIcon name="koi-link-circle" :size="18" />
        <span>管理连接</span>
      </el-button>
    </div>
    <el-alert v-if="namespaceError" :title="namespaceError" type="error" :closable="false" />
    <el-main class="main-content">
      <router-view v-slot="{ Component, route }">
        <transition :name="globalStore.transition" mode="out-in">
          <component :is="Component" :key="`${route.path}:${pageRevision}`" />
        </transition>
      </router-view>
    </el-main>
  </div>
</template>
<script setup lang="ts">
import { inject, ref, watch, type Ref } from "vue";
import { useMediaQuery } from "@vueuse/core";
import Tabs from "@/layouts/components/Tabs/index.vue";
import useGlobalStore from "@/stores/modules/global";
import { useNamespaceStore } from "@/stores/namespace";
import { useAuthStore } from "@/stores/auth";
const globalStore = useGlobalStore();
const namespace = useNamespaceStore();
const auth = useAuthStore();
const pageRevision = inject<Ref<number>>("pageRevision", ref(0));
const showTabs = useMediaQuery("(min-width: 520px)");
const namespaceError = inject<Ref<string>>("namespaceError", ref(""));

watch(
  () => globalStore.maximize,
  (value) => document.getElementById("app")?.classList.toggle("main-maximize", value),
  { immediate: true }
);
</script>
<style scoped lang="scss">
@use "../../../styles/koi/transition.scss";
.main-wrapper {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-width: 0;
  min-height: 0;
}
.main-content {
  flex: 1;
  min-width: 0;
  min-height: 0;
  padding: 16px;
  overflow: auto;
  background: var(--el-bg-color-page);
}
.runtime-context-bar {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  flex-wrap: wrap;
  gap: 14px;
  padding: 10px 18px;
  border-top: 1px solid var(--el-border-color-lighter);
  background: var(--el-bg-color);
  color: var(--el-text-color-secondary);
  font-size: 12px;
}
.runtime-context-bar .el-select {
  width: 220px;
}
.manage-connections { min-height: 36px; padding: 0 18px; border-radius: 8px; font-weight: 600; }
.manage-connections .el-icon { margin-right: 8px; }
@media (max-width: 520px) { .runtime-context-bar { gap: 8px; } .runtime-context-bar .el-select { flex: 1; min-width: 130px; width: auto; } }
</style>
