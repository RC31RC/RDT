<template>
  <div class="koi-layout-root" data-ui="koi-ui">
    <component :is="isMobile ? LayoutMobile : layouts[globalStore.layout] || LayoutVertical" />
    <ThemeConfig />
  </div>
</template>
<script setup lang="ts">
import { computed, watch, ref, provide } from "vue";
import { useMediaQuery } from "@vueuse/core";
import useGlobalStore from "@/stores/modules/global";
import { useTheme } from "@/utils/theme";
import { useConnectionsStore } from "@/stores/connections";
import { useNamespaceStore } from "@/stores/namespace";
import LayoutVertical from "./LayoutVertical/index.vue";
import LayoutClassic from "./LayoutClassic/index.vue";
import LayoutColumns from "./LayoutColumns/index.vue";
import LayoutHorizontal from "./LayoutHorizontal/index.vue";
import LayoutOptimum from "./LayoutOptimum/index.vue";
import LayoutMobile from "./LayoutMobile/index.vue";
import ThemeConfig from "./components/ThemeConfig/index.vue";
import type { Component } from "vue";
const globalStore = useGlobalStore();
useTheme();
const isMobile = useMediaQuery("(max-width: 767px)");
const layouts: Record<string, Component> = {
  vertical: LayoutVertical,
  classic: LayoutClassic,
  columns: LayoutColumns,
  horizontal: LayoutHorizontal,
  optimum: LayoutOptimum
};
const connections = useConnectionsStore();
const namespace = useNamespaceStore();
const namespaceError = ref("");
const pageRevision = ref(0);
provide("pageRevision", pageRevision);
provide("refresh", () => {
  pageRevision.value++;
});
provide("namespaceError", namespaceError);
const revision = computed(() => connections.settings?.revision ?? connections.status?.revision);
watch(
  revision,
  async (value) => {
    namespaceError.value = "";
    namespace.reset();
    if (!(connections.settings?.runtime || connections.status?.runtimeConfigured)) return;
    try {
      await namespace.load();
    } catch {
      if (revision.value === value) namespaceError.value = "Namespace 加载失败，请检查管理 API 后刷新页面。";
    }
  },
  { immediate: true }
);
</script>
<style scoped>
.koi-layout-root {
  width: 100%;
  height: 100%;
  min-width: 0;
}
</style>
