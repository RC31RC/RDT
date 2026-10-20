<template>
  <el-config-provider
    :z-index="3000"
    :size="layout.dimension as ComponentSize"
    :locale="layout.language === 'en' ? en : zhCn"
  >
    <router-view />
  </el-config-provider>
</template>
<script setup lang="ts">
import zhCn from "element-plus/es/locale/lang/zh-cn";
import en from "element-plus/es/locale/lang/en";
import type { ComponentSize } from "element-plus";
import { useLayoutStore } from "@/stores/layout";
import { useTheme } from "@/utils/theme";
const layout = useLayoutStore();
// Register the shared theme watchers in the root lifetime, across login/logout and layout switches.
useTheme();
</script>
