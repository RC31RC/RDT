<template>
  <button class="koi-header-action koi-rotate-i" type="button" aria-label="刷新当前页面" @click="refresh?.()">
    <el-tooltip content="刷新当前页面">
      <KoiGlobalIcon name="koi-refresh-arrow" />
    </el-tooltip>
  </button>
</template>
<script setup lang="ts">
import { inject } from "vue";
const refresh = inject<() => void>("refresh");
</script>
