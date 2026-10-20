<template>
  <el-icon :size="size">
    <KoiSvgIcon v-if="typeof name === 'string' && name.startsWith('koi-')" :name="name" :width="size" :height="size" />
    <component v-else :is="name" />
  </el-icon>
</template>
<script setup lang="ts">
import type { Component } from "vue";
import KoiSvgIcon from "@/components/KoiSvgIcon/index.vue";
withDefaults(defineProps<{ name?: string | Component; size?: number | string }>(), { name: "Sunny", size: 18 });
</script>
