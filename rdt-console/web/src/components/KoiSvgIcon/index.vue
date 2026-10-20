<template>
  <svg class="koi-svg-icon" :style="{ width: length(width), height: length(height), color }" aria-hidden="true">
    <use :href="`#icon-${name}`" />
  </svg>
</template>
<script setup lang="ts">
withDefaults(defineProps<{ name?: string; width?: string | number; height?: string | number; color?: string }>(), {
  width: 18,
  height: 18,
  color: "inherit"
});
const length = (value: string | number) => /^\d+(\.\d+)?$/.test(String(value)) ? `${value}px` : String(value);
</script>
<style scoped>
.koi-svg-icon { display: inline-block; flex-shrink: 0; vertical-align: middle; fill: currentColor; }
</style>
