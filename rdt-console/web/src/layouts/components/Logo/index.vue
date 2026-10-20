<template>
  <router-link class="koi-logo" to="/overview" :class="{ 'koi-logo--collapsed': isCollapse }">
    <img src="/rdt-mark.svg" alt="RDT" width="34" height="34" />
    <span v-if="!isCollapse" class="koi-logo__title animate__animated animate__fadeInLeft">RDT Console</span>
  </router-link>
</template>

<script setup lang="ts">
withDefaults(defineProps<{ isCollapse?: boolean; layout?: string }>(), {
  isCollapse: false
});
</script>

<style scoped lang="scss">
.koi-logo {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  height: 56px;
  gap: 10px;
  padding: 10px;
  white-space: nowrap;
  img {
    flex-shrink: 0;
    position: relative;
    z-index: 1;
  }
}
.koi-logo__title {
  font-size: 19px;
  font-weight: 650;
  letter-spacing: -0.4px;
  color: var(--el-aside-logo-text-color);
  --animate-duration: 0.35s;
}
.koi-logo--collapsed {
  padding: 10px 0;
}
</style>
