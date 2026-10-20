<template>
  <el-breadcrumb :separator-icon="ArrowRight" class="koi-breadcrumb">
    <el-breadcrumb-item to="/overview">RDT</el-breadcrumb-item>
    <transition name="fade-slide" mode="out-in">
      <el-breadcrumb-item :key="route.path">{{ title }}</el-breadcrumb-item>
    </transition>
  </el-breadcrumb>
</template>
<script setup lang="ts">
import { computed } from "vue";
import { useRoute } from "vue-router";
import { ArrowRight } from "@element-plus/icons-vue";
import { consoleMenuItems } from "@/router/navigation";
const route = useRoute();
const title = computed(() => consoleMenuItems.find((item) => item.path === route.path)?.meta.title || "个人中心");
</script>
<style scoped>
.koi-breadcrumb {
  margin-left: 12px;
}
</style>
