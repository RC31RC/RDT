<template>
  <template v-for="item in menuList" :key="item.path">
    <el-sub-menu v-if="item.children?.length" :index="item.path">
      <template #title>
        <KoiGlobalIcon :name="item.meta.icon" class="menu-icon" />
        <span>{{ item.meta.title }}</span>
      </template>
      <AsideSubMenu :menu-list="item.children" />
    </el-sub-menu>
    <el-menu-item v-else :index="item.path" @click="router.push(item.path)">
      <KoiGlobalIcon :name="item.meta.icon" class="menu-icon" />
      <template #title>
        <span>{{ item.meta.title }}</span>
      </template>
    </el-menu-item>
  </template>
</template>
<script setup lang="ts">
import AsideSubMenu from "./AsideSubMenu.vue";
import { useRouter } from "vue-router";
import KoiGlobalIcon from "@/components/KoiGlobalIcon/index.vue";
import type { ConsoleMenuItem } from "@/router/navigation";
defineProps<{ menuList: ConsoleMenuItem[] }>();
const router = useRouter();
</script>

<style scoped lang="scss">
.el-menu-item:hover .menu-icon { animation: koi-menu-bounce 0.6s ease-in-out; }
@keyframes koi-menu-bounce { 50% { transform: scale(1.2) rotate(-8deg); } }
</style>
