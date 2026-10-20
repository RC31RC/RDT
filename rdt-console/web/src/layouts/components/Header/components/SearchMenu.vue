<template>
  <button class="koi-header-action" aria-label="搜索菜单" @click="visible = true">
    <el-tooltip content="搜索菜单 Ctrl+K">
      <KoiGlobalIcon name="koi-search" />
    </el-tooltip>
  </button>
  <el-dialog v-model="visible" title="搜索菜单" width="min(540px, 92vw)" append-to-body>
    <el-input v-model="query" placeholder="搜索页面名称" :prefix-icon="Search" clearable autofocus />
    <div class="menu-search-results">
      <button v-for="item in results" :key="item.path" class="menu-search-item" @click="navigate(item.path)">
        <KoiGlobalIcon :name="item.meta.icon" />
        <span>{{ item.meta.title }}</span>
        <el-icon><Right /></el-icon>
      </button>
      <el-empty v-if="!results.length" description="未找到可访问的菜单" />
    </div>
  </el-dialog>
</template>
<script setup lang="ts">
import { computed, ref } from "vue";
import { useRouter } from "vue-router";
import { onKeyStroke } from "@vueuse/core";
import { Search, Right } from "@element-plus/icons-vue";
import useMenuStore from "@/stores/modules/auth";
const router = useRouter();
const menus = useMenuStore();
const visible = ref(false);
const query = ref("");
const results = computed(() => menus.showMenuList.filter((item) => item.meta.title.includes(query.value)));
function navigate(path: string) {
  visible.value = false;
  router.push(path);
}
onKeyStroke("k", (event) => {
  if (event.ctrlKey || event.metaKey) {
    event.preventDefault();
    visible.value = !visible.value;
  }
});
</script>
<style scoped>
.menu-search-results {
  margin-top: 16px;
}
.menu-search-item {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 14px;
  border: 0;
  border-radius: 8px;
  color: var(--el-text-color-primary);
  background: transparent;
  cursor: pointer;
}
.menu-search-item:hover {
  background: var(--el-color-primary-light-9);
  color: var(--el-color-primary);
}
.menu-search-item span {
  flex: 1;
  text-align: left;
}
</style>
