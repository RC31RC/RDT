<template>
  <nav
    class="layout-tabs-bar layout-tabs"
    :class="`layout-tabs--${layout.tabsStyle === 'card' ? 'glass-cards' : layout.tabsStyle}`"
    aria-label="已打开页面"
  >
    <KoiScrollNav
      class="layout-tabs-bar__scroll"
      height="42px"
      :gap="6"
      :active-selector="`[data-tab-path='${route.path}']`"
    >
      <div
        v-for="path in tabs"
        :key="path"
        class="layout-tabs-bar__item"
        :class="{ 'is-active': route.path === path }"
        :data-tab-path="path"
        role="tab"
        :aria-selected="route.path === path"
        tabindex="0"
        @keydown.enter="router.push(path)"
        @click="router.push(path)"
        @contextmenu.prevent="openMenu($event, path)"
      >
        <div class="layout-tabs-bar__label tab-label-inner">
          <el-icon class="m-r-6px"><component :is="item(path)?.meta.icon" /></el-icon>
          <span class="layout-tabs-bar__title">{{ item(path)?.meta.title }}</span>
        </div>
        <button
          v-if="path !== '/overview'"
          class="layout-tabs-bar__close"
          type="button"
          :aria-label="`关闭 ${item(path)?.meta.title}`"
          @click.stop="close(path)"
        >
          <el-icon :size="14"><Close /></el-icon>
        </button>
      </div>
    </KoiScrollNav>
    <Teleport to="body">
      <div
        v-if="context"
        class="tabs-context"
        role="menu"
        :style="{ left: context.x + 'px', top: context.y + 'px' }"
        @click.stop
      >
        <button role="menuitem" @click="refreshPage">刷新当前页</button>
        <button
          role="menuitem"
          :disabled="context.path === '/overview'"
          @click="
            close(context.path);
            context = undefined;
          "
        >
          关闭当前页
        </button>
        <button role="menuitem" @click="keepOnly(context.path)">关闭其他页</button>
        <button role="menuitem" @click="keepOnly('/overview')">关闭全部页</button>
      </div>
    </Teleport>
  </nav>
</template>
<script setup lang="ts">
import { ref, watch, inject } from "vue";
import { useEventListener } from "@vueuse/core";
import { useRoute, useRouter } from "vue-router";
import { Close } from "@element-plus/icons-vue";
import KoiScrollNav from "@/components/KoiScrollNav/Index.vue";
import { consoleMenuItems } from "@/router/navigation";
import { useLayoutStore } from "@/stores/layout";
const layout = useLayoutStore();
const route = useRoute();
const router = useRouter();
const refresh = inject<() => Promise<void>>("refresh");
const context = ref<{ x: number; y: number; path: string }>();
function openMenu(event: MouseEvent, path: string) {
  context.value = {
    x: Math.min(event.clientX, window.innerWidth - 160),
    y: Math.min(event.clientY, window.innerHeight - 170),
    path
  };
}
useEventListener(document, "click", () => {
  context.value = undefined;
});
useEventListener(document, "keydown", (event) => {
  if (event.key === "Escape") context.value = undefined;
});
async function refreshPage() {
  const path = context.value?.path;
  context.value = undefined;
  if (path && route.path !== path) await router.push(path);
  await refresh?.();
}
function keepOnly(path: string) {
  tabs.value = path === "/overview" ? ["/overview"] : ["/overview", path];
  context.value = undefined;
  if (!tabs.value.includes(route.path)) void router.push(path);
}
const tabs = ref(["/overview"]);
const item = (path: string) => consoleMenuItems.find((menu) => menu.path === path);
watch(
  () => route.path,
  (path) => {
    if (item(path) && !tabs.value.includes(path)) tabs.value.push(path);
  },
  { immediate: true }
);
function close(path: string) {
  tabs.value = tabs.value.filter((tab) => tab !== path);
  if (route.path === path) router.push(tabs.value.at(-1) || "/overview");
}
</script>
<style lang="scss" scoped>
.tabs-context {
  position: fixed;
  z-index: 3000;
  width: 160px;
  padding: 6px;
  border: 1px solid var(--el-border-color-light);
  border-radius: 8px;
  background: var(--el-bg-color-overlay);
  box-shadow: var(--el-box-shadow-light);
  button {
    width: 100%;
    border: 0;
    background: transparent;
    color: var(--el-text-color-primary);
    text-align: left;
    padding: 9px;
    border-radius: 4px;
    cursor: pointer;
  }
  button:hover {
    background: var(--el-color-primary-light-9);
    color: var(--el-color-primary);
  }
  button:disabled {
    color: var(--el-text-color-disabled);
    cursor: default;
  }
}
.layout-tabs-bar {
  width: 100%;
  min-width: 0;
  flex-shrink: 0;
  border-top: 1px solid var(--el-border-color-lighter);
  background-color: var(--el-bg-color);
  box-sizing: border-box;
}

.layout-tabs-bar__scroll {
  width: 100%;
  min-width: 0;
  flex: 1;
  padding: 0 6px;
  box-sizing: border-box;
}

.layout-tabs-bar__item {
  position: relative;
  display: inline-flex;
  flex-shrink: 0;
  align-items: center;
  max-width: 220px;
  height: 32px;
  margin-top: 0;
  padding: 0 10px 0 12px;
  font-family: var(--el-font-family);
  font-size: var(--el-font-size-base);
  font-weight: 500;
  line-height: 32px;
  color: var(--el-text-color-primary);
  cursor: pointer;
  user-select: none;
  outline: none;
  box-sizing: border-box;
  background: transparent;
  border: none;
  transition:
    color 0.15s ease,
    border-color 0.15s ease,
    background-color 0.15s ease;
}

.layout-tabs-bar__label {
  display: inline-flex;
  align-items: center;
  min-width: 0;
  flex: 1;
}

.layout-tabs-bar__label.tab-label-inner {
  gap: 2px;
}

.layout-tabs-bar__title {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.layout-tabs-bar__close {
  display: inline-flex;
  flex-shrink: 0;
  align-items: center;
  justify-content: center;
  margin-left: 8px;
  padding: 0;
  color: var(--el-text-color-secondary);
  cursor: pointer;
  background: transparent;
  border: none;
  outline: none;
}

.layout-tabs-bar__item.is-active .layout-tabs-bar__close {
  color: inherit;
}

.layout-tabs-bar__item.is-active .layout-tabs-bar__close:hover {
  background-color: color-mix(in srgb, var(--el-bg-color) 88%, var(--el-color-primary));
}

/** 卡片 */
.layout-tabs--glass-cards .layout-tabs-bar__item {
  margin: 0 1px;
  padding: 0 10px 0 12px;
  border: 1px solid var(--el-border-color);
  background: var(--el-bg-color);
  border-radius: 6px;

  &:hover:not(.is-active) {
    background-color: var(--el-fill-color-light);
    border-color: color-mix(in srgb, var(--el-color-primary) 42%, var(--el-border-color));
  }

  &.is-active {
    color: var(--el-color-primary);
    background: var(--el-color-primary-light-9);
    border-color: var(--el-color-primary);
  }

  &.is-active:hover {
    background: var(--el-color-primary-light-9);
    border-color: var(--el-color-primary);
  }
}

/** 谷歌 */
.layout-tabs--google .layout-tabs-bar__item {
  margin-left: 4px;
  margin-top: 6px;
  padding: 0 12px 0 14px;
  border-radius: 8px;

  &:first-of-type {
    margin-left: 8px;
  }

  .line {
    position: absolute;
    top: 0;
    bottom: 0;
    left: 0;
    width: 1px;
    height: 16px;
    margin: auto;
    background: var(--el-border-color);
    transition: opacity 0.3s ease;
    opacity: 1;
  }

  &:first-child .line {
    opacity: 0;
  }

  &:hover:not(.is-active) {
    background-color: var(--el-fill-color-light);

    .line {
      opacity: 0;
    }
  }

  &.is-active {
    color: var(--el-color-primary);
    background: var(--el-color-primary-light-9);

    &::before,
    &::after {
      position: absolute;
      bottom: 0;
      width: 20px;
      height: 20px;
      content: "";
      border-radius: 50%;
      box-shadow: 0 0 0 30px var(--el-color-primary-light-9);
    }

    &::before {
      left: -20px;
      clip-path: inset(50% -10px 0 50%);
    }

    &::after {
      right: -20px;
      clip-path: inset(50% 50% 0 -10px);
    }

    .line {
      opacity: 0;
    }
  }

  &:hover + .layout-tabs-bar__item .line,
  &.is-active + .layout-tabs-bar__item .line {
    opacity: 0;
  }
}

/** 简约：圆角描边卡片，选中后图标/文字/叉号均为主题色 */
.layout-tabs--plain .layout-tabs-bar__item {
  margin: 0 2px;
  padding: 0 12px;
  color: var(--el-text-color-regular);
  background: var(--el-bg-color);
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;

  &.is-active {
    color: var(--el-color-primary);
  }

  .layout-tabs-bar__close {
    color: inherit;
  }

  :deep(.el-icon) {
    color: inherit;
  }
}

.layout-tabs--google .layout-tabs-bar__item:focus,
.layout-tabs--google .layout-tabs-bar__item:focus-visible,
.layout-tabs--google .layout-tabs-bar__item:focus-within,
.layout-tabs--glass-cards .layout-tabs-bar__item:focus,
.layout-tabs--glass-cards .layout-tabs-bar__item:focus-visible,
.layout-tabs--glass-cards .layout-tabs-bar__item:focus-within,
.layout-tabs--plain .layout-tabs-bar__item:focus,
.layout-tabs--plain .layout-tabs-bar__item:focus-visible,
.layout-tabs--plain .layout-tabs-bar__item:focus-within {
  outline: none;
  box-shadow: none;
}
</style>
