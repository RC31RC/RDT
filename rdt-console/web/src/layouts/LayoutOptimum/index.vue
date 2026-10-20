<template>
  <!-- 混合布局 -->
  <el-container class="layout-container layout-optimum">
    <el-aside
      class="layout-optimum-aside transition-all"
      :style="{
        width: !globalStore.isCollapse ? globalStore.menuWidth + 'px' : settings.columnMenuCollapseWidth
      }"
      v-if="currentSubMenuTree.length > 0"
    >
      <Logo :isCollapse="globalStore.isCollapse" :layout="globalStore.layout"></Logo>
      <div class="layout-optimum-menu-scroller">
        <div class="layout-optimum-menu-pad">
          <el-menu
            :default-active="activeMenu"
            :collapse="globalStore.isCollapse"
            :collapse-transition="false"
            :uniqueOpened="globalStore.uniqueOpened"
            :router="false"
            :class="menuAnimate"
          >
            <ColumnSubMenu :menuList="currentSubMenuTree"></ColumnSubMenu>
          </el-menu>
        </div>
      </div>
    </el-aside>
    <el-container>
      <el-header class="layout-header">
        <div class="koi-header">
          <div class="header-left" :style="{ paddingRight: toolbarWidth + 10 + 'px' }">
            <!-- 左侧菜单展开和折叠图标 -->
            <Collapse></Collapse>
            <div class="layout-row m-l-6px">
              <KoiScrollNav
                ref="scrollNavRef"
                class="layout-optimum-scroll-nav"
                height="52px"
                :gap="4"
                prev-aria-label="向左滚动菜单"
                next-aria-label="向右滚动菜单"
                :active-selector="activeTopMenuSelector"
              >
                <div
                  v-for="item in topLevelMenus"
                  :key="item.meta.menuId"
                  class="left-row line-clamp-1"
                  :class="{
                    'is-active': activeTopMenuId == item.meta?.menuId
                  }"
                  :data-nav-id="item.meta?.menuId"
                  @click="handleTopMenuClick()"
                >
                  <el-tooltip :content="getMenuLanguage(item.meta?.title)" :show-after="1500" placement="right">
                    <div class="left-row-tooltip-trigger">
                      <KoiGlobalIcon v-if="item.meta.icon" :name="item.meta.icon" size="18"></KoiGlobalIcon>
                      <span class="title" v-text="getMenuLanguage(item.meta?.title)"></span>
                    </div>
                  </el-tooltip>
                </div>
              </KoiScrollNav>
            </div>
          </div>
          <!-- 工具栏 -->
          <Toolbar @widthChange="handleToolbarWidthChange"></Toolbar>
        </div>
      </el-header>
      <!-- 路由页面 -->
      <Main></Main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import settings from "@/settings";
import Logo from "@/layouts/components/Logo/index.vue";
import ColumnSubMenu from "@/layouts/components/Menu/ColumnSubMenu.vue";
import Main from "@/layouts/components/Main/index.vue";
import { ref, computed } from "vue";
import { useRoute } from "vue-router";
import useAuthStore from "@/stores/modules/auth";
import useGlobalStore from "@/stores/modules/global";
import { getMenuLanguage } from "@/utils";
import Toolbar from "@/layouts/components/Header/components/Toolbar.vue";
import Collapse from "@/layouts/components/Header/components/Collapse.vue";
import KoiScrollNav from "@/components/KoiScrollNav/Index.vue";
const route = useRoute();
const authStore = useAuthStore();
const globalStore = useGlobalStore();
const menuAnimate = ref(settings.menuAnimate);
const activeMenu = computed(() => route.path);
const topLevelMenus = computed(() => [
  {
    path: "/overview",
    meta: { title: "RDT 管理", menuId: "runtime", icon: "koi-monitor" },
    children: authStore.showMenuList
  }
]);
const activeTopMenuId = ref("runtime");
const currentSubMenuTree = computed(() => authStore.showMenuList);
const toolbarWidth = ref(0);
const scrollNavRef = ref();
const activeTopMenuSelector = computed(() => '[data-nav-id="runtime"]');
function handleTopMenuClick() {
  globalStore.isCollapse = false;
}
function handleToolbarWidthChange(width: number) {
  toolbarWidth.value = width;
}
</script>

<style lang="scss" scoped>
.koi-header {
  position: relative; /* 为绝对定位的子元素提供参考 */
  display: flex;
  justify-content: space-between;
  height: $aside-header-height;

  .header-left {
    display: flex;
    align-items: center;
    flex: 1;
    min-width: 0;
    overflow: hidden; /* 防止内容溢出 */
    white-space: nowrap;
    z-index: 1; /* 确保在 Toolbar 下方 */
    transition: padding-right 0.3s ease;
  }

  /* 让 Toolbar 覆盖在 header-left 上方 */
  :deep(.header-right) {
    position: absolute;
    top: 50%;
    right: 0px;
    z-index: 10; /* 确保在 header-left 上方 */
    height: 40px;
    transform: translateY(-50%);
    padding: 2px 6px;
    background-color: var(--el-header-bg-color);
    border: 1px solid var(--el-header-toolbar-border-color); /* 添加边框 */
    border-radius: 20px; /* 圆角卡片效果 */
    box-shadow: 0 4px 12px rgb(0 0 0 / 15%); /* 漂浮阴影效果 */
    transition: all 0.3s ease;
  }
}

.layout-row {
  display: flex;
  height: 100%;
  flex: 1;
  min-width: 0;
  user-select: none;
  background-color: var(--el-header-bg-color);
  padding: 0 4px;
  box-sizing: border-box;
}

.layout-optimum-scroll-nav {
  width: 100%;
  min-width: 0;

  :deep(.koi-scroll-nav__track) {
    min-height: 48px;
    height: 100%;
    padding: 2px 0;
  }

  :deep(.koi-scroll-nav__btn) {
    width: 26px;
    height: 26px;
    color: var(--el-text-color-secondary);
    background: var(--el-header-optimum-nav-btn-bg-color);
    border: 1px solid var(--el-header-optimum-nav-btn-border-color);
    border-radius: 8px;
    box-shadow: none;
    transition:
      color 0.18s ease,
      border-color 0.18s ease,
      background-color 0.18s ease;

    &:not(.is-disabled):hover {
      color: var(--el-color-primary);
      border-color: var(--el-header-optimum-nav-btn-hover-border-color);
      background: var(--el-header-optimum-nav-btn-hover-bg-color);
    }
  }
}

.left-row {
  display: inline-flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  min-width: 68px;
  max-width: 112px;
  height: 100%;
  padding: 4px 10px;
  margin: 0;
  cursor: pointer;
  border: 1px solid transparent;
  border-radius: 8px;
  color: var(--el-header-optimum-color);
  box-sizing: border-box;
  transition:
    color 0.18s ease,
    background-color 0.18s ease;

  .left-row-tooltip-trigger {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    width: 100%;
    height: 100%;
    gap: 2px;
  }

  .title {
    margin-top: 4px;
    font-size: 12px;
    font-weight: $aside-menu-font-weight;
    line-height: 14px;
    text-align: center;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 100%;
    letter-spacing: 0.2px;
    opacity: 0.88;
    transition:
      opacity 0.18s ease,
      color 0.18s ease;
  }

  :deep(.el-icon),
  :deep(svg) {
    opacity: 0.85;
    transition:
      opacity 0.18s ease,
      color 0.18s ease;
  }

  &:hover {
    color: var(--el-header-optimum-hover-color);
    background: var(--el-header-optimum-hover-bg-color);
    border-color: transparent;

    .title {
      opacity: 1;
    }

    :deep(.el-icon),
    :deep(svg) {
      opacity: 1;
    }
  }

  &.is-active {
    color: var(--el-header-optimum-active-color);
    background: var(--el-header-optimum-active-bg-color);
    border-color: transparent;

    .title {
      opacity: 1;
    }

    :deep(.el-icon),
    :deep(svg) {
      opacity: 1;
      color: var(--el-header-optimum-active-color);
    }
  }

  &.is-active:hover {
    color: var(--el-header-optimum-active-color);
    background: var(--el-header-optimum-active-bg-color);
    border-color: transparent;
  }
}

.layout-container {
  width: 100vw;
  height: 100vh;
  overflow: hidden;

  .layout-optimum-aside {
    display: flex;
    flex-direction: column;
    height: 100vh;
    min-height: 0;
    padding-left: $column-menu-padding-left;
    padding-right: 0;
    background-color: var(--el-menu-bg-color);
    border-right: 1px solid var(--el-aside-border-right-color);
    box-sizing: border-box;
    user-select: none;
  }

  .layout-optimum-aside :deep(.el-menu) {
    height: auto;
    min-height: 0;
    max-height: none;
    border-right: none;
    box-sizing: border-box;
  }

  .layout-optimum-aside :deep(.el-menu.el-menu--collapse) {
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
  }

  .layout-header {
    height: $aside-header-height;
    background-color: var(--el-header-bg-color);
  }

  .layout-main {
    box-sizing: border-box;
    padding: 0;
    overflow-x: hidden;
    background-color: var(--el-bg-color);
  }
}

.layout-optimum-menu-scroller {
  flex: 1;
  min-height: 0;
  width: 100%;
  overflow-x: hidden;
  overflow-y: auto;
  overscroll-behavior: contain;
  -ms-scroll-chaining: none;
}

.layout-optimum-menu-pad {
  padding-right: $column-menu-padding-right;
  box-sizing: border-box;
}
</style>
