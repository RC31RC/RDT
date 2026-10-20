<template>
  <!-- 分栏布局 -->
  <el-container class="layout-container layout-columns">
    <div
      class="layout-column"
      :class="{ 'is-collapse': isFirstColumnCollapse }"
      :style="{ width: isFirstColumnCollapse ? '64px' : '80px' }"
    >
      <el-scrollbar class="column-scrollbar">
        <el-tooltip
          v-for="(item, index) in topLevelMenus"
          :key="item.meta?.menuId || index"
          :content="getMenuLanguage(item.meta?.title)"
          :show-after="isFirstColumnCollapse ? 0 : 1500"
          placement="right"
        >
          <div
            class="left-column koi-scale-i"
            :class="{
              'is-active': activeTopMenuId == item.meta?.menuId
            }"
            @click="handleTopMenuClick()"
          >
            <KoiGlobalIcon v-if="item.meta?.icon" :name="item.meta?.icon" size="18"></KoiGlobalIcon>
            <span v-if="!isFirstColumnCollapse" class="title line-clamp-2">
              {{ getMenuLanguage(item.meta?.title) }}
            </span>
          </div>
        </el-tooltip>
      </el-scrollbar>
      <div class="column-footer-dock">
        <div class="column-footer-btn column-switch-btn" @click="toggleFirstColumn">
          <KoiGlobalIcon name="koi-arrow-left-right" size="20"></KoiGlobalIcon>
        </div>
        <el-popover
          placement="right-start"
          :width="240"
          trigger="hover"
          :show-arrow="false"
          :offset="8"
          popper-class="layout-column-user-popover"
          popper-style="border-radius: 10px; box-shadow: 0 8px 32px rgba(0, 0, 0, 0.12);"
        >
          <template #reference>
            <div class="column-footer-btn" role="button" tabindex="0">
              <!-- <el-image
                class="column-user-avatar"
                fit="cover"
                :src="authStore.loginUser?.avatar || 'https://pic4.zhimg.com/v2-702a23ebb518199355099df77a3cfe07_b.webp'"
              /> -->
              <el-image class="column-user-avatar" fit="cover" :src="avatar"></el-image>
            </div>
          </template>
          <div class="user-card-content">
            <div class="user-card-header">
              <!-- <el-image
                class="w-36px h-36px rounded-full select-none"
                :src="authStore.loginUser?.avatar || 'https://pic4.zhimg.com/v2-702a23ebb518199355099df77a3cfe07_b.webp'"
              /> -->
              <el-image class="w-36px h-36px rounded-full select-none" :src="avatar"></el-image>
              <div class="user-info">
                <!-- <div class="user-name">{{ authStore.loginUser?.userName || "无名" }}</div>
                <div class="user-phone">{{ authStore.loginUser?.phone || "暂无电话" }}</div> -->
                <div class="user-name">{{ userName || "无名" }}</div>
                <div class="user-phone">{{ userPhone || "暂无电话" }}</div>
              </div>
            </div>
            <div class="user-card-menu">
              <!-- v-if="authStore.buttonList.includes('system:personage:list') || authStore.buttonList.includes('*')" -->
              <div class="user-menu-item" @click="handleCommand('koiMine')">
                <el-icon :size="15"><User /></el-icon>
                <span>{{ $t("header.personalCenter") }}</span>
              </div>
            </div>
            <div class="user-card-footer">
              <el-button icon="SwitchButton" plain @click="handleCommand('logout')">
                {{ $t("header.logout") }}
              </el-button>
            </div>
          </div>
        </el-popover>
      </div>
    </div>
    <el-aside
      class="layout-aside layout-columns-second-aside transition-all"
      :style="{
        width: !globalStore.isCollapse ? globalStore.menuWidth + 'px' : settings.columnMenuCollapseWidth
      }"
      v-if="currentSubMenuTree.length > 0"
    >
      <Logo :isCollapse="globalStore.isCollapse" :layout="globalStore.layout"></Logo>
      <div class="layout-columns-second-menu-scroller">
        <div class="layout-columns-second-menu-pad">
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
        <Header></Header>
      </el-header>
      <!-- 路由页面 -->
      <Main></Main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import settings from "@/settings";
import Logo from "@/layouts/components/Logo/index.vue";
import Header from "@/layouts/components/Header/index.vue";
import ColumnSubMenu from "@/layouts/components/Menu/ColumnSubMenu.vue";
import Main from "@/layouts/components/Main/index.vue";
import { ref, computed } from "vue";
import { useRoute, useRouter } from "vue-router";
import useAuthStore from "@/stores/modules/auth";
import useGlobalStore from "@/stores/modules/global";
import { getMenuLanguage } from "@/utils";
import { useAuthStore as useIdentityStore } from "@/stores/auth";
import { User } from "@element-plus/icons-vue";
const route = useRoute();
const router = useRouter();
const authStore = useAuthStore();
const identity = useIdentityStore();
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
const isFirstColumnCollapse = computed(() => globalStore.isColumnFirstCollapse);
const avatar = "/rdt-mark.svg";
const userName = computed(() => identity.principal?.displayName);
const userPhone = computed(() => identity.principal?.role);
function handleTopMenuClick() {
  globalStore.isCollapse = false;
}
function toggleFirstColumn() {
  globalStore.isColumnFirstCollapse = !globalStore.isColumnFirstCollapse;
}
function handleCommand(command: string) {
  if (command === "logout") identity.logout();
  else router.push("/profile");
}
</script>

<style lang="scss" scoped>
/** 第一列菜单样式 */
.layout-column {
  display: flex;
  flex-direction: column;
  height: 100%;
  flex-shrink: 0;
  transition: width 0.25s ease;
  user-select: none;
  background-color: var(--el-menu-bg-color);
  border-right: 1px solid var(--el-aside-border-right-color);
  position: relative;
  padding-bottom: 98px;
  box-sizing: border-box;

  .column-footer-dock {
    position: absolute;
    left: 0;
    right: 0;
    bottom: 6px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 6px;
    pointer-events: none;

    > * {
      pointer-events: auto;
    }
  }

  .column-footer-btn {
    width: 40px;
    height: 40px;
    border-radius: 6px;
    border: 1px solid transparent;
    background: transparent;
    color: var(--el-text-color-secondary);
    cursor: pointer;
    transition: all 0.2s ease;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    box-sizing: border-box;

    &:hover {
      color: var(--el-text-color-primary);
      background: var(--el-menu-hover-bg-color);
    }
  }

  .column-user-avatar {
    width: 36px;
    height: 36px;
    border-radius: 50%;
    cursor: pointer;
    flex-shrink: 0;
  }

  .column-scrollbar {
    flex: 1;
    min-height: 0;
  }

  :deep(.el-scrollbar__bar.is-vertical) {
    width: 4px;
    right: 2px;
  }

  :deep(.el-scrollbar__bar.is-vertical .el-scrollbar__thumb) {
    background: color-mix(in srgb, var(--el-text-color-secondary) 30%, transparent);
    border-radius: 999px;
  }

  .left-column {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    width: auto;
    min-height: 56px;
    margin: 4px 6px;
    padding: 4px 6px;
    color: var(--el-menu-text-color);
    cursor: pointer;
    border-radius: 8px;
    border: 1px solid transparent;
    transition: all 0.2s ease;

    &:hover {
      color: var(--el-menu-hover-text-color);
      background: var(--el-menu-hover-bg-color);
    }

    &.is-active {
      color: var(--el-menu-active-text-color);
      background: var(--el-menu-active-bg-color);
    }

    .el-icon {
      font-size: 18px;
    }

    .title {
      margin-top: 6px;
      font-size: 12px;
      font-weight: $aside-menu-font-weight;
      line-height: 14px;
      text-align: center;
      letter-spacing: 0.5px;
    }
  }

  &.is-collapse {
    .left-column {
      min-height: 48px;
      padding: 4px 0;
    }
  }
}

.layout-container {
  width: 100vw;
  height: 100vh;
  overflow: hidden;

  .layout-columns-second-aside {
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

  .layout-columns-second-aside :deep(.el-menu) {
    height: auto;
    min-height: 0;
    max-height: none;
    border-right: none;
    box-sizing: border-box;
  }

  .layout-columns-second-aside :deep(.el-menu.el-menu--collapse) {
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

.layout-columns-second-menu-scroller {
  flex: 1;
  min-height: 0;
  width: 100%;
  overflow-x: hidden;
  overflow-y: auto;
  overscroll-behavior: contain;
  -ms-scroll-chaining: none;
}

.layout-columns-second-menu-pad {
  padding-right: $column-menu-padding-right;
  box-sizing: border-box;
}

/* 第一列头像 Popover 挂载在 body，需非 scoped */
.layout-column-user-popover {
  .user-card-content {
    padding: 0;
  }

  .user-card-header {
    display: flex;
    align-items: center;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--el-border-color-lighter);
    margin-bottom: 10px;
  }

  .user-info {
    margin-left: 12px;
    flex: 1;
  }

  .user-name {
    font-size: 15px;
    font-weight: 500;
    color: var(--el-text-color-primary);
    margin-bottom: 3px;
    line-height: 1.2;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 160px;
  }

  .user-phone {
    font-size: 13px;
    color: var(--el-text-color-regular);
    line-height: 1.2;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 160px;
  }

  .user-card-menu {
    display: flex;
    flex-direction: column;
    gap: 3px;
  }

  .user-menu-item {
    display: flex;
    align-items: center;
    width: auto;
    height: 36px;
    padding: 8px 10px;
    font-size: 13px;
    user-select: none;
    background-color: transparent;
    border-radius: 6px;
    transition: all 0.3s ease;
    cursor: pointer;
    line-height: 1;

    &:hover {
      color: var(--el-color-primary);
      background-color: var(--el-color-primary-light-9);
    }

    .el-icon {
      margin-right: 8px;
      font-size: 14px;
      flex-shrink: 0;
    }

    span {
      font-size: 13px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      flex: 1;
      line-height: 1;
    }
  }

  .user-card-footer {
    display: flex;
    align-items: center;
    padding-top: 12px;
    border-top: 1px solid var(--el-border-color-lighter);
    margin-top: 10px;

    .el-button {
      width: 100%;
    }
  }
}
</style>
