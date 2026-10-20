import type { Component } from "vue";

export interface ConsoleMenuItem {
  path: string;
  meta: {
    title: string;
    icon: Component | string;
    permission?: string;
    menuId?: string;
  };
  children?: ConsoleMenuItem[];
}

export const consoleMenuItems: ConsoleMenuItem[] = [
  { path: "/overview", meta: { title: "运行概览", icon: "koi-dashboard" } },
  { path: "/instances", meta: { title: "运行节点", icon: "koi-driver" } },
  { path: "/streams", meta: { title: "消息主题", icon: "koi-conversation-box" } },
  { path: "/tasks", meta: { title: "定时任务", icon: "koi-money-time" } },
  { path: "/dead-letters", meta: { title: "死信记录", icon: "koi-notes" } },
  {
    path: "/users",
    meta: { title: "用户与权限", icon: "koi-enhance-user", permission: "users:manage" }
  },
  { path: "/profile", meta: { title: "个人中心", icon: "koi-profile-circle" } },
  {
    path: "/settings",
    meta: {
      title: "连接设置",
      icon: "koi-setting",
      permission: "connections:manage"
    }
  }
];
