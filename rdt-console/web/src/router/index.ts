import { createRouter, createWebHistory } from "vue-router";
import { useConnectionsStore } from "@/stores/connections";
import Connections from "@/views/settings/Connections.vue";
import { useAuthStore } from "@/stores/auth";
import ConsoleLayout from "@/layouts/index.vue";
import Users from "@/views/system/Users.vue";
import Profile from "@/views/system/Profile.vue";
import Login from "@/views/auth/Login.vue";
import Overview from "@/views/dashboard/Overview.vue";
import Streams from "@/views/streams/Streams.vue";
import Tasks from "@/views/tasks/Tasks.vue";
import DeadLetters from "@/views/dead-letters/DeadLetters.vue";
import Instances from "@/views/instances/Instances.vue";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/login", name: "login", component: Login, meta: { public: true } },
    {
      path: "/",
      component: ConsoleLayout,
      redirect: "/overview",
      children: [
        {
          path: "settings",
          name: "settings",
          component: Connections,
          meta: { permission: "connections:manage" }
        },
        {
          path: "users",
          name: "users",
          component: Users,
          meta: { permission: "users:manage" }
        },
        { path: "profile", name: "profile", component: Profile },
        { path: "overview", name: "overview", component: Overview },
        { path: "streams", name: "streams", component: Streams },
        { path: "tasks", name: "tasks", component: Tasks },
        { path: "dead-letters", name: "dead-letters", component: DeadLetters },
        { path: "instances", name: "instances", component: Instances }
      ]
    },
    { path: "/:pathMatch(.*)*", redirect: "/overview" }
  ]
});

router.beforeEach(async (to) => {
  const auth = useAuthStore();
  if (!to.meta.public && !auth.isAuthenticated) return { name: "login", query: { redirect: to.fullPath } };
  if (auth.isAuthenticated && !auth.principal) {
    try {
      await auth.load();
    } catch {
      auth.clear();
      return { name: "login" };
    }
  }
  if (to.name === "login" && auth.isAuthenticated) return { name: "overview" };
  if (to.meta.permission && !auth.can(String(to.meta.permission))) return { name: "overview" };
  if (!to.meta.public) {
    const connections = useConnectionsStore();
    if (!connections.status) await connections.loadStatus();
    if (
      !connections.status?.runtimeConfigured &&
      auth.can("connections:manage") &&
      !["settings", "users", "profile"].includes(String(to.name))
    )
      return { name: "settings" };
  }
});
export default router;
