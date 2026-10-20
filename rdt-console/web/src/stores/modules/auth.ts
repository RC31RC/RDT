import { computed } from "vue";
import { useAuthStore } from "@/stores/auth";
import { consoleMenuItems } from "@/router/navigation";
export default function useMenuStore() {
  const auth = useAuthStore();
  return {
    get showMenuList() {
      return consoleMenuItems.filter((item) => !item.meta.permission || auth.can(item.meta.permission));
    },
    menus: computed(() => consoleMenuItems.filter((item) => !item.meta.permission || auth.can(item.meta.permission)))
  };
}
