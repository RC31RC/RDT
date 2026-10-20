import { computed, ref } from "vue";
import { defineStore } from "pinia";
import { managementApi, type Principal } from "@/api/management";
import { useConnectionsStore } from "@/stores/connections";
import { useNamespaceStore } from "@/stores/namespace";

export const useAuthStore = defineStore("rdt-auth", () => {
  const token = ref(localStorage.getItem("rdt-management-token") || "");
  const principal = ref<Principal | null>(null);
  const username = computed(() => principal.value?.username || "");
  const isAuthenticated = computed(() => Boolean(token.value));
  const can = (permission: string) => Boolean(principal.value?.permissions.includes(permission));
  async function login(account: string, password: string) {
    const { data } = await managementApi.login({ username: account, password });
    clearContext();
    token.value = data.token;
    principal.value = data.principal;
    localStorage.setItem("rdt-management-token", data.token);
  }
  async function load() {
    principal.value = (await managementApi.me()).data;
  }
  function clearContext() {
    useConnectionsStore().reset();
    useNamespaceStore().reset();
  }
  function clear() {
    token.value = "";
    principal.value = null;
    localStorage.removeItem("rdt-management-token");
    localStorage.removeItem("rdt-management-user");
    clearContext();
  }
  async function logout() {
    try {
      if (token.value) await managementApi.logout();
    } finally {
      clear();
      window.location.assign("/login");
    }
  }
  return {
    token,
    principal,
    username,
    isAuthenticated,
    can,
    login,
    load,
    clear,
    logout
  };
});
