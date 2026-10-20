import { ref } from "vue";
import { defineStore } from "pinia";
import http from "@/api/http";
import { connectionsApi, type ConnectionSettings } from "@/api/connections";

export const useConnectionsStore = defineStore("rdt-connections", () => {
  const status = ref<{ revision: number; runtimeConfigured: boolean } | null>(null);
  const settings = ref<ConnectionSettings | null>(null);
  async function load() {
    settings.value = (await connectionsApi.get()).data;
  }
  async function loadStatus() {
    status.value = (await http.get<{ revision: number; runtimeConfigured: boolean }>("/status")).data;
  }
  function reset() {
    settings.value = null;
    status.value = null;
  }
  return { settings, status, load, loadStatus, reset };
});
