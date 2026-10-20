import axios from "axios";
import router from "@/router";
import { useConnectionsStore } from "@/stores/connections";
import { useAuthStore } from "@/stores/auth";

const http = axios.create({
  baseURL: import.meta.env.VITE_RDT_API_BASE_URL || "/rdt/management",
  timeout: 10000,
  headers: { "Content-Type": "application/json" }
});

http.interceptors.request.use((config) => {
  const token = localStorage.getItem("rdt-management-token");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  if (config.method !== "get" && !config.url?.startsWith("/auth") && !config.url?.startsWith("/connections")) {
    const connections = useConnectionsStore();
    const revision = connections.settings?.revision ?? connections.status?.revision;
    if (revision !== undefined) config.headers["X-Rdt-Connection-Revision"] = String(revision);
  }
  return config;
});

http.interceptors.response.use(
  (response) => response,
  async (error) => {
    if (error.response?.status === 401) {
      useAuthStore().clear();
      if (router.currentRoute.value.name !== "login") {
        await router.push({ name: "login" });
      }
    }
    return Promise.reject(error);
  }
);

export default http;
