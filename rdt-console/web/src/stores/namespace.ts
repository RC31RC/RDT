import { computed, ref } from "vue";
import { defineStore } from "pinia";
import { managementApi } from "@/api/management";

const STORAGE_KEY = "rdt-console-namespace";

export const useNamespaceStore = defineStore("rdt-namespace", () => {
  const namespaces = ref<string[]>([]);
  const selected = ref(localStorage.getItem(STORAGE_KEY) || "");
  const loading = ref(false);
  let generation = 0;

  const hasSelection = computed(() => Boolean(selected.value));

  async function load() {
    const requested = ++generation;
    loading.value = true;
    try {
      const response = await managementApi.namespaces();
      if (requested !== generation) return;
      namespaces.value = response.data;
      if (!namespaces.value.includes(selected.value)) {
        selected.value = namespaces.value[0] || "";
      }
      persist();
    } finally {
      if (requested === generation) loading.value = false;
    }
  }

  function reset() {
    generation++;
    loading.value = false;
    namespaces.value = [];
    selected.value = "";
    persist();
  }

  function select(namespace: string) {
    selected.value = namespace;
    persist();
  }

  function persist() {
    if (selected.value) {
      localStorage.setItem(STORAGE_KEY, selected.value);
    } else {
      localStorage.removeItem(STORAGE_KEY);
    }
  }

  return { namespaces, selected, loading, hasSelection, load, select, reset };
});
