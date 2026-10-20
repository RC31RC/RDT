import { computed, reactive, toRefs, watch } from "vue";
import { defineStore } from "pinia";

const defaults = {
  layout: "vertical",
  isCollapse: false,
  isColumnFirstCollapse: false,
  menuWidth: 220,
  maximize: false,
  isFullScreen: false,
  isDark: false,
  themeColor: "#2992FF",
  language: "zh",
  dimension: "default",
  tabsStyle: "google",
  transition: "fade-default",
  uniqueOpened: true,
  isGrey: false,
  isWeak: false,
  asideInverted: false,
  headerInverted: false
};
export default defineStore("rdt-koi-global", () => {
  const raw = localStorage.getItem("rdt-console-appearance");
  let saved: Partial<typeof defaults> = {};
  try {
    saved = raw ? JSON.parse(raw) : {};
  } catch {
    localStorage.removeItem("rdt-console-appearance");
  }
  const state = reactive({
    ...defaults,
    ...saved,
    maximize: false,
    isCollapse: window.innerWidth < 1200
  });
  watch(state, (value) => localStorage.setItem("rdt-console-appearance", JSON.stringify(value)), { deep: true });
  function setGlobalState<K extends keyof typeof defaults>(key: K, value: (typeof defaults)[K]) {
    state[key] = value;
  }
  const dark = computed({
    get: () => state.isDark,
    set: (value) => {
      state.isDark = value;
    }
  });
  return {
    ...toRefs(state),
    dark,
    setGlobalState,
    setCollapse: (value: boolean) => {
      state.isCollapse = value;
    },
    setColumnFirstCollapse: (value: boolean) => {
      state.isColumnFirstCollapse = value;
    },
    setDimension: (value: string) => {
      state.dimension = value;
    },
    $reset: () => Object.assign(state, defaults)
  };
});
