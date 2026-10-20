import { createApp } from "vue";
import { createPinia } from "pinia";
import ElementPlus from "element-plus";
import "@unocss/reset/normalize.css";
import "element-plus/dist/index.css";
import "element-plus/theme-chalk/dark/css-vars.css";
import "virtual:uno.css";
import "virtual:svg-icons-register";
import App from "./App.vue";
import router from "./router";
import "./styles/index.scss";
import "animate.css";
import i18n from "@/languages";
import KoiGlobalIcon from "@/components/KoiGlobalIcon/index.vue";
import KoiSvgIcon from "@/components/KoiSvgIcon/index.vue";
import KoiScrollNav from "@/components/KoiScrollNav/Index.vue";
import { useAuthStore } from "@/stores/auth";
import RdtTable from "@/components/RdtTable.vue";
import * as ElementIcons from "@element-plus/icons-vue";

const app = createApp(App);
for (const [name, component] of Object.entries(ElementIcons)) app.component(name, component);
app
  .component("RdtTable", RdtTable)
  .component("KoiGlobalIcon", KoiGlobalIcon)
  .component("KoiSvgIcon", KoiSvgIcon)
  .component("KoiScrollNav", KoiScrollNav);
app.directive("permission", {
  mounted(element, binding) {
    element.hidden = !useAuthStore().can(binding.value);
    if (element.hidden) element.style.display = "none";
  },
  updated(element, binding) {
    element.style.display = useAuthStore().can(binding.value) ? "" : "none";
  }
});
app.use(createPinia()).use(i18n).use(router).use(ElementPlus).mount("#app");
