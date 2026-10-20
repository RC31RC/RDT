<template><AsideSubMenu :menu-list="menuList" /></template>
<script setup lang="ts">
import AsideSubMenu from "./AsideSubMenu.vue";
import type { ConsoleMenuItem } from "@/router/navigation";
defineProps<{ menuList: ConsoleMenuItem[] }>();
</script>
