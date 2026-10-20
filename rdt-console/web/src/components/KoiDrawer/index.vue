<template>
  <el-drawer
    v-model="visible"
    :title="title"
    :size="size"
    :class="drawerClass"
    :modal-class="modalClass"
    :lock-scroll="lockScroll"
    :close-on-click-modal="closeOnClickModel"
    append-to-body
  >
    <slot name="content" />
    <template v-if="!footerHidden" #footer><slot name="footer" /></template>
  </el-drawer>
</template>
<script setup lang="ts">
import { ref } from "vue";
withDefaults(
  defineProps<{
    title?: string;
    size?: string;
    footerHidden?: boolean;
    closeOnClickModel?: boolean;
    lockScroll?: boolean;
    drawerClass?: string;
    modalClass?: string;
  }>(),
  { size: "320", closeOnClickModel: true, lockScroll: true }
);
const visible = ref(false);
defineExpose({
  koiOpen: () => {
    visible.value = true;
  },
  koiClose: () => {
    visible.value = false;
  }
});
</script>
