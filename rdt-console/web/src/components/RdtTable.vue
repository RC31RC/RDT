<template>
  <el-table
    class="rdt-table"
    border
    highlight-current-row
    :header-cell-style="{ textAlign: 'center' }"
    :cell-style="{ textAlign: 'center' }"
    v-bind="$attrs"
  >
    <template v-for="(_, name) in $slots" #[name]="scope">
      <slot :name="name" v-bind="scope || {}" />
    </template>
  </el-table>
</template>
<script setup lang="ts">
defineOptions({ inheritAttrs: false });
</script>
<style scoped lang="scss">
.rdt-table {
  width: 100%;
  border-radius: 8px;
  --el-table-header-bg-color: var(--el-fill-color-light);
  :deep(.el-table__cell) { padding: 12px 0; }
  :deep(.cell) { line-height: 24px; }
  :deep(.cell .el-space), :deep(.cell .el-button-group) { vertical-align: middle; }
  :deep(code) { font-family: ui-monospace, SFMono-Regular, monospace; font-size: 12px; }
}
</style>
