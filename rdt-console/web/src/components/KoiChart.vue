<template>
  <div class="koi-chart-wrap">
    <div ref="container" class="koi-chart" role="img" :aria-label="label" />
    <el-empty v-if="empty" :description="emptyText" :image-size="72" />
  </div>
</template>
<script setup lang="ts">
import { nextTick, onMounted, onUnmounted, ref, watch } from "vue";
import { init, use, type EChartsCoreOption, type ECharts } from "echarts/core";
import { BarChart, LineChart, PieChart } from "echarts/charts";
import { GridComponent, LegendComponent, TooltipComponent } from "echarts/components";
import { SVGRenderer } from "echarts/renderers";
import useGlobalStore from "@/stores/modules/global";
use([BarChart, LineChart, PieChart, GridComponent, LegendComponent, TooltipComponent, SVGRenderer]);
const props = withDefaults(defineProps<{ option: EChartsCoreOption; label: string; empty?: boolean; emptyText?: string }>(), {
  empty: false,
  emptyText: "暂无数据"
});
const global = useGlobalStore();
const container = ref<HTMLElement>();
let chart: ECharts | undefined;
let observer: ResizeObserver | undefined;
let disposed = false;
async function render() {
  await nextTick();
  const element = container.value;
  if (disposed || !element?.clientWidth || !element.clientHeight) return;
  chart ??= init(element, undefined, { renderer: "svg" });
  const styles = window.getComputedStyle(element);
  chart.setOption({
    textStyle: { color: styles.getPropertyValue("--el-text-color-secondary").trim(), fontFamily: "inherit" },
    animation: false,
    animationDuration: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 400,
    ...props.option,
    tooltip: {
      confine: true,
      backgroundColor: styles.getPropertyValue("--el-bg-color-overlay").trim(),
      borderColor: styles.getPropertyValue("--el-border-color").trim(),
      textStyle: { color: styles.getPropertyValue("--el-text-color-primary").trim() },
      ...(props.option.tooltip as Record<string, unknown> | undefined)
    }
  }, { notMerge: true });
  chart.resize();
}
onMounted(() => {
  observer = new ResizeObserver(() => { if (chart) chart.resize(); else void render(); });
  if (container.value) observer.observe(container.value);
  void render();
});
watch(() => [props.option, global.isDark, global.themeColor], () => { void render(); }, { deep: true });
onUnmounted(() => { disposed = true; observer?.disconnect(); chart?.dispose(); });
</script>
<style scoped>
.koi-chart-wrap { position: relative; min-width: 0; }
.koi-chart { width: 100%; height: 300px; min-width: 0; }
.el-empty { position: absolute; inset: 0; background: var(--el-bg-color); }
</style>
