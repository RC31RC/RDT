<template>
  <div class="runtime-charts">
    <el-card class="trend-panel" shadow="never">
      <div class="chart-heading"><h3>消息状态观察</h3><el-tag size="small" effect="plain">每 30 秒采样</el-tag></div>
      <p>仅记录本次打开页面后的快照，最多保留 30 次；切换命名空间后重新采样。</p>
      <KoiChart :option="trendOption" :empty="!samples.length" label="本次页面观察到的待确认消息与死信数量曲线" />
    </el-card>
    <el-card shadow="never">
      <div class="chart-heading"><h3>主题与消费组</h3><el-tag size="small" effect="plain">{{ streams.length }} / {{ streamTotal }} 个主题 · {{ groupCount ?? "—" }} 个消费组</el-tag></div>
      <p>当前加载主题中保留消息最多的 8 个。保留消息包含已消费数据，不能等同于积压量。</p>
      <KoiChart :option="topicOption" :empty="!streams.length" :empty-text="streamError || '暂无消息主题'" label="当前加载的消息主题保留消息数柱状图" />
    </el-card>
    <el-card shadow="never">
      <div class="chart-heading"><h3>在线节点状态</h3><el-tag size="small" effect="plain">{{ instances.length }} 个节点</el-tag></div>
      <p>按正常接单、压力保护、停止接单分类。停止接单节点仍可能在处理已接收的工作。</p>
      <KoiChart :option="nodeOption" :empty="!instances.length" :empty-text="instanceError || '暂无在线节点'" label="在线节点接单状态环形图" />
    </el-card>
  </div>
</template>
<script setup lang="ts">
import { computed } from "vue";
import { graphic } from "echarts/core";
import KoiChart from "@/components/KoiChart.vue";
import useGlobalStore from "@/stores/modules/global";
import type { RuntimeInstance, StreamView } from "@/api/management";
export interface Observation { at: number; pending: number; deadLetters: number }
const props = defineProps<{
  samples: Observation[];
  streams: StreamView[];
  streamTotal: number;
  instances: RuntimeInstance[];
  groupCount?: number;
  streamError?: string;
  instanceError?: string;
}>();
const global = useGlobalStore();
const palette = computed(() => [global.themeColor, "#5ad8a6", "#f6bd16", "#e8686b"]);
const axes = computed(() => ({
  axisLine: { show: false },
  axisTick: { show: false },
  axisLabel: { color: global.isDark ? "#b8bdc8" : "#606266" },
  splitLine: { lineStyle: { color: global.isDark ? "#ffffff14" : "#0000000f", type: "dashed" } }
}));
const trendOption = computed(() => ({
  color: [palette.value[0], palette.value[3]],
  grid: { left: 16, right: 20, top: 45, bottom: 12, containLabel: true },
  tooltip: { trigger: "axis" },
  legend: { top: 0, data: ["待确认消息", "死信记录"], textStyle: { color: axes.value.axisLabel.color } },
  xAxis: { ...axes.value, type: "category", boundaryGap: false, data: props.samples.map(sample => new Date(sample.at).toLocaleTimeString("zh-CN", { hour12: false })) },
  yAxis: { ...axes.value, type: "value", min: 0, minInterval: 1 },
  series: [
    { name: "待确认消息", type: "line", symbolSize: 6, areaStyle: { opacity: 0.1 }, data: props.samples.map(sample => sample.pending) },
    { name: "死信记录", type: "line", symbolSize: 6, data: props.samples.map(sample => sample.deadLetters) }
  ]
}));
const ranked = computed(() => [...props.streams].sort((a, b) => b.length - a.length).slice(0, 8).reverse());
const topicOption = computed(() => ({
  grid: { left: 8, right: 40, top: 10, bottom: 12, containLabel: true },
  tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
  xAxis: { ...axes.value, type: "value", minInterval: 1 },
  yAxis: { ...axes.value, type: "category", axisLabel: { ...axes.value.axisLabel, width: 100, overflow: "truncate" }, data: ranked.value.map(stream => stream.displayName || stream.destination) },
  series: [{ name: "保留消息数", type: "bar", barMaxWidth: 22, label: { show: true, position: "right", color: axes.value.axisLabel.color }, itemStyle: { borderRadius: [0, 5, 5, 0], color: new graphic.LinearGradient(0, 0, 1, 0, [{ offset: 0, color: palette.value[1] }, { offset: 1, color: palette.value[0] }]) }, data: ranked.value.map(stream => stream.length) }]
}));
const nodeOption = computed(() => {
  const draining = props.instances.filter(instance => instance.draining).length;
  const protectedCount = props.instances.filter(instance => !instance.draining && instance.pressure.paused).length;
  return {
    color: [palette.value[1], palette.value[2], palette.value[3]],
    tooltip: { trigger: "item" },
    legend: { bottom: 0, textStyle: { color: axes.value.axisLabel.color } },
    series: [{ name: "在线节点", type: "pie", radius: ["48%", "70%"], center: ["50%", "43%"], avoidLabelOverlap: true, label: { show: false }, emphasis: { label: { show: true, formatter: "{b}\n{c} 个节点", fontSize: 16, fontWeight: "bold", position: "center" } }, itemStyle: { borderRadius: 6, borderColor: global.isDark ? "#1d1e1f" : "#fff", borderWidth: 3 }, data: [
      { name: "正常接单", value: props.instances.length - draining - protectedCount },
      { name: "压力保护", value: protectedCount },
      { name: "停止接单", value: draining }
    ] }]
  };
});
</script>
<style scoped>
.runtime-charts { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; min-width: 0; }
.trend-panel { grid-column: 1 / -1; }
.el-card { min-width: 0; border-radius: 8px; }
.chart-heading { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
h3 { margin: 0; font-size: 16px; font-weight: 700; }
p { margin: 8px 0 14px; color: var(--el-text-color-secondary); line-height: 1.7; font-size: 12px; }
@media (max-width: 992px) { .runtime-charts { grid-template-columns: minmax(0, 1fr); gap: 16px; } }
</style>
