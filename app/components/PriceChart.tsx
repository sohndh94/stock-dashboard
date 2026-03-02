"use client";

import ReactECharts from "echarts-for-react";

import { ChartSeries } from "@/lib/types";

interface PriceChartProps {
  series: ChartSeries[];
  height?: number;
  monochrome?: boolean;
}

const MONO_COLORS = ["#111111", "#333333", "#555555", "#777777", "#999999", "#bbbbbb"];
const DEFAULT_COLORS = ["#111111", "#4a4a4a", "#707070", "#8a8a8a", "#a0a0a0", "#b9b9b9"];

export default function PriceChart({
  series,
  height = 340,
  monochrome = false
}: PriceChartProps) {
  const colors = monochrome ? MONO_COLORS : DEFAULT_COLORS;

  const option = {
    color: colors,
    tooltip: {
      trigger: "axis",
      backgroundColor: "#fff",
      borderColor: "#111",
      borderWidth: 1,
      textStyle: { color: "#111" }
    },
    legend: {
      type: "scroll",
      top: 0,
      textStyle: { color: "#222" }
    },
    grid: {
      left: 14,
      right: 14,
      top: 48,
      bottom: 18,
      containLabel: true
    },
    xAxis: {
      type: "time",
      axisLabel: {
        color: "#666"
      },
      axisLine: {
        lineStyle: {
          color: "#c8c8c8"
        }
      }
    },
    yAxis: {
      type: "value",
      axisLabel: {
        formatter: (value: number) => `${value.toFixed(0)}`,
        color: "#666"
      },
      splitLine: {
        lineStyle: {
          color: "#ececec"
        }
      }
    },
    series: series.map((item, index) => ({
      name: item.label,
      type: "line",
      showSymbol: false,
      smooth: 0.1,
      lineStyle: {
        width: index === 0 ? 2.2 : 1.8,
        type: index % 3 === 0 ? "solid" : "dashed"
      },
      data: item.points.map((point) => [point.date, point.value])
    }))
  };

  return (
    <ReactECharts
      option={option}
      notMerge
      lazyUpdate
      style={{ width: "100%", height }}
      opts={{ renderer: "canvas" }}
    />
  );
}
