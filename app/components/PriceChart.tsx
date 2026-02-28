"use client";

import ReactECharts from "echarts-for-react";

import { ChartResponse } from "@/lib/types";

interface PriceChartProps {
  data: ChartResponse;
}

const COLORS = ["#00684a", "#145d8a", "#9f5f00", "#8f345f", "#496c0b", "#5f4d98"];

export default function PriceChart({ data }: PriceChartProps) {
  const option = {
    color: COLORS,
    tooltip: {
      trigger: "axis"
    },
    legend: {
      type: "scroll",
      top: 0
    },
    grid: {
      left: 18,
      right: 18,
      top: 52,
      bottom: 20,
      containLabel: true
    },
    xAxis: {
      type: "time",
      axisLabel: {
        color: "#465046"
      }
    },
    yAxis: {
      type: "value",
      axisLabel: {
        formatter: (value: number) => `${value.toFixed(0)}`,
        color: "#465046"
      },
      splitLine: {
        lineStyle: {
          color: "#e3e7df"
        }
      }
    },
    series: data.series.map((series) => ({
      name: series.label,
      type: "line",
      showSymbol: false,
      smooth: 0.15,
      lineStyle: {
        width: 2
      },
      data: series.points.map((point) => [point.date, point.value])
    }))
  };

  return (
    <ReactECharts
      option={option}
      notMerge
      lazyUpdate
      style={{ width: "100%", height: 340 }}
      opts={{ renderer: "canvas" }}
    />
  );
}
