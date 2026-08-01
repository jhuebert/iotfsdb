import { useMemo, useRef } from "react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip as ChartTooltip,
  Legend,
  Filler,
  type ChartOptions,
  type ChartData,
  type ScriptableContext,
} from "chart.js";
import zoomPlugin from "chartjs-plugin-zoom";
import { Line } from "react-chartjs-2";
import { colorForIndex } from "./chart-colors";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, ChartTooltip, Legend, Filler, zoomPlugin);

export interface ChartSeries {
  id: string;
  values: (number | null)[];
}

type ChartInstance = ChartJS<"line"> | undefined;

export interface SeriesChartProps {
  labels: (string | Date)[];
  series: ChartSeries[];
  height?: number;
  chartRef?: React.Ref<ChartInstance>;
}

const POINT_RADIUS = 1.5;
const HOVER_RADIUS = 4;

function resolveColor(context: ScriptableContext<"line">): string {
  return colorForIndex(context.datasetIndex);
}

export function SeriesChart({ labels, series, height = 400, chartRef }: SeriesChartProps) {
  const innerRef = useRef<ChartJS<"line"> | null>(null);
  const lineRef = useMemo(
    () => (node: ChartInstance | null) => {
      innerRef.current = node ?? null;
      if (typeof chartRef === "function") {
        chartRef(node);
      } else if (chartRef) {
        chartRef.current = node;
      }
    },
    [chartRef],
  );

  const data: ChartData<"line"> = useMemo(
    () => ({
      labels,
      datasets: series.map((s, index) => ({
        label: s.id,
        data: s.values,
        borderColor: colorForIndex(index),
        backgroundColor: (context: ScriptableContext<"line">) => {
          const color = resolveColor(context);
          return color.replace(")", " / 0.15)");
        },
        fill: series.length === 1,
        tension: 0.1,
        borderWidth: 1.5,
        pointRadius: POINT_RADIUS,
        pointHoverRadius: HOVER_RADIUS,
        pointBackgroundColor: colorForIndex(index),
        spanGaps: true,
      })),
    }),
    [labels, series],
  );

  const options: ChartOptions<"line"> = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      animation: { duration: 150 },
      scales: {
        x: {
          type: "category",
          ticks: {
            maxTicksLimit: 12,
            color: "var(--muted-foreground)",
            maxRotation: 0,
          },
          grid: { color: "var(--border)" },
        },
        y: {
          type: "linear",
          ticks: { color: "var(--muted-foreground)" },
          grid: { color: "var(--border)" },
        },
      },
      plugins: {
        legend: {
          display: series.length <= 8,
          position: "top",
          labels: { color: "var(--muted-foreground)", boxWidth: 12, usePointStyle: true },
        },
        tooltip: {
          callbacks: {
            label: (context) => {
              const value = context.parsed.y;
              return ` ${context.dataset.label}: ${value === null ? "—" : value}`;
            },
          },
        },
        zoom: {
          pan: {
            enabled: true,
            mode: "x",
            modifierKey: "shift",
            onPanComplete: () => innerRef.current?.update("none"),
          },
          zoom: {
            wheel: { enabled: true, speed: 0.1 },
            pinch: { enabled: true },
            mode: "x",
            onZoomComplete: () => innerRef.current?.update("none"),
          },
        },
      },
    }),
    [series.length],
  );

  return (
    <div style={{ height }} className="w-full">
      <Line ref={lineRef} data={data} options={options} />
    </div>
  );
}

export function downloadChartPng(chart: ChartJS<"line"> | null, filename = "iotfsdb-chart.png") {
  if (!chart) {
    return;
  }
  const link = document.createElement("a");
  link.href = chart.toBase64Image("image/png", 1);
  link.download = filename;
  link.click();
}

export function downloadChartCsv(labels: (string | Date)[], series: ChartSeries[], filename = "iotfsdb-data.csv") {
  const header = ["time", ...series.map((s) => s.id)].join(",");
  const rows = labels.map((label, index) => {
    const time = label instanceof Date ? label.toISOString() : label;
    return [time, ...series.map((s) => (s.values[index] ?? "") as string | number)].join(",");
  });
  const blob = new Blob([[header, ...rows].join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}
