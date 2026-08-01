/**
 * Per-series color palette. Values are CSS variables (chart-1..chart-8) so the
 * palette adapts to the active theme automatically.
 */
export const CHART_COLOR_VARS = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
  "var(--chart-6)",
  "var(--chart-7)",
  "var(--chart-8)",
] as const;

export function colorForIndex(index: number): string {
  return CHART_COLOR_VARS[index % CHART_COLOR_VARS.length];
}
