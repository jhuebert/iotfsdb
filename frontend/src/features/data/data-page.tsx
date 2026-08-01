import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Download, ImageDown, Play, SearchX } from "lucide-react";
import type { Chart as ChartJS } from "chart.js";
import { findData } from "@/api/data";
import { getUiConfig } from "@/api/series";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { downloadChartCsv, downloadChartPng, SeriesChart, type ChartSeries } from "@/components/chart/series-chart";
import { SeriesFilterInput } from "@/features/filters/series-filter-input";
import { useSeriesSearch } from "@/features/filters/use-series-search";
import { AdvancedOptions } from "./advanced-options";
import { buildDataQuery } from "./query-request";
import { SamplingControls } from "./sampling-controls";
import { SeriesSelector } from "./series-selector";
import { TimeRangePopover } from "./time-range-popover";
import { ValuesTable } from "./values-table";
import { decodeDataUrl, defaultDataState, encodeDataUrl, type DataUrlState } from "@/lib/url-state";

const AUTO_SELECT_COUNT = 10;
const LIVE_INTERVAL_MS = 15_000;

export function DataPage() {
  const [, setSearchParams] = useSearchParams();
  const [state, setState] = useState<DataUrlState>(() => defaultDataState());
  const [hydrated, setHydrated] = useState(false);
  const [manualClear, setManualClear] = useState(false);
  const filterRef = useRef<HTMLInputElement>(null);
  const chartRef = useRef<ChartJS<"line"> | undefined | null>(null);

  const { data: config } = useQuery({ queryKey: ["ui", "config"], queryFn: getUiConfig, staleTime: Infinity });

  // Hydrate state from the URL (deep links / share links).
  useEffect(() => {
    let cancelled = false;
    decodeDataUrl(window.location.search).then((decoded) => {
      if (!cancelled) {
        setState(decoded);
        setHydrated(true);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  // Sync state back to the URL (replace, no server round-trip).
  useEffect(() => {
    if (!hydrated) {
      return;
    }
    let cancelled = false;
    encodeDataUrl(state).then((query) => {
      if (!cancelled) {
        setSearchParams(query, { replace: true });
      }
    });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hydrated, state]);

  const search = useSeriesSearch(state.q);
  const matchedIds = useMemo(() => new Set((search.data ?? []).map((f) => f.definition.id)), [search.data]);

  // Prune stale selections when the matched set changes.
  useEffect(() => {
    if (!hydrated) {
      return;
    }
    setState((current) => {
      const pruned = current.sel.filter((id) => matchedIds.has(id));
      return pruned.length === current.sel.length ? current : { ...current, sel: pruned };
    });
  }, [matchedIds, hydrated]);

  // Auto-select the first N matches when nothing is selected (and the user has
  // not explicitly cleared the selection).
  useEffect(() => {
    if (!hydrated || manualClear || state.sel.length > 0 || !search.data?.length) {
      return;
    }
    setState((current) => ({
      ...current,
      sel: search.data!.slice(0, AUTO_SELECT_COUNT).map((f) => f.definition.id),
    }));
  }, [search.data, hydrated, manualClear, state.sel.length]);

  function patchState(patch: Partial<DataUrlState>) {
    setState((current) => ({ ...current, ...patch }));
  }

  const { request, error: queryError } = useMemo(() => buildDataQuery(state), [state]);

  const dataQuery = useQuery({
    queryKey: ["data", "find", state, request],
    queryFn: () => findData(request),
    enabled: hydrated && !queryError && state.sel.length > 0,
    refetchInterval: state.live ? LIVE_INTERVAL_MS : undefined,
  });

  // Shape chart + table data from the response.
  const chartSeries: ChartSeries[] = useMemo(
    () =>
      (dataQuery.data ?? []).map((response) => ({
        id: response.series.definition.id,
        values: response.data.map((d) => d.value),
      })),
    [dataQuery.data],
  );

  const labels: (string | Date)[] = useMemo(
    () => (dataQuery.data?.[0]?.data ?? []).map((d) => d.time),
    [dataQuery.data],
  );

  const tableSeries = useMemo(
    () =>
      (dataQuery.data ?? []).map((response) => ({
        id: response.series.definition.id,
        type: response.series.definition.type,
        values: response.data.map((d) => d.value),
      })),
    [dataQuery.data],
  );

  // Keyboard: "/" focuses the filter, Esc clears it.
  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      const target = event.target as HTMLElement;
      const typing = ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName);
      if (event.key === "/" && !typing) {
        event.preventDefault();
        filterRef.current?.focus();
      }
      if (event.key === "Escape" && target === filterRef.current) {
        patchState({ q: "" });
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  function handleExportCsv() {
    downloadChartCsv(labels, chartSeries);
  }

  function handleExportPng() {
    downloadChartPng(chartRef.current ?? null);
  }

  const showChart = state.sel.length > 0 && !queryError;

  return (
    <div className="grid gap-4 lg:grid-cols-[360px_minmax(0,1fr)]">
      {/* Left panel: query builder */}
      <div className="space-y-3">
        <h1 className="text-xl font-semibold tracking-tight">Data</h1>
        <Card>
          <CardContent className="space-y-3 p-3">
            <SeriesFilterInput
              value={state.q}
              onChange={(q) => {
                setManualClear(false);
                patchState({ q });
              }}
              disabled={!hydrated}
            />
            <TimeRangePopover
              preset={state.preset}
              from={state.from}
              to={state.to}
              tz={state.tz}
              onChange={(patch) => patchState(patch)}
            />
            <SamplingControls
              interval={state.interval}
              size={state.size}
              maxQuerySize={config?.maxQuerySize ?? 1000}
              onChange={(patch) => patchState(patch)}
            />
            <AdvancedOptions
              timeReducer={state.timeReducer}
              seriesReducer={state.seriesReducer}
              usePrevious={state.usePrevious}
              nullValue={state.nullValue}
              live={state.live}
              onChange={(patch) => patchState(patch)}
            />
            <div className="h-56">
              <SeriesSelector
                files={search.data ?? []}
                selected={new Set(state.sel)}
                onToggle={(id) => {
                  setManualClear(false);
                  const next = new Set(state.sel);
                  if (next.has(id)) {
                    next.delete(id);
                  } else {
                    next.add(id);
                  }
                  patchState({ sel: [...next] });
                }}
                onSelectAll={() => patchState({ sel: matchedIds.size ? [...matchedIds] : [] })}
                onSelectNone={() => {
                  setManualClear(true);
                  patchState({ sel: [] });
                }}
              />
            </div>
            {search.isLoading ? (
              <p className="text-xs text-muted-foreground">Matching series…</p>
            ) : (
              <p className="text-xs text-muted-foreground">
                {search.data?.length ?? 0} series match the filter
                {state.live && (
                  <span className="ml-1 inline-flex items-center gap-1 text-primary">
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-primary" />
                    live
                  </span>
                )}
              </p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Main panel: chart + table */}
      <div className="space-y-4">
        <Card>
          <CardContent className="space-y-3 p-4">
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <Button size="sm" variant="outline" onClick={handleExportCsv} disabled={!chartSeries.length}>
                  <Download />
                  CSV
                </Button>
                <Button size="sm" variant="outline" onClick={handleExportPng} disabled={!chartSeries.length}>
                  <ImageDown />
                  PNG
                </Button>
              </div>
              {dataQuery.isFetching && (
                <span className="text-xs text-muted-foreground">
                  {dataQuery.isRefetching ? "Refreshing…" : "Loading…"}
                </span>
              )}
            </div>

            {queryError && (
              <div
                className="rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive"
                role="alert"
              >
                {queryError.message}
              </div>
            )}

            {dataQuery.isError && (
              <div
                className="rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive"
                role="alert"
              >
                Query failed: {dataQuery.error.message}
              </div>
            )}

            {showChart ? (
              dataQuery.isLoading ? (
                <div className="space-y-2">
                  <Skeleton className="h-[400px] w-full" />
                </div>
              ) : (
                <>
                  <SeriesChart labels={labels} series={chartSeries} chartRef={chartRef} height={420} />
                  {dataQuery.data && dataQuery.data.length === 0 && (
                    <div className="flex flex-col items-center gap-2 py-8 text-center text-muted-foreground">
                      <SearchX className="h-8 w-8" />
                      <p className="text-sm">No data found for the selected series in this time range.</p>
                    </div>
                  )}
                </>
              )
            ) : (
              <div className="flex flex-col items-center gap-2 py-16 text-center text-muted-foreground">
                <Play className="h-8 w-8" />
                <p className="text-sm">Select at least one series to query data.</p>
              </div>
            )}
          </CardContent>
        </Card>

        {showChart && chartSeries.length > 0 && (
          <Card>
            <CardContent className="p-4">
              <ValuesTable labels={labels} series={tableSeries} />
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
