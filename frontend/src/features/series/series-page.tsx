import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { toast } from "sonner";
import { exportData } from "@/api/data";
import { getUiConfig, patternForIds } from "@/api/series";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { SeriesFilterInput } from "@/features/filters/series-filter-input";
import { useSeriesSearch } from "@/features/filters/use-series-search";
import { DeleteSeriesDialog } from "./delete-series-dialog";
import { MetadataEditor } from "./metadata-editor";
import { SeriesTable } from "./series-table";

export function SeriesPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [q, setQ] = useState(searchParams.get("q") ?? "");
  const [editingId, setEditingId] = useState<string | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const exportInFlight = useRef<Set<string>>(new Set());

  const { data: config } = useQuery({ queryKey: ["ui", "config"], queryFn: getUiConfig, staleTime: Infinity });
  const search = useSeriesSearch(q);

  // Keep the URL in sync with the filter box (debounced via deferred value).
  useEffect(() => {
    const next = new URLSearchParams(searchParams);
    if (q) {
      next.set("q", q);
    } else {
      next.delete("q");
    }
    setSearchParams(next, { replace: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);

  async function handleExport(id: string) {
    if (exportInFlight.current.has(id)) {
      return;
    }
    exportInFlight.current.add(id);
    try {
      await exportData({ pattern: patternForIds([id]) });
      toast.success(`Export started for "${id}"`);
    } catch (error) {
      toast.error(`Export failed: ${(error as Error).message}`);
    } finally {
      exportInFlight.current.delete(id);
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3">
        <h1 className="text-xl font-semibold tracking-tight">Series</h1>
        <SeriesFilterInput value={q} onChange={setQ} autoFocus />
      </div>

      <Card>
        <CardContent className="p-0">
          {search.isLoading ? (
            <div className="space-y-2 p-4">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : (
            <SeriesTable
              series={search.data ?? []}
              readOnly={config?.readOnly ?? true}
              onExport={handleExport}
              onEditMetadata={setEditingId}
              onDelete={setDeletingId}
            />
          )}
        </CardContent>
      </Card>

      <p className="text-xs text-muted-foreground">
        {search.hasQuery ? `${search.data?.length ?? 0} matching series` : `${search.data?.length ?? 0} total series`}
      </p>

      <MetadataEditor seriesId={editingId} onOpenChange={(open) => !open && setEditingId(null)} />
      <DeleteSeriesDialog seriesId={deletingId} onOpenChange={(open) => !open && setDeletingId(null)} />
    </div>
  );
}
