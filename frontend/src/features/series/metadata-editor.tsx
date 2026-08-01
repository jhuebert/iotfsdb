import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus, Save, X } from "lucide-react";
import { toast } from "sonner";
import { getSeries, updateMetadata } from "@/api/series";
import type { SeriesFile } from "@/api/types";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";

interface MetadataEditorProps {
  seriesId: string | null;
  onOpenChange: (open: boolean) => void;
}

interface Row {
  key: string;
  value: string;
  originalKey?: string;
  originalValue?: string;
}

export function MetadataEditor({ seriesId, onOpenChange }: MetadataEditorProps) {
  const queryClient = useQueryClient();
  const open = seriesId !== null;
  const [rows, setRows] = useState<Row[]>([]);

  const { data: series, isLoading } = useQuery({
    queryKey: ["series", seriesId, "detail"],
    queryFn: () => getSeries(seriesId!),
    enabled: open,
  });

  useEffect(() => {
    if (series) {
      setRows(
        Object.entries(series.metadata).map(([key, value]) => ({
          key,
          value,
          originalKey: key,
          originalValue: value,
        })),
      );
    } else {
      setRows([]);
    }
  }, [series]);

  const changed = useMemo(
    () => rows.some((row) => row.originalKey !== row.key || row.originalValue !== row.value),
    [rows],
  );

  const updateMutation = useMutation({
    mutationFn: async (rowsToSave: Row[]) => {
      const metadata: Record<string, string> = {};
      for (const row of rowsToSave) {
        if (row.key.trim()) {
          metadata[row.key.trim()] = row.value;
        }
      }
      await updateMetadata(seriesId!, metadata);
    },
    onSuccess: () => {
      toast.success("Metadata updated");
      queryClient.invalidateQueries({ queryKey: ["series"] });
      onOpenChange(false);
    },
    onError: (error: Error) => toast.error(`Could not update metadata: ${error.message}`),
  });

  function addRow() {
    setRows((current) => [...current, { key: "", value: "" }]);
  }

  function removeRow(index: number) {
    setRows((current) => current.filter((_, i) => i !== index));
  }

  function updateRow(index: number, patch: Partial<Row>) {
    setRows((current) => current.map((row, i) => (i === index ? { ...row, ...patch } : row)));
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Metadata — {seriesId}</DialogTitle>
          <DialogDescription>Key/value pairs associated with this series.</DialogDescription>
        </DialogHeader>
        {isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
          </div>
        ) : (
          <div className="grid gap-3">
            {rows.length === 0 && <p className="py-4 text-center text-sm text-muted-foreground">No metadata yet.</p>}
            {rows.map((row, index) => (
              <div key={index} className="grid grid-cols-[1fr_1fr_auto] items-center gap-2">
                <div className="grid gap-1">
                  <Label htmlFor={`meta-key-${index}`} className="sr-only">
                    Key
                  </Label>
                  <Input
                    id={`meta-key-${index}`}
                    value={row.key}
                    placeholder="key"
                    onChange={(event) => updateRow(index, { key: event.target.value })}
                    className="font-mono text-xs"
                  />
                </div>
                <div className="grid gap-1">
                  <Label htmlFor={`meta-value-${index}`} className="sr-only">
                    Value
                  </Label>
                  <Input
                    id={`meta-value-${index}`}
                    value={row.value}
                    placeholder="value"
                    onChange={(event) => updateRow(index, { value: event.target.value })}
                    className="font-mono text-xs"
                  />
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  aria-label={`Delete metadata ${row.key || "row"}`}
                  onClick={() => removeRow(index)}
                >
                  <X className="h-4 w-4" />
                </Button>
              </div>
            ))}
            <Button type="button" variant="outline" size="sm" onClick={addRow}>
              <Plus />
              Add key/value
            </Button>
          </div>
        )}
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            type="button"
            disabled={!changed || updateMutation.isPending}
            onClick={() => updateMutation.mutate(rows)}
          >
            <Save />
            Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export type { SeriesFile };
