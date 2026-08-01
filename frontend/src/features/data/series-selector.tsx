import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import { colorForIndex } from "@/components/chart/chart-colors";
import type { SeriesFile } from "@/api/types";
import { cn } from "@/lib/utils";

interface SeriesSelectorProps {
  files: SeriesFile[];
  selected: Set<string>;
  onToggle: (id: string) => void;
  onSelectAll: () => void;
  onSelectNone: () => void;
  disabled?: boolean;
}

export function SeriesSelector({
  files,
  selected,
  onToggle,
  onSelectAll,
  onSelectNone,
  disabled,
}: SeriesSelectorProps) {
  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between px-1 pb-2">
        <span className="text-xs font-medium text-muted-foreground">
          {selected.size} / {files.length} selected
        </span>
        <div className="flex gap-2 text-xs">
          <button
            type="button"
            className="cursor-pointer text-primary hover:underline"
            onClick={onSelectAll}
            disabled={disabled}
          >
            All
          </button>
          <button
            type="button"
            className="cursor-pointer text-primary hover:underline"
            onClick={onSelectNone}
            disabled={disabled}
          >
            None
          </button>
        </div>
      </div>
      <ScrollArea className="min-h-0 flex-1">
        <div className="space-y-0.5">
          {files.map((file, index) => {
            const id = file.definition.id;
            const isSelected = selected.has(id);
            return (
              <label
                key={id}
                className={cn(
                  "flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-accent",
                  isSelected && "bg-accent/60",
                )}
              >
                <Checkbox
                  checked={isSelected}
                  disabled={disabled}
                  onCheckedChange={() => onToggle(id)}
                  aria-label={`Select ${id}`}
                />
                <span
                  className="h-2 w-2 shrink-0 rounded-full"
                  style={{ backgroundColor: colorForIndex(index) }}
                  aria-hidden
                />
                <span className="truncate font-mono text-xs">{id}</span>
              </label>
            );
          })}
          {files.length === 0 && (
            <p className="px-2 py-6 text-center text-xs text-muted-foreground">
              No matching series. Adjust the filter above.
            </p>
          )}
        </div>
      </ScrollArea>
    </div>
  );
}
