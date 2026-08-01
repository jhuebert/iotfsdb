import { useMemo, useState } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { formatDate, formatNumber } from "@/lib/format";
import type { NumberType } from "@/lib/constants";

const PAGE_SIZE = 100;

export interface TableSeries {
  id: string;
  type?: NumberType;
  values: (number | null)[];
}

interface ValuesTableProps {
  labels: (string | Date)[];
  series: TableSeries[];
}

export function ValuesTable({ labels, series }: ValuesTableProps) {
  const [page, setPage] = useState(0);
  const pageCount = Math.max(1, Math.ceil(labels.length / PAGE_SIZE));
  const safePage = Math.min(page, pageCount - 1);

  const pageLabels = useMemo(() => labels.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE), [labels, safePage]);

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-sm font-medium">Values</p>
        <div className="flex items-center gap-1 text-xs text-muted-foreground">
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6"
            disabled={safePage === 0}
            onClick={() => setPage((p) => Math.max(0, p - 1))}
            aria-label="Previous page"
          >
            <ChevronLeft className="h-3.5 w-3.5" />
          </Button>
          <span>
            {safePage + 1} / {pageCount} ({labels.length} rows)
          </span>
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6"
            disabled={safePage >= pageCount - 1}
            onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
            aria-label="Next page"
          >
            <ChevronRight className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>
      <div className="max-h-80 overflow-auto rounded-md border scrollbar-thin">
        <Table className="font-mono text-xs">
          <TableHeader className="sticky top-0 bg-background">
            <TableRow>
              <TableHead className="min-w-[150px]">Time</TableHead>
              {series.map((s) => (
                <TableHead key={s.id} className="min-w-[110px] text-right">
                  {s.id}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {pageLabels.map((label, index) => {
              const rowIndex = safePage * PAGE_SIZE + index;
              return (
                <TableRow key={rowIndex}>
                  <TableCell className="text-muted-foreground">{formatDate(label)}</TableCell>
                  {series.map((s) => (
                    <TableCell key={s.id} className="text-right tabular-nums">
                      {formatNumber(s.values[rowIndex], s.type)}
                    </TableCell>
                  ))}
                </TableRow>
              );
            })}
            {pageLabels.length === 0 && (
              <TableRow>
                <TableCell colSpan={series.length + 1} className="h-16 text-center text-muted-foreground">
                  No data in this range.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
