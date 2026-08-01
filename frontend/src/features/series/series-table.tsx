import { Download, KeyRound, Trash2 } from "lucide-react";
import type { SeriesFile } from "@/api/types";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { formatInterval, formatNumber } from "@/lib/format";

interface SeriesTableProps {
  series: SeriesFile[];
  readOnly: boolean;
  onExport: (id: string) => void;
  onEditMetadata: (id: string) => void;
  onDelete: (id: string) => void;
}

export function SeriesTable({ series, readOnly, onExport, onEditMetadata, onDelete }: SeriesTableProps) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-[28%]">ID</TableHead>
          <TableHead>Type</TableHead>
          <TableHead>Partition</TableHead>
          <TableHead>Interval</TableHead>
          <TableHead>Range</TableHead>
          <TableHead className="w-[30%]">Metadata</TableHead>
          <TableHead className="text-right">Actions</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {series.map((file) => {
          const { definition } = file;
          const range =
            definition.min !== undefined && definition.max !== undefined
              ? `${formatNumber(definition.min)} – ${formatNumber(definition.max)}`
              : "—";
          const metadataSummary = Object.entries(file.metadata)
            .slice(0, 3)
            .map(([key, value]) => `${key}=${value}`)
            .join(", ");
          return (
            <TableRow key={definition.id}>
              <TableCell className="font-mono font-medium">{definition.id}</TableCell>
              <TableCell className="font-mono text-xs">{definition.type}</TableCell>
              <TableCell>{definition.partition}</TableCell>
              <TableCell>{formatInterval(definition.interval)}</TableCell>
              <TableCell className="font-mono text-xs">{range}</TableCell>
              <TableCell className="max-w-[240px] truncate text-xs text-muted-foreground" title={metadataSummary}>
                {metadataSummary || "—"}
              </TableCell>
              <TableCell className="text-right">
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button variant="ghost" size="sm" aria-label={`Actions for ${definition.id}`}>
                      Actions
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem onSelect={() => onExport(definition.id)}>
                      <Download />
                      Export data
                    </DropdownMenuItem>
                    <DropdownMenuItem onSelect={() => onEditMetadata(definition.id)}>
                      <KeyRound />
                      Edit metadata
                    </DropdownMenuItem>
                    {!readOnly && (
                      <DropdownMenuItem className="text-destructive" onSelect={() => onDelete(definition.id)}>
                        <Trash2 />
                        Delete series
                      </DropdownMenuItem>
                    )}
                  </DropdownMenuContent>
                </DropdownMenu>
              </TableCell>
            </TableRow>
          );
        })}
        {series.length === 0 && (
          <TableRow>
            <TableCell colSpan={7} className="h-24 text-center text-muted-foreground">
              No series match the current filter.
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  );
}
