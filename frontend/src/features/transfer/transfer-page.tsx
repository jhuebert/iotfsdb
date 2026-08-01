import { useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Download, FileArchive, Lock, UploadCloud } from "lucide-react";
import { toast } from "sonner";
import { exportData, importData } from "@/api/data";
import { getUiConfig } from "@/api/series";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { cn } from "@/lib/utils";

export function TransferPage() {
  const { data: config } = useQuery({ queryKey: ["ui", "config"], queryFn: getUiConfig, staleTime: Infinity });
  const readOnly = config?.readOnly ?? true;
  const [exporting, setExporting] = useState(false);
  const [exportProgress, setExportProgress] = useState(0);
  const [dragging, setDragging] = useState(false);
  const [importing, setImporting] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  async function handleExport() {
    setExporting(true);
    setExportProgress(0);
    const timer = setInterval(() => {
      setExportProgress((p) => Math.min(90, p + 8));
    }, 300);
    try {
      await exportData({ pattern: ".*" });
      setExportProgress(100);
      toast.success("Database export started");
    } catch (error) {
      toast.error(`Export failed: ${(error as Error).message}`);
    } finally {
      clearInterval(timer);
      setTimeout(() => {
        setExporting(false);
        setExportProgress(0);
      }, 800);
    }
  }

  async function handleFile(file: File | undefined | null) {
    if (!file) {
      return;
    }
    if (!file.name.endsWith(".zip")) {
      toast.error("Import file must be a zip archive");
      return;
    }
    setImporting(true);
    try {
      await importData(file);
      toast.success(`Imported "${file.name}"`);
    } catch (error) {
      toast.error(`Import failed: ${(error as Error).message}`);
    } finally {
      setImporting(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  }

  return (
    <div className="mx-auto max-w-3xl space-y-6">
      <h1 className="text-xl font-semibold tracking-tight">Transfer</h1>

      <Card>
        <CardHeader>
          <CardTitle>Export database</CardTitle>
          <CardDescription>Download a zip archive of every series and all of its data.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <Button onClick={handleExport} disabled={exporting}>
            <Download />
            {exporting ? "Exporting…" : "Export Database"}
          </Button>
          {exporting && <Progress value={exportProgress} className="max-w-sm" />}
        </CardContent>
      </Card>

      <Card className={cn(readOnly && "opacity-60")}>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            Import archive
            {readOnly && <Lock className="h-4 w-4 text-muted-foreground" />}
          </CardTitle>
          <CardDescription>
            Restore data from a previously exported zip archive. Existing data is overwritten.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {readOnly ? (
            <p className="text-sm text-muted-foreground">Import is disabled on this read-only server.</p>
          ) : (
            <>
              <div
                role="button"
                tabIndex={0}
                aria-label="Import zip archive"
                onDragOver={(event) => {
                  event.preventDefault();
                  setDragging(true);
                }}
                onDragLeave={() => setDragging(false)}
                onDrop={(event) => {
                  event.preventDefault();
                  setDragging(false);
                  void handleFile(event.dataTransfer.files?.[0]);
                }}
                onClick={() => fileInputRef.current?.click()}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    fileInputRef.current?.click();
                  }
                }}
                className={cn(
                  "flex cursor-pointer flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed p-10 text-center transition-colors",
                  dragging ? "border-primary bg-primary/5" : "border-border hover:border-primary/50",
                )}
              >
                <FileArchive className="h-8 w-8 text-muted-foreground" />
                <p className="text-sm font-medium">
                  {importing ? "Importing…" : "Drop a zip archive here or click to browse"}
                </p>
                <p className="text-xs text-muted-foreground">
                  <UploadCloud className="mr-1 inline h-3 w-3" />
                  Exported archives from iotfsdb
                </p>
              </div>
              <input
                ref={fileInputRef}
                type="file"
                accept=".zip,application/zip"
                className="hidden"
                onChange={(event) => void handleFile(event.target.files?.[0])}
              />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
