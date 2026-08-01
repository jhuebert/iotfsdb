import { useEffect, useState } from "react";
import { Gauge } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Separator } from "@/components/ui/separator";
import { INTERVAL_PRESETS } from "@/lib/constants";
import { formatInterval, parseInterval } from "@/lib/format";
import { cn } from "@/lib/utils";

interface SamplingControlsProps {
  interval?: number;
  size: number;
  maxQuerySize: number;
  onChange: (patch: { interval?: number; size: number }) => void;
  disabled?: boolean;
}

export function SamplingControls({ interval, size, maxQuerySize, onChange, disabled }: SamplingControlsProps) {
  const [customMs, setCustomMs] = useState("");

  useEffect(() => {
    if (interval !== undefined && !INTERVAL_PRESETS.some((p) => p.ms === interval)) {
      setCustomMs(String(interval));
    } else {
      setCustomMs("");
    }
  }, [interval]);

  function selectMs(ms: number | null) {
    if (ms === null) {
      onChange({ interval: undefined, size });
      return;
    }
    onChange({ interval: ms, size });
  }

  function applyCustom(raw: string) {
    const ms = parseInterval(raw);
    if (ms && ms > 0) {
      onChange({ interval: ms, size });
    }
  }

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="w-full justify-start" disabled={disabled}>
          <Gauge className="text-muted-foreground" />
          <span>Sampling</span>
          <span className="ml-auto font-mono text-xs text-muted-foreground">
            {interval === undefined ? "auto" : formatInterval(interval)} · {size} pts
          </span>
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72">
        <Label className="mb-2 block">Interval</Label>
        <div className="grid grid-cols-3 gap-1.5">
          {INTERVAL_PRESETS.map((item) => (
            <button
              key={item.label}
              type="button"
              onClick={() => selectMs(item.ms)}
              className={cn(
                "cursor-pointer rounded-md border px-2 py-1.5 text-xs font-medium transition-colors",
                (item.ms === null && interval === undefined) || item.ms === interval
                  ? "border-primary bg-primary/10 text-primary"
                  : "border-border hover:bg-accent",
              )}
            >
              {item.label}
            </button>
          ))}
        </div>
        <div className="mt-2 flex items-center gap-2">
          <Input
            placeholder="Custom (e.g. 30s, 5m, 1h)"
            value={customMs}
            onChange={(event) => setCustomMs(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                applyCustom(customMs);
              }
            }}
            className="h-8 text-xs"
            aria-label="Custom interval"
          />
          <Button type="button" size="sm" variant="secondary" onClick={() => applyCustom(customMs)}>
            Apply
          </Button>
        </div>
        <Separator className="my-3" />
        <div className="grid gap-1.5">
          <Label htmlFor="query-size">Max points per series</Label>
          <Input
            id="query-size"
            type="number"
            min={1}
            max={maxQuerySize}
            value={size}
            onChange={(event) => {
              const value = parseInt(event.target.value, 10);
              if (!Number.isNaN(value) && value > 0) {
                onChange({ interval, size: value });
              }
            }}
            className="h-8"
          />
          <p className="text-xs text-muted-foreground">
            The server resamples results to at most {maxQuerySize} points.
          </p>
        </div>
      </PopoverContent>
    </Popover>
  );
}
