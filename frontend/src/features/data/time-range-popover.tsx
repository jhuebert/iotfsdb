import { CalendarClock } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { DATE_TIME_PRESETS, DEFAULT_PRESET, type DateTimePreset } from "@/lib/constants";
import { cn } from "@/lib/utils";

interface TimeRangePopoverProps {
  preset: DateTimePreset | "";
  from?: string;
  to?: string;
  tz: string;
  onChange: (patch: { preset: DateTimePreset | ""; from?: string; to?: string }) => void;
  disabled?: boolean;
}

export function TimeRangePopover({ preset, from, to, tz, onChange, disabled }: TimeRangePopoverProps) {
  function selectPreset(value: DateTimePreset) {
    onChange({ preset: value, from: undefined, to: undefined });
  }

  function selectCustom() {
    onChange({ preset: "", from: from ?? "", to: to ?? "" });
  }

  const label = preset ? (DATE_TIME_PRESETS.find((p) => p.value === preset)?.label ?? preset) : "Custom";

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="w-full justify-start" disabled={disabled}>
          <CalendarClock className="text-muted-foreground" />
          <span>{label}</span>
          <span className="ml-auto text-xs text-muted-foreground">{tz}</span>
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-80">
        <div className="grid grid-cols-4 gap-1.5">
          {DATE_TIME_PRESETS.map((item) => (
            <button
              key={item.value}
              type="button"
              onClick={() => selectPreset(item.value)}
              className={cn(
                "cursor-pointer rounded-md border px-2 py-1.5 text-xs font-medium transition-colors",
                preset === item.value ? "border-primary bg-primary/10 text-primary" : "border-border hover:bg-accent",
              )}
            >
              {item.label}
            </button>
          ))}
        </div>
        <div className="mt-3 space-y-2 border-t border-border pt-3">
          <Button type="button" variant="secondary" size="sm" className="w-full" onClick={selectCustom}>
            Custom range
          </Button>
          {!preset && (
            <>
              <div className="grid gap-1.5">
                <Label htmlFor="range-from">From</Label>
                <Input
                  id="range-from"
                  type="datetime-local"
                  value={from ?? ""}
                  onChange={(event) => onChange({ preset: "", from: event.target.value, to: to ?? "" })}
                />
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="range-to">To</Label>
                <Input
                  id="range-to"
                  type="datetime-local"
                  value={to ?? ""}
                  onChange={(event) => onChange({ preset: "", from: from ?? "", to: event.target.value })}
                />
              </div>
            </>
          )}
          {preset === DEFAULT_PRESET && (
            <p className="text-xs text-muted-foreground">
              Relative ranges are evaluated against the current time in {tz}.
            </p>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}
