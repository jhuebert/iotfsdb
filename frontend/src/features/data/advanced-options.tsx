import { SlidersHorizontal } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { REDUCERS, type Reducer } from "@/lib/constants";

interface AdvancedOptionsProps {
  timeReducer: Reducer;
  seriesReducer?: Reducer;
  usePrevious: boolean;
  nullValue?: number;
  live: boolean;
  onChange: (
    patch: Partial<{
      timeReducer: Reducer;
      seriesReducer?: Reducer;
      usePrevious: boolean;
      nullValue?: number;
      live: boolean;
    }>,
  ) => void;
  disabled?: boolean;
}

export function AdvancedOptions({
  timeReducer,
  seriesReducer,
  usePrevious,
  nullValue,
  live,
  onChange,
  disabled,
}: AdvancedOptionsProps) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="w-full justify-start" disabled={disabled}>
          <SlidersHorizontal className="text-muted-foreground" />
          <span>Advanced</span>
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-80">
        <div className="space-y-4">
          <div className="grid gap-1.5">
            <Label htmlFor="time-reducer">Time reducer</Label>
            <Select value={timeReducer} onValueChange={(value) => onChange({ timeReducer: value as Reducer })}>
              <SelectTrigger id="time-reducer" className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {REDUCERS.map((reducer) => (
                  <SelectItem key={reducer} value={reducer}>
                    {reducer}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">
              Combines multiple values within a sampling interval into one.
            </p>
          </div>

          <div className="grid gap-1.5">
            <Label htmlFor="series-reducer">Series reducer</Label>
            <Select
              value={seriesReducer ?? "NONE"}
              onValueChange={(value) => onChange({ seriesReducer: value === "NONE" ? undefined : (value as Reducer) })}
            >
              <SelectTrigger id="series-reducer" className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="NONE">None</SelectItem>
                {REDUCERS.map((reducer) => (
                  <SelectItem key={reducer} value={reducer}>
                    {reducer}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">
              Produces a single "reduced" series from all selected series.
            </p>
          </div>

          <Separator />

          <div className="flex items-center justify-between">
            <div>
              <Label htmlFor="use-previous">Use previous value</Label>
              <p className="text-xs text-muted-foreground">Fill nulls with the last non-null value.</p>
            </div>
            <Switch
              id="use-previous"
              checked={usePrevious}
              onCheckedChange={(checked) => onChange({ usePrevious: checked })}
            />
          </div>

          <div className="grid gap-1.5">
            <Label htmlFor="null-value">Null value replacement</Label>
            <Input
              id="null-value"
              type="number"
              placeholder="None"
              value={nullValue ?? ""}
              onChange={(event) => {
                const raw = event.target.value;
                onChange({ nullValue: raw === "" ? undefined : Number(raw) });
              }}
              className="h-8"
            />
          </div>

          <Separator />

          <div className="flex items-center justify-between">
            <div>
              <Label htmlFor="live-refresh">Periodic refresh</Label>
              <p className="text-xs text-muted-foreground">Re-run the search every 15s.</p>
            </div>
            <Switch id="live-refresh" checked={live} onCheckedChange={(checked) => onChange({ live: checked })} />
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
