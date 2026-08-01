import { useEffect, useRef } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { z } from "zod";
import { createSeries, getUiConfig } from "@/api/series";
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
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { NUMBER_TYPES, PARTITION_PERIODS, RANGE_TYPES, type NumberType, type PartitionPeriod } from "@/lib/constants";

const idPattern = /^[a-z0-9][a-z0-9._-]{0,127}$/;

/** Optional number that treats an empty form value as undefined. */
function optionalNumber() {
  return z
    .union([z.literal(""), z.coerce.number()])
    .optional()
    .transform((value) => (value === "" || value === undefined ? undefined : value));
}

const seriesSchema = z
  .object({
    id: z.string().regex(idPattern, "Lowercase letters, digits, and . _ - (starts with a letter or digit)"),
    type: z.enum(NUMBER_TYPES),
    interval: z.coerce.number().int().positive("Interval must be a positive number of milliseconds"),
    partition: z.enum(PARTITION_PERIODS),
    min: optionalNumber(),
    max: optionalNumber(),
  })
  .superRefine((values, ctx) => {
    const isRangeType = RANGE_TYPES.has(values.type);
    if ((values.min === undefined) !== (values.max === undefined)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["min"],
        message: "Minimum and maximum must be provided together",
      });
    } else if (isRangeType && values.min !== undefined && values.max !== undefined && values.min >= values.max) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["max"], message: "Maximum must be greater than minimum" });
    }
    if (!isRangeType && (values.min !== undefined || values.max !== undefined)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["min"],
        message: "Minimum/maximum are only supported for MAPPED and CURVED types",
      });
    }
  });

type SeriesFormInput = z.input<typeof seriesSchema>;
type SeriesFormValues = z.output<typeof seriesSchema>;

interface CreateSeriesDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function CreateSeriesDialog({ open, onOpenChange }: CreateSeriesDialogProps) {
  const queryClient = useQueryClient();
  const { data: config } = useQuery({ queryKey: ["ui", "config"], queryFn: getUiConfig, staleTime: Infinity });

  const defaults = config?.defaultSeries?.definition;
  const form = useForm<SeriesFormInput, unknown, SeriesFormValues>({
    resolver: zodResolver(seriesSchema),
    defaultValues: {
      id: "",
      type: (defaults?.type as NumberType) ?? "FLOAT4",
      interval: defaults?.interval ?? 60_000,
      partition: (defaults?.partition as PartitionPeriod) ?? "MONTH",
      min: undefined,
      max: undefined,
    },
  });

  // Reset the form once per open transition. Resetting again when the config
  // arrives (or on any later re-render) would wipe whatever the user is typing.
  const prevOpen = useRef(false);
  useEffect(() => {
    if (open && !prevOpen.current) {
      form.reset({
        id: "",
        type: (defaults?.type as NumberType) ?? "FLOAT4",
        interval: defaults?.interval ?? 60_000,
        partition: (defaults?.partition as PartitionPeriod) ?? "MONTH",
        min: undefined,
        max: undefined,
      });
    }
    prevOpen.current = open;
  }, [open, defaults, form]);

  const watchType = form.watch("type");
  const isRangeType = RANGE_TYPES.has(watchType);

  const createMutation = useMutation({
    mutationFn: (values: SeriesFormValues) =>
      createSeries({
        definition: {
          id: values.id,
          type: values.type,
          interval: values.interval,
          partition: values.partition,
          ...(values.min !== undefined ? { min: values.min } : {}),
          ...(values.max !== undefined ? { max: values.max } : {}),
        },
        metadata: {},
      }),
    onSuccess: (_data, values) => {
      toast.success(`Series "${values.id}" created`);
      queryClient.invalidateQueries({ queryKey: ["series"] });
      onOpenChange(false);
    },
    onError: (error: Error) => {
      toast.error(`Could not create series: ${error.message}`);
    },
  });

  function onSubmit(values: SeriesFormValues) {
    createMutation.mutate(values);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Create Series</DialogTitle>
          <DialogDescription>Define a new time series. The definition is immutable once created.</DialogDescription>
        </DialogHeader>
        <form onSubmit={form.handleSubmit(onSubmit)} className="grid gap-4">
          <div className="grid gap-2">
            <Label htmlFor="series-id">ID</Label>
            <Input
              id="series-id"
              placeholder="temperature.room1"
              {...form.register("id")}
              aria-invalid={!!form.formState.errors.id}
            />
            {form.formState.errors.id && <p className="text-xs text-destructive">{form.formState.errors.id.message}</p>}
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="grid gap-2">
              <Label htmlFor="series-type">Number Type</Label>
              <Select
                value={form.watch("type")}
                onValueChange={(value) => form.setValue("type", value as NumberType, { shouldValidate: true })}
              >
                <SelectTrigger id="series-type">
                  <SelectValue placeholder="Type" />
                </SelectTrigger>
                <SelectContent>
                  {NUMBER_TYPES.map((type) => (
                    <SelectItem key={type} value={type}>
                      {type}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="series-partition">Partition</Label>
              <Select
                value={form.watch("partition")}
                onValueChange={(value) => form.setValue("partition", value as PartitionPeriod)}
              >
                <SelectTrigger id="series-partition">
                  <SelectValue placeholder="Partition" />
                </SelectTrigger>
                <SelectContent>
                  {PARTITION_PERIODS.map((partition) => (
                    <SelectItem key={partition} value={partition}>
                      {partition}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <div className="grid gap-2">
            <Label htmlFor="series-interval">Interval (ms)</Label>
            <Input id="series-interval" type="number" min={1} {...form.register("interval")} />
            {form.formState.errors.interval && (
              <p className="text-xs text-destructive">{form.formState.errors.interval.message}</p>
            )}
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="grid gap-2">
              <Label htmlFor="series-min" className={!isRangeType ? "opacity-50" : undefined}>
                Minimum
              </Label>
              <Input id="series-min" type="number" disabled={!isRangeType} placeholder="—" {...form.register("min")} />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="series-max" className={!isRangeType ? "opacity-50" : undefined}>
                Maximum
              </Label>
              <Input id="series-max" type="number" disabled={!isRangeType} placeholder="—" {...form.register("max")} />
            </div>
          </div>
          {form.formState.errors.min && <p className="text-xs text-destructive">{form.formState.errors.min.message}</p>}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={createMutation.isPending}>
              {createMutation.isPending ? "Creating…" : "Create"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
