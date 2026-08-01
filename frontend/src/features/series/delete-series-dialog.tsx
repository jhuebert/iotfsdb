import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { deleteSeries } from "@/api/series";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

interface DeleteSeriesDialogProps {
  seriesId: string | null;
  onOpenChange: (open: boolean) => void;
}

export function DeleteSeriesDialog({ seriesId, onOpenChange }: DeleteSeriesDialogProps) {
  const queryClient = useQueryClient();
  const open = seriesId !== null;

  const deleteMutation = useMutation({
    mutationFn: () => deleteSeries(seriesId!),
    onSuccess: () => {
      toast.success(`Series "${seriesId}" deleted`);
      queryClient.invalidateQueries({ queryKey: ["series"] });
      queryClient.invalidateQueries({ queryKey: ["data"] });
      onOpenChange(false);
    },
    onError: (error: Error) => toast.error(`Could not delete series: ${error.message}`),
  });

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Delete series</DialogTitle>
          <DialogDescription>
            Delete <span className="font-mono">{seriesId}</span> and all of its data? This cannot be undone.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            type="button"
            variant="destructive"
            disabled={deleteMutation.isPending}
            onClick={() => deleteMutation.mutate()}
          >
            {deleteMutation.isPending ? "Deleting…" : "Delete"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
