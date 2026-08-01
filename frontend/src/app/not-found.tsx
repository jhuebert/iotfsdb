import { Link } from "react-router";
import { Button } from "@/components/ui/button";

export function NotFoundPage() {
  return (
    <div className="flex min-h-[50vh] flex-col items-center justify-center gap-4 text-center">
      <p className="text-6xl font-bold text-muted-foreground">404</p>
      <p className="text-muted-foreground">This page does not exist.</p>
      <Button asChild>
        <Link to="/series">Back to Series</Link>
      </Button>
    </div>
  );
}
