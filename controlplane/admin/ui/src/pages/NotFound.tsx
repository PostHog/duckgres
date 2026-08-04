import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";

export function NotFound() {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-4 p-12 text-center">
      <p className="font-mono text-5xl font-semibold text-muted-foreground">404</p>
      <p className="text-sm text-muted-foreground">This page does not exist.</p>
      <Button asChild variant="outline" size="sm">
        <Link to="/">Back to overview</Link>
      </Button>
    </div>
  );
}
