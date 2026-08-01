import { useState } from "react";
import { Link, NavLink, Outlet } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Cpu, FileJson, Lock, Moon, Plus, Sun } from "lucide-react";
import { useTheme } from "next-themes";
import { getUiConfig } from "@/api/series";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { CreateSeriesDialog } from "@/features/series/create-series-dialog";

const NAV_ITEMS = [
  { to: "/series", label: "Series" },
  { to: "/data", label: "Data" },
  { to: "/transfer", label: "Transfer" },
];

export function AppShell() {
  const { data: config } = useQuery({
    queryKey: ["ui", "config"],
    queryFn: getUiConfig,
    staleTime: Infinity,
  });
  const { resolvedTheme, setTheme } = useTheme();
  const [createOpen, setCreateOpen] = useState(false);

  return (
    <div className="flex min-h-dvh flex-col">
      <header className="sticky top-0 z-40 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80">
        <div className="mx-auto flex h-14 max-w-[1600px] items-center gap-6 px-4">
          <Link to="/" className="flex items-center gap-2 font-semibold tracking-tight">
            <Cpu className="h-5 w-5 text-primary" />
            <span>iotfsdb</span>
            {config && <span className="text-xs font-normal text-muted-foreground">v{config.version}</span>}
          </Link>

          <nav className="flex items-center gap-1">
            {NAV_ITEMS.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  cn(
                    "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                    isActive ? "bg-accent text-accent-foreground" : "text-muted-foreground hover:text-foreground",
                  )
                }
              >
                {item.label}
              </NavLink>
            ))}
            {config?.springdocEnabled && (
              <a
                href="/swagger-ui/index.html"
                className="flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
              >
                <FileJson className="h-3.5 w-3.5" />
                API
              </a>
            )}
          </nav>

          <div className="ml-auto flex items-center gap-2">
            {config?.readOnly && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Badge variant="secondary" className="gap-1">
                    <Lock className="h-3 w-3" />
                    Read-only
                  </Badge>
                </TooltipTrigger>
                <TooltipContent>Write operations are disabled on this server</TooltipContent>
              </Tooltip>
            )}
            {!config?.readOnly && (
              <Button size="sm" onClick={() => setCreateOpen(true)}>
                <Plus />
                Create Series
              </Button>
            )}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon" aria-label="Toggle theme">
                  {resolvedTheme === "dark" ? <Moon className="h-4 w-4" /> : <Sun className="h-4 w-4" />}
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={() => setTheme("light")}>Light</DropdownMenuItem>
                <DropdownMenuItem onClick={() => setTheme("dark")}>Dark</DropdownMenuItem>
                <DropdownMenuItem onClick={() => setTheme("system")}>System</DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>
      </header>

      <main className="mx-auto w-full max-w-[1600px] flex-1 px-4 py-6">
        <Outlet />
      </main>

      <CreateSeriesDialog open={createOpen} onOpenChange={setCreateOpen} />
    </div>
  );
}
