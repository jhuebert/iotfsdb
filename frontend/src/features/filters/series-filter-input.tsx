import { useMemo, useRef, useState } from "react";
import { CircleHelp, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { parseSearch, stringifyFilter } from "@/lib/search-parser";
import { cn } from "@/lib/utils";
import { useMetadataKeys } from "./use-series-search";

interface SeriesFilterInputProps {
  value: string;
  onChange: (q: string) => void;
  placeholder?: string;
  disabled?: boolean;
  autoFocus?: boolean;
  className?: string;
}

const GRAMMAR_ROWS: [string, string][] = [
  ["temp.*", "Series ID regex pattern (at most one)"],
  ["room:kitchen", "Metadata filter — key exact match, value regex"],
  ['room:"kitchen floor"', 'Quoted when a key/value contains spaces or : " \\'],
];

/**
 * The shared filter input: a single text box written in the mini-language
 * (§6.5) plus removable metadata chips, inline parse errors with position,
 * metadata-key autocomplete, and a grammar help popover.
 */
export function SeriesFilterInput({
  value,
  onChange,
  placeholder = "temp.* room:kitchen",
  disabled,
  autoFocus,
  className,
}: SeriesFilterInputProps) {
  const [focused, setFocused] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const { filter, error } = useMemo(() => parseSearch(value), [value]);

  const { data: metadataKeys = [] } = useMetadataKeys();

  // Suggest metadata keys while the last token has no metadata separator yet.
  const suggestions = useMemo(() => {
    if (!focused || error) {
      return [];
    }
    const lastToken = value.trimEnd().split(/\s+/).pop() ?? "";
    if (lastToken.includes(":")) {
      return [];
    }
    const prefix = lastToken.toLowerCase();
    return metadataKeys.filter((key) => key.toLowerCase().includes(prefix)).slice(0, 8);
  }, [focused, error, value, metadataKeys]);

  function applySuggestion(key: string) {
    const trimmed = value.trimEnd();
    const parts = trimmed.split(/\s+/);
    if (parts.length === 0 || parts[parts.length - 1].includes(":")) {
      parts.push(`${key}:`);
    } else {
      parts[parts.length - 1] = `${key}:`;
    }
    onChange(parts.join(" "));
    inputRef.current?.focus();
  }

  function removeMetadataKey(key: string) {
    const next = { ...filter.metadata };
    delete next[key];
    onChange(stringifyFilter({ pattern: filter.pattern, metadata: next }));
  }

  return (
    <div className={cn("flex flex-col gap-1.5", className)}>
      <div className="relative flex items-center gap-1.5">
        <div className="relative flex-1">
          <Input
            ref={inputRef}
            value={value}
            onChange={(event) => onChange(event.target.value)}
            placeholder={placeholder}
            disabled={disabled}
            autoFocus={autoFocus}
            aria-label="Series filter"
            aria-invalid={!!error}
            className={cn("pr-9 font-mono text-xs", error && "border-destructive focus-visible:ring-destructive")}
            onFocus={() => setFocused(true)}
            onBlur={() => setFocused(false)}
          />
          <Popover open={suggestions.length > 0} onOpenChange={() => undefined}>
            <PopoverTrigger asChild>
              <button type="button" className="sr-only" tabIndex={-1} aria-hidden />
            </PopoverTrigger>
            <PopoverContent align="start" className="w-56 p-1" onOpenAutoFocus={(e) => e.preventDefault()}>
              <div className="px-2 py-1 text-xs font-medium text-muted-foreground">Metadata keys</div>
              {suggestions.map((key) => (
                <button
                  key={key}
                  type="button"
                  className="flex w-full cursor-pointer items-center rounded-sm px-2 py-1 text-left font-mono text-xs hover:bg-accent"
                  onMouseDown={(event) => {
                    event.preventDefault();
                    applySuggestion(key);
                  }}
                >
                  {key}
                </button>
              ))}
            </PopoverContent>
          </Popover>
        </div>
        <Popover>
          <PopoverTrigger asChild>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-9 w-9 shrink-0"
              aria-label="Filter syntax help"
            >
              <CircleHelp className="h-4 w-4" />
            </Button>
          </PopoverTrigger>
          <PopoverContent align="end" className="w-80">
            <div className="mb-2 text-sm font-medium">Filter syntax</div>
            <table className="w-full text-xs">
              <tbody>
                {GRAMMAR_ROWS.map(([token, meaning]) => (
                  <tr key={token} className="border-b border-border last:border-0">
                    <td className="py-1.5 pr-3 font-mono text-primary">{token}</td>
                    <td className="py-1.5 text-muted-foreground">{meaning}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-2 text-xs text-muted-foreground">
              Every token must match (AND). Quoting: <code className="font-mono">\"</code> and{" "}
              <code className="font-mono">\\</code> inside quotes.
            </p>
          </PopoverContent>
        </Popover>
      </div>

      {Object.keys(filter.metadata).length > 0 && (
        <div className="flex flex-wrap items-center gap-1.5">
          {Object.entries(filter.metadata).map(([key, metaValue]) => (
            <Badge key={key} variant="secondary" className="gap-1 py-0.5 pl-2 pr-1 font-mono text-[11px]">
              {key}:{metaValue}
              <button
                type="button"
                aria-label={`Remove filter ${key}`}
                className="ml-0.5 cursor-pointer rounded-full hover:text-destructive"
                onMouseDown={(event) => {
                  event.preventDefault();
                  removeMetadataKey(key);
                }}
              >
                <X className="h-3 w-3" />
              </button>
            </Badge>
          ))}
        </div>
      )}

      {error && (
        <p className="text-xs text-destructive" role="alert">
          {error.message} at position {error.position + 1}
        </p>
      )}
    </div>
  );
}
