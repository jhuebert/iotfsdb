import { useDeferredValue } from "react";
import { useQuery } from "@tanstack/react-query";
import { findSeries } from "@/api/series";
import { parseSearch, type SeriesFilter } from "@/lib/search-parser";

/**
 * Debounced series search backed by POST /v2/series/find. The query is derived
 * from the shared mini-language string, so the box, the URL and the request
 * always describe the same filter.
 */
export function useSeriesSearch(q: string) {
  const deferredQ = useDeferredValue(q);
  const parsed = parseSearch(deferredQ);
  const filter: SeriesFilter = parsed.filter;

  const query = useQuery({
    queryKey: ["series", "find", filter.pattern, filter.metadata],
    queryFn: () =>
      findSeries({
        pattern: filter.pattern || ".*",
        metadata: filter.metadata,
      }),
    enabled: !parsed.error,
    staleTime: 15_000,
  });

  return {
    ...query,
    filter,
    parseError: parsed.error,
    hasQuery: !!(filter.pattern || Object.keys(filter.metadata).length > 0),
  };
}

/** Distinct metadata keys across all series (used for autocomplete suggestions). */
export function useMetadataKeys() {
  return useQuery({
    queryKey: ["series", "metadata-keys"],
    queryFn: async () => {
      const files = await findSeries({ pattern: ".*", metadata: {} });
      const keys = new Set<string>();
      for (const file of files) {
        for (const key of Object.keys(file.metadata)) {
          keys.add(key);
        }
      }
      return [...keys].sort();
    },
    staleTime: 5 * 60_000,
  });
}
