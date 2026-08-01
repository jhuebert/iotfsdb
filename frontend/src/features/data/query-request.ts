import { fromZonedTime } from "date-fns-tz";
import type { FindDataRequest, FindSeriesRequest } from "@/api/types";
import { patternForIds } from "@/api/series";
import { parseSearch, type ParseError, type SeriesFilter } from "@/lib/search-parser";
import type { DataUrlState } from "@/lib/url-state";

export interface DataQuery {
  request: FindDataRequest;
  filter: SeriesFilter;
  error?: ParseError;
}

/**
 * Converts UI/URL state into a FindDataRequest. The selected-series list is
 * expressed as an anchored alternation pattern so only the checked series are
 * queried.
 */
export function buildDataQuery(state: DataUrlState): DataQuery {
  const parsed = parseSearch(state.q);
  const filter = parsed.filter;
  if (parsed.error) {
    return { request: emptyRequest(filter), filter, error: parsed.error };
  }
  const pattern = state.sel.length > 0 ? patternForIds(state.sel) : filter.pattern || ".*";
  const series: FindSeriesRequest = { pattern, metadata: filter.metadata };

  const request: FindDataRequest = {
    series,
    timezone: state.tz,
    size: state.size,
    includeNull: true,
    useBigDecimal: false,
    usePrevious: state.usePrevious,
    timeReducer: state.timeReducer,
  };
  if (state.seriesReducer) {
    request.seriesReducer = state.seriesReducer;
  }
  if (state.nullValue !== undefined) {
    request.nullValue = state.nullValue;
  }
  if (state.preset) {
    request.dateTimePreset = state.preset;
  } else {
    if (state.from) {
      request.from = fromZonedTime(state.from, state.tz).toISOString();
    }
    if (state.to) {
      request.to = fromZonedTime(state.to, state.tz).toISOString();
    }
  }
  if (state.interval !== undefined) {
    request.interval = state.interval * 1000;
  }
  return { request, filter };
}

function emptyRequest(filter: SeriesFilter): FindDataRequest {
  return {
    series: { pattern: filter.pattern || ".*", metadata: filter.metadata },
    includeNull: true,
    timeReducer: "AVERAGE",
  };
}
