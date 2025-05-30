package org.huebert.iotfsdb.mcp;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindDataResponse;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.Reducer;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.DataService;
import org.huebert.iotfsdb.service.QueryService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
public class McpTools {

    private static final int MAX_DATA_POINTS = 10;

    private final DataService dataService;

    private final QueryService queryService;

    public McpTools(DataService dataService, QueryService queryService) {
        this.dataService = dataService;
        this.queryService = queryService;
    }

    @CaptureStats(
        id = "mcp-series-search",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "mcp"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "search"),
            @CaptureStats.Metadata(key = "method", value = "mcp"),
        }
    )
    @Tool(
        name = "find-time-series",
        description = """
            Discovers available time series by searching through series IDs and metadata.
            RECOMMENDED FIRST STEP: Use this tool first to identify relevant time series before fetching data.
            
            WHAT IT RETURNS:
            - List of series objects containing seriesId and seriesMetadata
            - Each seriesId is unique and used as input for fetch-time-series-data
            - Metadata provides context about what each series measures (e.g., measurement type, location, units)
            
            WORKFLOW: First use this tool to find relevant seriesIds, then use those IDs with fetch-time-series-data.
            """,
        resultConverter = JsonConverter.class
    )
    public List<SeriesMcpResponse> searchSeries(
        @ToolParam(
            description = """
                Search terms to filter time series.
                - A series matches if any term appears in the series ID or metadata (OR logic)
                - Case-insensitive matching (e.g., 'Temp' will match 'temperature')
                - Partial word matching is supported (e.g., 'temp' will match 'temperature')
                """
        )
        Set<String> searchTerms
    ) {
        if (searchTerms == null || searchTerms.isEmpty()) {
            return List.of();
        }
        Set<String> terms = searchTerms.stream()
            .filter(a -> !a.isBlank())
            .map(String::toLowerCase)
            .collect(Collectors.toUnmodifiableSet());
        Predicate<String> matchesPattern = test ->
            test != null && terms.stream().anyMatch(test.toLowerCase()::contains);
        return dataService.getSeries().stream()
            .filter(seriesFile -> matchesPattern.test(seriesFile.getId()) ||
                seriesFile.getMetadata().values().stream().anyMatch(matchesPattern))
            .map(SeriesMcpResponse::new)
            .collect(Collectors.toList());
    }

    @CaptureStats(
        id = "mcp-data-search",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "mcp"),
            @CaptureStats.Metadata(key = "type", value = "data"),
            @CaptureStats.Metadata(key = "operation", value = "search"),
            @CaptureStats.Metadata(key = "method", value = "mcp"),
        }
    )
    @Tool(
        name = "fetch-time-series-data",
        description = """
            Retrieves actual time series data points within a specified time range.
            PREREQUISITE: You should first call find-time-series to identify relevant seriesIds.
            
            LIMITATIONS:
            Each series is limited to return a maximum of 10 data points to ensure efficient performance and prevent excessive memory usage.
            If the requested time range would result in more than this limit based on the series data point interval, the system will automatically downsample the data points to fit within the limit using averaging.
            If you require precise, unaltered values for a series, consider narrowing the time range so that the limit is not exceeded and downsampling does not occur.
            
            WHAT IT RETURNS:
            - List of data objects, each containing:
              - seriesId: The identifier of the time series
              - seriesData: Array of {dateTime, dataValue} pairs representing the actual measurements
            
            TIME RANGE GUIDANCE:
            - Start with smaller time ranges (hours to days) and expand if needed
            - Always ensure startDateTime is chronologically before endDateTime
            
            COMMON ERRORS:
            - Invalid seriesIds: Verify IDs using find-time-series first
            - No data in range: Try expanding the time range or check different series
            
            WORKFLOW EXAMPLE:
            1. Use get-current-time to find current time
            2. Calculate a start time (e.g., 24 hours earlier)
            3. Fetch data with appropriate seriesIds obtained from find-time-series
            """,
        resultConverter = JsonConverter.class
    )
    public List<DataMcpResponse> searchData(
        @ToolParam(
            description = """
                Start timestamp in ISO-8601 format with timezone.
                - Specifies the beginning of the time range (inclusive)
                - Must include timezone offset (e.g., Z for UTC, -05:00 for EST)
                - Must be chronologically before endDateTime
                
                FORMATS ACCEPTED:
                - Full ISO-8601: '2023-01-01T00:00:00-05:00'
                - UTC time: '2023-01-01T05:00:00Z'
                
                TIPS:
                - For relative times, use get-current-time and subtract the desired duration
                - For historical data, use explicit dates
                - Precision to the second is sufficient
                """
        )
        ZonedDateTime startDateTime,
        @ToolParam(
            description = """
                End timestamp in ISO-8601 format with timezone.
                - Specifies the end of the time range (inclusive)
                - Must include timezone offset (e.g., Z for UTC, -05:00 for EST)
                - Must be chronologically after startDateTime
                - For current data, use get-current-time
                
                FORMATS ACCEPTED:
                - Full ISO-8601: '2023-01-01T00:00:00-05:00'
                - UTC time: '2023-01-01T05:00:00Z'
                
                TIPS:
                - For "up to now" queries, use get-current-time for this parameter
                - Timezone should typically match startDateTime for consistency
                - System will align timestamps to the same timezone internally
                """
        )
        ZonedDateTime endDateTime,
        @ToolParam(
            description = """
                Set of series IDs to fetch data for.
                - Each ID must be a string matching a seriesId from find-time-series
                - Invalid IDs will be ignored (no error, but no data returned)
                - Order of IDs doesn't matter
                
                PERFORMANCE TIPS:
                - Request only the series you need for analysis
                - Limit to 1-5 series per request for best performance
                - For comparing many series, consider multiple focused requests
                """
        )
        Set<String> seriesIds,
        @ToolParam(
            description = """
                Time reducer to apply to the data points.
                - Determines how data points are aggregated (e.g., AVERAGE, MAXIMUM, etc.)
                - If not specified, defaults to no AVERAGE
                - Valid values: AVERAGE, COUNT, COUNT_DISTINCT, FIRST, LAST, MAXIMUM, MEDIAN, MINIMUM, MODE, MULTIPLY, SQUARE_SUM, SUM
                NOTE: If a series has more data points in the specified time range than the limit, it will be downsampled using this reducer.
                """
        )
        Reducer timeReducer
    ) {
        FindDataRequest request = new FindDataRequest();
        request.setFrom(startDateTime);
        request.setTo(endDateTime);
        request.setTimezone(TimeZone.getTimeZone(startDateTime.getZone()));
        request.setTimeReducer(timeReducer);
        request.setSize(MAX_DATA_POINTS);

        String seriesPattern = seriesIds.stream()
            .filter(id -> id != null && !id.isBlank())
            .map(Pattern::quote)
            .collect(Collectors.joining("|"));
        if (seriesPattern.isBlank()) {
            return List.of();
        }
        FindSeriesRequest seriesRequest = new FindSeriesRequest();
        seriesRequest.setPattern(Pattern.compile(seriesPattern));
        request.setSeries(seriesRequest);

        return queryService.findData(request).stream()
            .map(DataMcpResponse::new)
            .collect(Collectors.toList());
    }

    @CaptureStats(
        id = "mcp-current-time-get",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "mcp"),
            @CaptureStats.Metadata(key = "type", value = "time"),
            @CaptureStats.Metadata(key = "operation", value = "get"),
            @CaptureStats.Metadata(key = "method", value = "mcp"),
        }
    )
    @Tool(
        name = "get-current-time",
        description = """
            Returns the current server time as a ZonedDateTime in ISO-8601 format.
            
            WHAT IT RETURNS:
            - Current timestamp with timezone information (e.g., '2023-07-24T15:30:45.123456789-05:00')
            - Format is fully compatible with startDateTime/endDateTime parameters
            
            COMMON USES:
            - Create relative time ranges (e.g., "from 24 hours ago until now")
            - Get current server time to ensure time synchronization
            - Use as endDateTime for "up to current time" queries
            
            USAGE EXAMPLES:
            1. For last 24 hours:
               - endDateTime = get-current-time()
               - startDateTime = endDateTime minus 24 hours
            2. For last week:
               - endDateTime = get-current-time()
               - startDateTime = endDateTime minus 7 days
            3. For today only:
               - current = get-current-time()
               - startDateTime = current with time set to 00:00:00
               - endDateTime = current
            """,
        resultConverter = JsonConverter.class
    )
    public ZonedDateTime getCurrentDateTime() {
        return ZonedDateTime.now();
    }

    @Data
    public static class SeriesMcpResponse {
        private final String seriesId;
        private final Duration dataPointInterval;
        private final Map<String, String> seriesMetadata;

        public SeriesMcpResponse(SeriesFile seriesFile) {
            this.seriesId = seriesFile.getId();
            this.dataPointInterval = seriesFile.getDefinition().getIntervalDuration();
            this.seriesMetadata = seriesFile.getMetadata();
        }
    }

    @Data
    public static class DataMcpResponse {
        private final String seriesId;
        private final SortedMap<ZonedDateTime, Number> seriesData;

        public DataMcpResponse(FindDataResponse findDataResponse) {
            this.seriesId = findDataResponse.getSeries().getId();
            this.seriesData = findDataResponse.getData().stream()
                .collect(Collectors.toMap(
                    SeriesData::getTime,
                    SeriesData::getValue,
                    (a, b) -> b,
                    TreeMap::new
                ));
        }
    }

}
