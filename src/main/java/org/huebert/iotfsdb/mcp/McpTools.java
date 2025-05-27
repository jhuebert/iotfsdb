package org.huebert.iotfsdb.mcp;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.service.DataService;
import org.huebert.iotfsdb.service.QueryService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
public class McpTools {

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
            - Metadata provides context about what each series measures (e.g., measurement type, location)
            
            COMMON USES:
            - Find temperature series: ['temp']
            - Find series tied to specific locations: ['bedroom', 'kitchen']
            - Find specific measurement types: ['humidity', 'pressure', 'power']
            - List all available series: [] (empty array)
            
            WORKFLOW: First use this tool to find relevant seriesIds, then use those IDs with fetch-time-series-data.
            """,
        resultConverter = JsonConverter.class
    )
    public List<SeriesMcpResponse> searchSeries(
        @ToolParam(
            description = """
                Optional search terms to filter time series.
                - A series matches if any term appears in the series ID or metadata (OR logic)
                - Leave empty ([] or null) to return ALL available series
                - Case-insensitive matching (e.g., 'Temp' will match 'temperature')
                - Partial word matching is supported (e.g., 'temp' will match 'temperature')
                - No regex support, just simple substring matching
                
                EXAMPLES:
                - ['temperature'] - finds temperature series
                - ['living', 'room'] - finds any series in living room or any room
                - ['humidity', 'bedroom'] - finds humidity series OR bedroom series
                """,
            required = false
        )
        Set<String> searchTerms
    ) {
        return dataService.getSeries().stream()
            .filter(seriesFile -> {
                if (searchTerms == null || searchTerms.isEmpty()) {
                    return true;
                }
                String pattern = searchTerms.stream()
                    .map(term -> ".*" + term + ".*")
                    .collect(Collectors.joining("|"));
                Predicate<String> matchesPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).asMatchPredicate();
                return matchesPattern.test(seriesFile.getId()) ||
                    seriesFile.getMetadata().values().stream().anyMatch(matchesPattern);
            })
            .map(seriesFile -> SeriesMcpResponse.builder()
                .seriesId(seriesFile.getId())
                .seriesMetadata(seriesFile.getMetadata())
                .build())
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
            
            WHAT IT RETURNS:
            - List of data objects, each containing:
              - seriesId: The identifier of the time series
              - seriesData: Array of {dateTime, dataValue} pairs representing the actual measurements
            
            TIME RANGE GUIDANCE:
            - Start with smaller time ranges (hours to days) and expand if needed
            - For high-frequency data, request minutes to hours to avoid overloading
            - For sparse data, you may need to request days to weeks
            - Always ensure startDateTime is chronologically before endDateTime
            
            COMMON ERRORS:
            - Invalid seriesIds: Verify IDs using find-time-series first
            - Time range too large: Consider narrowing the time range
            - No data in range: Try expanding the time range or check different series
            
            WORKFLOW EXAMPLE:
            1. Use get-current-time to find current time
            2. Calculate a start time (e.g., 24 hours earlier)
            3. Fetch data with appropriate seriesIds from find-time-series
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
                List of series IDs to fetch data for.
                - Each ID must be a string matching a seriesId from find-time-series
                - Invalid IDs will be ignored (no error, but no data returned)
                - Order of IDs doesn't matter
                
                PERFORMANCE TIPS:
                - Request only the series you need for analysis
                - Limit to 1-5 series per request for best performance
                - For comparing many series, consider multiple focused requests
                
                EXAMPLES:
                - Single series: ['kitchen_temperature']
                - Multiple series: ['kitchen_temperature', 'living_room_temperature']
                - Complex analysis: ['power_consumption', 'outside_temperature']
                """
        )
        List<String> seriesIds
    ) {
        FindDataRequest request = new FindDataRequest();
        request.setFrom(startDateTime);
        request.setTo(endDateTime);
        request.setTimezone(TimeZone.getTimeZone(startDateTime.getZone()));

        String seriesPattern = seriesIds.stream()
            .map(Pattern::quote)
            .collect(Collectors.joining("|"));
        FindSeriesRequest seriesRequest = new FindSeriesRequest();
        seriesRequest.setPattern(Pattern.compile(seriesPattern));
        request.setSeries(seriesRequest);

        return queryService.findData(request).stream()
            .map(fdr -> DataMcpResponse.builder()
                .seriesId(fdr.getSeries().getId())
                .seriesData(fdr.getData().stream()
                    .map(sd -> DataValueMcpResponse.builder()
                        .dateTime(sd.getTime())
                        .dataValue(sd.getValue())
                        .build())
                    .collect(Collectors.toList()))
                .build())
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
    @Builder
    public static class SeriesMcpResponse {
        private final String seriesId;
        private final Map<String, String> seriesMetadata;
    }

    @Data
    @Builder
    public static class DataMcpResponse {
        private final String seriesId;
        private final List<DataValueMcpResponse> seriesData;
    }

    @Data
    @Builder
    public static class DataValueMcpResponse {
        private final ZonedDateTime dateTime;
        private final Number dataValue;
    }

}
