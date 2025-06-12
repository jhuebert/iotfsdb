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

import java.time.ZonedDateTime;
import java.util.List;
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
            Discovers available time series by searching through series IDs and metadata
            - Use this tool first to identify relevant time series before fetching data
            - This returns objects containing the series ID as well as metadata that provides additional context for the series
            """,
        resultConverter = JsonConverter.class
    )
    public List<SeriesMcpResponse> searchSeries(
        @ToolParam(
            description = """
                Search terms to filter time series
                - A series matches if any term appears in the series ID or metadata (OR logic)
                - Case-insensitive partial matching is used (e.g., 'Temp' will match 'temperature')
                """
        )
        Set<String> searchTerms
    ) {
        log.debug("Entering find-time-series: searchTerms={}", searchTerms);
        if (searchTerms == null || searchTerms.isEmpty()) {
            return List.of();
        }
        Set<String> terms = searchTerms.stream()
            .filter(a -> !a.isBlank())
            .map(String::toLowerCase)
            .collect(Collectors.toUnmodifiableSet());
        Predicate<String> matchesPattern = test ->
            test != null && terms.stream().anyMatch(test.toLowerCase()::contains);
        List<SeriesMcpResponse> results = dataService.getSeries().stream()
            .filter(seriesFile -> matchesPattern.test(seriesFile.getId()) ||
                seriesFile.getMetadata().values().stream().anyMatch(matchesPattern))
            .map(SeriesMcpResponse::new)
            .toList();
        log.debug("Exiting find-time-series: size={}", results.size());
        return results;
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
            Retrieves actual time series data points within a specified time range
            - You should first call find-time-series to identify relevant seriesIds
            - This returns objects that map a specific series ID to an array of {dateTime, dataValue} pairs representing the actual measurements
            - Each series is limited to return a maximum of 10 data points
            - If the requested time range would result in more than 10 data points, the data will be downsampled using the supplied time reducer
            - Start with smaller time ranges and expand if no data is found
            """,
        resultConverter = JsonConverter.class
    )
    public List<DataMcpResponse> searchData(
        @ToolParam(
            description = """
                Start timestamp in ISO-8601 format
                """
        )
        ZonedDateTime startDateTime,
        @ToolParam(
            description = """
                End timestamp in ISO-8601 format
                """
        )
        ZonedDateTime endDateTime,
        @ToolParam(
            description = """
                Set of series IDs to fetch data for
                - Each ID must be a string matching a seriesId from find-time-series
                - It is more efficient to include multiple series IDs instead of calling this tool for each individual series ID
                """
        )
        Set<String> seriesIds,
        @ToolParam(
            description = """
                Time reducer to apply to the data points
                - If the time range specified results in more than 10 data points, the data will be downsampled using this reducer
                - Valid values: AVERAGE, COUNT, COUNT_DISTINCT, FIRST, LAST, MAXIMUM, MEDIAN, MINIMUM, MODE, MULTIPLY, SQUARE_SUM, SUM
                - Default is AVERAGE
                """
        )
        Reducer timeReducer
    ) {
        log.debug("Entering fetch-time-series-data: startDateTime={}, endDateTime={}, seriesIds={}, timeReducer={}", startDateTime, endDateTime, seriesIds, timeReducer);
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

        List<DataMcpResponse> results = queryService.findData(request).stream()
            .map(DataMcpResponse::new)
            .collect(Collectors.toList());
        log.debug("Exiting fetch-time-series-data: size={}", results.size());
        return results;
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
            Returns the current server time in ISO-8601 format
            - Use this to calculate relative time ranges (e.g., "from 24 hours ago until now")
            """,
        resultConverter = JsonConverter.class
    )
    public ZonedDateTime getCurrentDateTime() {
        ZonedDateTime now = ZonedDateTime.now();
        log.info("get-current-time: now={}", now);
        return now;
    }

    @Data
    public static class SeriesMcpResponse {
        private final String seriesId;
        private final SortedMap<String, String> seriesMetadata;

        public SeriesMcpResponse(SeriesFile seriesFile) {
            this.seriesId = seriesFile.getId();
            this.seriesMetadata = new TreeMap<>(seriesFile.getMetadata());
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
