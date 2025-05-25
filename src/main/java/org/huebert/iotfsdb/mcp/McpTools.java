package org.huebert.iotfsdb.mcp;

import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindDataResponse;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.QueryService;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
public class McpTools {

    private final SeriesService seriesService;

    private final QueryService queryService;

    public McpTools(@NotNull SeriesService seriesService, QueryService queryService) {
        this.seriesService = seriesService;
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
    @Tool(name = "find-time-series", description = "Find time series definitions by filtering on metadata attributes. Use this to discover available time series before fetching data.", resultConverter = JsonConverter.class)
    public List<String> searchSeries(
        @ToolParam(description = "Key-value pairs of metadata attributes to filter series by (e.g., {\"location\": \"sensor1\", \"type\": \"temperature\"}).", required = false)
        Map<String, String> seriesMetadata
    ) {
        FindSeriesRequest request = new FindSeriesRequest();
        request.setMetadata(seriesMetadata != null
            ? seriesMetadata.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Pattern.compile(e.getValue())))
            : Map.of());
        return seriesService.findSeries(request).stream()
            .map(SeriesFile::getId)
            .collect(Collectors.toList());
    }

    @CaptureStats(
        id = "mcp-series-metadata-get",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "mcp"),
            @CaptureStats.Metadata(key = "type", value = "metadata"),
            @CaptureStats.Metadata(key = "operation", value = "get"),
            @CaptureStats.Metadata(key = "method", value = "mcp"),
        }
    )
    @Tool(name = "get-time-series-metadata", description = "Retrieve the full metadata for specific time series by their IDs. Use this to learn about attributes of time series.", resultConverter = JsonConverter.class)
    public Map<String, Map<String, String>> getSeriesMetadata(
        @ToolParam(description = "List of time series IDs to retrieve metadata for (obtained from find-time-series)")
        List<String> seriesDefinitionIds
    ) {
        FindSeriesRequest request = new FindSeriesRequest();
        request.setPattern(Pattern.compile(seriesDefinitionIds.stream().map(Pattern::quote).collect(Collectors.joining("|"))));
        return seriesService.findSeries(request).stream()
            .collect(Collectors.toMap(
                SeriesFile::getId,
                SeriesFile::getMetadata
            ));
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
    @Tool(name = "fetch-time-series-data", description = "Retrieve actual time series data points for specified series IDs within a time range. Returns timestamps and values for analysis.", resultConverter = JsonConverter.class)
    public Map<String, List<SeriesData>> searchData(
        @ToolParam(description = "Start of time range in ISO-8601 format (e.g., '2023-01-01T00:00:00-05:00')")
        ZonedDateTime startDateTime,
        @ToolParam(description = "End of time range in ISO-8601 format (e.g., '2023-01-02T00:00:00-05:00')")
        ZonedDateTime endDateTime,
        @ToolParam(description = "List of time series IDs to fetch data for (obtained from find-time-series)")
        List<String> seriesDefinitionIds
    ) {
        FindDataRequest request = new FindDataRequest();
        request.setFrom(startDateTime);
        request.setTo(endDateTime);
        request.setTimezone(TimeZone.getTimeZone(startDateTime.getZone()));
        FindSeriesRequest seriesRequest = new FindSeriesRequest();
        seriesRequest.setPattern(Pattern.compile(seriesDefinitionIds.stream().map(Pattern::quote).collect(Collectors.joining("|"))));
        request.setSeries(seriesRequest);
        return queryService.findData(request).stream()
            .collect(Collectors.toMap(
                sd -> sd.getSeries().getId(),
                FindDataResponse::getData
            ));
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
    @Tool(name = "get-current-time", description = "Get the current server time in ISO-8601 format. Useful for creating relative time ranges for data queries or setting default time boundaries.", resultConverter = JsonConverter.class)
    public ZonedDateTime getCurrentDateTime() {
        return ZonedDateTime.now();
    }

}
