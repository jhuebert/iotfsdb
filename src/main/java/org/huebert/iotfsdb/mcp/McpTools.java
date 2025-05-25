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

    @Tool(name = "search-series", description = "Search for series that match the given parameters", resultConverter = JsonConverter.class)
    public List<String> searchSeries(
        @ToolParam(description = "Metadata key and value pairs that each matching series metadata must contain", required = false)
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

    @Tool(name = "get-series-metadata", description = "Get metadata for each of the input series definition IDs", resultConverter = JsonConverter.class)
    public Map<String, Map<String, String>> getSeriesMetadata(
        @ToolParam(description = "List of series definition IDs of metadata to return")
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

    @Tool(name = "data-search", description = "Search for data for each of the series definition IDs in the specified time range", resultConverter = JsonConverter.class)
    public Map<String, List<SeriesData>> searchData(
        @ToolParam(description = "Earliest date and time in ISO-8601 format for returned values.")
        ZonedDateTime startDateTime,
        @ToolParam(description = "Latest date and time in ISO-8601 format for returned values.")
        ZonedDateTime endDateTime,
        @ToolParam(description = "List of series definition IDs of data to return")
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

    @Tool(name = "get-current-date-time", description = "Retrieves the current date and time in the server's time zone in ISO-8601 format", resultConverter = JsonConverter.class)
    public ZonedDateTime getCurrentDateTime() {
        return ZonedDateTime.now();
    }

}
