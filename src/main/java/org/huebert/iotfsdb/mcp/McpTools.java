package org.huebert.iotfsdb.mcp;

import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindDataResponse;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
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

    @Tool(name = "series-search", description = "Search for series that match the given parameters", resultConverter = JsonConverter.class)
    public List<SeriesFile> findSeries(
        @ToolParam(description = "Definition ID of series to return", required = false)
        String seriesDefinitionId,
        @ToolParam(description = "Metadata key and values that matching series metadata must contain", required = false)
        Map<String, String> seriesMetadata
    ) {
        FindSeriesRequest request = new FindSeriesRequest();
        request.setPattern(seriesDefinitionId != null ? Pattern.compile(seriesDefinitionId) : Pattern.compile(".*"));
        request.setMetadata(seriesMetadata != null
            ? seriesMetadata.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Pattern.compile(e.getValue())))
            : Map.of());
        return seriesService.findSeries(request);
    }

    @Tool(name = "data-search", description = "Finds series data matching the input request", resultConverter = JsonConverter.class)
    public List<FindDataResponse> findData(
        @ToolParam(description = "Earliest date and time for returned values in ISO-8601 format.")
        ZonedDateTime startDateTime,
        @ToolParam(description = "Latest date and time for returned values in ISO-8601 format.")
        ZonedDateTime endDateTime,
        @ToolParam(description = "Definition ID of series data to return")
        String seriesDefinitionId
    ) {
        FindDataRequest request = new FindDataRequest();
        request.setFrom(startDateTime);
        request.setTo(endDateTime);
        request.setTimezone(TimeZone.getTimeZone(startDateTime.getZone()));
        FindSeriesRequest seriesRequest = new FindSeriesRequest();
        seriesRequest.setPattern(seriesDefinitionId != null ? Pattern.compile(seriesDefinitionId) : Pattern.compile(".*"));
        request.setSeries(seriesRequest);
        return queryService.findData(request);
    }

    @Tool(name = "get-current-date-time", description = "Retrieves the current date and time in the server's time zone in ISO-8601 format", resultConverter = JsonConverter.class)
    public ZonedDateTime getCurrentDateTime() {
        return ZonedDateTime.now();
    }

}
