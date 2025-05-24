package org.huebert.iotfsdb.ui;

import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.DateTimePreset;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindDataResponse;
import org.huebert.iotfsdb.schema.Reducer;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.service.QueryService;
import org.huebert.iotfsdb.service.TimeConverter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.huebert.iotfsdb.ui.service.BasePageService;
import org.huebert.iotfsdb.ui.service.ObjectEncoder;
import org.huebert.iotfsdb.ui.service.PlotData;
import org.huebert.iotfsdb.ui.service.SearchParser;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.TimeZone;

@Slf4j
@Controller
@RequestMapping("/ui/data")
@ConditionalOnProperty(prefix = "iotfsdb", value = "ui", havingValue = "true")
public class DataUiController {

    private final QueryService queryService;

    private final ObjectEncoder objectEncoder;

    private final BasePageService basePageService;

    public DataUiController(QueryService queryService, ObjectEncoder objectEncoder, BasePageService basePageService) {
        this.queryService = queryService;
        this.objectEncoder = objectEncoder;
        this.basePageService = basePageService;
    }

    @CaptureStats(
        id = "ui-data-import",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "data"),
            @CaptureStats.Metadata(key = "operation", value = "index"),
            @CaptureStats.Metadata(key = "method", value = "get"),
        }
    )
    @GetMapping
    public String getIndex(Model model, @RequestParam(value = "request", required = false) String request) {
        PlotData plotData = PlotData.builder().labels(List.of()).data(List.of()).build();
        FindDataRequest defaultRequest = new FindDataRequest();
        defaultRequest.setDateTimePreset(DateTimePreset.LAST_24_HOURS);
        model.addAttribute("request", defaultRequest);

        try {
            if (request != null) {
                FindDataRequest findDataRequest = objectEncoder.decode(request, FindDataRequest.class);
                List<FindDataResponse> data = queryService.findData(findDataRequest);
                plotData = PlotData.builder()
                    .labels(getLabels(data))
                    .data(getData(data))
                    .build();
                model.addAttribute("request", findDataRequest);
            }
        } catch (IOException e) {
            log.warn("Could not parse request: {}", request);
        }

        model.addAttribute("plotData", plotData);
        model.addAttribute("basePage", basePageService.getBasePage());

        return "data/index";
    }

    @CaptureStats(
        id = "ui-data-search",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "data"),
            @CaptureStats.Metadata(key = "operation", value = "search"),
            @CaptureStats.Metadata(key = "method", value = "post"),
        }
    )
    @PostMapping("search")
    public String searchData(
        Model model,
        HttpServletResponse response,
        @RequestHeader("Iotfsdb-Tz") String timezone,
        @RequestParam("search") String search,
        @RequestParam("dateTimePreset") DateTimePreset dateTimePreset,
        @RequestParam(value = "from", required = false) LocalDateTime from,
        @RequestParam(value = "to", required = false) LocalDateTime to,
        @RequestParam(value = "interval", required = false, defaultValue = "60000") Long interval,
        @RequestParam(value = "size", required = false, defaultValue = "250") Integer size,
        @RequestParam(value = "includeNull", required = false) String includeNull,
        @RequestParam(value = "useBigDecimal", required = false) String useBigDecimal,
        @RequestParam(value = "usePrevious", required = false) String usePrevious,
        @RequestParam(value = "nullValue", required = false) Number nullValue,
        @RequestParam(value = "timeReducer", required = false, defaultValue = "AVERAGE") Reducer timeReducer,
        @RequestParam(value = "seriesReducer", required = false) Reducer seriesReducer
    ) {

        FindDataRequest request = new FindDataRequest();
        request.setSeries(SearchParser.fromSearch(search));
        request.setDateTimePreset(dateTimePreset);
        request.setTimezone(TimeZone.getTimeZone(timezone));
        request.setFrom(from != null ? TimeConverter.toUtc(from) : null);
        request.setTo(to != null ? TimeConverter.toUtc(to) : null);
        request.setInterval(interval);
        request.setSize(size);
        request.setIncludeNull("on".equalsIgnoreCase(includeNull));
        request.setUseBigDecimal("on".equalsIgnoreCase(useBigDecimal));
        request.setUsePrevious("on".equalsIgnoreCase(usePrevious));
        request.setNullValue(nullValue);
        request.setTimeReducer(timeReducer);
        request.setSeriesReducer(seriesReducer);

        List<FindDataResponse> data = queryService.findData(request);

        PlotData plotData = PlotData.builder()
            .labels(getLabels(data))
            .data(getData(data))
            .build();

        model.addAttribute("plotData", plotData);

        try {
            response.addHeader("HX-Push-Url", "/ui/data?request=" + objectEncoder.encode(request));
        } catch (IOException e) {
            log.warn("Could not serialize request: {}", request);
        }

        model.addAttribute("basePage", basePageService.getBasePage());

        return "data/fragments/script";
    }

    private List<ZonedDateTime> getLabels(List<FindDataResponse> data) {
        if (data.isEmpty()) {
            return List.of();
        }
        return data.getFirst().getData().stream().map(SeriesData::getTime).toList();
    }

    private List<PlotData.PlotSeries> getData(List<FindDataResponse> data) {
        return data.stream()
            .map(a -> PlotData.PlotSeries.builder()
                .id(a.getSeries().getId())
                .values(a.getData().stream()
                    .map(SeriesData::getValue)
                    .map(v -> v == null ? null : v.doubleValue())
                    .toList())
                .build())
            .toList();
    }
}
