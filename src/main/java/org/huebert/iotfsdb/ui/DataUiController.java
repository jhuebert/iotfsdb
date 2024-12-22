package org.huebert.iotfsdb.ui;

import org.apache.logging.log4j.util.Strings;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindDataResponse;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.Reducer;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.service.QueryService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.regex.Pattern;

@Controller
@RequestMapping("/ui/data")
public class DataUiController {

    private final QueryService queryService;

    public DataUiController(QueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public String getIndex(Model model) {
        PlotData plotData = PlotData.builder()
            .labels(List.of())
            .data(List.of())
            .build();
        model.addAttribute("plotData", plotData);
        return "data/index";
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
                .values(a.getData().stream().map(SeriesData::getValue).map(v -> v == null ? null : v.doubleValue()).toList())
                .build())
            .toList();
    }

    @PostMapping("search")
    public String search(
        Model model,
        @RequestParam("search") String pattern,
        @RequestParam("from") LocalDateTime from,
        @RequestParam("to") LocalDateTime to,
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
        FindSeriesRequest series = new FindSeriesRequest();
        if (!Strings.isBlank(pattern)) {
            series.setPattern(Pattern.compile(".*" + pattern + ".*"));
        }
        request.setSeries(series);
        request.setFrom(from.atZone(ZoneId.of("UTC")));
        request.setTo(to.atZone(ZoneId.of("UTC")));

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
        return "data/fragments/script";
    }
}
