package org.huebert.iotfsdb.ui;

import org.apache.logging.log4j.util.Strings;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.regex.Pattern;

@Controller
@RequestMapping("/ui")
public class SeriesUiController {

    private final SeriesService seriesService;

    public SeriesUiController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @GetMapping
    public String getIndex(Model model) {
        return "series/search";
    }

    @PostMapping
    public String search(Model model, @RequestParam("search") String pattern) {
        FindSeriesRequest findSeriesRequest = new FindSeriesRequest();
        if (!Strings.isBlank(pattern)) {
            findSeriesRequest.setPattern(Pattern.compile(".*" + pattern + ".*"));
        }
        model.addAttribute("series", seriesService.findSeries(findSeriesRequest));
        return "series/results";
    }

}
