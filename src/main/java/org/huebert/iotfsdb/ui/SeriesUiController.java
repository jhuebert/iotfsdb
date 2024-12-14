package org.huebert.iotfsdb.ui;

import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
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
        findSeriesRequest.setPattern(Pattern.compile(pattern));
        List<SeriesDefinition> results = seriesService.findSeries(findSeriesRequest).stream().map(SeriesFile::getDefinition).toList();
        model.addAttribute("series", results);
        return "series/results";
    }

}
