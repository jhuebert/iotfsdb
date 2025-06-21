package org.huebert.iotfsdb.api.ui;

import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;

@Controller
@RequestMapping("/ui")
@ConditionalOnProperty(prefix = "iotfsdb", value = "ui", havingValue = "true")
public class IndexUiController {

    @CaptureStats(
        id = "ui-index",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "ui"),
            @CaptureStats.Metadata(key = "operation", value = "index"),
            @CaptureStats.Metadata(key = "method", value = "get"),
        }
    )
    @GetMapping
    public RedirectView getIndex() {
        return new RedirectView("/ui/series");
    }

}
