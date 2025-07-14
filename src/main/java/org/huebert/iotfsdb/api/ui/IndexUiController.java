package org.huebert.iotfsdb.api.ui;

import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;

@Controller
@RequestMapping("/ui")
@ConditionalOnExpression("${iotfsdb.api.ui:true}")
public class IndexUiController {

    @CaptureStats(
        group = "ui", type = "root", operation = "index", javaClass = IndexUiController.class, javaMethod = "getIndex",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "get"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui"),
        }
    )
    @GetMapping
    public RedirectView getIndex() {
        return new RedirectView("/ui/series");
    }

}
