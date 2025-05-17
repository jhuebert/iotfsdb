package org.huebert.iotfsdb.ui;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;

@Controller
@RequestMapping("/ui")
@ConditionalOnProperty(prefix = "iotfsdb", value = "ui", havingValue = "true")
public class IndexUiController {

    @GetMapping
    public RedirectView getIndex() {
        return new RedirectView("/ui/series");
    }

}
