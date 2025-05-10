package org.huebert.iotfsdb.ui;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;

@Controller
@RequestMapping("/ui")
@ConditionalOnProperty(prefix = "iotfsdb", value = "ui", havingValue = "true")
@PreAuthorize("hasRole('UI_READ') or hasRole('UI_WRITE')")
public class IndexUiController {

    @GetMapping
    public RedirectView getIndex(Model model) {
        return new RedirectView("/ui/series");
    }

}
