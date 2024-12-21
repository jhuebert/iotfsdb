package org.huebert.iotfsdb.ui;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;

@Controller
@RequestMapping("/ui")
public class IndexUiController {

    @GetMapping
    public RedirectView getIndex(Model model) {
        return new RedirectView("/ui/series");
    }

}
