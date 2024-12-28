package org.huebert.iotfsdb.ui;

import org.huebert.iotfsdb.ui.service.ExportUiService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.servlet.view.RedirectView;

//TODO Conditionally load UI
@Controller
@RequestMapping("/ui")
public class IndexUiController {

    private final ExportUiService exportService;

    public IndexUiController(ExportUiService exportService) {
        this.exportService = exportService;
    }

    @GetMapping
    public RedirectView getIndex(Model model) {
        return new RedirectView("/ui/series");
    }

    @GetMapping(value = "export", produces = "application/zip")
    public ResponseEntity<StreamingResponseBody> export() {
        return exportService.export(null);
    }

}
