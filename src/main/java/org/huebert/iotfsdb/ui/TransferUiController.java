package org.huebert.iotfsdb.ui;

import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.ui.service.BasePageService;
import org.huebert.iotfsdb.ui.service.ExportUiService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@Slf4j
@Controller
@RequestMapping("/ui/transfer")
@ConditionalOnProperty(prefix = "iotfsdb", value = "ui", havingValue = "true")
@PreAuthorize("hasRole('UI_READ') or hasRole('UI_WRITE')")
public class TransferUiController {

    private final ExportUiService exportService;

    private final BasePageService basePageService;

    public TransferUiController(ExportUiService exportService, BasePageService basePageService) {
        this.exportService = exportService;
        this.basePageService = basePageService;
    }

    @GetMapping
    public String getIndex(Model model) {
        model.addAttribute("basePage", basePageService.getBasePage());
        return "transfer/index";
    }

    @GetMapping(value = "export", produces = "application/zip")
    public ResponseEntity<StreamingResponseBody> export() {
        return exportService.export(null);
    }

}
