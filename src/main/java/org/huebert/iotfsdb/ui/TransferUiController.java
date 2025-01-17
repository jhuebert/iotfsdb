package org.huebert.iotfsdb.ui;

import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.ui.service.ExportUiService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@Slf4j
@Controller
@RequestMapping("/ui/transfer")
@ConditionalOnProperty(prefix = "iotfsdb", value = "ui", havingValue = "true")
public class TransferUiController {

    private final ExportUiService exportService;

    private final Environment environment;

    public TransferUiController(ExportUiService exportService, Environment environment) {
        this.exportService = exportService;
        this.environment = environment;
    }

    @GetMapping
    public String getIndex(Model model) {
        model.addAttribute("environment", environment);
        return "transfer/index";
    }

    @GetMapping(value = "export", produces = "application/zip")
    public ResponseEntity<StreamingResponseBody> export() {
        return exportService.export(null);
    }

}