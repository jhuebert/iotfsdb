package org.huebert.iotfsdb.api.ui;

import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.huebert.iotfsdb.api.ui.service.BasePageService;
import org.huebert.iotfsdb.api.ui.service.ExportUiService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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

    private final BasePageService basePageService;

    public TransferUiController(ExportUiService exportService, BasePageService basePageService) {
        this.exportService = exportService;
        this.basePageService = basePageService;
    }

    @CaptureStats(
        id = "ui-transfer-index",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "transfer"),
            @CaptureStats.Metadata(key = "operation", value = "index"),
            @CaptureStats.Metadata(key = "method", value = "get"),
        }
    )
    @GetMapping
    public String getIndex(Model model) {
        model.addAttribute("basePage", basePageService.getBasePage());
        return "transfer/index";
    }

    @CaptureStats(
        id = "ui-transfer-export",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "transfer"),
            @CaptureStats.Metadata(key = "operation", value = "export"),
            @CaptureStats.Metadata(key = "method", value = "get"),
        }
    )
    @GetMapping("export")
    public ResponseEntity<StreamingResponseBody> exportData() {
        return exportService.export(null);
    }

}
