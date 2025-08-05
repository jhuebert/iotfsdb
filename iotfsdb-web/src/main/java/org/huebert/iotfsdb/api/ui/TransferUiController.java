package org.huebert.iotfsdb.api.ui;

import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.ui.service.BasePageService;
import org.huebert.iotfsdb.api.ui.service.ExportUiService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@Slf4j
@Controller
@RequestMapping("/ui/transfer")
@ConditionalOnExpression("${iotfsdb.api.ui:true}")
public class TransferUiController {

    private final ExportUiService exportService;

    private final BasePageService basePageService;

    public TransferUiController(ExportUiService exportService, BasePageService basePageService) {
        this.exportService = exportService;
        this.basePageService = basePageService;
    }

    @CaptureStats(
        group = "ui", type = "transfer", operation = "index", javaClass = TransferUiController.class, javaMethod = "getIndex",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "get"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui/transfer"),
        }
    )
    @GetMapping
    public String getIndex(Model model) {
        model.addAttribute("basePage", basePageService.getBasePage());
        return "transfer/index";
    }

    @CaptureStats(
        group = "ui", type = "transfer", operation = "export", javaClass = TransferUiController.class, javaMethod = "exportData",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "get"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui/transfer/export"),
        }
    )
    @GetMapping("export")
    public ResponseEntity<StreamingResponseBody> exportData() {
        return exportService.export(null);
    }

}
