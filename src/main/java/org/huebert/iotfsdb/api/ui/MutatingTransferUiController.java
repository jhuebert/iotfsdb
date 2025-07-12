package org.huebert.iotfsdb.api.ui;

import static org.springframework.http.HttpStatus.NO_CONTENT;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.service.ImportService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
@Controller
@RequestMapping("/ui/transfer")
@ConditionalOnExpression("${iotfsdb.api.ui:true} and not ${iotfsdb.read-only:false}")
public class MutatingTransferUiController {

    private final ImportService importService;

    public MutatingTransferUiController(@NotNull ImportService importService) {
        this.importService = importService;
    }

    @CaptureStats(
        group = "ui", type = "transfer", operation = "import", javaClass = MutatingTransferUiController.class, javaMethod = "importData",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
        }
    )
    @PostMapping("import")
    @ResponseStatus(NO_CONTENT)
    public void importData(@NotNull @Valid @RequestParam("file") MultipartFile file) throws IOException {

        if ((file.getOriginalFilename() != null) && !file.getOriginalFilename().endsWith(".zip")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File is not a zip");
        }

        if (file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File is empty");
        }

        Path tempFile = Files.createTempFile("iotfsdb-", ".zip");
        try {
            file.transferTo(tempFile);
            importService.importData(tempFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }

    }

}
