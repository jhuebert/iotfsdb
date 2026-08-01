package org.huebert.iotfsdb.api.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.info.BuildProperties;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Runtime configuration for the SPA. Replaces the per-page model attributes that
 * the old server-rendered UI injected via {@code BasePageService}.
 */
@Validated
@Slf4j
@RestController
@RequestMapping("/v2/ui")
@Tag(name = "UI", description = "Runtime configuration for the web UI")
@ConditionalOnExpression("${iotfsdb.api.ui:true}")
public class UiConfigController {

    private final IotfsdbProperties properties;

    private final BuildProperties buildProperties;

    @Value("${springdoc.swagger-ui.enabled:false}")
    private boolean springdocEnabled;

    public UiConfigController(IotfsdbProperties properties, ObjectProvider<BuildProperties> buildProperties) {
        this.properties = properties;
        this.buildProperties = buildProperties.getIfAvailable();
    }

    @Operation(summary = "Runtime configuration for the web UI", tags = "UI")
    @GetMapping("config")
    public UiConfig getConfig() {
        return new UiConfig(
            buildProperties == null ? "unknown" : buildProperties.getVersion(),
            properties.isReadOnly(),
            springdocEnabled,
            properties.getStats().isEnabled(),
            properties.getQuery().getMaxSize(),
            properties.getSeries().getDefaultSeries()
        );
    }

    /**
     * Runtime UI configuration response.
     */
    public record UiConfig(
        String version,
        boolean readOnly,
        boolean springdocEnabled,
        boolean statsEnabled,
        int maxQuerySize,
        SeriesFile defaultSeries
    ) {
    }
}
