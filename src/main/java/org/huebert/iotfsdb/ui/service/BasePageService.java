package org.huebert.iotfsdb.ui.service;

import lombok.Builder;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Service;

@Service
public class BasePageService {

    private final BuildProperties buildProperties;

    @Value("${iotfsdb.read-only:true}")
    private boolean readOnly;

    public BasePageService(BuildProperties buildProperties) {
        this.buildProperties = buildProperties;
    }

    public BasePage getBasePage() {
        return BasePage.builder()
            .readOnly(readOnly)
            .version(buildProperties.getVersion())
            .build();
    }

    @Data
    @Builder
    public static class BasePage {
        private String version;
        private boolean readOnly;
    }
}
