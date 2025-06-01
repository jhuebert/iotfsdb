package org.huebert.iotfsdb.ui.service;

import lombok.Builder;
import lombok.Data;
import org.huebert.iotfsdb.security.UserRole;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.info.BuildProperties;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

@Service
public class BasePageService {

    private final BuildProperties buildProperties;

    @Value("${iotfsdb.read-only:true}")
    private boolean readOnly;

    @Value("${springdoc.swagger-ui.enabled:false}")
    private boolean springdocEnabled;

    public BasePageService(BuildProperties buildProperties) {
        this.buildProperties = buildProperties;
    }

    public BasePage getBasePage() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        boolean hasUiWrite = authentication.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals(UserRole.UI_WRITE.getRoleName()));
        return BasePage.builder()
            .readOnly(readOnly || !hasUiWrite)
            .version(buildProperties.getVersion())
            .springdocEnabled(springdocEnabled)
            .build();
    }

    @Data
    @Builder
    public static class BasePage {
        private String version;
        private boolean readOnly;
        private boolean springdocEnabled;
    }
}
