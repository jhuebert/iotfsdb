package org.huebert.iotfsdb.ui.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.info.BuildProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BasePageServiceTest {

    @Mock
    private BuildProperties buildProperties;

    @InjectMocks
    private BasePageService basePageService;

    @Test
    public void testGetBasePage() {
        when(buildProperties.getVersion()).thenReturn("1.2.3");
        BasePageService.BasePage basePage = basePageService.getBasePage();
        assertThat(basePage.getVersion()).isEqualTo("1.2.3");
        assertThat(basePage.isReadOnly()).isFalse();
    }

}
