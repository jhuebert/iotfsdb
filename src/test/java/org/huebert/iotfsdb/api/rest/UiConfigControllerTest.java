package org.huebert.iotfsdb.api.rest;

import static org.huebert.iotfsdb.api.rest.UiConfigController.UiConfig;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

@WebMvcTest(UiConfigController.class)
@TestPropertySource(properties = "springdoc.swagger-ui.enabled=false")
class UiConfigControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private IotfsdbProperties properties;

    @Test
    void testGetConfig() throws Exception {

        SeriesFile defaultSeries = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("default")
                .type(NumberType.FLOAT4)
                .interval(60000L)
                .partition(PartitionPeriod.DAY)
                .build())
            .metadata(Map.of("createdBy", "iotfsdb"))
            .build();

        IotfsdbProperties.QueryProperties queryProperties = new IotfsdbProperties.QueryProperties();
        queryProperties.setMaxSize(1000);

        IotfsdbProperties.SeriesProperties seriesProperties = new IotfsdbProperties.SeriesProperties();
        seriesProperties.setDefaultSeries(defaultSeries);

        IotfsdbProperties.StatsProperties statsProperties = new IotfsdbProperties.StatsProperties();
        statsProperties.setEnabled(true);

        IotfsdbProperties.ApiProperties apiProperties = new IotfsdbProperties.ApiProperties();

        org.mockito.Mockito.when(properties.isReadOnly()).thenReturn(false);
        org.mockito.Mockito.when(properties.getQuery()).thenReturn(queryProperties);
        org.mockito.Mockito.when(properties.getSeries()).thenReturn(seriesProperties);
        org.mockito.Mockito.when(properties.getStats()).thenReturn(statsProperties);
        org.mockito.Mockito.when(properties.getApi()).thenReturn(apiProperties);

        mockMvc.perform(get("/v2/ui/config"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.version").isString())
            .andExpect(jsonPath("$.readOnly").value(false))
            .andExpect(jsonPath("$.springdocEnabled").value(false))
            .andExpect(jsonPath("$.statsEnabled").value(true))
            .andExpect(jsonPath("$.maxQuerySize").value(1000))
            .andExpect(jsonPath("$.defaultSeries.definition.type").value("FLOAT4"))
            .andExpect(jsonPath("$.defaultSeries.definition.interval").value(60000))
            .andExpect(jsonPath("$.defaultSeries.definition.partition").value("DAY"))
            .andExpect(jsonPath("$.defaultSeries.metadata.createdBy").value("iotfsdb"));
    }

    @Test
    void testUiConfigRecordSerializes() {
        UiConfig config = new UiConfig("2.2.18", true, false, false, 250, null);
        org.assertj.core.api.Assertions.assertThat(config.version()).isEqualTo("2.2.18");
        org.assertj.core.api.Assertions.assertThat(config.readOnly()).isTrue();
    }
}
