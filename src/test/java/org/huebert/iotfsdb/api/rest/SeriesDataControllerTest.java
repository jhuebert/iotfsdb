package org.huebert.iotfsdb.api.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.net.HttpHeaders;
import org.hamcrest.core.StringRegularExpression;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.QueryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@WebMvcTest(SeriesDataController.class)
public class SeriesDataControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private ExportService exportService;

    @MockitoBean
    private QueryService queryService;

    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Test
    void testFind() throws Exception {

        FindDataRequest request = new FindDataRequest();
        request.setSeries(new FindSeriesRequest(Pattern.compile("123"), Map.of()));
        request.setFrom(ZonedDateTime.parse("2024-08-11T00:00:00-06:00"));
        request.setTo(ZonedDateTime.parse("2024-09-11T00:00:00-06:00"));

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("123")
                .build())
            .metadata(Map.of())
            .build();

        FindDataResponse response = FindDataResponse.builder()
            .series(seriesFile)
            .data(List.of(new SeriesData(request.getFrom(), 1.23)))
            .build();

        when(queryService.findData(any())).thenReturn(List.of(response));

        mockMvc.perform(post("/v2/data/find")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(request)))
            .andExpect(status().isOk())
            .andExpect(content().string(mapper.writeValueAsString(List.of(response))));
    }

    @Test
    void testExport() throws Exception {

        FindSeriesRequest request = new FindSeriesRequest();

        mockMvc.perform(post("/v2/data/export")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(request)))
            .andExpect(status().isOk())
            .andExpect(header().string(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION))
            .andExpect(header().string(HttpHeaders.CONTENT_DISPOSITION, StringRegularExpression.matchesRegex("attachment;filename=iotfsdb-\\d{8}-\\d{6}.zip")));

        verify(exportService).export(any(), any());
    }

}
