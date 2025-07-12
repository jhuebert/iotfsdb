package org.huebert.iotfsdb.api.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@WebMvcTest(SeriesController.class)
public class SeriesControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private SeriesService seriesService;

    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Test
    void testFind() throws Exception {

        FindSeriesRequest request = new FindSeriesRequest();
        request.setPattern(Pattern.compile("123"));

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("123")
                .build())
            .metadata(Map.of())
            .build();

        when(seriesService.findSeries(any(FindSeriesRequest.class))).thenReturn(List.of(seriesFile));

        mockMvc.perform(post("/v2/series/find")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(request)))
            .andExpect(status().isOk())
            .andExpect(content().string(mapper.writeValueAsString(List.of(seriesFile))));
    }

    @Test
    void testGet() throws Exception {

        FindSeriesRequest request = new FindSeriesRequest();
        request.setPattern(Pattern.compile("123"));

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("123")
                .build())
            .metadata(Map.of())
            .build();

        when(seriesService.findSeries("123")).thenReturn(Optional.of(seriesFile));

        mockMvc.perform(get("/v2/series/123"))
            .andExpect(status().isOk())
            .andExpect(content().string(mapper.writeValueAsString(seriesFile)));
    }

    @Test
    void testGetMetadata() throws Exception {

        FindSeriesRequest request = new FindSeriesRequest();
        request.setPattern(Pattern.compile("123"));

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("123")
                .build())
            .metadata(Map.of("a", "1"))
            .build();

        when(seriesService.findSeries("123")).thenReturn(Optional.of(seriesFile));

        mockMvc.perform(get("/v2/series/123/metadata"))
            .andExpect(status().isOk())
            .andExpect(content().string(mapper.writeValueAsString(seriesFile.getMetadata())));
    }

}
