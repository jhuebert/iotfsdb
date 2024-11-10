package org.huebert.iotfsdb.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.SeriesService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MutatingSeriesController.class)
public class MutatingSeriesControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SeriesService seriesService;

    @MockBean
    private InsertService insertService;

    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Test
    void testCreate() throws Exception {

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("123")
                .build())
            .metadata(Map.of())
            .build();

        mockMvc.perform(post("/v2/series")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(seriesFile)))
            .andExpect(status().isNoContent());

        verify(seriesService).createSeries(seriesFile);
    }

    @Test
    void testDelete() throws Exception {
        mockMvc.perform(delete("/v2/series/123"))
            .andExpect(status().isNoContent());
        verify(seriesService).deleteSeries("123");
    }

    @Test
    void testUpdateMetadata() throws Exception {

        Map<String, String> metadata = Map.of("a", "1");

        mockMvc.perform(put("/v2/series/123/metadata")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(metadata)))
            .andExpect(status().isNoContent());

        verify(seriesService).updateMetadata("123", metadata);
    }

    @Test
    void testBatchInsert() throws Exception {

        mockMvc.perform(post("/v2/series/123/data/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(List.of(new SeriesData(ZonedDateTime.parse("2024-08-11T00:00:00-06:00"), 1.23)))))
            .andExpect(status().isNoContent());

        verify(insertService).insert(eq("123"), any());
    }

    @Test
    void testSingleInsert() throws Exception {

        mockMvc.perform(post("/v2/series/123/data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(new SeriesData(ZonedDateTime.parse("2024-08-11T00:00:00-06:00"), 1.23))))
            .andExpect(status().isNoContent());

        verify(insertService).insert(eq("123"), any());
    }

}
