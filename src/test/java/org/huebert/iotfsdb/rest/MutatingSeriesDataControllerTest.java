package org.huebert.iotfsdb.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.huebert.iotfsdb.schema.InsertRequest;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.service.InsertService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.ZonedDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MutatingSeriesDataController.class)
public class MutatingSeriesDataControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private InsertService insertService;

    private final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Test
    void testBatch() throws Exception {

        InsertRequest request = new InsertRequest();
        request.setSeries("123");
        request.setValues(List.of(new SeriesData(ZonedDateTime.parse("2024-08-11T00:00:00-06:00"), 1.23)));

        mockMvc.perform(post("/v2/data/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(List.of(request))))
            .andExpect(status().isNoContent());

        verify(insertService).insert(eq("123"), any());
    }

}
