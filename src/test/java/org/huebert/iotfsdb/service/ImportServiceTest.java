package org.huebert.iotfsdb.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.persistence.FilePersistenceAdapterTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.FileSystemUtils;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@ExtendWith(MockitoExtension.class)
public class ImportServiceTest {

    @Mock
    private InsertService insertService;

    @Mock
    private SeriesService seriesService;

    private final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Test
    public void testImportData() throws Exception {

        Path temp = Files.createTempFile("iotfsdb", ".zip");
        try (InputStream is = FilePersistenceAdapterTest.class.getResourceAsStream("/import.zip"); OutputStream os = new FileOutputStream(temp.toFile())) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }

        new ImportService(seriesService, insertService, objectMapper).importData(temp);

        verify(seriesService).findSeries("test-series");

        ArgumentCaptor<SeriesFile> seriesCaptor = ArgumentCaptor.forClass(SeriesFile.class);
        verify(seriesService).createSeries(seriesCaptor.capture());
        assertThat(seriesCaptor.getValue().getId()).isEqualTo("test-series");
        assertThat(seriesCaptor.getValue().getDefinition().getType()).isEqualTo(NumberType.MAPPED1);

        ArgumentCaptor<InsertRequest> insertCaptor = ArgumentCaptor.forClass(InsertRequest.class);
        verify(insertService).insert(insertCaptor.capture());
        assertThat(insertCaptor.getValue().getSeries()).isEqualTo("test-series");
        assertThat(insertCaptor.getValue().getReducer()).isEqualTo(Reducer.FIRST);
        assertThat(insertCaptor.getValue().getValues().size()).isEqualTo(85242);

        if (!FileSystemUtils.deleteRecursively(temp)) {
            throw new RuntimeException("unable to delete root");
        }
    }

}
