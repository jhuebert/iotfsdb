package org.huebert.iotfsdb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExportServiceTest {

    @Test
    public void testExport() throws IOException {

        DataService dataService = mock(DataService.class);
        SeriesService seriesService = mock(SeriesService.class);
        ObjectMapper objectMapper = new ObjectMapper();
        ExportService exportService1 = new ExportService(dataService, seriesService, objectMapper);

        FindSeriesRequest request = new FindSeriesRequest();
        request.setPattern(Pattern.compile("abc"));
        request.setMetadata(Map.of("a", Pattern.compile("1")));

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder().id("abc").build())
            .metadata(Map.of("a", "1"))
            .build();

        when(seriesService.findSeries(request.getPattern(), request.getMetadata())).thenReturn(List.of(seriesFile));

        PartitionKey key = new PartitionKey("abc", "123");

        when(dataService.getPartitions(seriesFile.getId())).thenReturn(Set.of(key));

        ByteBuffer byteBuffer = ByteBuffer.allocate(8000);
        for (int i = 0; i < 2000; i++) {
            byteBuffer.asIntBuffer().put(i, (byte) i);
        }

        when(dataService.getBuffer(key)).thenReturn(Optional.of(byteBuffer.slice(0, 8000)));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        exportService1.export(request, outputStream);

        assertThat(outputStream.size()).isEqualTo(782);

        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(outputStream.toByteArray()));

        assertThat(zis.getNextEntry().getName()).isEqualTo("abc/series.json");
        SeriesFile seriesFileOut = objectMapper.readValue(zis.readAllBytes(), SeriesFile.class);
        assertThat(seriesFileOut).isEqualTo(seriesFile);

        assertThat(zis.getNextEntry().getName()).isEqualTo("abc/123");
        ByteArrayOutputStream partitionOut = new ByteArrayOutputStream();
        zis.transferTo(partitionOut);
        assertThat(partitionOut.toByteArray()).isEqualTo(byteBuffer.array());
    }
}
