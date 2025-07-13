package org.huebert.iotfsdb.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@ExtendWith(MockitoExtension.class)
public class SeriesServiceTest {

    @Captor
    private ArgumentCaptor<SeriesFile> captor;

    @Mock
    private DataService dataService;

    @InjectMocks
    private SeriesService seriesService;

    @Test
    public void testCreateSeries() {
        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        when(dataService.getSeries("abc")).thenReturn(Optional.empty());
        seriesService.createSeries(seriesFile);
        verify(dataService).saveSeries(seriesFile);
    }

    @Test
    public void testCreateSeries_AlreadyExists() {
        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        when(dataService.getSeries("abc")).thenReturn(Optional.of(seriesFile));
        assertThrows(IllegalArgumentException.class, () -> seriesService.createSeries(seriesFile));
    }

    @Test
    public void testUpdateMetadata() {

        SeriesDefinition definition = SeriesDefinition.builder().id("123").build();
        SeriesFile seriesFile = SeriesFile.builder().definition(definition).build();
        Map<String, String> metadata = Map.of("a", "1");

        when(dataService.getSeries("123")).thenReturn(Optional.of(seriesFile));

        seriesService.updateMetadata("123", metadata, false);

        verify(dataService).getSeries("123");
        verify(dataService).saveSeries(captor.capture());
        assertThat(captor.getValue().getDefinition()).isEqualTo(definition);
        assertThat(captor.getValue().getMetadata()).isEqualTo(metadata);
    }

    @Test
    public void testDeleteSeries() {
        seriesService.deleteSeries("123");
        verify(dataService).deleteSeries("123");
    }

    @Test
    public void testFindSeries_Single() {
        seriesService.findSeries("123");
        verify(dataService).getSeries("123");
    }

    @Test
    public void testFindSeries_PatternAndMetadata() {
        List<SeriesFile> all = List.of(
            SeriesFile.builder()
                .definition(SeriesDefinition.builder().id("abc123").build())
                .metadata(Map.of("a", "1"))
                .build(),
            SeriesFile.builder()
                .definition(SeriesDefinition.builder().id("abc456").build())
                .metadata(Map.of("a", "1"))
                .build(),
            SeriesFile.builder()
                .definition(SeriesDefinition.builder().id("def123").build())
                .metadata(Map.of("a", "2"))
                .build(),
            SeriesFile.builder()
                .definition(SeriesDefinition.builder().id("ghi123").build())
                .metadata(Map.of())
                .build()
        );
        when(dataService.getSeries()).thenReturn(all);

        FindSeriesRequest request = new FindSeriesRequest(Pattern.compile("^.*123$"), Map.of("a", Pattern.compile("1")));
        List<SeriesFile> series = seriesService.findSeries(request);

        verify(dataService).getSeries();
        assertThat(series).hasSize(1);
        assertThat(series).contains(all.getFirst());
    }

    @Test
    public void testFindSeries_PatternOnly() {
        List<SeriesFile> all = List.of(
            SeriesFile.builder()
                .definition(SeriesDefinition.builder().id("abc123").build())
                .metadata(Map.of("a", "1"))
                .build(),
            SeriesFile.builder()
                .definition(SeriesDefinition.builder().id("abc456").build())
                .metadata(Map.of("a", "1"))
                .build(),
            SeriesFile.builder()
                .definition(SeriesDefinition.builder().id("def123").build())
                .metadata(Map.of("a", "2"))
                .build(),
            SeriesFile.builder()
                .definition(SeriesDefinition.builder().id("ghi123").build())
                .metadata(Map.of())
                .build()
        );
        when(dataService.getSeries()).thenReturn(all);

        FindSeriesRequest request = new FindSeriesRequest(Pattern.compile("^.*123$"), Map.of());
        List<SeriesFile> series = seriesService.findSeries(request);

        verify(dataService).getSeries();
        assertThat(series).hasSize(3);
        assertThat(series).contains(all.get(0), all.get(2), all.get(3));
    }

}
