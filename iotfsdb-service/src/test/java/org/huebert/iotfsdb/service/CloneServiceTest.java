package org.huebert.iotfsdb.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Range;
import org.huebert.iotfsdb.api.schema.PartitionKey;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ExtendWith(MockitoExtension.class)
class CloneServiceTest {

    @Mock
    private InsertService insertService;

    @Mock
    private SeriesService seriesService;

    @Mock
    private DataService dataService;

    @Mock
    private PartitionService partitionService;

    @InjectMocks
    private CloneService cloneService;

    @Captor
    private ArgumentCaptor<SeriesFile> seriesFileCaptor;

    private SeriesFile sourceSeries;
    private SeriesDefinition sourceDefinition;
    private PartitionKey partitionKey1;
    private PartitionKey partitionKey2;
    private ByteBuffer sourceBuffer;
    private ByteBuffer destinationBuffer;
    private PartitionRange sourceRange;
    private PartitionRange destinationRange;
    private PartitionAdapter adapter;

    @BeforeEach
    void setUp() {

        // Setup common test data
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

        sourceDefinition = SeriesDefinition.builder()
            .id("source-series")
            .partition(PartitionPeriod.DAY)
            .interval(60000L)
            .build();

        sourceSeries = SeriesFile.builder()
            .definition(sourceDefinition)
            .metadata(metadata)
            .build();

        partitionKey1 = new PartitionKey("source-series", "20240701");
        partitionKey2 = new PartitionKey("source-series", "20240702");

        // Create mock buffers
        sourceBuffer = ByteBuffer.allocate(100);
        destinationBuffer = ByteBuffer.allocate(100);

        // Setup adapter mock
        adapter = mock(PartitionAdapter.class);

        // Setup partition ranges
        sourceRange = new PartitionRange(
            partitionKey1,
            Range.closed(
                LocalDateTime.of(2024, 7, 1, 0, 0),
                LocalDateTime.of(2024, 7, 2, 0, 0).minusNanos(1)
            ),
            Duration.ofMinutes(1),
            adapter,
            new ReentrantReadWriteLock()
        );

        destinationRange = new PartitionRange(
            new PartitionKey("dest-series", "20240701"),
            Range.closed(
                LocalDateTime.of(2024, 7, 1, 0, 0),
                LocalDateTime.of(2024, 7, 2, 0, 0).minusNanos(1)
            ),
            Duration.ofMinutes(1),
            adapter,
            new ReentrantReadWriteLock()
        );
    }

    @Test
    void testCloneSeriesWithoutData() {

        // Arrange
        when(seriesService.findSeries("source-series")).thenReturn(Optional.of(sourceSeries));

        // Act
        cloneService.cloneSeries("source-series", "dest-series", false);

        // Assert
        verify(seriesService).findSeries("source-series");
        verify(seriesService).createSeries(seriesFileCaptor.capture());

        // Verify the cloned series has the right properties
        SeriesFile capturedSeries = seriesFileCaptor.getValue();
        assertEquals("dest-series", capturedSeries.getId());
        assertEquals(sourceSeries.getDefinition().getType(), capturedSeries.getDefinition().getType());
        assertEquals(sourceSeries.getDefinition().getPartition(), capturedSeries.getDefinition().getPartition());
        assertEquals(sourceSeries.getDefinition().getInterval(), capturedSeries.getDefinition().getInterval());
        assertEquals(sourceSeries.getMetadata(), capturedSeries.getMetadata());

        // Verify no partitions were cloned
        verify(dataService, never()).getBuffer(any(PartitionKey.class));
    }

    @Test
    void testCloneSeriesWithData() {

        // Arrange
        when(seriesService.findSeries("source-series")).thenReturn(Optional.of(sourceSeries));
        when(dataService.getPartitions("source-series")).thenReturn(Set.of(partitionKey1, partitionKey2));

        // Setup partition service behavior
        when(partitionService.getRange(eq(partitionKey1))).thenReturn(sourceRange);
        when(partitionService.getRange(eq(partitionKey2))).thenReturn(sourceRange); // Reuse for simplicity
        when(partitionService.getRange(eq(new PartitionKey("dest-series", "20240701")))).thenReturn(destinationRange);
        when(partitionService.getRange(eq(new PartitionKey("dest-series", "20240702")))).thenReturn(destinationRange); // Reuse for simplicity

        // Setup data service behavior
        when(dataService.getBuffer(eq(partitionKey1))).thenReturn(Optional.of(sourceBuffer));
        when(dataService.getBuffer(eq(partitionKey2))).thenReturn(Optional.of(sourceBuffer));
        when(dataService.getBuffer(eq(new PartitionKey("dest-series", "20240701")), anyLong(), any())).thenReturn(destinationBuffer);
        when(dataService.getBuffer(eq(new PartitionKey("dest-series", "20240702")), anyLong(), any())).thenReturn(destinationBuffer);

        // Act
        cloneService.cloneSeries("source-series", "dest-series", true);

        // Assert
        verify(seriesService).findSeries("source-series");
        verify(seriesService).createSeries(seriesFileCaptor.capture());

        // Verify the cloned series has the right properties
        SeriesFile capturedSeries = seriesFileCaptor.getValue();
        assertEquals("dest-series", capturedSeries.getId());

        // Verify partitions were cloned
        verify(dataService).getBuffer(eq(partitionKey1));
        verify(dataService).getBuffer(eq(partitionKey2));
        verify(dataService).getBuffer(eq(new PartitionKey("dest-series", "20240701")), anyLong(), any());
        verify(dataService).getBuffer(eq(new PartitionKey("dest-series", "20240702")), anyLong(), any());
    }

    @Test
    void testCloneSeriesSourceNotFound() {

        // Arrange
        when(seriesService.findSeries("source-series")).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(NoSuchElementException.class, () ->
            cloneService.cloneSeries("source-series", "dest-series", false));

        // Verify no series was created
        verify(seriesService, never()).createSeries(any());
    }

    @Test
    void testUpdateDefinition() {

        // Arrange
        SeriesDefinition updatedDefinition = SeriesDefinition.builder()
            .id("source-series")
            .partition(PartitionPeriod.MONTH)  // Changed from DAY
            .interval(30000L)                 // Changed from 60000L
            .build();

        when(seriesService.findSeries("source-series")).thenReturn(Optional.of(sourceSeries));

        // Act
        cloneService.updateDefinition("source-series", updatedDefinition);

        // Assert
        verify(seriesService, times(2)).findSeries("source-series");
        verify(seriesService, times(2)).createSeries(seriesFileCaptor.capture());
        verify(seriesService, times(2)).deleteSeries(any());
        verify(dataService, times(2)).getPartitions(any());

        // Verify only the appropriate fields were updated
        SeriesFile capturedSeries = seriesFileCaptor.getAllValues().getFirst();
        assertNotEquals("source-series", capturedSeries.getId());
        assertEquals(PartitionPeriod.DAY, capturedSeries.getDefinition().getPartition());
        assertEquals(60000L, capturedSeries.getDefinition().getInterval());
        assertEquals(sourceSeries.getMetadata(), capturedSeries.getMetadata());

        // Verify only the appropriate fields were updated
        capturedSeries = seriesFileCaptor.getAllValues().get(1);
        assertEquals("source-series", capturedSeries.getId());
        assertEquals(PartitionPeriod.MONTH, capturedSeries.getDefinition().getPartition());
        assertEquals(30000L, capturedSeries.getDefinition().getInterval());
        assertEquals(sourceSeries.getMetadata(), capturedSeries.getMetadata());
    }

    @Test
    void testUpdateDefinitionSeriesNotFound() {

        // Arrange
        SeriesDefinition updatedDefinition = SeriesDefinition.builder()
            .id("source-series")
            .partition(PartitionPeriod.MONTH)
            .interval(30000L)
            .build();

        when(seriesService.findSeries("source-series")).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(NoSuchElementException.class, () ->
            cloneService.updateDefinition("source-series", updatedDefinition));

        // Verify no series was updated
        verify(dataService, never()).saveSeries(any());
    }

    @Test
    void testClonePartition() {

        // Arrange
        when(partitionService.getRange(eq(partitionKey1))).thenReturn(sourceRange);
        when(partitionService.getRange(eq(new PartitionKey("dest-series", "20240701")))).thenReturn(destinationRange);
        when(dataService.getPartitions("source-series")).thenReturn(Set.of(partitionKey1));
        when(dataService.getBuffer(eq(partitionKey1))).thenReturn(Optional.of(sourceBuffer));
        when(dataService.getBuffer(eq(new PartitionKey("dest-series", "20240701")), anyLong(), any())).thenReturn(destinationBuffer);
        when(seriesService.findSeries("source-series")).thenReturn(Optional.of(sourceSeries));

        // Act
        cloneService.cloneSeries("source-series", "dest-series", true);

        // Verify the partition cloning behavior
        verify(seriesService).createSeries(seriesFileCaptor.capture());
        verify(dataService).getBuffer(eq(partitionKey1));
        verify(dataService).getBuffer(eq(new PartitionKey("dest-series", "20240701")), eq(sourceRange.getSize()), eq(sourceRange.getAdapter()));

        SeriesFile capturedSeries = seriesFileCaptor.getValue();
        assertEquals("dest-series", capturedSeries.getId());
        assertEquals(PartitionPeriod.DAY, capturedSeries.getDefinition().getPartition());
        assertEquals(60000L, capturedSeries.getDefinition().getInterval());
        assertEquals(sourceSeries.getMetadata(), capturedSeries.getMetadata());
    }
}
