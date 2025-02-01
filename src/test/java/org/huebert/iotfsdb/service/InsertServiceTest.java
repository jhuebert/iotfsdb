package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.huebert.iotfsdb.schema.InsertRequest;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class InsertServiceTest {

    @Mock
    private DataService dataService;

    @Mock
    private PartitionService partitionService;

    @Mock
    private PartitionAdapter partitionAdapter;

    @InjectMocks
    private InsertService insertService;

    @Test
    public void testGetRange() {

        ZonedDateTime time1 = ZonedDateTime.parse("2024-08-11T00:00:00-06:00");
        ZonedDateTime time2 = ZonedDateTime.parse("2024-09-11T01:00:00-06:00");
        ZonedDateTime time3 = ZonedDateTime.parse("2024-10-11T02:00:00-06:00");
        ZonedDateTime time4 = ZonedDateTime.parse("2024-11-11T03:00:00-06:00");

        when(dataService.getSeries("123")).thenReturn(Optional.of(SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .partition(PartitionPeriod.DAY)
                .interval(3600000L)
                .build())
            .build()));

        PartitionKey key1 = new PartitionKey("123", "20240811");
        PartitionKey key2 = new PartitionKey("123", "20240911");
        PartitionKey key3 = new PartitionKey("123", "20241011");
        PartitionKey key4 = new PartitionKey("123", "20241111");

        when(partitionService.getRange(key1)).thenReturn(new PartitionRange(key1, Range.closed(LocalDateTime.parse("2024-08-11T00:00:00"), LocalDateTime.parse("2024-08-12T00:00:00").minusNanos(1)), Duration.ofHours(1), partitionAdapter, new ReentrantReadWriteLock()));
        when(partitionService.getRange(key2)).thenReturn(new PartitionRange(key2, Range.closed(LocalDateTime.parse("2024-09-11T00:00:00"), LocalDateTime.parse("2024-09-12T00:00:00").minusNanos(1)), Duration.ofHours(1), partitionAdapter, new ReentrantReadWriteLock()));
        when(partitionService.getRange(key3)).thenReturn(new PartitionRange(key3, Range.closed(LocalDateTime.parse("2024-10-11T00:00:00"), LocalDateTime.parse("2024-10-12T00:00:00").minusNanos(1)), Duration.ofHours(1), partitionAdapter, new ReentrantReadWriteLock()));
        when(partitionService.getRange(key4)).thenReturn(new PartitionRange(key4, Range.closed(LocalDateTime.parse("2024-11-11T00:00:00"), LocalDateTime.parse("2024-11-12T00:00:00").minusNanos(1)), Duration.ofHours(1), partitionAdapter, new ReentrantReadWriteLock()));

        ByteBuffer byteBuffer1 = ByteBuffer.allocate(24);
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(24);
        ByteBuffer byteBuffer3 = ByteBuffer.allocate(24);
        ByteBuffer byteBuffer4 = ByteBuffer.allocate(24);

        when(dataService.getBuffer(key1, 24L, partitionAdapter)).thenReturn(byteBuffer1);
        when(dataService.getBuffer(key2, 24L, partitionAdapter)).thenReturn(byteBuffer2);
        when(dataService.getBuffer(key3, 24L, partitionAdapter)).thenReturn(byteBuffer3);
        when(dataService.getBuffer(key4, 24L, partitionAdapter)).thenReturn(byteBuffer4);

        insertService.insert(new InsertRequest("123", List.of(
            new SeriesData(time1, 1),
            new SeriesData(time2, null),
            new SeriesData(time3, 2),
            new SeriesData(time4, 3)
        ), null));

        verify(partitionAdapter).put(byteBuffer1, 6, 1);
        verify(partitionAdapter).put(byteBuffer2, 7, null);
        verify(partitionAdapter).put(byteBuffer3, 8, 2);
        verify(partitionAdapter).put(byteBuffer4, 9, 3);
    }

}
