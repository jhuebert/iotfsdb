package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.huebert.iotfsdb.collectors.CountingCollector;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
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
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class QueryServiceTest {

    @Mock
    private PartitionAdapter partitionAdapter;

    @Mock
    private DataService dataService;

    @Mock
    private ReducerService reducerService;

    @Mock
    private IntervalService intervalService;

    @Mock
    private PartitionService partitionService;

    @Mock
    private SeriesService seriesService;

    @InjectMocks
    private QueryService queryService;

    @Test
    void testFindData() {

        FindDataRequest request = new FindDataRequest();
        ZonedDateTime findStart = ZonedDateTime.parse("2024-11-10T02:00:00-06:00");
        request.setFrom(findStart);
        request.setTo(findStart.plusHours(2));

        when(intervalService.getIntervalRanges(request)).thenReturn(List.of(
            Range.closed(findStart, findStart.plusHours(1).minusNanos(1)),
            Range.closed(findStart.plusHours(1), findStart.plusHours(2).minusNanos(1))
        ));

        PartitionKey key = new PartitionKey("abc", "20241110");
        LocalDateTime partitionStart = LocalDateTime.parse("2024-11-10T00:00:00");
        Range<LocalDateTime> range = Range.closed(partitionStart, partitionStart.plusDays(1).minusNanos(1));

        RangeMap<LocalDateTime, PartitionRange> rangeMap = TreeRangeMap.create();
        rangeMap.put(range, new PartitionRange(key, range, Duration.ofHours(1), partitionAdapter, new ReentrantReadWriteLock()));
        when(partitionService.getRangeMap("abc")).thenReturn(rangeMap);

        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        when(dataService.getBuffer(key)).thenReturn(Optional.of(byteBuffer));

        when(partitionAdapter.getStream(byteBuffer, 8, 1)).thenReturn(IntStream.range(0, 100).mapToObj(a -> a));
        when(partitionAdapter.getStream(byteBuffer, 9, 1)).thenReturn(IntStream.range(0, 10).mapToObj(a -> a));

        when(reducerService.getCollector(eq(request), eq(Reducer.AVERAGE))).then(invocation -> new CountingCollector());

        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        when(seriesService.findSeries(any(FindSeriesRequest.class))).thenReturn(List.of(seriesFile));

        List<FindDataResponse> response = queryService.findData(request);
        assertThat(response.size()).isEqualTo(1);
        assertThat(response.getFirst().getSeries()).isEqualTo(seriesFile);
        assertThat(response.getFirst().getData().size()).isEqualTo(2);
        assertThat(response.getFirst().getData().get(0)).isEqualTo(new SeriesData(findStart, 100L));
        assertThat(response.getFirst().getData().get(1)).isEqualTo(new SeriesData(findStart.plusHours(1), 10L));

    }

    @Test
    void testFindData_WithSeriesReducer() {

        FindDataRequest request = new FindDataRequest();
        ZonedDateTime findStart = ZonedDateTime.parse("2024-11-10T02:00:00-06:00");
        request.setFrom(findStart);
        request.setTo(findStart.plusHours(2));
        request.setSeriesReducer(Reducer.SUM);

        when(intervalService.getIntervalRanges(request)).thenReturn(List.of(
            Range.closed(findStart, findStart.plusHours(1).minusNanos(1)),
            Range.closed(findStart.plusHours(1), findStart.plusHours(2).minusNanos(1))
        ));

        PartitionKey key = new PartitionKey("abc", "20241110");
        LocalDateTime partitionStart = LocalDateTime.parse("2024-11-10T00:00:00");
        Range<LocalDateTime> range = Range.closed(partitionStart, partitionStart.plusDays(1).minusNanos(1));

        RangeMap<LocalDateTime, PartitionRange> rangeMap = TreeRangeMap.create();
        rangeMap.put(range, new PartitionRange(key, range, Duration.ofHours(1), partitionAdapter, new ReentrantReadWriteLock()));
        when(partitionService.getRangeMap("abc")).thenReturn(rangeMap);

        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        when(dataService.getBuffer(key)).thenReturn(Optional.of(byteBuffer));

        when(partitionAdapter.getStream(byteBuffer, 8, 1)).thenReturn(IntStream.range(0, 100).mapToObj(a -> a));
        when(partitionAdapter.getStream(byteBuffer, 9, 1)).thenReturn(IntStream.range(0, 10).mapToObj(a -> a));

        when(reducerService.getCollector(eq(request), eq(Reducer.AVERAGE))).then(invocation -> new CountingCollector());

        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        when(seriesService.findSeries(any(FindSeriesRequest.class))).thenReturn(List.of(seriesFile));

        SeriesFile reduced = SeriesFile.builder().definition(SeriesDefinition.builder().id("reduced").build()).build();
        when(reducerService.reduce(any(), eq(request))).thenReturn(FindDataResponse.builder()
            .series(reduced)
            .data(List.of(
                new SeriesData(findStart, 101L),
                new SeriesData(findStart.plusHours(1), 11L)
            ))
            .build());

        List<FindDataResponse> response = queryService.findData(request);
        assertThat(response.size()).isEqualTo(1);
        assertThat(response.getFirst().getSeries()).isEqualTo(reduced);
        assertThat(response.getFirst().getData().size()).isEqualTo(2);
        assertThat(response.getFirst().getData().get(0)).isEqualTo(new SeriesData(findStart, 101L));
        assertThat(response.getFirst().getData().get(1)).isEqualTo(new SeriesData(findStart.plusHours(1), 11L));
    }

}
