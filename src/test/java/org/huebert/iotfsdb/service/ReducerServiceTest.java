package org.huebert.iotfsdb.service;

import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindDataResponse;
import org.huebert.iotfsdb.schema.NumberType;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.Reducer;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class ReducerServiceTest {

    @InjectMocks
    private ReducerService reducerService;

    private final FindDataRequest request = new FindDataRequest();

    @Test
    public void testReduceSeries() {
        request.setSeriesReducer(Reducer.AVERAGE);
        request.setInterval(3600000L);

        ZonedDateTime time1 = ZonedDateTime.parse("2024-11-11T00:00:00-06:00");
        ZonedDateTime time2 = ZonedDateTime.parse("2024-11-11T01:00:00-06:00");
        ZonedDateTime time3 = ZonedDateTime.parse("2024-11-11T02:00:00-06:00");
        ZonedDateTime time4 = ZonedDateTime.parse("2024-11-11T03:00:00-06:00");

        List<FindDataResponse> responses = List.of(
            FindDataResponse.builder()
                .data(List.of(new SeriesData(time1, 1), new SeriesData(time2, null), new SeriesData(time3, 2), new SeriesData(time4, 3)))
                .series(SeriesFile.builder().metadata(Map.of("a", "1", "b", "1", "c", "1")).build())
                .build(),
            FindDataResponse.builder()
                .data(List.of(new SeriesData(time1, 14), new SeriesData(time2, null), new SeriesData(time3, 15), new SeriesData(time4, 16)))
                .series(SeriesFile.builder().metadata(Map.of("a", "1", "b", "1", "d", "1")).build())
                .build(),
            FindDataResponse.builder()
                .data(List.of(new SeriesData(time1, 3), new SeriesData(time2, null), new SeriesData(time3, 4), new SeriesData(time4, 5)))
                .series(SeriesFile.builder().metadata(Map.of("a", "1", "b", "2", "e", "1")).build())
                .build()
        );

        FindDataResponse result = reducerService.reduce(responses, request);
        assertThat(result.getSeries().getId()).isEqualTo("reduced");
        assertThat(result.getSeries().getDefinition().getInterval()).isEqualTo(3600000L);
        assertThat(result.getSeries().getDefinition().getPartition()).isEqualTo(PartitionPeriod.DAY);
        assertThat(result.getSeries().getDefinition().getType()).isEqualTo(NumberType.FLOAT8);
        assertThat(result.getSeries().getMetadata()).isEqualTo(Map.of("a", "1"));
        assertThat(result.getData().size()).isEqualTo(3);
        assertThat(result.getData().get(0)).isEqualTo(new SeriesData(time1, 6.0));
        assertThat(result.getData().get(1)).isEqualTo(new SeriesData(time3, 7.0));
        assertThat(result.getData().get(2)).isEqualTo(new SeriesData(time4, 8.0));

        request.setIncludeNull(true);

        result = reducerService.reduce(responses, request);
        assertThat(result.getData().size()).isEqualTo(4);
        assertThat(result.getData().get(0)).isEqualTo(new SeriesData(time1, 6.0));
        assertThat(result.getData().get(1)).isEqualTo(new SeriesData(time2, null));
        assertThat(result.getData().get(2)).isEqualTo(new SeriesData(time3, 7.0));
        assertThat(result.getData().get(3)).isEqualTo(new SeriesData(time4, 8.0));

        request.setUsePrevious(true);

        result = reducerService.reduce(responses, request);
        assertThat(result.getData().size()).isEqualTo(4);
        assertThat(result.getData().get(0)).isEqualTo(new SeriesData(time1, 6.0));
        assertThat(result.getData().get(1)).isEqualTo(new SeriesData(time2, 6.0));
        assertThat(result.getData().get(2)).isEqualTo(new SeriesData(time3, 7.0));
        assertThat(result.getData().get(3)).isEqualTo(new SeriesData(time4, 8.0));
    }

    private Optional<Number> reduce(FindDataRequest request, Stream<Number> stream, Reducer reducer) {
        return Optional.ofNullable(stream.collect(reducerService.getCollector(request, reducer)));
    }

    @Test
    public void testAverage() {
        assertThat(reduce(request, Stream.of(), Reducer.AVERAGE)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.AVERAGE)).isEqualTo(Optional.of(3.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.AVERAGE)).isEqualTo(Optional.of(2.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.AVERAGE)).isEqualTo(Optional.of(4.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.AVERAGE)).isEqualTo(Optional.of(2.25));
    }

    @Test
    public void testCount() {
        assertThat(reduce(request, Stream.of(), Reducer.COUNT)).isEqualTo(Optional.of((long) 0));
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.COUNT)).isEqualTo(Optional.of((long) 1));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.COUNT)).isEqualTo(Optional.of((long) 2));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.COUNT)).isEqualTo(Optional.of((long) 3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.COUNT)).isEqualTo(Optional.of((long) 4));
    }

    @Test
    public void testCountDistinct() {
        assertThat(reduce(request, Stream.of(), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(0));
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(1));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(2));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(3));
    }

    @Test
    public void testFirst() {
        assertThat(reduce(request, Stream.of(), Reducer.FIRST)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.FIRST)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.FIRST)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.FIRST)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.FIRST)).isEqualTo(Optional.of(3));
    }

    @Test
    public void testLast() {
        assertThat(reduce(request, Stream.of(), Reducer.LAST)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.LAST)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.LAST)).isEqualTo(Optional.of(1));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.LAST)).isEqualTo(Optional.of(8));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.LAST)).isEqualTo(Optional.of(4));
    }

    @Test
    public void testMaximum() {
        assertThat(reduce(request, Stream.of(), Reducer.MAXIMUM)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.MAXIMUM)).isEqualTo(Optional.of(3.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.MAXIMUM)).isEqualTo(Optional.of(3.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.MAXIMUM)).isEqualTo(Optional.of(8.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.MAXIMUM)).isEqualTo(Optional.of(4.0));
    }

    @Test
    public void testMedian() {
        assertThat(reduce(request, Stream.of(), Reducer.MEDIAN)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.MEDIAN)).isEqualTo(Optional.of(3.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.MEDIAN)).isEqualTo(Optional.of(2.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.MEDIAN)).isEqualTo(Optional.of(3.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.MEDIAN)).isEqualTo(Optional.of(2.0));
    }

    @Test
    public void testMinimum() {
        assertThat(reduce(request, Stream.of(), Reducer.MINIMUM)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.MINIMUM)).isEqualTo(Optional.of(3.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.MINIMUM)).isEqualTo(Optional.of(1.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.MINIMUM)).isEqualTo(Optional.of(1.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.MINIMUM)).isEqualTo(Optional.of(1.0));
    }

    @Test
    public void testMode() {
        assertThat(reduce(request, Stream.of(), Reducer.MODE)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.MODE)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.MODE)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.MODE)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.MODE)).isEqualTo(Optional.of(1));
    }

    @Test
    public void testSquareSum() {
        assertThat(reduce(request, Stream.of(), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(0.0));
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(9.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(10.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(74.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(27.0));
    }

    @Test
    public void testSum() {
        assertThat(reduce(request, Stream.of(), Reducer.SUM)).isEqualTo(Optional.of(0.0));
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.SUM)).isEqualTo(Optional.of(3.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.SUM)).isEqualTo(Optional.of(4.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.SUM)).isEqualTo(Optional.of(12.0));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.SUM)).isEqualTo(Optional.of(9.0));
    }

    @Test
    public void testAverage_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.AVERAGE)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.AVERAGE)).isEqualTo(Optional.of(new BigDecimal("3")));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.AVERAGE)).isEqualTo(Optional.of(new BigDecimal("2")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.AVERAGE)).isEqualTo(Optional.of(new BigDecimal("4")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.AVERAGE)).isEqualTo(Optional.of(new BigDecimal("2.25")));
    }

    @Test
    public void testCount_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.COUNT)).isEqualTo(Optional.of((long) 0));
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.COUNT)).isEqualTo(Optional.of((long) 1));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.COUNT)).isEqualTo(Optional.of((long) 2));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.COUNT)).isEqualTo(Optional.of((long) 3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.COUNT)).isEqualTo(Optional.of((long) 4));
    }

    @Test
    public void testCountDistinct_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(0));
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(1));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(2));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.COUNT_DISTINCT)).isEqualTo(Optional.of(3));
    }

    @Test
    public void testFirst_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.FIRST)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.FIRST)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.FIRST)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.FIRST)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.FIRST)).isEqualTo(Optional.of(3));
    }

    @Test
    public void testLast_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.LAST)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.LAST)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.LAST)).isEqualTo(Optional.of(1));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.LAST)).isEqualTo(Optional.of(8));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.LAST)).isEqualTo(Optional.of(4));
    }

    @Test
    public void testMaximum_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.MAXIMUM)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.MAXIMUM)).isEqualTo(Optional.of(new BigDecimal("3")));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.MAXIMUM)).isEqualTo(Optional.of(new BigDecimal("3")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.MAXIMUM)).isEqualTo(Optional.of(new BigDecimal("8")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.MAXIMUM)).isEqualTo(Optional.of(new BigDecimal("4")));
    }

    @Test
    public void testMedian_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.MEDIAN)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.MEDIAN)).isEqualTo(Optional.of(new BigDecimal("3")));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.MEDIAN)).isEqualTo(Optional.of(new BigDecimal("2")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.MEDIAN)).isEqualTo(Optional.of(new BigDecimal("3")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.MEDIAN)).isEqualTo(Optional.of(new BigDecimal("2")));
    }

    @Test
    public void testMinimum_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.MINIMUM)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.MINIMUM)).isEqualTo(Optional.of(new BigDecimal("3")));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.MINIMUM)).isEqualTo(Optional.of(new BigDecimal("1")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.MINIMUM)).isEqualTo(Optional.of(new BigDecimal("1")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.MINIMUM)).isEqualTo(Optional.of(new BigDecimal("1")));
    }

    @Test
    public void testMode_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.MODE)).isEqualTo(Optional.empty());
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.MODE)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.MODE)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.MODE)).isEqualTo(Optional.of(3));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.MODE)).isEqualTo(Optional.of(1));
    }

    @Test
    public void testSquareSum_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(new BigDecimal("0")));
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(new BigDecimal("9")));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(new BigDecimal("10")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(new BigDecimal("74")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.SQUARE_SUM)).isEqualTo(Optional.of(new BigDecimal("27")));
    }

    @Test
    public void testSum_BigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(), Reducer.SUM)).isEqualTo(Optional.of(new BigDecimal("0")));
        assertThat(reduce(request, Stream.of(null, 3, null), Reducer.SUM)).isEqualTo(Optional.of(new BigDecimal("3")));
        assertThat(reduce(request, Stream.of(null, 3, 1, null), Reducer.SUM)).isEqualTo(Optional.of(new BigDecimal("4")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 8, null), Reducer.SUM)).isEqualTo(Optional.of(new BigDecimal("12")));
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.SUM)).isEqualTo(Optional.of(new BigDecimal("9")));
    }

    @Test
    public void testSum_BigDecimalWithBigDecimal() {
        request.setUseBigDecimal(true);
        assertThat(reduce(request, Stream.of(null, new BigDecimal("3"), new BigDecimal("1"), new BigDecimal("1"), new BigDecimal("4"), null), Reducer.SUM)).isEqualTo(Optional.of(new BigDecimal("9")));
    }

    @Test
    public void testSum_WithNullValue() {
        request.setNullValue(5);
        assertThat(reduce(request, Stream.of(null, 3, 1, 1, 4, null), Reducer.SUM)).isEqualTo(Optional.of(19.0));
    }
}
