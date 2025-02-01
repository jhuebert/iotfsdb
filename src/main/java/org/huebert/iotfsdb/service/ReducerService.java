package org.huebert.iotfsdb.service;

import com.google.common.collect.Maps;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.collectors.AveragingCollector;
import org.huebert.iotfsdb.collectors.BigDecimalAveragingCollector;
import org.huebert.iotfsdb.collectors.BigDecimalMaximumCollector;
import org.huebert.iotfsdb.collectors.BigDecimalMedianCollector;
import org.huebert.iotfsdb.collectors.BigDecimalMinimumCollector;
import org.huebert.iotfsdb.collectors.BigDecimalMultiplyingCollector;
import org.huebert.iotfsdb.collectors.BigDecimalSummingCollector;
import org.huebert.iotfsdb.collectors.CountingCollector;
import org.huebert.iotfsdb.collectors.CountingDistinctCollector;
import org.huebert.iotfsdb.collectors.FirstCollector;
import org.huebert.iotfsdb.collectors.LastCollector;
import org.huebert.iotfsdb.collectors.MaximumCollector;
import org.huebert.iotfsdb.collectors.MedianCollector;
import org.huebert.iotfsdb.collectors.MinimumCollector;
import org.huebert.iotfsdb.collectors.ModeCollector;
import org.huebert.iotfsdb.collectors.MultiplyingCollector;
import org.huebert.iotfsdb.collectors.SummingCollector;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindDataResponse;
import org.huebert.iotfsdb.schema.Reducer;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Validated
@Slf4j
@Service
public class ReducerService {

    private static final Map<Reducer, Collector<Number, ?, Number>> SELECTING_REDUCERS = new EnumMap<>(Map.of(
        Reducer.COUNT, new CountingCollector(),
        Reducer.COUNT_DISTINCT, new CountingDistinctCollector(),
        Reducer.FIRST, new FirstCollector(),
        Reducer.LAST, new LastCollector(),
        Reducer.MODE, new ModeCollector()
    ));

    private static final Map<Reducer, Collector<Double, ?, Number>> DOUBLE_REDUCERS = new EnumMap<>(Map.of(
        Reducer.AVERAGE, new AveragingCollector(),
        Reducer.MAXIMUM, new MaximumCollector(),
        Reducer.MEDIAN, new MedianCollector(),
        Reducer.MINIMUM, new MinimumCollector(),
        Reducer.MULTIPLY, new MultiplyingCollector(),
        Reducer.SQUARE_SUM, Collectors.mapping(v -> v * v, new SummingCollector()),
        Reducer.SUM, new SummingCollector()
    ));

    private static final Map<Reducer, Collector<BigDecimal, ?, Number>> BIG_DECIMAL_REDUCERS = new EnumMap<>(Map.of(
        Reducer.AVERAGE, new BigDecimalAveragingCollector(),
        Reducer.MAXIMUM, new BigDecimalMaximumCollector(),
        Reducer.MEDIAN, new BigDecimalMedianCollector(),
        Reducer.MINIMUM, new BigDecimalMinimumCollector(),
        Reducer.MULTIPLY, new BigDecimalMultiplyingCollector(),
        Reducer.SQUARE_SUM, Collectors.mapping(v -> v.pow(2), new BigDecimalSummingCollector()),
        Reducer.SUM, new BigDecimalSummingCollector()
    ));

    public Collector<Number, ?, Number> getCollector(@Valid @NotNull FindDataRequest request, @NotNull Reducer reducer) {
        return getCollector(reducer, request.isUseBigDecimal(), request.getNullValue());
    }

    public Collector<Number, ?, Number> getCollector(@NotNull Reducer reducer, boolean useBigDecimal, Number nullValue) {
        Collector<Number, ?, Number> collector = SELECTING_REDUCERS.get(reducer);
        if (collector == null) {
            if (useBigDecimal) {
                collector = getBigDecimalCollector(reducer);
            } else {
                collector = getDoubleCollector(reducer);
            }
        }
        return Collectors.mapping(
            v -> v != null ? v : nullValue,
            Collectors.filtering(
                Objects::nonNull,
                collector
            )
        );
    }

    private static Collector<Number, ?, Number> getDoubleCollector(Reducer reducer) {
        Collector<Double, ?, Number> collector = DOUBLE_REDUCERS.get(reducer);
        if (collector == null) {
            throw new IllegalArgumentException(String.format("reducer %s not supported", reducer));
        }
        return Collectors.mapping(Number::doubleValue, collector);
    }

    private static Collector<Number, ?, Number> getBigDecimalCollector(Reducer reducer) {
        Collector<BigDecimal, ?, Number> collector = BIG_DECIMAL_REDUCERS.get(reducer);
        if (collector == null) {
            throw new IllegalArgumentException(String.format("reducer %s not supported", reducer));
        }
        return Collectors.mapping(ReducerService::toBigDecimal, collector);
    }

    private static BigDecimal toBigDecimal(Number n) {
        return (n instanceof BigDecimal bd) ? bd : new BigDecimal(n.toString());
    }

    public FindDataResponse reduce(@NotNull List<FindDataResponse> responses, @Valid @NotNull FindDataRequest request) {

        Map<ZonedDateTime, List<SeriesData>> grouped = responses.parallelStream()
            .map(FindDataResponse::getData)
            .flatMap(Collection::stream)
            .collect(Collectors.groupingByConcurrent(SeriesData::getTime));

        Collector<Number, ?, Number> collector = getCollector(request, request.getSeriesReducer());

        List<SeriesData> data = grouped.entrySet().stream()
            .map(e -> new SeriesData(
                e.getKey(),
                e.getValue().stream()
                    .map(SeriesData::getValue)
                    .collect(collector)
            ))
            .sorted(Comparator.comparing(SeriesData::getTime))
            .peek(request.getPreviousConsumer())
            .filter(request.getNullPredicate())
            .toList();

        Map<String, String> metadata = responses.stream()
            .map(FindDataResponse::getSeries)
            .map(SeriesFile::getMetadata)
            .reduce((a, b) -> Maps.difference(a, b).entriesInCommon())
            .orElse(Map.of());

        return FindDataResponse.builder()
            .series(SeriesFile.builder()
                .definition(SeriesDefinition.builder()
                    .id("reduced")
                    .build())
                .metadata(metadata)
                .build())
            .data(data)
            .build();
    }

}
