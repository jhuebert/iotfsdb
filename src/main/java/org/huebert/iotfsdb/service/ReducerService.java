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
import org.huebert.iotfsdb.collectors.BigDecimalSummingCollector;
import org.huebert.iotfsdb.collectors.CountingCollector;
import org.huebert.iotfsdb.collectors.CountingDistinctCollector;
import org.huebert.iotfsdb.collectors.FirstCollector;
import org.huebert.iotfsdb.collectors.LastCollector;
import org.huebert.iotfsdb.collectors.MaximumCollector;
import org.huebert.iotfsdb.collectors.MedianCollector;
import org.huebert.iotfsdb.collectors.MinimumCollector;
import org.huebert.iotfsdb.collectors.ModeCollector;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Validated
@Slf4j
@Service
public class ReducerService {

    public Collector<Number, ?, Number> getCollector(@Valid @NotNull FindDataRequest request, @NotNull Reducer reducer) {
        return getCollector(reducer, request.isUseBigDecimal(), request.getNullValue());
    }

    public Collector<Number, ?, Number> getCollector(@NotNull Reducer reducer, boolean useBigDecimal, Number nullValue) {
        Collector<Number, ?, Number> collector;
        if (reducer == Reducer.COUNT) {
            collector = new CountingCollector();
        } else if (reducer == Reducer.FIRST) {
            collector = new FirstCollector();
        } else if (reducer == Reducer.LAST) {
            collector = new LastCollector();
        } else if (reducer == Reducer.COUNT_DISTINCT) {
            collector = new CountingDistinctCollector();
        } else if (reducer == Reducer.MODE) {
            collector = new ModeCollector();
        } else if (useBigDecimal) {
            collector = getBigDecimalCollector(reducer);
        } else {
            collector = getDoubleCollector(reducer);
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
        Collector<Double, ?, Number> collector;
        if (reducer == Reducer.AVERAGE) {
            collector = new AveragingCollector();
        } else if (reducer == Reducer.SUM) {
            collector = new SummingCollector();
        } else if (reducer == Reducer.MINIMUM) {
            collector = new MinimumCollector();
        } else if (reducer == Reducer.MAXIMUM) {
            collector = new MaximumCollector();
        } else if (reducer == Reducer.SQUARE_SUM) {
            collector = Collectors.mapping(v -> v * v, new SummingCollector());
        } else if (reducer == Reducer.MEDIAN) {
            collector = new MedianCollector();
        } else {
            throw new IllegalArgumentException(String.format("reducer %s not supported", reducer));
        }
        return Collectors.mapping(Number::doubleValue, collector);
    }

    private static Collector<Number, ?, Number> getBigDecimalCollector(Reducer reducer) {
        Collector<BigDecimal, ?, Number> collector;
        if (reducer == Reducer.AVERAGE) {
            collector = new BigDecimalAveragingCollector();
        } else if (reducer == Reducer.SUM) {
            collector = new BigDecimalSummingCollector();
        } else if (reducer == Reducer.MINIMUM) {
            collector = new BigDecimalMinimumCollector();
        } else if (reducer == Reducer.MAXIMUM) {
            collector = new BigDecimalMaximumCollector();
        } else if (reducer == Reducer.SQUARE_SUM) {
            collector = Collectors.mapping(v -> v.pow(2), new BigDecimalSummingCollector());
        } else if (reducer == Reducer.MEDIAN) {
            collector = new BigDecimalMedianCollector();
        } else {
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
