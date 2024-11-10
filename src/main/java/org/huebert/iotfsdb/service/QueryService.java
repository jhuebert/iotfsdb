package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindDataResponse;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Validated
@Service
public class QueryService {

    private final DataService dataService;

    private final ReducerService reducerService;

    private final IntervalService intervalService;

    private final PartitionService partitionService;

    public QueryService(@NotNull DataService dataService, @NotNull ReducerService reducerService, @NotNull IntervalService intervalService, @NotNull PartitionService partitionService) {
        this.dataService = dataService;
        this.reducerService = reducerService;
        this.intervalService = intervalService;
        this.partitionService = partitionService;
    }

    public List<FindDataResponse> findData(@Valid @NotNull FindDataRequest request, @NotNull List<SeriesFile> series) {

        List<Range<ZonedDateTime>> ranges = intervalService.getIntervalRanges(request);

        List<FindDataResponse> result = series.parallelStream() // TODO Test the impact of parallel
            .map(s -> findDataForSeries(request, ranges, s))
            .filter(r -> r.getData().stream().map(SeriesData::getValue).anyMatch(Objects::nonNull))
            .sorted(Comparator.comparing(r -> r.getSeries().getId()))
            .toList();

        if (result.isEmpty() || (request.getSeriesReducer() == null)) {
            return result;
        }

        return List.of(reducerService.reduce(result, request));
    }

    private FindDataResponse findDataForSeries(@Valid @NotNull FindDataRequest request, @NotNull List<Range<ZonedDateTime>> ranges, SeriesFile series) {
        RangeMap<LocalDateTime, PartitionRange> rangeMap = partitionService.getRangeMap(series.getId());
        return new FindDataResponse(
            series,
            ranges.stream()
                .map(current -> findDataOverPartitions(request, rangeMap, current))
                .peek(request.getPreviousConsumer())
                .filter(request.getNullPredicate())
                .toList()
        );
    }

    private SeriesData findDataOverPartitions(FindDataRequest request, RangeMap<LocalDateTime, PartitionRange> rangeMap, Range<ZonedDateTime> current) {
        Range<LocalDateTime> local = TimeConverter.toUtc(current);
        Number value = rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
            .flatMap(pr -> findDataFromPartition(pr, local))
            .collect(reducerService.getCollector(request, request.getTimeReducer()));
        return new SeriesData(current.lowerEndpoint(), value); // TODO Midpoint?
    }

    private Stream<Number> findDataFromPartition(PartitionRange partitionRange, Range<LocalDateTime> current) {
        Range<LocalDateTime> intersection = partitionRange.range().intersection(current);
        int fromIndex = partitionRange.getIndex(intersection.lowerEndpoint());
        int toIndex = partitionRange.getIndex(intersection.upperEndpoint());
        return dataService.getBuffer(partitionRange.key())
            .map(b -> partitionRange.adapter().getStream(b, fromIndex, toIndex - fromIndex + 1))
            .orElse(Stream.empty());
    }

}
