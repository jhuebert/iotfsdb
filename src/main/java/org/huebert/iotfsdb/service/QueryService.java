package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Stream;

@Validated
@Service
public class QueryService {

    private final DataService dataService;

    private final ReducerService reducerService;

    private final IntervalService intervalService;

    private final PartitionService partitionService;

    private final SeriesService seriesService;

    public QueryService(@NotNull DataService dataService, @NotNull ReducerService reducerService, @NotNull IntervalService intervalService, @NotNull PartitionService partitionService, @NotNull SeriesService seriesService) {
        this.dataService = dataService;
        this.reducerService = reducerService;
        this.intervalService = intervalService;
        this.partitionService = partitionService;
        this.seriesService = seriesService;
    }

    public List<FindDataResponse> findData(@Valid @NotNull FindDataRequest request) {

        List<SeriesFile> series = seriesService.findSeries(request.getSeries());

        List<Range<ZonedDateTime>> ranges = intervalService.getIntervalRanges(request);

        List<FindDataResponse> result = ParallelUtil.map(series, s -> findDataForSeries(request, ranges, s)).stream()
            .filter(r -> r.getData().stream().anyMatch(data -> data.getValue() != null))
            .sorted(Comparator.comparing(r -> r.getSeries().getId()))
            .toList();

        return request.getSeriesReducer() != null
            ? List.of(reducerService.reduce(result, request))
            : result;
    }

    private FindDataResponse findDataForSeries(@Valid @NotNull FindDataRequest request, @NotNull List<Range<ZonedDateTime>> ranges, SeriesFile series) {
        RangeMap<LocalDateTime, PartitionRange> rangeMap = partitionService.getRangeMap(series.getId());
        Collector<Number, ?, Number> collector = reducerService.getCollector(request, request.getTimeReducer());
        return new FindDataResponse(
            series,
            ranges.stream()
                .map(current -> findDataOverPartitions(collector, rangeMap, current))
                .peek(request.getPreviousConsumer())
                .filter(request.getNullPredicate())
                .toList()
        );
    }

    private SeriesData findDataOverPartitions(Collector<Number, ?, Number> collector, RangeMap<LocalDateTime, PartitionRange> rangeMap, Range<ZonedDateTime> current) {
        Range<LocalDateTime> local = TimeConverter.toUtc(current);
        Collection<PartitionRange> covered = rangeMap.subRangeMap(local).asMapOfRanges().values();
        covered.forEach(c -> c.getRwLock().readLock().lock());
        try {
            Number value = covered.stream()
                .flatMap(pr -> findDataFromPartition(pr, local))
                .collect(collector);
            return new SeriesData(current.lowerEndpoint(), value);
        } finally {
            covered.forEach(c -> c.getRwLock().readLock().unlock());
        }
    }

    private Stream<Number> findDataFromPartition(PartitionRange partitionRange, Range<LocalDateTime> current) {
        return dataService.getBuffer(partitionRange.getKey())
            .map(b -> partitionRange.getStream(b, current).stream())
            .orElse(Stream.empty());
    }

}
