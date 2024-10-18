package org.huebert.iotfsdb.service;

import com.github.f4b6a3.ulid.UlidCreator;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.partition.PartitionFactory;
import org.huebert.iotfsdb.rest.schema.FindDataRequest;
import org.huebert.iotfsdb.rest.schema.FindDataResponse;
import org.huebert.iotfsdb.rest.schema.SeriesData;
import org.huebert.iotfsdb.series.Reducer;
import org.huebert.iotfsdb.series.Series;
import org.huebert.iotfsdb.series.SeriesDefinition;
import org.huebert.iotfsdb.util.Util;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class SeriesService {

    private final IotfsdbProperties properties;

    private final Map<String, Series> seriesMap = new ConcurrentHashMap<>();

    public SeriesService(IotfsdbProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void postConstruct() {
        File root = Util.checkDirectory(properties.getRoot());

        File[] files = root.listFiles();
        if (files == null) {
            throw new IllegalArgumentException(String.format("database directory (%s) contains no files", root));
        }

        // TODO Log

        Stream.of(files)
            .parallel()
            .filter(File::isDirectory)
            .forEach(file -> {
                Series series = new Series(file);
                seriesMap.put(series.getDefinition().getId(), series);
            });

        // TODO Log
    }

    @Scheduled(fixedRate = 5, timeUnit = TimeUnit.MINUTES)
    public void closeIfIdle() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("closeIfIdle(request): trace={}", trace);
        long count = seriesMap.values().parallelStream().map(Series::closeIfIdle).mapToLong(a -> a).sum();
        log.debug("closeIfIdle(response): trace={}, elapsed={}, count={}", trace, stopwatch.stop(), count);
    }

    public Series getSeries(String seriesId) {
        Series series = seriesMap.get(seriesId);
        if (series == null) {
            throw new IllegalArgumentException(String.format("series (%s) does not exist", seriesId));
        }
        return series;
    }

    public Map<String, String> updateMetadata(String seriesId, Map<String, String> metadata) {
        // TODO Log
        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }
        return getSeries(seriesId).updateMetadata(metadata);
    }

    public void deleteSeries(String seriesId) {
        // TODO Log

        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }

        Series series = seriesMap.remove(seriesId);
        if (series == null) {
            throw new IllegalArgumentException(String.format("series (%s) does not exist", seriesId));
        }

        try {
            series.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!FileSystemUtils.deleteRecursively(series.getRoot())) {
            throw new RuntimeException(String.format("unable to delete series (%s)", seriesId));
        }
    }

    public List<Series> findSeries(Pattern pattern, Map<String, String> metadata) {
        // TODO Log
        return seriesMap.values().parallelStream()
            .filter(s -> matchesMetadata(s, metadata))
            .filter(s -> pattern.matcher(s.getDefinition().getId()).matches())
            .sorted(Comparator.comparing(s -> s.getDefinition().getId()))
            .toList();
    }

    private boolean matchesMetadata(Series series, Map<String, String> metadata) {
        Map<String, String> seriesMetadata = series.getMetadata();

        if ((metadata == null) || metadata.isEmpty()) {
            return true;
        } else if (metadata.size() > seriesMetadata.size()) {
            return false;
        }

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (!Objects.equals(seriesMetadata.get(entry.getKey()), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public void createSeries(SeriesDefinition definition) {
        // TODO Log

        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }

        if (seriesMap.containsKey(definition.getId())) {
            throw new IllegalArgumentException(String.format("series (%s) already exists", definition.getId()));
        }

        File seriesRoot = new File(Util.checkDirectory(properties.getRoot()), UlidCreator.getUlid().toLowerCase());
        seriesMap.computeIfAbsent(definition.getId(), id -> new Series(seriesRoot, definition));
    }

    public void insert(String seriesId, Iterable<SeriesData> dataValues) {
        // TODO Log
        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }
        getSeries(seriesId).set(dataValues);
    }

    public void insert(String seriesId, SeriesData dataValue) {
        insert(seriesId, List.of(dataValue));
    }

    public List<FindDataResponse> find(FindDataRequest request) {
        // TODO Log

        if (request.getRange().isEmpty()) {
            return List.of();
        }

        List<Series> series = findSeries(request.getPattern(), request.getMetadata());
        if (series.isEmpty()) {
            return List.of();
        }

        List<Range<ZonedDateTime>> ranges = getRanges(request);

        List<FindDataResponse> result = series.parallelStream()
            .map(s -> FindDataResponse.builder()
                .series(s.getDefinition().getId())
                .metadata(s.getMetadata())
                .data(s.get(ranges, request.isIncludeNull(), request.getTimeReducer()))
                .build())
            .filter(r -> !r.getData().isEmpty())
            .sorted(Comparator.comparing(FindDataResponse::getSeries))
            .toList();

        if (request.getSeriesReducer() == null) {
            return result;
        }

        return List.of(reduce(result, request.getSeriesReducer(), request.isIncludeNull()));
    }

    private FindDataResponse reduce(List<FindDataResponse> result, Reducer reducer, boolean includeNull) {

        Map<ZonedDateTime, List<SeriesData>> grouped = result.parallelStream()
            .map(FindDataResponse::getData)
            .flatMap(Collection::stream)
            .collect(Collectors.groupingBy(SeriesData::getTime));

        List<SeriesData> data = grouped.entrySet().parallelStream()
            .map(e -> {
                Stream<? extends Number> stream = e.getValue().stream().map(SeriesData::getValue);
                Number value = PartitionFactory.reduce(stream, reducer).orElse(null);
                return SeriesData.builder().time(e.getKey()).value(value).build();
            })
            .filter(t -> includeNull || (t.getValue() != null))
            .sorted(Comparator.comparing(SeriesData::getTime))
            .toList();

        Map<String, String> metadata = result.stream()
            .map(FindDataResponse::getMetadata)
            .reduce(Map.of(), (a, b) -> Maps.difference(a, b).entriesInCommon());

        return FindDataResponse.builder()
            .series("reduced")
            .metadata(metadata)
            .data(data)
            .build();
    }

    private List<Range<ZonedDateTime>> getRanges(FindDataRequest request) {
        // TODO Log

        int count = properties.getMaxQuerySize();

        Integer maxSize = request.getSize();
        if ((maxSize != null) && (maxSize < count)) {
            count = maxSize;
        }

        Range<ZonedDateTime> range = request.getRange();
        if (request.getInterval() != null) {
            Duration duration = Duration.ofSeconds(request.getInterval());
            int intervalCount = (int) Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(duration) + 1;
            if (intervalCount < count) {
                count = intervalCount;
            }
        }

        Duration duration = Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(count);

        List<Range<ZonedDateTime>> ranges = new ArrayList<>(count);
        ZonedDateTime start = range.lowerEndpoint();
        for (int i = 0; i < count; i++) {
            ZonedDateTime end = start.plus(duration);
            ranges.add(Range.closed(start, end.minusNanos(1)));
            start = end;
        }
        return ranges;
    }

}
