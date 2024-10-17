package org.huebert.iotfsdb.service;

import com.github.f4b6a3.ulid.UlidCreator;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.partition.PartitionFactory;
import org.huebert.iotfsdb.rest.DataRequest;
import org.huebert.iotfsdb.rest.DataValue;
import org.huebert.iotfsdb.series.Series;
import org.huebert.iotfsdb.series.SeriesDefinition;
import org.huebert.iotfsdb.util.Tuple;
import org.huebert.iotfsdb.util.Util;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
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

        Stream.of(files)
            .parallel()
            .filter(File::isDirectory)
            .forEach(file -> {
                Series series = new Series(file);
                seriesMap.put(series.getDefinition().getId(), series);
            });
    }

    @Scheduled(fixedRate = 5, timeUnit = TimeUnit.MINUTES)
    public void closeIfIdle() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("closeIfIdle(request): trace={}", trace);
        long count = seriesMap.values().parallelStream().map(Series::closeIfIdle).mapToLong(a -> a).sum();
        log.debug("closeIfIdle(response): trace={}, elapsed={}, count={}", trace, stopwatch.stop(), count);
    }

    private Series getSeries(String seriesId) {
        Series series = seriesMap.get(seriesId);
        if (series == null) {
            throw new IllegalArgumentException(String.format("series (%s) does not exist", seriesId));
        }
        return series;
    }

    public SeriesDefinition getSeriesDefinition(String seriesId) {
        return getSeries(seriesId).getDefinition();
    }

    public Map<String, String> getMetadata(String seriesId) {
        return getSeries(seriesId).getMetadata();
    }

    public Map<String, String> updateMetadata(String seriesId, Map<String, String> metadata) {
        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }
        return getSeries(seriesId).updateMetadata(metadata);
    }

    public void deleteSeries(String seriesId) {

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

        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }

        if (seriesMap.containsKey(definition.getId())) {
            throw new IllegalArgumentException(String.format("series (%s) already exists", definition.getId()));
        }

        File seriesRoot = new File(Util.checkDirectory(properties.getRoot()), UlidCreator.getUlid().toLowerCase());
        seriesMap.computeIfAbsent(definition.getId(), id -> new Series(seriesRoot, definition));
    }

    public void set(String seriesId, Iterable<DataValue> dataValues) {
        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }
        getSeries(seriesId).set(dataValues);
    }

    public void set(String seriesId, DataValue dataValue) {
        set(seriesId, List.of(dataValue));
    }

    public Map<String, Map<ZonedDateTime, ? extends Number>> get(DataRequest request) {

        Map<String, Map<ZonedDateTime, ? extends Number>> result = new ConcurrentSkipListMap<>();
        if (request.getRange().isEmpty()) {
            return result;
        }

        List<Range<ZonedDateTime>> ranges = calculateRanges(request);
        findSeries(request.getPattern(), request.getMetadata()).parallelStream()
            .forEach(series -> result.put(series.getDefinition().getId(), series.get(ranges, request.isIncludeNull(), request.getAggregation1())));

        if ((result.size() > 1) && (request.getAggregation2() != null)) {
            Map<ZonedDateTime, Number> combined = new ConcurrentSkipListMap<>();
            result.values().stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.groupingBy(Map.Entry::getKey)).entrySet().stream()
                .map(e -> {
                    Stream<? extends Number> stream = e.getValue().stream().map(Map.Entry::getValue);
                    return new Tuple<>(e.getKey(), PartitionFactory.aggregate(stream, request.getAggregation2()).orElse(null));
                })
                .filter(t -> request.isIncludeNull() || (t.value() != null))
                .forEach(t -> combined.put(t.key(), t.value()));
            result.clear();
            result.put("result", combined);
        }

        return result;
    }

    private List<Range<ZonedDateTime>> calculateRanges(DataRequest request) {

        int count = properties.getMaxQuerySize();

        Integer maxSize = request.getMaxSize();
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
