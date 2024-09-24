package org.huebert.iotfsdb.service;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.partition.PartitionFactory;
import org.huebert.iotfsdb.rest.schema.ArchiveRequest;
import org.huebert.iotfsdb.rest.schema.FindDataRequest;
import org.huebert.iotfsdb.rest.schema.FindDataResponse;
import org.huebert.iotfsdb.rest.schema.SeriesData;
import org.huebert.iotfsdb.rest.schema.SeriesStats;
import org.huebert.iotfsdb.series.Reducer;
import org.huebert.iotfsdb.series.Series;
import org.huebert.iotfsdb.series.SeriesDefinition;
import org.huebert.iotfsdb.util.FileUtil;
import org.huebert.iotfsdb.util.TimeUtil;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.nio.file.Files;
import java.nio.file.Path;
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
        log.debug("SeriesService(enter): properties={}", properties);
        this.properties = properties;

        Path root = FileUtil.checkDirectory(properties.getRoot());

        FileUtil.list(root)
            .parallel()
            .filter(Files::isDirectory)
            .forEach(file -> {
                Series series = new Series(file);
                seriesMap.put(series.getSeriesFile().getDefinition().getId(), series);
            });

        log.debug("SeriesService(exit): root={}, seriesMap={}", root, seriesMap.size());
    }

    @Scheduled(fixedRateString = "${iotfsdb.max-idle-seconds}", timeUnit = TimeUnit.SECONDS)
    public void closeIdlePartitions() {
        log.debug("closeIfIdle(enter)");
        long count = seriesMap.values().parallelStream().map(Series::closeIdlePartitions).mapToLong(a -> a).sum();
        log.debug("closeIfIdle(exit): count={}", count);
    }

    public Series getSeries(String seriesId) {
        log.debug("getSeries(enter): seriesId={}", seriesId);
        Series series = seriesMap.get(seriesId);
        if (series == null) {
            throw new IllegalArgumentException(String.format("series (%s) does not exist", seriesId));
        }
        log.debug("getSeries(exit): seriesRoot={}, seriesDefinition={}", series.getRoot(), series.getSeriesFile().getDefinition());
        return series;
    }

    public SeriesStats getSeriesStats(String seriesId) {
        log.debug("getSeriesStats(enter): seriesId={}", seriesId);
        Series series = seriesMap.get(seriesId);
        if (series == null) {
            throw new IllegalArgumentException(String.format("series (%s) does not exist", seriesId));
        }
        SeriesStats result = series.getStats();
        log.debug("getSeriesStats(exit): seriesRoot={}, result={}", series.getRoot(), result);
        return result;
    }

    public SeriesStats getCombinedStats() {
        log.debug("getCombinedStats(enter)");
        SeriesStats result = seriesMap.keySet().parallelStream().map(this::getSeriesStats).reduce(SeriesStats.builder().build(), (a, b) -> SeriesStats.builder()
            .regularSize(a.getRegularSize() + b.getRegularSize())
            .regularNumPartitions(a.getRegularNumPartitions() + b.getRegularNumPartitions())
            .archiveSize(a.getArchiveSize() + b.getArchiveSize())
            .archiveNumPartitions(a.getArchiveNumPartitions() + b.getArchiveNumPartitions())
            .totalSize(a.getTotalSize() + b.getTotalSize())
            .totalNumPartitions(a.getTotalNumPartitions() + b.getTotalNumPartitions())
            .from(TimeUtil.min(a.getFrom(), b.getFrom()))
            .to(TimeUtil.max(a.getTo(), b.getTo()))
            .build());
        log.debug("getCombinedStats(exit): result={}", result);
        return result;
    }

    public void updateMetadata(String seriesId, Map<String, String> metadata) {
        log.debug("updateMetadata(enter): seriesId={}, metadata={}", seriesId, metadata);
        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }
        getSeries(seriesId).updateMetadata(metadata);
        log.debug("updateMetadata(exit): seriesId={}, metadata={}", seriesId, metadata);
    }

    public void deleteSeries(String seriesId) {
        log.debug("deleteSeries(enter): seriesId={}", seriesId);

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

        if (!FileSystemUtils.deleteRecursively(series.getRoot().toFile())) {
            throw new RuntimeException(String.format("unable to delete series (%s)", seriesId));
        }

        log.debug("deleteSeries(exit): seriesId={}", seriesId);
    }

    public void archiveSeries(String seriesId, ArchiveRequest request) {
        log.debug("archiveSeries(enter): seriesId={}, request={}", seriesId, request);
        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }
        getSeries(seriesId).archive(request.getRange());
        log.debug("archiveSeries(exit): seriesId={}", seriesId);
    }

    public void unarchiveSeries(String seriesId, ArchiveRequest request) {
        log.debug("unarchiveSeries(enter): seriesId={}, request={}", seriesId, request);
        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }
        getSeries(seriesId).unarchive(request.getRange());
        log.debug("unarchiveSeries(exit): seriesId={}", seriesId);
    }

    public List<Series> findSeries(Pattern pattern, Map<String, String> metadata) {
        log.debug("findSeries(enter): pattern={}, metadata={}", pattern, metadata);
        List<Series> result = seriesMap.values().parallelStream()
            .filter(s -> matchesMetadata(s, metadata))
            .filter(s -> pattern.matcher(s.getSeriesFile().getDefinition().getId()).matches())
            .sorted(Comparator.comparing(s -> s.getSeriesFile().getDefinition().getId()))
            .toList();
        log.debug("findSeries(exit): pattern={}, metadata={}, result={}", pattern, metadata, result.size());
        return result;
    }

    private boolean matchesMetadata(Series series, Map<String, String> metadata) {
        Map<String, String> seriesMetadata = series.getSeriesFile().getMetadata();

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
        log.debug("createSeries(enter): definition={}", definition);

        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }

        if (seriesMap.containsKey(definition.getId())) {
            throw new IllegalArgumentException(String.format("series (%s) already exists", definition.getId()));
        }

        Path seriesRoot = FileUtil.checkDirectory(properties.getRoot()).resolve(definition.getId());
        seriesMap.computeIfAbsent(definition.getId(), id -> new Series(seriesRoot, definition));

        log.debug("createSeries(exit): definition={}, seriesRoot={}", definition, seriesRoot);
    }

    public void insert(String seriesId, List<SeriesData> values) {

        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }
        getSeries(seriesId).set(values);
        log.debug("insert(exit): seriesId={}, values={}", seriesId, values.size());
    }

    public void insert(String seriesId, SeriesData value) {
        log.debug("insert(enter): seriesId={}, value={}", seriesId, value);
        insert(seriesId, List.of(value));
        log.debug("insert(exit): seriesId={}, value={}", seriesId, value);
    }

    public List<FindDataResponse> find(FindDataRequest request) {
        log.debug("find(enter): request={}", request);

        if (request.getRange().isEmpty()) {
            log.debug("find(exit): empty range");
            return List.of();
        }

        List<Series> series = findSeries(request.getPattern(), request.getMetadata());
        if (series.isEmpty()) {
            log.debug("find(exit): no matching series");
            return List.of();
        }

        List<Range<ZonedDateTime>> ranges = getRanges(request);

        List<FindDataResponse> result = series.parallelStream()
            .map(s -> FindDataResponse.builder()
                .series(s.getSeriesFile().getDefinition().getId())
                .metadata(s.getSeriesFile().getMetadata())
                .data(s.get(ranges, request.isIncludeNull(), request.getTimeReducer()))
                .build())
            .filter(r -> r.getData().stream().map(SeriesData::getValue).anyMatch(Objects::nonNull))
            .sorted(Comparator.comparing(FindDataResponse::getSeries))
            .toList();

        log.debug("find(checkpoint): result={}", result.size());

        if (result.isEmpty() || (request.getSeriesReducer() == null)) {
            log.debug("find(exit): no series reduction");
            return result;
        }

        List<FindDataResponse> seriesResult = List.of(reduce(result, request.getSeriesReducer(), request.isIncludeNull()));
        log.debug("find(exit): seriesResult={}", seriesResult.size());
        return seriesResult;
    }

    private FindDataResponse reduce(List<FindDataResponse> responses, Reducer reducer, boolean includeNull) {
        log.debug("reduce(enter): responses={}, reducer={}, includeNull={}", responses.size(), reducer, includeNull);

        Map<ZonedDateTime, List<SeriesData>> grouped = responses.parallelStream()
            .map(FindDataResponse::getData)
            .flatMap(Collection::stream)
            .collect(Collectors.groupingBy(SeriesData::getTime));

        log.debug("reduce(checkpoint): grouped={}", grouped.size());

        List<SeriesData> data = grouped.entrySet().parallelStream()
            .map(e -> {
                Stream<? extends Number> stream = e.getValue().stream().map(SeriesData::getValue);
                Number value = PartitionFactory.reduce(stream, reducer).orElse(null);
                return SeriesData.builder().time(e.getKey()).value(value).build();
            })
            .filter(t -> includeNull || (t.getValue() != null))
            .sorted(Comparator.comparing(SeriesData::getTime))
            .toList();

        Map<String, String> metadata = responses.stream()
            .map(FindDataResponse::getMetadata)
            .reduce(Map.of(), (a, b) -> Maps.difference(a, b).entriesInCommon());

        FindDataResponse result = FindDataResponse.builder()
            .series("reduced")
            .metadata(metadata)
            .data(data)
            .build();

        log.debug("reduce(checkpoint): metadata={}, data={}", result.getMetadata(), result.getData().size());

        return result;
    }

    private List<Range<ZonedDateTime>> getRanges(FindDataRequest request) {
        log.debug("getRanges(enter): request={}", request);

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

        if (duration.compareTo(Duration.ofSeconds(1)) < 0) {
            duration = Duration.ofSeconds(1);
            count = (int) Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(duration);
        }

        log.debug("getRanges(checkpoint): count={}, duration={}", count, duration);

        List<Range<ZonedDateTime>> ranges = new ArrayList<>(count);
        ZonedDateTime start = range.lowerEndpoint();
        for (int i = 0; i < count; i++) {
            ZonedDateTime end = start.plus(duration);
            ranges.add(Range.closed(start, end.minusNanos(1)));
            start = end;
        }
        log.debug("getRanges(exit): ranges={}", ranges.size());
        return ranges;
    }

}
