package org.huebert.iotfsdb.service;

import com.github.f4b6a3.ulid.UlidCreator;
import com.google.common.collect.Range;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.partition.PartitionFactory;
import org.huebert.iotfsdb.series.Aggregation;
import org.huebert.iotfsdb.series.Series;
import org.huebert.iotfsdb.series.SeriesDefinition;
import org.huebert.iotfsdb.util.Util;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;
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
                seriesMap.put(series.getDefinition().id(), series);
            });
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

    public synchronized Map<String, String> updateMetadata(String seriesId, Map<String, String> metadata) throws IOException {

        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }

        Series series = getSeries(seriesId);
        return series.updateMetadata(metadata);
    }

    public synchronized void deleteSeries(String seriesId) {

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

    public List<SeriesDefinition> findSeries(Pattern pattern, Map<String, String> metadata) {
        return seriesMap.values().parallelStream()
            .filter(s -> matchesMetadata(s, metadata))
            .map(Series::getDefinition)
            .filter(s -> pattern.matcher(s.id()).matches())
            .sorted(Comparator.comparing(SeriesDefinition::id))
            .toList();
    }

    private boolean matchesMetadata(Series series, Map<String, String> metadata) {
        Map<String, String> seriesMetadata = series.getMetadata();

        if (metadata.isEmpty()) {
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

    public synchronized SeriesDefinition createSeries(SeriesDefinition seriesDefinition) throws IOException {

        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }

        if (seriesMap.containsKey(seriesDefinition.id())) {
            throw new IllegalArgumentException(String.format("series (%s) already exists", seriesDefinition.id()));
        }

        File seriesRoot = new File(Util.checkDirectory(properties.getRoot()), String.valueOf(UlidCreator.getUlid()));
        Series series = new Series(seriesRoot, seriesDefinition);
        seriesMap.put(seriesDefinition.id(), series);

        return seriesDefinition;
    }

    public void set(String seriesId, ZonedDateTime dateTime, String value) {

        if (properties.isReadOnly()) {
            throw new IllegalStateException("database is read only");
        }

        getSeries(seriesId).set(dateTime, value);
    }

    public Map<String, Map<ZonedDateTime, ? extends Number>> get(
        Pattern pattern,
        Map<String, String> metadata,
        Range<ZonedDateTime> range,
        Integer interval,
        Integer maxSize,
        boolean includeNull,
        Aggregation timeAggregation,
        Aggregation seriesAggregation
    ) {

        Map<String, Map<ZonedDateTime, ? extends Number>> result = new ConcurrentSkipListMap<>();
        if (range.isEmpty()) {
            return result;
        }

        List<Range<ZonedDateTime>> ranges = calculateRanges(range, interval, maxSize);
        findSeries(pattern, metadata).parallelStream()
            .forEach(definition -> {
                Series series = getSeries(definition.id());
                result.put(definition.id(), series.get(ranges, includeNull, timeAggregation));
            });

        if ((result.size() > 1) && (seriesAggregation != null)) {
            Map<ZonedDateTime, Number> combined = new ConcurrentSkipListMap<>();
            for (Range<ZonedDateTime> rr : ranges) {
                Stream<? extends Number> stream = result.values().stream().map(map -> map.get(rr.lowerEndpoint()));
                Optional<? extends Number> aggregate = PartitionFactory.aggregate(stream, seriesAggregation);
                if (includeNull || aggregate.isPresent()) {
                    combined.put(rr.lowerEndpoint(), aggregate.orElse(null));
                }
            }
            result.clear();
            result.put("aggregation", combined);
        }

        return result;
    }

    private List<Range<ZonedDateTime>> calculateRanges(Range<ZonedDateTime> range, Integer interval, Integer maxSize) {

        int count = properties.getMaxQuerySize();

        if ((maxSize != null) && (maxSize < count)) {
            if (maxSize < 1) {
                throw new IllegalArgumentException();
            }
            count = maxSize;
        }

        if (interval != null) {
            if (interval < 1) {
                throw new IllegalArgumentException();
            }
            Duration duration = Duration.ofSeconds(interval);
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
