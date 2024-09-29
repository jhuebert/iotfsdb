package org.huebert.iotfsdb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.schema.Series;
import org.huebert.iotfsdb.schema.SeriesType;
import org.huebert.iotfsdb.series.SeriesAggregation;
import org.huebert.iotfsdb.series.SeriesContainer;
import org.huebert.iotfsdb.series.adapter.BooleanTypeAdapter;
import org.huebert.iotfsdb.series.adapter.FloatTypeAdapter;
import org.huebert.iotfsdb.series.adapter.IntegerTypeAdapter;
import org.huebert.iotfsdb.series.adapter.SeriesTypeAdapter;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

@Slf4j
@Service
public class SeriesService {

    private static final String METADATA_JSON = "metadata.json";

    private static final String DEFINITION_JSON = "definition.json";

    private final ObjectMapper mapper = new ObjectMapper();

    private final IotfsdbProperties properties;

    private final Map<String, SeriesContainer<?>> seriesMap = new ConcurrentHashMap<>();

    public SeriesService(IotfsdbProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void postConstruct() throws IOException {
        File root = Preconditions.checkNotNull(properties.getRoot(), "database root not set");

        File[] files = root.listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            if (!file.isDirectory() || !file.canRead()) {
                log.warn("skipping series directory: {}", file);
                continue;
            }
            File definitionFile = new File(file, DEFINITION_JSON);
            if (!definitionFile.exists() || !definitionFile.canRead()) {
                log.warn("unable to read series definition: {}", definitionFile);
                continue;
            }
            File metadataFile = new File(file, METADATA_JSON);
            if (!metadataFile.exists() || !metadataFile.canRead()) {
                log.warn("unable to read series metadata: {}", metadataFile);
                continue;
            }
            Series series = mapper.readValue(definitionFile, Series.class);
            Map<String, String> metadata = mapper.readValue(metadataFile, new TypeReference<>() {
            });
            SeriesTypeAdapter<?> adapter = getAdapter(series.type());
            seriesMap.put(series.id(), new SeriesContainer<>(file, series, metadata, adapter, properties.isReadOnly()));
        }
    }

    private SeriesTypeAdapter<?> getAdapter(SeriesType type) {
        if (type == SeriesType.FLOAT) {
            return new FloatTypeAdapter();
        } else if (type == SeriesType.INTEGER) {
            return new IntegerTypeAdapter();
        } else if (type == SeriesType.BOOLEAN) {
            return new BooleanTypeAdapter();
        }
        throw new RuntimeException("invalid series type");
    }

    public Optional<Series> getSeries(String seriesId) {
        Preconditions.checkNotNull(seriesId);
        return Optional.ofNullable(seriesMap.get(seriesId))
            .map(SeriesContainer::getSeries);
    }

    public Optional<Map<String, String>> getMetadata(String seriesId) {
        Preconditions.checkNotNull(seriesId);
        return Optional.ofNullable(seriesMap.get(seriesId))
            .map(SeriesContainer::getMetadata);
    }

    public synchronized Map<String, String> updateMetadata(String seriesId, Map<String, String> metadata) throws IOException {
        Preconditions.checkNotNull(seriesId);
        Preconditions.checkNotNull(metadata);
        SeriesContainer<?> container = seriesMap.get(seriesId);
        File seriesRoot = container.getSeriesRoot();
        mapper.writeValue(new File(seriesRoot, METADATA_JSON), metadata);
        container.setMetadata(metadata);
        return metadata;
    }

    public synchronized void deleteSeries(String seriesId) {
        Preconditions.checkNotNull(seriesId);

        SeriesContainer<?> seriesContainer = seriesMap.get(seriesId);

        //TODO Delete directory and content
//        seriesContainer.getSeriesRoot();

        seriesMap.remove(seriesId);
    }

    public List<Series> findSeries(Pattern pattern, Map<String, String> metadata) {
        Preconditions.checkNotNull(pattern);
        Preconditions.checkNotNull(metadata);
        return seriesMap.values().stream()
            .filter(s -> matchesMetadata(s, metadata))
            .map(SeriesContainer::getSeries)
            .filter(s -> pattern.matcher(s.id()).matches())
            .toList();
    }

    private boolean matchesMetadata(SeriesContainer<?> container, Map<String, String> metadata) {
        Map<String, String> containerMetadata = container.getMetadata();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (!Objects.equals(containerMetadata.get(entry.getKey()), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public synchronized Series createSeries(Series series) throws IOException {
        Preconditions.checkNotNull(series);
        Preconditions.checkArgument(!seriesMap.containsKey(series.id()));

        File seriesRoot = new File(properties.getRoot(), UUID.randomUUID().toString());
        Preconditions.checkArgument(seriesRoot.mkdirs());

        mapper.writeValue(new File(seriesRoot, DEFINITION_JSON), series);
        mapper.writeValue(new File(seriesRoot, METADATA_JSON), Map.of());

        SeriesTypeAdapter<?> adapter = getAdapter(series.type());
        SeriesContainer<?> container = new SeriesContainer<>(seriesRoot, series, Map.of(), adapter, properties.isReadOnly());
        seriesMap.put(series.id(), container);

        return series;
    }

    public void set(String seriesId, LocalDateTime dateTime, String value) {
        Preconditions.checkNotNull(seriesId);
        Preconditions.checkNotNull(dateTime);
        SeriesContainer<?> seriesContainer = seriesMap.get(seriesId);
        Preconditions.checkNotNull(seriesContainer);
        seriesContainer.set(dateTime, value);
    }

    public Map<String, Map<LocalDateTime, ?>> get(
        Pattern pattern,
        Map<String, String> metadata,
        Range<LocalDateTime> range,
        int valueInterval,
        boolean includeNull,
        SeriesAggregation aggregation
    ) {
        Preconditions.checkNotNull(pattern);
        Preconditions.checkNotNull(metadata);
        Preconditions.checkNotNull(range);
        Preconditions.checkArgument(valueInterval > 0);

        Map<String, Map<LocalDateTime, ?>> result = new LinkedHashMap<>();
        if (range.isEmpty()) {
            return result;
        }

        List<Range<LocalDateTime>> ranges = calculateRanges(range, valueInterval);
        for (Series series : findSeries(pattern, metadata)) {
            SeriesContainer<?> seriesContainer = seriesMap.get(series.id());
            Map<LocalDateTime, ?> localDateTimeMap = seriesContainer.get(ranges, valueInterval, includeNull, aggregation);
            result.put(series.id(), localDateTimeMap);
        }

        return result;
    }

    private List<Range<LocalDateTime>> calculateRanges(Range<LocalDateTime> range, int valueInterval) {
        Duration valueDuration = Duration.of(valueInterval, ChronoUnit.SECONDS);
        int count = (int) Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(valueDuration);
        List<Range<LocalDateTime>> ranges = new ArrayList<>(count);
        LocalDateTime start = range.lowerEndpoint();
        for (int i = 0; i <= count; i++) {
            LocalDateTime end = start.plus(valueDuration);
            ranges.add(Range.closedOpen(start, end));
            start = end;
        }
        return ranges;
    }
}
