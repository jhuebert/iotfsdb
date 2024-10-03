package org.huebert.iotfsdb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.Util;
import org.huebert.iotfsdb.schema.Series;
import org.huebert.iotfsdb.schema.SeriesType;
import org.huebert.iotfsdb.series.SeriesAggregation;
import org.huebert.iotfsdb.series.SeriesContainer;
import org.huebert.iotfsdb.series.adapter.BooleanTypeAdapter;
import org.huebert.iotfsdb.series.adapter.FloatTypeAdapter;
import org.huebert.iotfsdb.series.adapter.IntegerTypeAdapter;
import org.huebert.iotfsdb.series.adapter.SeriesTypeAdapter;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Slf4j
@Service
public class SeriesService {

    private static final String METADATA_JSON = "metadata.json";

    private static final String DEFINITION_JSON = "definition.json";

    private static final Map<SeriesType, SeriesTypeAdapter<?>> ADAPTER_MAP = Map.of(
        SeriesType.BOOLEAN, new BooleanTypeAdapter(),
        SeriesType.FLOAT, new FloatTypeAdapter(),
        SeriesType.INTEGER, new IntegerTypeAdapter()
    );

    private final ObjectMapper mapper = new ObjectMapper();

    private final IotfsdbProperties properties;

    private final Map<String, SeriesContainer<?>> seriesMap = new ConcurrentHashMap<>();

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
                Series series = readDefinition(file);
                Map<String, String> metadata = readMetadata(file);
                SeriesTypeAdapter<?> adapter = ADAPTER_MAP.get(series.type());
                seriesMap.put(series.id(), new SeriesContainer<>(file, series, metadata, adapter, properties.isReadOnly()));
            });
    }

    private Series readDefinition(File seriesRoot) {
        File definitionFile = Util.checkFile(new File(seriesRoot, DEFINITION_JSON));
        Series series;
        try {
            series = mapper.readValue(definitionFile, Series.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Series.checkValid(series);
        return series;
    }

    private Map<String, String> readMetadata(File seriesRoot) {
        File metadataFile = Util.checkFile(new File(seriesRoot, METADATA_JSON));
        Map<String, String> metadata;
        try {
            metadata = mapper.readValue(metadataFile, new TypeReference<>() {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metadata;
    }

    private SeriesContainer<?> getContainer(String seriesId) {
        SeriesContainer<?> container = seriesMap.get(seriesId);
        if (container == null) {
            throw new IllegalArgumentException(String.format("series (%s) does not exist", seriesId));
        }
        return container;
    }

    public Series getSeries(String seriesId) {
        return getContainer(seriesId).getSeries();
    }

    public Map<String, String> getMetadata(String seriesId) {
        return getContainer(seriesId).getMetadata();
    }

    public synchronized Map<String, String> updateMetadata(String seriesId, Map<String, String> metadata) throws IOException {

        if (properties.isReadOnly()) {
            throw new IllegalArgumentException("database is read only");
        }

        SeriesContainer<?> container = getContainer(seriesId);
        File seriesRoot = container.getSeriesRoot();
        File metadataFile = Util.checkFileWrite(new File(seriesRoot, METADATA_JSON));
        mapper.writeValue(metadataFile, metadata);
        container.setMetadata(metadata);
        return metadata;
    }

    public synchronized void deleteSeries(String seriesId) {

        if (properties.isReadOnly()) {
            throw new IllegalArgumentException("database is read only");
        }

        SeriesContainer<?> container = seriesMap.remove(seriesId);
        if (container == null) {
            throw new IllegalArgumentException(String.format("series (%s) does not exist", seriesId));
        }

        try {
            container.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!FileSystemUtils.deleteRecursively(container.getSeriesRoot())) {
            throw new RuntimeException(String.format("unable to delete series (%s)", seriesId));
        }
    }

    public List<Series> findSeries(Pattern pattern, Map<String, String> metadata) {
        return seriesMap.values().parallelStream()
            .filter(s -> matchesMetadata(s, metadata))
            .map(SeriesContainer::getSeries)
            .filter(s -> pattern.matcher(s.id()).matches())
            .toList();
    }

    private boolean matchesMetadata(SeriesContainer<?> container, Map<String, String> metadata) {
        Map<String, String> containerMetadata = container.getMetadata();

        if (metadata.isEmpty()) {
            return true;
        } else if (metadata.size() > containerMetadata.size()) {
            return false;
        }

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (!Objects.equals(containerMetadata.get(entry.getKey()), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public synchronized Series createSeries(Series series) throws IOException {

        if (properties.isReadOnly()) {
            throw new IllegalArgumentException("database is read only");
        }

        if (seriesMap.containsKey(series.id())) {
            throw new IllegalArgumentException(String.format("series (%s) already exists", series.id()));
        }

        File seriesRoot = new File(Util.checkDirectory(properties.getRoot()), UUID.randomUUID().toString());
        if (!seriesRoot.mkdirs()) {
            throw new RuntimeException(String.format("unable to create series directory (%s)", seriesRoot));
        }

        mapper.writeValue(new File(seriesRoot, DEFINITION_JSON), series);
        Map<String, String> metadata = Map.of();
        mapper.writeValue(new File(seriesRoot, METADATA_JSON), metadata);

        SeriesTypeAdapter<?> adapter = ADAPTER_MAP.get(series.type());
        SeriesContainer<?> container = new SeriesContainer<>(seriesRoot, series, metadata, adapter, false);
        seriesMap.put(series.id(), container);

        return series;
    }

    public void set(String seriesId, ZonedDateTime dateTime, String value) {

        if (properties.isReadOnly()) {
            throw new IllegalArgumentException("database is read only");
        }

        getContainer(seriesId).set(dateTime, value);
    }

    public Map<String, Map<ZonedDateTime, ?>> get(
        Pattern pattern,
        Map<String, String> metadata,
        Range<ZonedDateTime> range,
        int valueInterval,
        boolean includeNull,
        SeriesAggregation aggregation
    ) {

        Map<String, Map<ZonedDateTime, ?>> result = new ConcurrentHashMap<>();
        if (range.isEmpty()) {
            return result;
        }

        List<Range<ZonedDateTime>> ranges = calculateRanges(range, valueInterval);
        findSeries(pattern, metadata).parallelStream()
            .forEach(series -> {
                SeriesContainer<?> container = getContainer(series.id());
                result.put(series.id(), container.get(ranges, includeNull, aggregation));
            });

        return result;
    }

    private List<Range<ZonedDateTime>> calculateRanges(Range<ZonedDateTime> range, int valueInterval) {

        Duration valueDuration = Duration.of(valueInterval, ChronoUnit.SECONDS);
        int count = (int) Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(valueDuration);

        if (count > properties.getMaxQuerySize()) {
            count = properties.getMaxQuerySize();
            valueDuration = Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(count);
        }

        List<Range<ZonedDateTime>> ranges = new ArrayList<>(count);
        ZonedDateTime start = range.lowerEndpoint();
        for (int i = 0; i <= count; i++) {
            ZonedDateTime end = start.plus(valueDuration);
            ranges.add(Range.closedOpen(start, end));
            start = end;
        }
        return ranges;
    }
}
