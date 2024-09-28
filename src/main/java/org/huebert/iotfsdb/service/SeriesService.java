package org.huebert.iotfsdb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.micrometer.common.lang.NonNull;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.schema.DataValue;
import org.huebert.iotfsdb.schema.Series;
import org.huebert.iotfsdb.schema.SeriesType;
import org.huebert.iotfsdb.series.BooleanTypeAdapter;
import org.huebert.iotfsdb.series.FloatTypeAdapter;
import org.huebert.iotfsdb.series.IntegerTypeAdapter;
import org.huebert.iotfsdb.series.SeriesContainer;
import org.huebert.iotfsdb.series.SeriesTypeAdapter;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

@Validated
@Slf4j
@Service
public class SeriesService {

    private final ObjectMapper mapper = new ObjectMapper();

    private final IotfsdbProperties properties;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final Map<String, SeriesContainer<?>> seriesMap = new ConcurrentHashMap<>();

    public SeriesService(IotfsdbProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void postConstruct() throws IOException {
        File root = Preconditions.checkNotNull(properties.getRoot(), "database root not set");
        for (File file : root.listFiles()) {
            if (!file.isDirectory() || !file.canRead()) {
                continue;
            }
            File definitionFile = new File(file, "definition.json");
            if (!definitionFile.exists() || !definitionFile.canRead()) {
                continue;
            }
            File metadataFile = new File(file, "metadata.json");
            if (!metadataFile.exists() || !metadataFile.canRead()) {
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
        throw new RuntimeException();
    }

    public Optional<Series> getSeries(String seriesId) {
        return Optional.ofNullable(seriesMap.get(seriesId))
            .map(SeriesContainer::getSeries);
    }

    public Map<String, String> getMetadata(String seriesId) {
        return seriesMap.get(seriesId).getMetadata();
    }

    public Map<String, String> updateMetadata(String seriesId, Map<String, String> metadata) throws IOException {
        SeriesContainer<?> seriesContainer = seriesMap.get(seriesId);
        File seriesRoot = seriesContainer.getSeriesRoot();
        mapper.writeValue(new File(seriesRoot, "metadata.json"), metadata);
        seriesContainer.setMetadata(metadata);
        return metadata;
    }

    public void deleteSeries(String seriesId) {
        //TODO Need sync

        SeriesContainer<?> seriesContainer = seriesMap.get(seriesId);

        //TODO Delete directory and content


        seriesMap.remove(seriesId);
    }

    public List<Series> findSeries(String pattern,
                                   Map<String, String> metadata) {
        Pattern p = Pattern.compile(pattern);
        return seriesMap.values().stream()
            .filter(s -> matchesMetadata(s, metadata))
            .map(SeriesContainer::getSeries)
            .filter(s -> p.matcher(s.id()).matches())
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

    public Series createSeries(@NonNull Series series) throws IOException {
        Preconditions.checkNotNull(series);
        Preconditions.checkArgument(!seriesMap.containsKey(series.id()));

        //TODO Need sync

        File seriesRoot = new File(properties.getRoot(), UUID.randomUUID().toString());
        seriesRoot.mkdirs();

        mapper.writeValue(new File(seriesRoot, "definition.json"), series);
        mapper.writeValue(new File(seriesRoot, "metadata.json"), Map.of());

        SeriesTypeAdapter<?> adapter = getAdapter(series.type());
        SeriesContainer<?> value = new SeriesContainer<>(seriesRoot, series, Map.of(), adapter, properties.isReadOnly());
        value.setMetadata(Map.of());
        seriesMap.put(series.id(), value);

        return series;
    }

    public void set(String seriesId, LocalDateTime dateTime, String value) {
        Preconditions.checkNotNull(seriesId);
        Preconditions.checkNotNull(dateTime);
        SeriesContainer<?> seriesContainer = seriesMap.get(seriesId);
        Preconditions.checkNotNull(seriesContainer);
        seriesContainer.set(dateTime, value);
    }
//
//    public Map<String, Map<LocalDateTime, ?>> get(
//        String pattern,
//        Map<String, String> metadata,
//        Range<LocalDateTime> range,
//        Duration interval,
//        boolean includeNull
//    ) {
//        Map<String, Map<LocalDateTime, ?>> r = new LinkedHashMap<>();
//        for (Series series : findSeries(pattern, metadata)) {
//            SeriesContainer<?> seriesContainer = seriesMap.get(series.id());
//            Map<LocalDateTime, ?> localDateTimeMap = seriesContainer.get(range, interval, includeNull);
//            r.put(series.id(), localDateTimeMap);
//        }
//        return r;
//    }
}
