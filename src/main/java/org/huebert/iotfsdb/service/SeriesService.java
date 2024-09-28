package org.huebert.iotfsdb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import io.micrometer.common.lang.NonNull;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.rest.schema.Series;
import org.huebert.iotfsdb.rest.schema.SeriesType;
import org.huebert.iotfsdb.series.BooleanTypeAdapter;
import org.huebert.iotfsdb.series.FloatTypeAdapter;
import org.huebert.iotfsdb.series.IntegerTypeAdapter;
import org.huebert.iotfsdb.series.SeriesContainer;
import org.huebert.iotfsdb.series.SeriesTypeAdapter;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
    public void postConstruct() {
        Stream.of(properties.getRoot())
            .filter(File::isDirectory)
            .filter(File::canRead)
            .filter(d -> new File(d, "definition.json").exists())
            .filter(d -> new File(d, "metadata.json").exists())
            .forEach(f -> {

                Series series = mapper.readValue(new File(f, "definition.json"), Series.class);
                Map<String, String> metadata = mapper.readValue(new File(f, "metadata.json"), SeriesMetadata.class);

                SeriesTypeAdapter<?> adapter = getAdapter(series.type());
                seriesMap.put(series.id(), new SeriesContainer<>(f, series, adapter, properties.getReadOnly()));
            });

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

    public Series getSeries(String seriesId) {
        return seriesMap.get(seriesId).getSeries();
    }

    public void deleteSeries(String seriesId) {
        //TODO Need sync
        SeriesContainer<?> seriesContainer = seriesMap.get(seriesId);


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

    public Series createSeries(@NonNull Series series) {
        Preconditions.checkNotNull(series);
        Preconditions.checkArgument(seriesMap.containsKey(series.id()));

        //TODO Need sync

        File seriesRoot = new File(properties.getRoot(), UUID.randomUUID().toString());
        seriesRoot.mkdirs();

        mapper.writeValue(new File(seriesRoot, "definition.json"), series);
        mapper.writeValue(new File(seriesRoot, "metadata.json"), Map.of());

        SeriesTypeAdapter<?> adapter = getAdapter(series.type());
        seriesMap.put(series.id(), new SeriesContainer<>(seriesRoot, series, adapter, properties.getReadOnly()));


    }

    public void set(String seriesId, LocalDateTime dateTime, Object value) {
        seriesMap.get(seriesId).set(dateTime, value);
    }

    public Map<String, Map<LocalDateTime, ?>> get(
        String pattern,
        Map<String, String> metadata,
        Range<LocalDateTime> range,
        Duration interval,
        boolean includeNull
    ) {
        Map<String, Map<LocalDateTime, ?>> r = new LinkedHashMap<>();
        for (Series series : findSeries(pattern, metadata)) {
            SeriesContainer<?> seriesContainer = seriesMap.get(series.id());
            Map<LocalDateTime, ?> localDateTimeMap = seriesContainer.get(range, interval, includeNull);
            r.put(series.id(), localDateTimeMap);
        }
        return r;
    }
}
