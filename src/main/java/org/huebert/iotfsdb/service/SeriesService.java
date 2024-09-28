package org.huebert.iotfsdb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.micrometer.common.lang.NonNull;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.rest.schema.Series;
import org.huebert.iotfsdb.rest.schema.SeriesMetadata;
import org.huebert.iotfsdb.rest.schema.SeriesType;
import org.huebert.iotfsdb.series.BooleanTypeAdapter;
import org.huebert.iotfsdb.series.FloatTypeAdapter;
import org.huebert.iotfsdb.series.IntegerTypeAdapter;
import org.huebert.iotfsdb.series.SeriesContainer;
import org.huebert.iotfsdb.series.SeriesTypeAdapter;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Validated
@Slf4j
@Service
public class SeriesService {

    private final IotfsdbProperties properties;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final Map<String, SeriesType> seriesTypeMap = new ConcurrentHashMap<>();

    private final Map<String, SeriesContainer<?>> seriesMap = new ConcurrentHashMap<>();

    private final SeriesTypeAdapter<Float> floatAdapter = new FloatTypeAdapter();

    private final SeriesTypeAdapter<Integer> integerAdapter = new IntegerTypeAdapter();

    private final SeriesTypeAdapter<Boolean> booleanAdapter = new BooleanTypeAdapter();

    public SeriesService(IotfsdbProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void postConstruct() {
        ObjectMapper mapper = new ObjectMapper();
        Stream.of(properties.getRoot())
            .filter(File::isDirectory)
            .filter(File::canRead)
            .filter(d -> new File(d, "definition.json").exists())
            .filter(d -> new File(d, "metadata.json").exists())
            .forEach(f -> {

                Series series = mapper.readValue(new File(f, "definition.json"), Series.class);
                SeriesMetadata metadata = mapper.readValue(new File(f, "metadata.json"), SeriesMetadata.class);

                floatSeries.put(series.id(), new SeriesContainer<>(f, series, floatAdapter, properties.getReadOnly()));
                seriesMap.put(series.id(), new SeriesContainer<>(f, series, floatAdapter, properties.getReadOnly()));
                Object o = seriesMap.get(series.id()).get(LocalDateTime.now());
                Float d = floatSeries.get(series.id()).get(LocalDateTime.now());


//                SeriesContainer<?> container = new SeriesContainer.FloatContainer(f, series, properties.getReadOnly());

                seriesMap.put(f.getName(), );
            });

    }

    public Series getSeries(String id) {
        return seriesMap.get(id).getSeries();
    }

    public void deleteSeries(String id) {
        seriesMap.get(id).getSeries();
    }

    public List<Series> findSeries(String pattern) {
        Pattern p = Pattern.compile(pattern);
        return seriesMap.values().stream()
            .map(SeriesContainer::getSeries)
            .filter(s -> p.matcher(s.id()).matches())
            .toList();
    }

    public Series createSeries(@NonNull Series series) {
        Preconditions.checkNotNull(series);

        seriesMap.containsKey(series.id());


    }

    public void addValue(String id, Object value) {

    }
}
