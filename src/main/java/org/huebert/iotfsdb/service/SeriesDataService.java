package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.rest.schema.Series;
import org.huebert.iotfsdb.series.SeriesContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Service
public class SeriesDataService {

    private IotfsdbProperties properties;

    private final Map<Series, SeriesContainer> containers = new ConcurrentHashMap<>();

    private final ConcurrentMap<Series, Boolean> updated = new ConcurrentHashMap<>();

    @Scheduled
    public void flush() throws IOException {
        for (Map.Entry<Series, Boolean> entry : updated.entrySet()) {
            entry.setValue(false);
            containers.get(entry.getKey()).flush();
        }
    }

    public SeriesDataService(IotfsdbProperties properties) {
        this.properties = properties;
    }

    public float[] getValues(Series series, Range<LocalDateTime> range, Duration interval) {
        return null;
    }

    public float getValue(Series series, LocalDateTime dateTime) {
        return containers.computeIfAbsent(series, SeriesContainer::new).getValue(dateTime);
    }

    public boolean setValue(Series series, LocalDateTime dateTime, float value) {
        SeriesContainer container = containers.computeIfAbsent(series, SeriesContainer::new);
        boolean changed = container.setValue(properties, dateTime, value);
        if (changed) {
            updated.put(series, true);
        }
        return changed;
    }
}
