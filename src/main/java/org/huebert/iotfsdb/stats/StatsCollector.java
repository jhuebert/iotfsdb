package org.huebert.iotfsdb.stats;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.schema.InsertRequest;
import org.huebert.iotfsdb.schema.NumberType;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.Reducer;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.DataService;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.LockUtil;
import org.huebert.iotfsdb.service.ParallelUtil;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

@Slf4j
@Aspect
@Component
public class StatsCollector {

    private static final long MEASUREMENT_INTERVAL = 60000L;
    private static final ReadWriteLock RW_LOCK = new ReentrantReadWriteLock();
    private static final Map<CaptureStats, Accumulator> STATS_MAP = new ConcurrentHashMap<>();
    private static final Set<CaptureStats> SERIES_MAP = Sets.newConcurrentHashSet();
    private static final double NS_PER_S = 1e9;

    private final InsertService insertService;
    private final IotfsdbProperties properties;
    private final DataService dataService;

    public StatsCollector(InsertService insertService, IotfsdbProperties properties, DataService dataService) {
        this.insertService = insertService;
        this.properties = properties;
        this.dataService = dataService;
    }

    @Around("@annotation(captureAnnotation)")
    public Object captureExecutionTime(ProceedingJoinPoint joinPoint, CaptureStats captureAnnotation) throws Throwable {
        if (properties.isReadOnly() || !properties.isCaptureStats()) {
            return joinPoint.proceed();
        }
        long startTime = System.nanoTime();
        Object result = joinPoint.proceed();
        long duration = System.nanoTime() - startTime;
        LockUtil.withRead(RW_LOCK, () ->
            STATS_MAP.computeIfAbsent(captureAnnotation, Accumulator::new).add(duration)
        );
        return result;
    }

    @Scheduled(fixedRate = MEASUREMENT_INTERVAL, timeUnit = TimeUnit.MILLISECONDS)
    public void calculateMeasurements() {
        if (STATS_MAP.isEmpty()) {
            return;
        }

        Map<CaptureStats, Accumulator> localStats = new HashMap<>();
        LockUtil.withWrite(RW_LOCK, () -> {
            localStats.putAll(STATS_MAP);
            STATS_MAP.clear();
        });

        // Make sure all the series exist
        localStats.keySet().stream()
            .filter(SERIES_MAP::add)
            .map(StatsCollector::createSeries)
            .flatMap(List::stream)
            .forEach(dataService::saveSeries);

        ZonedDateTime now = ZonedDateTime.now();
        List<InsertRequest> requests = localStats.values().stream()
            .flatMap(v -> create(now, v).stream())
            .toList();

        ParallelUtil.forEach(requests, insertService::insert);
    }

    private static String getSeriesId(CaptureStats annotation, Stat stat) {
        return annotation.id() + "-" + stat.getKey();
    }

    private static List<InsertRequest> create(ZonedDateTime time, Accumulator stats) {
        return List.of(
            createRequest(getSeriesId(stats.getAnnotation(), Stat.MIN), time, stats.getMin() / NS_PER_S),
            createRequest(getSeriesId(stats.getAnnotation(), Stat.MAX), time, stats.getMax() / NS_PER_S),
            createRequest(getSeriesId(stats.getAnnotation(), Stat.MEAN), time, stats.getMean() / NS_PER_S),
            createRequest(getSeriesId(stats.getAnnotation(), Stat.COUNT), time, stats.getCount())
        );
    }

    private static InsertRequest createRequest(String series, ZonedDateTime time, double value) {
        return InsertRequest.builder()
            .series(series)
            .reducer(Reducer.LAST)
            .values(List.of(new SeriesData(time, value)))
            .build();
    }

    private static List<SeriesFile> createSeries(CaptureStats annotation) {
        return Stream.of(Stat.values())
            .map(stat -> createSeries(annotation, stat))
            .toList();
    }

    private static SeriesFile createSeries(CaptureStats annotation, Stat stat) {
        Map<String, String> metadata = new HashMap<>(Map.of(
            "source", "iotfsdb",
            "stat", stat.getKey(),
            "unit", stat.getUnit()
        ));
        for (CaptureStats.Metadata m : annotation.metadata()) {
            metadata.put(m.key(), m.value());
        }
        return SeriesFile.builder()
            .definition(createDefinition(annotation, stat))
            .metadata(metadata)
            .build();
    }

    private static SeriesDefinition createDefinition(CaptureStats annotation, Stat stat) {
        return SeriesDefinition.builder()
            .id(getSeriesId(annotation, stat))
            .type(stat.getType())
            .partition(PartitionPeriod.MONTH)
            .interval(MEASUREMENT_INTERVAL)
            .build();
    }

    private static class Accumulator {

        @Getter
        private final CaptureStats annotation;
        private final AtomicLong count = new AtomicLong();
        private final AtomicLong sum = new AtomicLong();
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

        private Accumulator(CaptureStats annotation) {
            this.annotation = annotation;
        }

        public void add(long value) {
            count.incrementAndGet();
            sum.addAndGet(value);
            min.accumulateAndGet(value, Math::min);
            max.accumulateAndGet(value, Math::max);
        }

        public long getCount() {
            return count.get();
        }

        public long getMean() {
            return count.get() == 0 ? 0 : sum.get() / count.get();
        }

        public long getMin() {
            return count.get() == 0 ? 0 : min.get();
        }

        public long getMax() {
            return count.get() == 0 ? 0 : max.get();
        }
    }

    @Getter
    @AllArgsConstructor
    private enum Stat {
        MIN("min", "second", NumberType.FLOAT2),
        MAX("max", "second", NumberType.FLOAT2),
        MEAN("mean", "second", NumberType.FLOAT2),
        COUNT("count", "count", NumberType.INTEGER2);

        private final String key;
        private final String unit;
        private final NumberType type;
    }
}
