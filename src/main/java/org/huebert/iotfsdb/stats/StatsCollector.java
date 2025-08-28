package org.huebert.iotfsdb.stats;

import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.DataService;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.LockUtil;
import org.huebert.iotfsdb.service.ParallelUtil;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    private static final double MEGABYTE = 1024.0 * 1024.0;
    private static final SeriesFile USED_MEMORY = SeriesFile.builder()
        .definition(SeriesDefinition.builder()
            .id("iotfsdb-runtime-memory-used")
            .type(NumberType.FLOAT4)
            .partition(PartitionPeriod.DAY)
            .interval(MEASUREMENT_INTERVAL)
            .build())
        .metadata("source", "iotfsdb")
        .metadata("group", "runtime")
        .metadata("type", "memory")
        .metadata("operation", "used")
        .metadata("unit", "MB")
        .build();

    private final InsertService insertService;
    private final DataService dataService;
    private final boolean enabled;

    public StatsCollector(IotfsdbProperties properties, InsertService insertService, DataService dataService) {
        this.insertService = insertService;
        this.dataService = dataService;
        this.enabled = !properties.isReadOnly() && properties.getStats().isEnabled();
    }

    @PostConstruct
    public void init() {
        if (!enabled) {
            log.info("Stats collection is disabled");
            return;
        }
        log.info("Stats collection is enabled");
        createAndUpdate(USED_MEMORY);
    }

    @Around("@annotation(captureAnnotation)")
    public Object captureExecutionTime(ProceedingJoinPoint joinPoint, CaptureStats captureAnnotation) throws Throwable {
        if (!enabled) {
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

        if (!enabled) {
            return;
        }

        Map<CaptureStats, Accumulator> localStats = new HashMap<>();
        LockUtil.withWrite(RW_LOCK, () -> {
            localStats.putAll(STATS_MAP);
            STATS_MAP.clear();
        });

        ZonedDateTime now = ZonedDateTime.now();

        Runtime runtime = Runtime.getRuntime();
        insertService.insert(new InsertRequest(
            USED_MEMORY.getId(),
            List.of(new SeriesData(now, (runtime.totalMemory() - runtime.freeMemory()) / MEGABYTE)),
            Reducer.LAST)
        );

        if (localStats.isEmpty()) {
            return;
        }

        log.debug("Saving measurements for {} stats", localStats.size());

        localStats.keySet().stream()
            .filter(SERIES_MAP::add)
            .map(StatsCollector::createSeries)
            .flatMap(List::stream)
            .forEach(this::createAndUpdate);

        List<InsertRequest> requests = localStats.values().stream()
            .flatMap(v -> createRequests(now, v).stream())
            .toList();

        ParallelUtil.forEach(requests, insertService::insert);
    }

    private void createAndUpdate(SeriesFile seriesFile) {
        dataService.getSeries(seriesFile.getId()).ifPresentOrElse(sf -> {
            log.debug("Updating existing series: {}", seriesFile.getId());
            Map<String, String> metadata = new HashMap<>(sf.getMetadata());
            metadata.putAll(seriesFile.getMetadata());
            sf.setMetadata(metadata);
            dataService.saveSeries(sf);
        }, () -> {
            log.debug("Creating new series: {}", seriesFile.getId());
            dataService.saveSeries(seriesFile);
        });
    }

    private static String getSeriesId(CaptureStats annotation, Stat stat) {
        List<String> components = new ArrayList<>(List.of(
            "iotfsdb",
            annotation.group(),
            annotation.type(),
            annotation.operation(),
            stat.getKey(),
            annotation.version()
        ));
        components.removeIf(String::isBlank);
        return String.join("-", components);
    }

    private static List<InsertRequest> createRequests(ZonedDateTime time, Accumulator stats) {
        return Stream.of(Stat.values())
            .map(stat -> createRequest(time, stats, stat))
            .toList();
    }

    private static InsertRequest createRequest(ZonedDateTime time, Accumulator stats, Stat stat) {
        return InsertRequest.builder()
            .series(getSeriesId(stats.getAnnotation(), stat))
            .reducer(Reducer.LAST)
            .values(List.of(new SeriesData(time, stats.getStat(stat))))
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
            "group", annotation.group(),
            "type", annotation.type(),
            "operation", annotation.operation(),
            "version", annotation.version(),
            "javaClass", annotation.javaClass().getName(),
            "javaMethod", annotation.javaMethod(),
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
            .partition(PartitionPeriod.DAY)
            .interval(MEASUREMENT_INTERVAL)
            .build();
    }

    private static class Accumulator {

        @Getter
        private final CaptureStats annotation;
        private final AtomicInteger count = new AtomicInteger();
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

        public double getStat(Stat stat) {
            int localCount = count.get();
            if (localCount == 0) {
                return 0;
            }
            return switch (stat) {
                case MIN -> toMilliseconds(min);
                case MAX -> toMilliseconds(max);
                case MEAN -> toMilliseconds(sum) / localCount;
                case COUNT -> localCount;
            };
        }

        private double toMilliseconds(AtomicLong value) {
            return value.get() / 1e6;
        }

    }

    @Getter
    @AllArgsConstructor
    private enum Stat {
        MIN("min", "millisecond", NumberType.FLOAT4),
        MAX("max", "millisecond", NumberType.FLOAT4),
        MEAN("mean", "millisecond", NumberType.FLOAT4),
        COUNT("count", "count", NumberType.INTEGER4);

        private final String key;
        private final String unit;
        private final NumberType type;
    }

}
