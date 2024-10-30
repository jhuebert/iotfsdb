package org.huebert.iotfsdb.series;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.partition.Partition;
import org.huebert.iotfsdb.partition.PartitionFactory;
import org.huebert.iotfsdb.rest.schema.SeriesData;
import org.huebert.iotfsdb.rest.schema.SeriesStats;
import org.huebert.iotfsdb.util.FileUtil;
import org.huebert.iotfsdb.util.TimeUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Series implements AutoCloseable {

    private static final String SERIES_JSON = "series.json";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ConcurrentMap<LocalDateTime, Partition> partitionMap;

    private volatile RangeMap<LocalDateTime, Partition> cachedRangeMap = null;

    @Getter
    private final Path root;

    @Getter
    private final SeriesFile seriesFile;

    public Series(Path root, SeriesDefinition definition) {
        log.debug("Series(enter): root={}, definition={}", root, definition);

        this.root = root;

        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.seriesFile = SeriesFile.builder()
            .definition(definition)
            .metadata(Map.of())
            .build();

        try {
            MAPPER.writeValue(root.resolve(SERIES_JSON).toFile(), seriesFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.partitionMap = new ConcurrentHashMap<>();

        log.debug("Series(exit): root={}", root);
    }

    public Series(Path root) {
        log.debug("Series(enter): root={}", root);

        this.root = FileUtil.checkDirectory(root);

        Path seriesFilePath = FileUtil.checkFile(root.resolve(SERIES_JSON));
        try {
            this.seriesFile = MAPPER.readValue(seriesFilePath.toFile(), SeriesFile.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        PartitionPeriod partitionPeriod = seriesFile.getDefinition().getPartition();
        this.partitionMap = new ConcurrentHashMap<>(FileUtil.list(root)
            .filter(Files::isRegularFile)
            .filter(path -> partitionPeriod == PartitionPeriod.findMatch(path.getFileName().toString()))
            .map(path -> {
                LocalDateTime start = partitionPeriod.parseStart(path.getFileName().toString());
                return PartitionFactory.create(seriesFile.getDefinition(), path, start);
            })
            .collect(Collectors.toMap(p -> p.getRange().lowerEndpoint(), p -> p)));

        log.debug("Series(exit): root={}, seriesFile={}, partitionMap={}", root, seriesFile, partitionMap.size());
    }

    public void updateMetadata(Map<String, String> metadata) {
        log.debug("updateMetadata(enter): root={}, metadata={}", root, metadata);
        seriesFile.setMetadata(new HashMap<>(metadata));
        Path path = FileUtil.checkFileWrite(root.resolve(SERIES_JSON));
        try {
            MAPPER.writeValue(path.toFile(), seriesFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.debug("updateMetadata(exit): root={}, metadata={}", root, metadata);
    }

    public List<SeriesData> get(List<Range<ZonedDateTime>> ranges, boolean includeNull, Reducer reducer, boolean useBigDecimal) {
        log.debug("get(enter): root={}, ranges={}, includeNull={}, reducer={}", root, ranges.size(), includeNull, reducer);

        if (ranges.isEmpty()) {
            log.debug("get(exit): root={}, result=0", root);
            return List.of();
        }

        RangeMap<LocalDateTime, Partition> rangeMap = getRangeMap();

        List<SeriesData> result = ranges.parallelStream()
            .map(current -> {
                Range<LocalDateTime> local = TimeUtil.convertToUtc(current);
                Stream<? extends Number> stream = rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
                    .flatMap(partition -> partition.get(local).stream());
                return SeriesData.builder().time(current.lowerEndpoint()).value(PartitionFactory.reduce(stream, reducer, useBigDecimal).orElse(null)).build();
            })
            .filter(t -> includeNull || (t.getValue() != null))
            .sorted(Comparator.comparing(SeriesData::getTime))
            .toList();

        log.debug("get(exit): root={}, result={}", root, result.size());

        return result;
    }

    public void set(SeriesData data) {
        log.debug("set(enter): root={}, data={}", root, data);
        set(List.of(data));
        log.debug("set(exit): root={}, data={}", root, data);
    }

    public void set(List<SeriesData> values) {
        log.debug("set(enter): root={}, values={}", root, values.size());
        PartitionPeriod partitionPeriod = seriesFile.getDefinition().getPartition();
        for (SeriesData value : values) {
            LocalDateTime local = TimeUtil.convertToUtc(value.getTime());
            LocalDateTime start = partitionPeriod.getStart(local);
            partitionMap.computeIfAbsent(start, this::create).set(local, value.getValue());
        }
        log.debug("set(exit): root={}, values={}", root, values.size());
    }

    public void archive(Range<ZonedDateTime> range) {
        log.debug("archive(enter): root={}, range={}", root, range);

        if (range.isEmpty()) {
            log.debug("archive(exit): root={}, empty range", root);
            return;
        }

        RangeMap<LocalDateTime, Partition> rangeMap = getRangeMap();

        LocalDateTime now = TimeUtil.convertToUtc(ZonedDateTime.now());
        Range<LocalDateTime> local = TimeUtil.convertToUtc(range);

        rangeMap.subRangeMap(local).asMapOfRanges().values().parallelStream()
            .filter(e -> !e.getRange().contains(now))
            .filter(e -> local.encloses(e.getRange()))
            .filter(e -> !e.isArchive())
            .forEach(Partition::archive);

        log.debug("archive(exit): root={}", root);
    }

    public void unarchive(Range<ZonedDateTime> range) {
        log.debug("unarchive(enter): root={}, range={}", root, range);

        if (range.isEmpty()) {
            log.debug("unarchive(exit): root={}, empty range", root);
            return;
        }

        RangeMap<LocalDateTime, Partition> rangeMap = getRangeMap();
        Range<LocalDateTime> local = TimeUtil.convertToUtc(range);

        rangeMap.subRangeMap(local).asMapOfRanges().values().parallelStream()
            .filter(e -> local.encloses(e.getRange()))
            .filter(Partition::isArchive)
            .forEach(Partition::unarchive);

        log.debug("unarchive(exit): root={}", root);
    }

    public SeriesStats getStats() {

        long archiveSize = 0;
        long regularSize = 0;
        long regularPartitions = 0;
        ZonedDateTime from = null;
        ZonedDateTime to = null;

        if (!partitionMap.isEmpty()) {
            RangeSet<LocalDateTime> rangeSet = TreeRangeSet.create();
            for (Partition partition : partitionMap.values()) {
                rangeSet.add(partition.getRange());
                try {
                    long size = Files.size(partition.getPath());
                    if (partition.isArchive()) {
                        archiveSize += size;
                    } else {
                        regularSize += size;
                        regularPartitions++;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            Range<LocalDateTime> span = rangeSet.span();
            from = ZonedDateTime.of(span.lowerEndpoint(), ZoneOffset.UTC);
            to = ZonedDateTime.of(span.upperEndpoint(), ZoneOffset.UTC);
        }

        return SeriesStats.builder()
            .numSeries(1)
            .regularSize(regularSize)
            .archiveSize(archiveSize)
            .totalSize(regularSize + archiveSize)
            .regularNumPartitions(regularPartitions)
            .archiveNumPartitions(partitionMap.size() - regularPartitions)
            .totalNumPartitions(partitionMap.size())
            .from(from)
            .to(to)
            .build();
    }

    @Override
    public void close() {
        log.debug("close(enter): root={}", root);
        partitionMap.values().forEach(Partition::close);
        log.debug("close(exit): root={}", root);
    }

    public long closeIdlePartitions() {
        log.debug("closeIfIdle(enter): root={}", root);
        long result = partitionMap.values().stream().map(Partition::closeIfIdle).filter(r -> r).count();
        log.debug("closeIfIdle(exit): root={}, result={}", root, result);
        return result;
    }

    private Partition create(LocalDateTime start) {
        log.debug("create(enter): root={}, start={}", root, start);
        SeriesDefinition definition = seriesFile.getDefinition();
        String filename = definition.getPartition().getFilename(start);
        Path path = FileUtil.checkDirectory(root).resolve(filename);
        Partition result = PartitionFactory.create(definition, path, start);
        cachedRangeMap = null;
        log.debug("create(exit): root={}, path={}", root, path);
        return result;
    }

    private RangeMap<LocalDateTime, Partition> getRangeMap() {
        RangeMap<LocalDateTime, Partition> rangeMap = cachedRangeMap;
        if (rangeMap == null) {
            log.debug("getRangeMap: root={}", root);
            rangeMap = TreeRangeMap.create();
            for (Partition p : partitionMap.values()) {
                rangeMap.put(p.getRange(), p);
            }
            cachedRangeMap = rangeMap;
        }
        return rangeMap;
    }

}
