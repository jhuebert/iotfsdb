package org.huebert.iotfsdb.series;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.partition.Partition;
import org.huebert.iotfsdb.partition.PartitionFactory;
import org.huebert.iotfsdb.util.Util;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

@Slf4j
public class Series implements AutoCloseable {

    private static final String METADATA_JSON = "metadata.json";

    private static final String DEFINITION_JSON = "definition.json";

    @Getter
    private final File root;

    @Getter
    private final SeriesDefinition definition;

    @Getter
    @Setter
    private Map<String, String> metadata;

    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicLong lastRequest = new AtomicLong(); //TODO

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final RangeMap<LocalDateTime, Partition<?>> rangeMap = TreeRangeMap.create();

    public Series(File root, SeriesDefinition definition) throws IOException {

        if (!root.mkdirs()) {
            throw new RuntimeException(String.format("unable to create series directory (%s)", root));
        }
        this.root = root;

        this.definition = definition;
        mapper.writeValue(new File(root, DEFINITION_JSON), definition);

        this.metadata = Map.of();
        mapper.writeValue(new File(root, METADATA_JSON), metadata);
    }

    public Series(File root) {
        this.root = Util.checkDirectory(root);
        this.definition = readDefinition(root);
        this.metadata = readMetadata(root);

        File[] files = root.listFiles();
        if (files == null) {
            throw new IllegalArgumentException(String.format("series directory (%s) contains no files", root));
        }

        PartitionPeriod partitionPeriod = definition.partition();
        Stream.of(files)
            .filter(File::isFile)
            .filter(file -> partitionPeriod == PartitionPeriod.findMatch(file.getName()))
            .forEach(file -> {
                LocalDateTime start = partitionPeriod.parseStart(file.getName());
                Partition<?> partition = PartitionFactory.create(definition.type(), file, start, partitionPeriod.getPeriod(), Duration.ofSeconds(definition.interval()));
                rangeMap.put(partition.getRange(), partition);
            });
    }

    private SeriesDefinition readDefinition(File seriesRoot) {
        File definitionFile = Util.checkFile(new File(seriesRoot, DEFINITION_JSON));
        SeriesDefinition seriesDefinition;
        try {
            seriesDefinition = mapper.readValue(definitionFile, SeriesDefinition.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        SeriesDefinition.checkValid(seriesDefinition);
        return seriesDefinition;
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

    public Map<String, String> updateMetadata(Map<String, String> metadata) throws IOException {
        rwLock.writeLock().lock();
        try {
            File metadataFile = Util.checkFileWrite(new File(root, METADATA_JSON));
            mapper.writeValue(metadataFile, metadata);
            this.metadata = metadata;
            return metadata;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private record Tuple(ZonedDateTime dateTime, Number value) {
    }

    public Map<ZonedDateTime, ? extends Number> get(List<Range<ZonedDateTime>> ranges, boolean includeNull, Aggregation aggregation) {

        Map<ZonedDateTime, Number> result = new LinkedHashMap<>();
        if (ranges.isEmpty()) {
            return result;
        }

        rwLock.readLock().lock();
        try {
            ranges.parallelStream()
                .map(current -> {
                    Range<LocalDateTime> local = convertToUtc(current);
                    Stream<? extends Number> stream = rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
                        .flatMap(f -> f.get(local).stream());
                    return new Tuple(current.lowerEndpoint(), PartitionFactory.aggregate(stream, aggregation).orElse(null));
                })
                .filter(t -> includeNull || (t.value != null))
                .forEachOrdered(t -> result.put(t.dateTime, t.value));
        } finally {
            rwLock.readLock().unlock();
        }

        lastRequest.updateAndGet(r -> System.currentTimeMillis());

        return result;
    }

    private LocalDateTime convertToUtc(ZonedDateTime zonedDateTime) {
        return zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private Range<LocalDateTime> convertToUtc(Range<ZonedDateTime> zonedRange) {
        return Range.range(
            convertToUtc(zonedRange.lowerEndpoint()),
            zonedRange.lowerBoundType(),
            convertToUtc(zonedRange.upperEndpoint()),
            zonedRange.upperBoundType()
        );
    }

    public void set(ZonedDateTime dateTime, String value) {

        LocalDateTime local = convertToUtc(dateTime);
        PartitionPeriod partitionPeriod = definition.partition();
        String filename = partitionPeriod.getFilename(local);
        LocalDateTime start = partitionPeriod.parseStart(filename);

        rwLock.writeLock().lock();
        try {
            Partition<?> partition = rangeMap.get(local);
            if (partition == null) {
                File file = new File(Util.checkDirectory(root), filename);
                partition = PartitionFactory.create(definition.type(), file, start, partitionPeriod.getPeriod(), Duration.of(definition.interval(), ChronoUnit.SECONDS));
                rangeMap.put(partition.getRange(), partition);
            }
            partition.set(local, value);
        } finally {
            rwLock.writeLock().unlock();
        }

        lastRequest.updateAndGet(r -> System.currentTimeMillis());
    }

    @Override
    public void close() throws Exception {
        rwLock.writeLock().lock();
        try {
            for (Partition<?> value : rangeMap.asMapOfRanges().values()) {
                value.close();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
