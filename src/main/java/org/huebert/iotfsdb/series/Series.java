package org.huebert.iotfsdb.series;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.partition.Partition;
import org.huebert.iotfsdb.partition.PartitionFactory;
import org.huebert.iotfsdb.rest.DataValue;
import org.huebert.iotfsdb.util.Tuple;
import org.huebert.iotfsdb.util.Util;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

@Slf4j
public class Series implements AutoCloseable {

    private static final String METADATA_JSON = "metadata.json";

    private static final String DEFINITION_JSON = "definition.json";

    private final ObjectMapper mapper = new ObjectMapper();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final RangeMap<LocalDateTime, Partition<?>> rangeMap = TreeRangeMap.create();

    @Getter
    private final File root;

    @Getter
    private final SeriesDefinition definition;

    @Getter
    private Map<String, String> metadata;

    public Series(File root, SeriesDefinition definition) {

        if (!root.mkdirs()) {
            throw new RuntimeException(String.format("unable to create series directory (%s)", root));
        }
        this.root = root;

        try {

            this.definition = definition;
            mapper.writeValue(new File(root, DEFINITION_JSON), definition);

            this.metadata = Map.of();
            mapper.writeValue(new File(root, METADATA_JSON), metadata);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Series(File root) {
        this.root = Util.checkDirectory(root);
        this.definition = readDefinition(root);
        this.metadata = readMetadata(root);

        File[] files = root.listFiles();
        if (files == null) {
            throw new IllegalArgumentException(String.format("series directory (%s) contains no files", root));
        }

        PartitionPeriod partitionPeriod = definition.getPartition();
        Stream.of(files)
            .filter(File::isFile)
            .filter(file -> partitionPeriod == PartitionPeriod.findMatch(file.getName()))
            .forEach(file -> {
                LocalDateTime start = partitionPeriod.parseStart(file.getName());
                Partition<?> partition = PartitionFactory.create(definition.getType(), file, start, partitionPeriod.getPeriod(), Duration.ofSeconds(definition.getInterval()));
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

    public Map<String, String> updateMetadata(Map<String, String> metadata) {
        rwLock.writeLock().lock();
        try {
            File metadataFile = Util.checkFileWrite(new File(root, METADATA_JSON));
            mapper.writeValue(metadataFile, metadata);
            this.metadata = metadata;
            return metadata;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            rwLock.writeLock().unlock();
        }
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
                    Range<LocalDateTime> local = Util.convertToUtc(current);
                    Stream<? extends Number> stream = rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
                        .flatMap(partition -> partition.get(local).stream());
                    return new Tuple<>(current.lowerEndpoint(), PartitionFactory.aggregate(stream, aggregation).orElse(null));
                })
                .filter(t -> includeNull || (t.value() != null))
                .forEachOrdered(t -> result.put(t.key(), t.value()));
        } finally {
            rwLock.readLock().unlock();
        }

        return result;
    }

    public void set(Iterable<DataValue> dataValues) {
        rwLock.writeLock().lock();
        try {
            for (DataValue dataValue : dataValues) {
                LocalDateTime local = Util.convertToUtc(dataValue.getDateTime());
                Partition<?> partition = rangeMap.get(local);
                if (partition == null) {
                    PartitionPeriod partitionPeriod = definition.getPartition();
                    String filename = partitionPeriod.getFilename(local);
                    File file = new File(Util.checkDirectory(root), filename);
                    LocalDateTime start = partitionPeriod.parseStart(filename);
                    partition = PartitionFactory.create(definition.getType(), file, start, partitionPeriod.getPeriod(), Duration.of(definition.getInterval(), ChronoUnit.SECONDS));
                    rangeMap.put(partition.getRange(), partition);
                }
                partition.set(local, dataValue.getValue());
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void set(DataValue dataValue) {
        set(List.of(dataValue));
    }

    public long closeIfIdle() {
        rwLock.writeLock().lock();
        try {
            return rangeMap.asMapOfRanges().values().stream().map(Partition::closeIfIdle).filter(r -> r).count();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public long sync() {
        rwLock.writeLock().lock();
        try {
            return rangeMap.asMapOfRanges().values().stream().map(Partition::sync).filter(r -> r).count();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        rwLock.writeLock().lock();
        try {
            rangeMap.asMapOfRanges().values().forEach(Partition::close);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

}
