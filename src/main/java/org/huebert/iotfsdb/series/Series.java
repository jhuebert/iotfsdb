package org.huebert.iotfsdb.series;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeRangeMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.partition.Partition;
import org.huebert.iotfsdb.partition.PartitionFactory;
import org.huebert.iotfsdb.rest.schema.SeriesData;
import org.huebert.iotfsdb.util.Util;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Series implements AutoCloseable {

    private static final String ARCHIVE_ZIP = "archive.zip";

    private static final String DEFINITION_JSON = "definition.json";

    private static final String METADATA_JSON = "metadata.json";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ConcurrentMap<LocalDateTime, Partition<?>> partitionMap;

    private volatile RangeMap<LocalDateTime, Partition<?>> cachedRangeMap = null;

    @Getter
    private final Path root;

    @Getter
    private final SeriesDefinition definition;

    @Getter
    private Map<String, String> metadata;

    public Series(Path root, SeriesDefinition definition) {
        log.debug("Series(enter): root={}, definition={}", root, definition);

        this.root = Util.createDirectories(root);

        try {

            this.definition = definition;
            MAPPER.writeValue(root.resolve(DEFINITION_JSON).toFile(), definition);

            this.metadata = Map.of();
            MAPPER.writeValue(root.resolve(METADATA_JSON).toFile(), metadata);

            Path archiveFile = root.resolve(ARCHIVE_ZIP);
            try (FileSystem ignored = FileSystems.newFileSystem(archiveFile, Map.of("create", "true"))) {
            }

            this.partitionMap = new ConcurrentHashMap<>();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.debug("Series(exit): root={}", root);
    }

    public Series(Path root) {
        log.debug("Series(enter): root={}", root);

        this.root = Util.checkDirectory(root);

        Path definitionFile = Util.checkFile(root.resolve(DEFINITION_JSON));
        try {
            this.definition = MAPPER.readValue(definitionFile.toFile(), SeriesDefinition.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path metadataFile = Util.checkFile(root.resolve(METADATA_JSON));
        try {
            this.metadata = MAPPER.readValue(metadataFile.toFile(), new TypeReference<>() {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path archiveFile = root.resolve(ARCHIVE_ZIP);
        PartitionPeriod partitionPeriod = definition.getPartition();
        try (FileSystem archive = FileSystems.newFileSystem(archiveFile, Map.of("create", "true"))) {
            this.partitionMap = new ConcurrentHashMap<>(Streams.concat(Util.list(archive.getPath("/")), Util.list(root))
                .filter(Files::isRegularFile)
                .filter(path -> partitionPeriod == PartitionPeriod.findMatch(path.getFileName().toString()))
                .map(path -> {
                    LocalDateTime start = partitionPeriod.parseStart(path.getFileName().toString());
                    return PartitionFactory.create(definition, path, start);
                })
                .collect(Collectors.toMap(p -> p.getRange().lowerEndpoint(), p -> p)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.debug("Series(exit): root={}, definition={}, metadata={}, partitionMap={}", root, definition, metadata, partitionMap.size());
    }

    public void updateMetadata(Map<String, String> metadata) {
        log.debug("updateMetadata(enter): root={}, metadata={}", root, metadata);
        Path metadataFile = Util.checkFileWrite(root.resolve(METADATA_JSON));
        try {
            MAPPER.writeValue(metadataFile.toFile(), metadata);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.metadata = metadata;
        log.debug("updateMetadata(exit): root={}, metadata={}", root, metadata);
    }

    public List<SeriesData> get(List<Range<ZonedDateTime>> ranges, boolean includeNull, Reducer reducer) {
        log.debug("get(enter): root={}, ranges={}, includeNull={}, reducer={}", root, ranges.size(), includeNull, reducer);

        if (ranges.isEmpty()) {
            log.debug("get(exit): root={}, result=0", root);
            return List.of();
        }

        RangeMap<LocalDateTime, Partition<?>> rangeMap = getRangeMap();

        List<SeriesData> result = ranges.parallelStream()
            .map(current -> {
                Range<LocalDateTime> local = Util.convertToUtc(current);
                Stream<? extends Number> stream = rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
                    .flatMap(partition -> partition.get(local).stream());
                return SeriesData.builder().time(current.lowerEndpoint()).value(PartitionFactory.reduce(stream, reducer).orElse(null)).build();
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
        PartitionPeriod partitionPeriod = definition.getPartition();
        for (SeriesData value : values) {
            LocalDateTime local = Util.convertToUtc(value.getTime());
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

        RangeMap<LocalDateTime, Partition<?>> rangeMap = getRangeMap();

        LocalDateTime now = Util.convertToUtc(ZonedDateTime.now());
        Range<LocalDateTime> local = Util.convertToUtc(range);

        try (FileSystem archive = FileSystems.newFileSystem(root.resolve(ARCHIVE_ZIP), Map.of("create", "true"))) {
            rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
                .filter(e -> !e.getUri().getScheme().equals("jar"))
                .filter(e -> !e.getRange().contains(now))
                .filter(e -> local.encloses(e.getRange()))
                .forEach(p -> {
                    Path uncompressedPath = Path.of(p.getUri());
                    log.debug("archiving {}", uncompressedPath);

                    LocalDateTime start = p.getRange().lowerEndpoint();
                    partitionMap.remove(start).close();

                    Path compressedPath = archive.getPath(uncompressedPath.getFileName().toString());
                    Util.copy(uncompressedPath, compressedPath);
                    partitionMap.put(start, PartitionFactory.create(definition, compressedPath, start));

                    try {
                        Files.deleteIfExists(uncompressedPath);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            this.cachedRangeMap = null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.debug("archive(exit): root={}", root);
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

    private Partition<?> create(LocalDateTime start) {
        log.debug("create(enter): root={}, start={}", root, start);
        String filename = definition.getPartition().getFilename(start);
        Path path = Util.checkDirectory(root).resolve(filename);
        Partition<?> result = PartitionFactory.create(definition, path, start);
        cachedRangeMap = null;
        log.debug("create(exit): root={}, path={}", root, path);
        return result;
    }

    private RangeMap<LocalDateTime, Partition<?>> getRangeMap() {
        RangeMap<LocalDateTime, Partition<?>> rangeMap = cachedRangeMap;
        if (rangeMap == null) {
            log.debug("getRangeMap: root={}", root);
            rangeMap = TreeRangeMap.create();
            for (Partition<?> p : partitionMap.values()) {
                rangeMap.put(p.getRange(), p);
            }
            cachedRangeMap = rangeMap;
        }
        return rangeMap;
    }

}
