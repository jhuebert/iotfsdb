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
import org.huebert.iotfsdb.rest.schema.SeriesData;
import org.huebert.iotfsdb.util.Util;

import java.io.File;
import java.io.IOException;
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

    private static final String METADATA_JSON = "metadata.json";

    private static final String DEFINITION_JSON = "definition.json";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ConcurrentMap<LocalDateTime, Partition<?>> partitionMap;

    private volatile RangeMap<LocalDateTime, Partition<?>> cachedRangeMap = null;

    @Getter
    private final File root;

    @Getter
    private final SeriesDefinition definition;

    @Getter
    private Map<String, String> metadata;

    public Series(File root, SeriesDefinition definition) {
        log.debug("Series(enter): root={}, definition={}", root, definition);

        if (!root.mkdirs()) {
            throw new RuntimeException(String.format("unable to create series directory (%s)", root));
        }
        this.root = root;

        try {

            this.definition = definition;
            MAPPER.writeValue(new File(root, DEFINITION_JSON), definition);

            this.metadata = Map.of();
            MAPPER.writeValue(new File(root, METADATA_JSON), metadata);

            this.partitionMap = new ConcurrentHashMap<>();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.debug("Series(exit): root={}", root);
    }

    public Series(File root) {
        log.debug("Series(enter): root={}", root);

        this.root = Util.checkDirectory(root);

        File definitionFile = Util.checkFile(new File(root, DEFINITION_JSON));
        try {
            this.definition = MAPPER.readValue(definitionFile, SeriesDefinition.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        File metadataFile = Util.checkFile(new File(root, METADATA_JSON));
        try {
            this.metadata = MAPPER.readValue(metadataFile, new TypeReference<>() {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        File[] files = root.listFiles();
        if (files == null) {
            throw new IllegalArgumentException(String.format("series directory (%s) contains no files", root));
        }

        PartitionPeriod partitionPeriod = definition.getPartition();
        this.partitionMap = new ConcurrentHashMap<>(Stream.of(files)
            .filter(File::isFile)
            .filter(file -> partitionPeriod == PartitionPeriod.findMatch(file.getName()))
            .map(file -> {
                LocalDateTime start = partitionPeriod.parseStart(file.getName());
                return PartitionFactory.create(definition, file, start);
            })
            .collect(Collectors.toMap(p -> p.getRange().lowerEndpoint(), p -> p)));

        log.debug("Series(exit): root={}, definition={}, metadata={}, partitionMap={}", root, definition, metadata, partitionMap.size());
    }

    public void updateMetadata(Map<String, String> metadata) {
        log.debug("updateMetadata(enter): root={}, metadata={}", root, metadata);
        File metadataFile = Util.checkFileWrite(new File(root, METADATA_JSON));
        try {
            MAPPER.writeValue(metadataFile, metadata);
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

    @Override
    public void close() {
        log.debug("close(enter): root={}", root);
        partitionMap.values().forEach(Partition::close);
        log.debug("close(exit): root={}", root);
    }

    public long closeIfIdle() {
        log.debug("closeIfIdle(enter): root={}", root);
        long result = partitionMap.values().stream().map(Partition::closeIfIdle).filter(r -> r).count();
        log.debug("closeIfIdle(exit): root={}, result={}", root, result);
        return result;
    }

    private Partition<?> create(LocalDateTime start) {
        log.debug("create(enter): root={}, start={}", root, start);
        String filename = definition.getPartition().getFilename(start);
        File file = new File(Util.checkDirectory(root), filename);
        Partition<?> result = PartitionFactory.create(definition, file, start);
        cachedRangeMap = null;
        log.debug("create(exit): root={}, file={}", root, file);
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
