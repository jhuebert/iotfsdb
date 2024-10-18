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

    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentMap<LocalDateTime, Partition<?>> partitionMap;

    private volatile RangeMap<LocalDateTime, Partition<?>> cachedRangeMap = null;

    @Getter
    private final File root;

    @Getter
    private final SeriesDefinition definition;

    @Getter
    private Map<String, String> metadata;

    public Series(File root, SeriesDefinition definition) {
        // TODO Log

        if (!root.mkdirs()) {
            throw new RuntimeException(String.format("unable to create series directory (%s)", root));
        }
        this.root = root;

        try {

            this.definition = definition;
            mapper.writeValue(new File(root, DEFINITION_JSON), definition);

            this.metadata = Map.of();
            mapper.writeValue(new File(root, METADATA_JSON), metadata);

            this.partitionMap = new ConcurrentHashMap<>();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Series(File root) {
        // TODO Log
        this.root = Util.checkDirectory(root);
        this.definition = readDefinition(root);
        this.metadata = readMetadata(root);

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
        // TODO Log
        try {
            File metadataFile = Util.checkFileWrite(new File(root, METADATA_JSON));
            mapper.writeValue(metadataFile, metadata);
            this.metadata = metadata;
            return metadata;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<SeriesData> get(List<Range<ZonedDateTime>> ranges, boolean includeNull, Reducer reducer) {
        // TODO Log

        if (ranges.isEmpty()) {
            return List.of();
        }

        RangeMap<LocalDateTime, Partition<?>> rangeMap = getRangeMap();

        return ranges.parallelStream()
            .map(current -> {
                Range<LocalDateTime> local = Util.convertToUtc(current);
                Stream<? extends Number> stream = rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
                    .flatMap(partition -> partition.get(local).stream());
                return SeriesData.builder().time(current.lowerEndpoint()).value(PartitionFactory.reduce(stream, reducer).orElse(null)).build();
            })
            .filter(t -> includeNull || (t.getValue() != null))
            .sorted(Comparator.comparing(SeriesData::getTime))
            .toList();
    }

    private RangeMap<LocalDateTime, Partition<?>> getRangeMap() {
        RangeMap<LocalDateTime, Partition<?>> rangeMap = cachedRangeMap;
        if (rangeMap == null) {
            rangeMap = TreeRangeMap.create();
            for (Partition<?> p : partitionMap.values()) {
                rangeMap.put(p.getRange(), p);
            }
            cachedRangeMap = rangeMap;
        }
        return rangeMap;
    }

    public void set(Iterable<SeriesData> values) {
        // TODO Log
        PartitionPeriod partitionPeriod = definition.getPartition();
        for (SeriesData value : values) {
            LocalDateTime local = Util.convertToUtc(value.getTime());
            LocalDateTime start = partitionPeriod.getStart(local);
            partitionMap.computeIfAbsent(start, this::create).set(local, value.getValue());
        }
    }

    private Partition<?> create(LocalDateTime start) {
        String filename = definition.getPartition().getFilename(start);
        File file = new File(Util.checkDirectory(root), filename);
        Partition<?> result = PartitionFactory.create(definition, file, start);
        cachedRangeMap = null;
        return result;
    }

    public void set(SeriesData dataValue) {
        set(List.of(dataValue));
    }

    public long closeIfIdle() {
        // TODO Log
        return partitionMap.values().stream().map(Partition::closeIfIdle).filter(r -> r).count();
    }

    @Override
    public void close() {
        // TODO Log
        partitionMap.values().forEach(Partition::close);
    }

}
