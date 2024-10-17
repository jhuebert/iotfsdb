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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Series implements AutoCloseable {

    private static final String METADATA_JSON = "metadata.json";

    private static final String DEFINITION_JSON = "definition.json";

    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentMap<LocalDateTime, Partition<?>> partitionMap;

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

            this.partitionMap = new ConcurrentHashMap<>();

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
        this.partitionMap = new ConcurrentHashMap<>(Stream.of(files)
            .filter(File::isFile)
            .filter(file -> partitionPeriod == PartitionPeriod.findMatch(file.getName()))
            .map(file -> {
                LocalDateTime start = partitionPeriod.parseStart(file.getName());
                return PartitionFactory.create(definition.getType(), file, start, partitionPeriod.getPeriod(), Duration.ofSeconds(definition.getInterval()));
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
        try {
            File metadataFile = Util.checkFileWrite(new File(root, METADATA_JSON));
            mapper.writeValue(metadataFile, metadata);
            this.metadata = metadata;
            return metadata;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<ZonedDateTime, ? extends Number> get(List<Range<ZonedDateTime>> ranges, boolean includeNull, Aggregation aggregation) {

        Map<ZonedDateTime, Number> result = new LinkedHashMap<>();
        if (ranges.isEmpty()) {
            return result;
        }

        RangeMap<LocalDateTime, Partition<?>> rangeMap = TreeRangeMap.create();
        partitionMap.values().forEach(p -> rangeMap.put(p.getRange(), p));

        ranges.parallelStream()
            .map(current -> {
                Range<LocalDateTime> local = Util.convertToUtc(current);
                Stream<? extends Number> stream = rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
                    .flatMap(partition -> partition.get(local).stream());
                return new Tuple<>(current.lowerEndpoint(), PartitionFactory.aggregate(stream, aggregation).orElse(null));
            })
            .filter(t -> includeNull || (t.value() != null))
            .forEachOrdered(t -> result.put(t.key(), t.value()));

        return result;
    }

    public void set(Iterable<DataValue> dataValues) {
        PartitionPeriod partitionPeriod = definition.getPartition();
        for (DataValue dataValue : dataValues) {
            LocalDateTime local = Util.convertToUtc(dataValue.getDateTime());
            LocalDateTime start = partitionPeriod.getStart(local);
            partitionMap.computeIfAbsent(start, this::create).set(local, dataValue.getValue());
        }
    }

    private Partition<?> create(LocalDateTime start) {
        PartitionPeriod partitionPeriod = definition.getPartition();
        String filename = partitionPeriod.getFilename(start);
        Duration duration = Duration.of(definition.getInterval(), ChronoUnit.SECONDS);
        File file = new File(Util.checkDirectory(root), filename);
        return PartitionFactory.create(definition.getType(), file, start, partitionPeriod.getPeriod(), duration);
    }

    public void set(DataValue dataValue) {
        set(List.of(dataValue));
    }

    public long closeIfIdle() {
        return partitionMap.values().stream().map(Partition::closeIfIdle).filter(r -> r).count();
    }

    @Override
    public void close() {
        partitionMap.values().forEach(Partition::close);
    }

}
