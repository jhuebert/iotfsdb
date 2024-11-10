package org.huebert.iotfsdb.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.partition.BytePartition;
import org.huebert.iotfsdb.partition.CurvedMappedPartition;
import org.huebert.iotfsdb.partition.DoublePartition;
import org.huebert.iotfsdb.partition.FloatPartition;
import org.huebert.iotfsdb.partition.HalfFloatPartition;
import org.huebert.iotfsdb.partition.IntegerPartition;
import org.huebert.iotfsdb.partition.LongPartition;
import org.huebert.iotfsdb.partition.MappedPartition;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.huebert.iotfsdb.partition.ShortPartition;
import org.huebert.iotfsdb.schema.NumberType;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Validated
@Slf4j
@Service
public class PartitionService {

    private static final Set<NumberType> CURVED = EnumSet.of(NumberType.CURVED1, NumberType.CURVED2, NumberType.CURVED4);

    private static final Set<NumberType> MAPPED = EnumSet.of(NumberType.MAPPED1, NumberType.MAPPED2, NumberType.MAPPED4);

    private static final Map<NumberType, PartitionAdapter> ADAPTER_MAP;

    private final DataService dataService;

    private final LoadingCache<PartitionKey, PartitionRange> partitionCache;

    static {
        ADAPTER_MAP = new HashMap<>();
        ADAPTER_MAP.put(NumberType.CURVED1, new BytePartition());
        ADAPTER_MAP.put(NumberType.CURVED2, new ShortPartition());
        ADAPTER_MAP.put(NumberType.CURVED4, new IntegerPartition());
        ADAPTER_MAP.put(NumberType.FLOAT2, new HalfFloatPartition());
        ADAPTER_MAP.put(NumberType.FLOAT4, new FloatPartition());
        ADAPTER_MAP.put(NumberType.FLOAT8, new DoublePartition());
        ADAPTER_MAP.put(NumberType.INTEGER1, new BytePartition());
        ADAPTER_MAP.put(NumberType.INTEGER2, new ShortPartition());
        ADAPTER_MAP.put(NumberType.INTEGER4, new IntegerPartition());
        ADAPTER_MAP.put(NumberType.INTEGER8, new LongPartition());
        ADAPTER_MAP.put(NumberType.MAPPED1, new BytePartition());
        ADAPTER_MAP.put(NumberType.MAPPED2, new ShortPartition());
        ADAPTER_MAP.put(NumberType.MAPPED4, new IntegerPartition());
    }

    public PartitionService(@NotNull IotfsdbProperties properties, @NotNull DataService dataService) {
        this.dataService = dataService;
        this.partitionCache = CacheBuilder.from(properties.getPartitionCache())
            .build(new CacheLoader<>(this::calculateRange));
    }

    public PartitionRange getRange(@Valid @NotNull PartitionKey key) {
        return partitionCache.getUnchecked(key);
    }

    public RangeMap<LocalDateTime, PartitionRange> getRangeMap(@NotBlank String seriesId) {
        RangeMap<LocalDateTime, PartitionRange> rangeMap = TreeRangeMap.create();
        dataService.getPartitions(seriesId).stream()
            .map(this::getRange)
            .forEach(pr -> rangeMap.put(pr.range(), pr));
        return rangeMap;
    }

    private PartitionRange calculateRange(PartitionKey key) {
        SeriesFile series = dataService.getSeries(key.seriesId()).orElseThrow();
        SeriesDefinition definition = series.getDefinition();
        Range<LocalDateTime> range = getRange(definition, key.partitionId());
        PartitionAdapter adapter = getAdapter(definition);
        //TODO Get interval from actual partition file?
        return new PartitionRange(key, range, definition.getIntervalDuration(), adapter);
    }

    private static Range<LocalDateTime> getRange(SeriesDefinition definition, String partitionId) {
        PartitionPeriod partitionPeriod = definition.getPartition();
        LocalDateTime start = partitionPeriod.parseStart(partitionId);
        LocalDateTime end = start.plus(partitionPeriod.getPeriod()).minusNanos(1);
        return Range.closed(start, end);
    }

    private static PartitionAdapter getAdapter(SeriesDefinition definition) {

        NumberType type = definition.getType();
        PartitionAdapter adapter = ADAPTER_MAP.get(type);
        if (adapter == null) {
            throw new IllegalArgumentException(String.format("series type %s not supported", type));
        }

        if (MAPPED.contains(type)) {
            adapter = new MappedPartition(adapter, definition.getMin(), definition.getMax());
        } else if (CURVED.contains(type)) {
            adapter = new CurvedMappedPartition(adapter, definition.getMin(), definition.getMax());
        }

        return adapter;
    }

}
