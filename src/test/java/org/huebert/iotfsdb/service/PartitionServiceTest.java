package org.huebert.iotfsdb.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ExtendWith(MockitoExtension.class)
public class PartitionServiceTest {

    @Test
    public void testGetRange() {

        DataService dataService = mock(DataService.class);
        PartitionService partitionService = new PartitionService(new IotfsdbProperties(), dataService);

        PartitionKey key = new PartitionKey("123", "20241111");

        when(dataService.getSeries("123")).thenReturn(Optional.of(SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .partition(PartitionPeriod.DAY)
                .interval(3600000L)
                .type(NumberType.INTEGER1)
                .build())
            .build()));

        LocalDateTime local = LocalDateTime.parse("2024-11-11T00:00:00");
        PartitionRange expected = new PartitionRange(key, Range.closed(local, local.plusDays(1).minusNanos(1)), Duration.ofHours(1), new BytePartition(), new ReentrantReadWriteLock());
        PartitionRange range = partitionService.getRange(key);
        assertThat(range.getKey()).isEqualTo(expected.getKey());
        assertThat(range.getRange()).isEqualTo(expected.getRange());
        assertThat(range.getInterval()).isEqualTo(expected.getInterval());
        assertThat(range.getAdapter()).isOfAnyClassIn(BytePartition.class);

        when(dataService.getPartitions("123")).thenReturn(Set.of(range.getKey()));

        RangeMap<LocalDateTime, PartitionRange> rangeMap = partitionService.getRangeMap("123");
        assertThat(rangeMap.asMapOfRanges()).isEqualTo(Map.of(range.getRange(), range));
    }

    @Test
    public void testGetAdapter() {

        DataService dataService = mock(DataService.class);

        SeriesDefinition definition = SeriesDefinition.builder()
            .partition(PartitionPeriod.DAY)
            .interval(3600000L)
            .build();
        when(dataService.getSeries("abc")).thenReturn(Optional.of(SeriesFile.builder()
            .definition(definition)
            .build()));

        definition.setType(NumberType.FLOAT2);
        assertThat(new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter()).isOfAnyClassIn(HalfFloatPartition.class);
        definition.setType(NumberType.FLOAT4);
        assertThat(new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter()).isOfAnyClassIn(FloatPartition.class);
        definition.setType(NumberType.FLOAT8);
        assertThat(new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter()).isOfAnyClassIn(DoublePartition.class);
        definition.setType(NumberType.INTEGER1);
        assertThat(new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter()).isOfAnyClassIn(BytePartition.class);
        definition.setType(NumberType.INTEGER2);
        assertThat(new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter()).isOfAnyClassIn(ShortPartition.class);
        definition.setType(NumberType.INTEGER4);
        assertThat(new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter()).isOfAnyClassIn(IntegerPartition.class);
        definition.setType(NumberType.INTEGER8);
        assertThat(new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter()).isOfAnyClassIn(LongPartition.class);

        definition.setMin(1.0);
        definition.setMax(2.0);

        definition.setType(NumberType.CURVED1);
        PartitionAdapter adapter = new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter();
        assertThat(adapter).isOfAnyClassIn(CurvedMappedPartition.class);
        assertThat(((CurvedMappedPartition) adapter).getInnerAdapter()).isOfAnyClassIn(BytePartition.class);
        assertThat(((CurvedMappedPartition) adapter).getMapper().getDecodedMin()).isEqualTo(1.0);
        assertThat(((CurvedMappedPartition) adapter).getMapper().getDecodedMax()).isEqualTo(2.0);

        definition.setType(NumberType.CURVED2);
        adapter = new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter();
        assertThat(adapter).isOfAnyClassIn(CurvedMappedPartition.class);
        assertThat(((CurvedMappedPartition) adapter).getInnerAdapter()).isOfAnyClassIn(ShortPartition.class);
        assertThat(((CurvedMappedPartition) adapter).getMapper().getDecodedMin()).isEqualTo(1.0);
        assertThat(((CurvedMappedPartition) adapter).getMapper().getDecodedMax()).isEqualTo(2.0);

        definition.setType(NumberType.CURVED4);
        adapter = new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter();
        assertThat(adapter).isOfAnyClassIn(CurvedMappedPartition.class);
        assertThat(((CurvedMappedPartition) adapter).getInnerAdapter()).isOfAnyClassIn(IntegerPartition.class);
        assertThat(((CurvedMappedPartition) adapter).getMapper().getDecodedMin()).isEqualTo(1.0);
        assertThat(((CurvedMappedPartition) adapter).getMapper().getDecodedMax()).isEqualTo(2.0);

        definition.setType(NumberType.MAPPED1);
        adapter = new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter();
        assertThat(adapter).isOfAnyClassIn(MappedPartition.class);
        assertThat(((MappedPartition) adapter).getInnerAdapter()).isOfAnyClassIn(BytePartition.class);
        assertThat(((MappedPartition) adapter).getMapper().getDecodedMin()).isEqualTo(1.0);
        assertThat(((MappedPartition) adapter).getMapper().getDecodedMax()).isEqualTo(2.0);

        definition.setType(NumberType.MAPPED2);
        adapter = new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter();
        assertThat(adapter).isOfAnyClassIn(MappedPartition.class);
        assertThat(((MappedPartition) adapter).getInnerAdapter()).isOfAnyClassIn(ShortPartition.class);
        assertThat(((MappedPartition) adapter).getMapper().getDecodedMin()).isEqualTo(1.0);
        assertThat(((MappedPartition) adapter).getMapper().getDecodedMax()).isEqualTo(2.0);

        definition.setType(NumberType.MAPPED4);
        adapter = new PartitionService(new IotfsdbProperties(), dataService).getRange(new PartitionKey("abc", "20241110")).getAdapter();
        assertThat(adapter).isOfAnyClassIn(MappedPartition.class);
        assertThat(((MappedPartition) adapter).getInnerAdapter()).isOfAnyClassIn(IntegerPartition.class);
        assertThat(((MappedPartition) adapter).getMapper().getDecodedMin()).isEqualTo(1.0);
        assertThat(((MappedPartition) adapter).getMapper().getDecodedMax()).isEqualTo(2.0);
    }

}
