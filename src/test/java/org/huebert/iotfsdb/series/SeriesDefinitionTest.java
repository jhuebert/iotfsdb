package org.huebert.iotfsdb.series;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SeriesDefinitionTest {

    @Test
    public void testCheckValid() {
        SeriesDefinition.checkValid(new SeriesDefinition("a", SeriesType.FLOAT4, 1, PartitionPeriod.DAY));
        assertThrows(IllegalArgumentException.class, () -> SeriesDefinition.checkValid(null));
        assertThrows(IllegalArgumentException.class, () -> SeriesDefinition.checkValid(new SeriesDefinition(null, SeriesType.FLOAT4, 1, PartitionPeriod.DAY)));
        assertThrows(IllegalArgumentException.class, () -> SeriesDefinition.checkValid(new SeriesDefinition("", SeriesType.FLOAT4, 1, PartitionPeriod.DAY)));
        assertThrows(IllegalArgumentException.class, () -> SeriesDefinition.checkValid(new SeriesDefinition("a", null, 1, PartitionPeriod.DAY)));
        assertThrows(IllegalArgumentException.class, () -> SeriesDefinition.checkValid(new SeriesDefinition("a", SeriesType.FLOAT4, 0, PartitionPeriod.DAY)));
        assertThrows(IllegalArgumentException.class, () -> SeriesDefinition.checkValid(new SeriesDefinition("a", SeriesType.FLOAT4, 86401, PartitionPeriod.DAY)));
        assertThrows(IllegalArgumentException.class, () -> SeriesDefinition.checkValid(new SeriesDefinition("a", SeriesType.FLOAT4, 1, null)));
    }

}
