package org.huebert.iotfsdb.service;

import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionKeyTest {

    @Test
    public void testGetKey() {
        PartitionKey key = PartitionKey.getKey("123", PartitionPeriod.MONTH, LocalDateTime.parse("2024-11-11T12:34:56"));
        assertThat(key).isEqualTo(new PartitionKey("123", "202411"));
    }

}
