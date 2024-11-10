package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class PartitionRangeTest {

    @Test
    public void testGetIndex() {
        LocalDateTime from = LocalDateTime.parse("2024-11-11T00:00:00");
        LocalDateTime to = from.plusDays(1).minusNanos(1);
        Duration interval = Duration.ofHours(1);
        PartitionAdapter adapter = mock(PartitionAdapter.class);
        PartitionRange partitionRange = new PartitionRange(null, Range.closed(from, to), interval, adapter);
        for (int i = 0; i < 24; i++) {
            assertThat(partitionRange.getIndex(from.plusHours(i))).isEqualTo(i);
        }
    }

    @Test
    public void testGetSize() {
        LocalDateTime from = LocalDateTime.parse("2024-11-11T00:00:00");
        LocalDateTime to = from.plusDays(1).minusNanos(1);
        Duration interval = Duration.ofHours(1);
        PartitionAdapter adapter = mock(PartitionAdapter.class);
        PartitionRange partitionRange = new PartitionRange(null, Range.closed(from, to), interval, adapter);
        assertThat(partitionRange.getSize()).isEqualTo(24);
    }

}
