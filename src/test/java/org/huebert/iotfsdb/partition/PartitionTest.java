package org.huebert.iotfsdb.partition;

import com.google.common.collect.Range;
import org.huebert.iotfsdb.partition.adapter.FloatPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionTest {

    private static final LocalDateTime START = LocalDateTime.parse("2024-10-10T00:00:00");

    private static final Period PERIOD = Period.ofDays(1);

    private static final Duration INTERVAL = Duration.of(1, ChronoUnit.HOURS);

    private File file;

    @BeforeEach
    public void beforeEach() throws IOException {
        file = File.createTempFile(PartitionTest.class.getSimpleName(), "");
        file.deleteOnExit();
        file.delete();
    }

    @AfterEach
    public void afterEach() {
        file.delete();
    }

    @Test
    public void testCreate() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            assertThat(partition.size()).isEqualTo(24);
            assertThat(file.exists()).isEqualTo(true);
            assertThat(file.canRead()).isEqualTo(true);
            assertThat(file.canWrite()).isEqualTo(true);
            assertThat(file.isFile()).isEqualTo(true);
            assertThat(file.length()).isEqualTo(96);
            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo(null);
            }
        }
    }

    @Test
    public void testReadEmpty() throws Exception {
        file.createNewFile();
        assertThrows(IllegalArgumentException.class, () -> new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition()));
    }

    @Test
    public void testReadInvalidSize() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(5);
        raf.close();
        assertThrows(IllegalArgumentException.class, () -> new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition()));
    }

    @Test
    public void testRead() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(40);
        raf.close();
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            for (int i = 0; i < 10; i++) {
                assertThat(partition.get(i)).isEqualTo(0.0f);
            }
            partition.set(0, 500.0f);
            assertThat(partition.get(0)).isEqualTo(500.0f);
        }
    }

    @Test
    public void testGetRangeEmpty() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            LocalDateTime testTime = START.plusHours(12);
            List<Number> result = partition.get(Range.closedOpen(testTime, testTime));
            assertThat(result.size()).isEqualTo(0);
        }
    }

    @Test
    public void testGetRangeAllExact() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            Float[] expected = new Float[24];
            for (int i = 0; i < 24; i++) {
                partition.set(i, (float) i);
                expected[i] = (float) i;
            }
            assertThat(partition.get(Range.closed(START, START.plus(PERIOD).minusNanos(1)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeSingle() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (float) i);
            }
            LocalDateTime testTime = START.plusHours(12);
            List<Number> result = partition.get(Range.closed(testTime, testTime));
            assertThat(result).isEqualTo(List.of(12.0f));
        }
    }

    @Test
    public void testGetRangeContained() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (float) i);
            }
            Float[] expected = new Float[]{4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
            assertThat(partition.get(Range.closed(START.plusHours(4), START.plusHours(8)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeLowerOverlap() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (float) i);
            }
            Float[] expected = new Float[]{0.0f, 1.0f, 2.0f, 3.0f, 4.0f};
            assertThat(partition.get(Range.closed(START.minusHours(4), START.plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeUpperOverlap() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (float) i);
            }
            Float[] expected = new Float[]{20.0f, 21.0f, 22.0f, 23.0f};
            assertThat(partition.get(Range.closed(START.plus(PERIOD).minusHours(4), START.plus(PERIOD).plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeCompleteOverlap() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            Float[] expected = new Float[24];
            for (int i = 0; i < 24; i++) {
                partition.set(i, (float) i);
                expected[i] = (float) i;
            }
            assertThat(partition.get(Range.closed(START.minusHours(4), START.plus(PERIOD).plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testSetAndGetIndex() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo(null);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(i, (float) i);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo((float) i);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(i, null);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo(null);
            }
        }
    }

    @Test
    public void testSetAndGetDateTime() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo(null);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(START.plusHours(i), i);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo((float) i);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(START.plusHours(i), (Float) null);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo(null);
            }
        }
    }

    @Test
    public void testCreateOpenAndClose() {
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            assertThat(partition.isOpen()).isEqualTo(true);

            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            assertThat(partition.get(0)).isEqualTo(null);
            assertThat(partition.isOpen()).isEqualTo(true);
            partition.open();
            assertThat(partition.isOpen()).isEqualTo(true);

            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            partition.open();
            assertThat(partition.isOpen()).isEqualTo(true);
            partition.open();
            assertThat(partition.isOpen()).isEqualTo(true);
        }
    }

    @Test
    public void testReadOpenAndClose() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(40);
        raf.close();
        try (Partition partition = new Partition(file.toPath(), START, PERIOD, INTERVAL, new FloatPartition())) {
            assertThat(partition.isOpen()).isEqualTo(false);

            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            assertThat(partition.get(0)).isEqualTo(0.0f);
            assertThat(partition.isOpen()).isEqualTo(true);
            partition.open();
            assertThat(partition.isOpen()).isEqualTo(true);

            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            partition.open();
            assertThat(partition.isOpen()).isEqualTo(true);
            partition.open();
            assertThat(partition.isOpen()).isEqualTo(true);
        }
    }

}
