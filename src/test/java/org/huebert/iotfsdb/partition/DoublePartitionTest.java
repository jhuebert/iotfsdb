package org.huebert.iotfsdb.partition;

import com.google.common.collect.Range;
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

public class DoublePartitionTest {

    private static final LocalDateTime START = LocalDateTime.parse("2024-10-10T00:00:00");

    private static final Period PERIOD = Period.ofDays(1);

    private static final Duration INTERVAL = Duration.of(1, ChronoUnit.HOURS);

    private File file;

    @BeforeEach
    public void beforeEach() throws IOException {
        file = File.createTempFile(DoublePartitionTest.class.getSimpleName(), "");
        file.deleteOnExit();
        file.delete();
    }

    @AfterEach
    public void afterEach() {
        file.delete();
    }

    @Test
    public void testCreate() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            assertThat(partition.size()).isEqualTo(24);
            assertThat(file.exists()).isEqualTo(true);
            assertThat(file.canRead()).isEqualTo(true);
            assertThat(file.canWrite()).isEqualTo(true);
            assertThat(file.isFile()).isEqualTo(true);
            assertThat(file.length()).isEqualTo(192);
            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo(null);
            }
        }
    }

    @Test
    public void testReadEmpty() throws Exception {
        file.createNewFile();
        assertThrows(IllegalArgumentException.class, () -> new DoublePartition(file, START, PERIOD, INTERVAL));
    }

    @Test
    public void testReadInvalidSize() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(5);
        raf.close();
        assertThrows(IllegalArgumentException.class, () -> new DoublePartition(file, START, PERIOD, INTERVAL));
    }

    @Test
    public void testRead() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(80);
        raf.close();
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 10; i++) {
                assertThat(partition.get(i)).isEqualTo(0.0f);
            }
            partition.set(0, 500.0);
            assertThat(partition.get(0)).isEqualTo(500.0f);
        }
    }

    @Test
    public void testGetRangeEmpty() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            LocalDateTime testTime = START.plusHours(12);
            List<Double> result = partition.get(Range.closedOpen(testTime, testTime));
            assertThat(result.size()).isEqualTo(0);
        }
    }

    @Test
    public void testGetRangeAllExact() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            Double[] expected = new Double[24];
            for (int i = 0; i < 24; i++) {
                partition.set(i, (double) i);
                expected[i] = (double) i;
            }
            assertThat(partition.get(Range.closed(START, START.plus(PERIOD).minusNanos(1)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeSingle() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (double) i);
            }
            LocalDateTime testTime = START.plusHours(12);
            List<Double> result = partition.get(Range.closed(testTime, testTime));
            assertThat(result).isEqualTo(List.of(12.0));
        }
    }

    @Test
    public void testGetRangeContained() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (double) i);
            }
            Double[] expected = new Double[]{4.0, 5.0, 6.0, 7.0, 8.0};
            assertThat(partition.get(Range.closed(START.plusHours(4), START.plusHours(8)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeLowerOverlap() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (double) i);
            }
            Double[] expected = new Double[]{0.0, 1.0, 2.0, 3.0, 4.0};
            assertThat(partition.get(Range.closed(START.minusHours(4), START.plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeUpperOverlap() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (double) i);
            }
            Double[] expected = new Double[]{20.0, 21.0, 22.0, 23.0};
            assertThat(partition.get(Range.closed(START.plus(PERIOD).minusHours(4), START.plus(PERIOD).plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeCompleteOverlap() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
            Double[] expected = new Double[24];
            for (int i = 0; i < 24; i++) {
                partition.set(i, (double) i);
                expected[i] = (double) i;
            }
            assertThat(partition.get(Range.closed(START.minusHours(4), START.plus(PERIOD).plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testSetAndGetIndex() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo(null);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(i, (double) i);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo((double) i);
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
    public void testSetAndGetDateTime() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo(null);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(START.plusHours(i), (double) i);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo((double) i);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(START.plusHours(i), (Double) null);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo(null);
            }
        }
    }

    @Test
    public void testCreateOpenAndClose() throws Exception {
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
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
        try (DoublePartition partition = new DoublePartition(file, START, PERIOD, INTERVAL)) {
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
