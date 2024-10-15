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

public class BytePartitionTest {

    private static final LocalDateTime START = LocalDateTime.parse("2024-10-10T00:00:00");

    private static final Period PERIOD = Period.ofDays(1);

    private static final Duration INTERVAL = Duration.of(1, ChronoUnit.HOURS);

    private File file;

    @BeforeEach
    public void beforeEach() throws IOException {
        file = File.createTempFile(BytePartitionTest.class.getSimpleName(), "");
        file.deleteOnExit();
        file.delete();
    }

    @AfterEach
    public void afterEach() {
        file.delete();
    }

    @Test
    public void testCreate() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            assertThat(partition.size()).isEqualTo(24);
            assertThat(file.exists()).isEqualTo(true);
            assertThat(file.canRead()).isEqualTo(true);
            assertThat(file.canWrite()).isEqualTo(true);
            assertThat(file.isFile()).isEqualTo(true);
            assertThat(file.length()).isEqualTo(24);
            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo(null);
            }
        }
    }

    @Test
    public void testReadEmpty() throws Exception {
        file.createNewFile();
        assertThrows(IllegalArgumentException.class, () -> new BytePartition(file, START, PERIOD, INTERVAL));
    }

    @Test
    public void testRead() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(40);
        raf.close();
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 40; i++) {
                assertThat(partition.get(i)).isEqualTo((byte) 0);
            }
            partition.set(0, (byte) 100);
            assertThat(partition.get(0)).isEqualTo((byte) 100);
        }
    }

    @Test
    public void testGetRangeEmpty() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            LocalDateTime testTime = START.plusHours(12);
            List<Byte> result = partition.get(Range.closedOpen(testTime, testTime));
            assertThat(result.size()).isEqualTo(0);
        }
    }

    @Test
    public void testGetRangeAllExact() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            Byte[] expected = new Byte[24];
            for (int i = 0; i < 24; i++) {
                partition.set(i, (byte) i);
                expected[i] = (byte) i;
            }
            assertThat(partition.get(Range.closed(START, START.plus(PERIOD).minusNanos(1)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeSingle() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (byte) i);
            }
            LocalDateTime testTime = START.plusHours(12);
            List<Byte> result = partition.get(Range.closed(testTime, testTime));
            assertThat(result).isEqualTo(List.of((byte) 12));
        }
    }

    @Test
    public void testGetRangeContained() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (byte) i);
            }
            Byte[] expected = new Byte[]{4, 5, 6, 7, 8};
            assertThat(partition.get(Range.closed(START.plusHours(4), START.plusHours(8)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeLowerOverlap() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (byte) i);
            }
            Byte[] expected = new Byte[]{0, 1, 2, 3, 4};
            assertThat(partition.get(Range.closed(START.minusHours(4), START.plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeUpperOverlap() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            for (int i = 0; i < 24; i++) {
                partition.set(i, (byte) i);
            }
            Byte[] expected = new Byte[]{20, 21, 22, 23};
            assertThat(partition.get(Range.closed(START.plus(PERIOD).minusHours(4), START.plus(PERIOD).plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testGetRangeCompleteOverlap() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            Byte[] expected = new Byte[24];
            for (int i = 0; i < 24; i++) {
                partition.set(i, (byte) i);
                expected[i] = (byte) i;
            }
            assertThat(partition.get(Range.closed(START.minusHours(4), START.plus(PERIOD).plusHours(4)))).isEqualTo(Arrays.asList(expected));
        }
    }

    @Test
    public void testSetAndGetIndex() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo(null);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(i, (byte) i);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(i)).isEqualTo((byte) i);
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
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo(null);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(START.plusHours(i), (byte) i);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo((byte) i);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(START.plusHours(i), (Byte) null);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo(null);
            }
        }
    }

    @Test
    public void testSetAndGetDateTimeText() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo(null);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(START.plusHours(i), String.valueOf((byte) i));
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo((byte) i);
            }

            for (int i = 0; i < 24; i++) {
                partition.set(START.plusHours(i), (String) null);
            }

            for (int i = 0; i < 24; i++) {
                assertThat(partition.get(START.plusHours(i))).isEqualTo(null);
            }
        }
    }

    @Test
    public void testCreateOpenAndClose() throws Exception {
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
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
        try (BytePartition partition = new BytePartition(file, START, PERIOD, INTERVAL)) {
            assertThat(partition.isOpen()).isEqualTo(false);

            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            partition.close();
            assertThat(partition.isOpen()).isEqualTo(false);
            assertThat(partition.get(0)).isEqualTo((byte) 0);
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
