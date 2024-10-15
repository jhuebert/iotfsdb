package org.huebert.iotfsdb.partition;

import org.huebert.iotfsdb.series.Aggregation;
import org.huebert.iotfsdb.series.SeriesType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionFactoryTest {

    @Test
    public void testCreate() throws Exception {

        File file = File.createTempFile(PartitionFactoryTest.class.getSimpleName(), "");
        file.deleteOnExit();

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(40);
        raf.close();

        assertThat(PartitionFactory.create(SeriesType.BYTE, file, LocalDateTime.now(), Period.ofDays(1), null).size()).isEqualTo(40);
        assertThat(PartitionFactory.create(SeriesType.DOUBLE, file, LocalDateTime.now(), Period.ofDays(1), null).size()).isEqualTo(5);
        assertThat(PartitionFactory.create(SeriesType.FLOAT, file, LocalDateTime.now(), Period.ofDays(1), null).size()).isEqualTo(10);
        assertThat(PartitionFactory.create(SeriesType.INTEGER, file, LocalDateTime.now(), Period.ofDays(1), null).size()).isEqualTo(10);
        assertThat(PartitionFactory.create(SeriesType.LONG, file, LocalDateTime.now(), Period.ofDays(1), null).size()).isEqualTo(5);
        assertThat(PartitionFactory.create(SeriesType.SHORT, file, LocalDateTime.now(), Period.ofDays(1), null).size()).isEqualTo(20);

        file.delete();
    }

    @Test
    public void testAverage() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.AVERAGE)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.AVERAGE)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.AVERAGE)).isEqualTo(Optional.of(2.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.AVERAGE)).isEqualTo(Optional.of(4.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.AVERAGE)).isEqualTo(Optional.of(2.25));
    }

    @Test
    public void testCount() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.COUNT)).isEqualTo(Optional.of((long) 0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.COUNT)).isEqualTo(Optional.of((long) 1));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.COUNT)).isEqualTo(Optional.of((long) 2));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.COUNT)).isEqualTo(Optional.of((long) 3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.COUNT)).isEqualTo(Optional.of((long) 4));
    }

    @Test
    public void testCountDistinct() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.COUNT_DISTINCT)).isEqualTo(Optional.of((long) 0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.COUNT_DISTINCT)).isEqualTo(Optional.of((long) 1));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.COUNT_DISTINCT)).isEqualTo(Optional.of((long) 2));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.COUNT_DISTINCT)).isEqualTo(Optional.of((long) 3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.COUNT_DISTINCT)).isEqualTo(Optional.of((long) 3));
    }

    @Test
    public void testFirst() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.FIRST)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.FIRST)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.FIRST)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.FIRST)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.FIRST)).isEqualTo(Optional.of(3));
    }

    @Test
    public void testLast() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.LAST)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.LAST)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.LAST)).isEqualTo(Optional.of(1));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.LAST)).isEqualTo(Optional.of(8));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.LAST)).isEqualTo(Optional.of(4));
    }

    @Test
    public void testMaximum() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.MAXIMUM)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.MAXIMUM)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.MAXIMUM)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.MAXIMUM)).isEqualTo(Optional.of(8.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.MAXIMUM)).isEqualTo(Optional.of(4.0));
    }

    @Test
    public void testMedian() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.MEDIAN)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.MEDIAN)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.MEDIAN)).isEqualTo(Optional.of(2.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.MEDIAN)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.MEDIAN)).isEqualTo(Optional.of(2.0));
    }

    @Test
    public void testMinimum() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.MINIMUM)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.MINIMUM)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.MINIMUM)).isEqualTo(Optional.of(1.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.MINIMUM)).isEqualTo(Optional.of(1.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.MINIMUM)).isEqualTo(Optional.of(1.0));
    }

    @Test
    public void testMode() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.MODE)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.MODE)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.MODE)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.MODE)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.MODE)).isEqualTo(Optional.of(1));
    }

    @Test
    public void testSquareSum() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.SQUARE_SUM)).isEqualTo(Optional.of(0.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.SQUARE_SUM)).isEqualTo(Optional.of(9.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.SQUARE_SUM)).isEqualTo(Optional.of(10.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.SQUARE_SUM)).isEqualTo(Optional.of(74.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.SQUARE_SUM)).isEqualTo(Optional.of(27.0));
    }

    @Test
    public void testSum() {
        assertThat(PartitionFactory.aggregate(Stream.of(), Aggregation.SUM)).isEqualTo(Optional.of(0.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, null), Aggregation.SUM)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, null), Aggregation.SUM)).isEqualTo(Optional.of(4.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 8, null), Aggregation.SUM)).isEqualTo(Optional.of(12.0));
        assertThat(PartitionFactory.aggregate(Stream.of(null, 3, 1, 1, 4, null), Aggregation.SUM)).isEqualTo(Optional.of(9.0));
    }

}
