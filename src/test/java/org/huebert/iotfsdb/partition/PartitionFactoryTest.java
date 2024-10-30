package org.huebert.iotfsdb.partition;

import org.huebert.iotfsdb.series.NumberType;
import org.huebert.iotfsdb.series.PartitionPeriod;
import org.huebert.iotfsdb.series.Reducer;
import org.huebert.iotfsdb.series.SeriesDefinition;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.time.LocalDateTime;
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

        SeriesDefinition definition = SeriesDefinition.builder()
            .partition(PartitionPeriod.DAY)
            .build();

        definition.setType(NumberType.INTEGER1);
        assertThat(PartitionFactory.create(definition, file.toPath(), LocalDateTime.now()).size()).isEqualTo(40);
        definition.setType(NumberType.FLOAT8);
        assertThat(PartitionFactory.create(definition, file.toPath(), LocalDateTime.now()).size()).isEqualTo(5);
        definition.setType(NumberType.FLOAT4);
        assertThat(PartitionFactory.create(definition, file.toPath(), LocalDateTime.now()).size()).isEqualTo(10);
        definition.setType(NumberType.INTEGER4);
        assertThat(PartitionFactory.create(definition, file.toPath(), LocalDateTime.now()).size()).isEqualTo(10);
        definition.setType(NumberType.INTEGER8);
        assertThat(PartitionFactory.create(definition, file.toPath(), LocalDateTime.now()).size()).isEqualTo(5);
        definition.setType(NumberType.INTEGER2);
        assertThat(PartitionFactory.create(definition, file.toPath(), LocalDateTime.now()).size()).isEqualTo(20);

        file.delete();
    }

    @Test
    public void testAverage() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.AVERAGE, false)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.AVERAGE, false)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.AVERAGE, false)).isEqualTo(Optional.of(2.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.AVERAGE, false)).isEqualTo(Optional.of(4.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.AVERAGE, false)).isEqualTo(Optional.of(2.25));
    }

    @Test
    public void testCount() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.COUNT, false)).isEqualTo(Optional.of((long) 0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.COUNT, false)).isEqualTo(Optional.of((long) 1));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.COUNT, false)).isEqualTo(Optional.of((long) 2));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.COUNT, false)).isEqualTo(Optional.of((long) 3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.COUNT, false)).isEqualTo(Optional.of((long) 4));
    }

    @Test
    public void testCountDistinct() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.COUNT_DISTINCT, false)).isEqualTo(Optional.of((long) 0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.COUNT_DISTINCT, false)).isEqualTo(Optional.of((long) 1));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.COUNT_DISTINCT, false)).isEqualTo(Optional.of((long) 2));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.COUNT_DISTINCT, false)).isEqualTo(Optional.of((long) 3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.COUNT_DISTINCT, false)).isEqualTo(Optional.of((long) 3));
    }

    @Test
    public void testFirst() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.FIRST, false)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.FIRST, false)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.FIRST, false)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.FIRST, false)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.FIRST, false)).isEqualTo(Optional.of(3));
    }

    @Test
    public void testLast() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.LAST, false)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.LAST, false)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.LAST, false)).isEqualTo(Optional.of(1));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.LAST, false)).isEqualTo(Optional.of(8));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.LAST, false)).isEqualTo(Optional.of(4));
    }

    @Test
    public void testMaximum() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.MAXIMUM, false)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.MAXIMUM, false)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.MAXIMUM, false)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.MAXIMUM, false)).isEqualTo(Optional.of(8.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.MAXIMUM, false)).isEqualTo(Optional.of(4.0));
    }

    @Test
    public void testMedian() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.MEDIAN, false)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.MEDIAN, false)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.MEDIAN, false)).isEqualTo(Optional.of(2.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.MEDIAN, false)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.MEDIAN, false)).isEqualTo(Optional.of(2.0));
    }

    @Test
    public void testMinimum() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.MINIMUM, false)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.MINIMUM, false)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.MINIMUM, false)).isEqualTo(Optional.of(1.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.MINIMUM, false)).isEqualTo(Optional.of(1.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.MINIMUM, false)).isEqualTo(Optional.of(1.0));
    }

    @Test
    public void testMode() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.MODE, false)).isEqualTo(Optional.empty());
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.MODE, false)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.MODE, false)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.MODE, false)).isEqualTo(Optional.of(3));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.MODE, false)).isEqualTo(Optional.of(1));
    }

    @Test
    public void testSquareSum() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.SQUARE_SUM, false)).isEqualTo(Optional.of(0.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.SQUARE_SUM, false)).isEqualTo(Optional.of(9.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.SQUARE_SUM, false)).isEqualTo(Optional.of(10.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.SQUARE_SUM, false)).isEqualTo(Optional.of(74.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.SQUARE_SUM, false)).isEqualTo(Optional.of(27.0));
    }

    @Test
    public void testSum() {
        assertThat(PartitionFactory.reduce(Stream.of(), Reducer.SUM, false)).isEqualTo(Optional.of(0.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, null), Reducer.SUM, false)).isEqualTo(Optional.of(3.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, null), Reducer.SUM, false)).isEqualTo(Optional.of(4.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 8, null), Reducer.SUM, false)).isEqualTo(Optional.of(12.0));
        assertThat(PartitionFactory.reduce(Stream.of(null, 3, 1, 1, 4, null), Reducer.SUM, false)).isEqualTo(Optional.of(9.0));
    }

}
