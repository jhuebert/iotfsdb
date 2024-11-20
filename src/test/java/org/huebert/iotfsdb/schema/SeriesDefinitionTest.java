package org.huebert.iotfsdb.schema;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;

public class SeriesDefinitionTest {

    @Test
    public void testIsTypeValid() {
        EnumSet<NumberType> typesWithRange = EnumSet.of(NumberType.CURVED1, NumberType.CURVED2, NumberType.CURVED4, NumberType.MAPPED1, NumberType.MAPPED2, NumberType.MAPPED4);
        for (NumberType t : NumberType.values()) {
            assertThat(SeriesDefinition.builder().type(t).build().isTypeValid()).isEqualTo(!typesWithRange.contains(t));
            assertThat(SeriesDefinition.builder().type(t).min(5.0).build().isTypeValid()).isFalse();
            assertThat(SeriesDefinition.builder().type(t).max(5.0).build().isTypeValid()).isFalse();
            assertThat(SeriesDefinition.builder().type(t).min(5.0).max(1.0).build().isTypeValid()).isFalse();
            assertThat(SeriesDefinition.builder().type(t).min(1.0).max(5.0).build().isTypeValid()).isEqualTo(typesWithRange.contains(t));
        }
    }

    @Test
    public void testIsIntervalValid() {

        assertThat(SeriesDefinition.builder().interval(1L).partition(PartitionPeriod.DAY).build().isIntervalValid()).isTrue();
        assertThat(SeriesDefinition.builder().interval(Duration.ofDays(1).toMillis()).partition(PartitionPeriod.DAY).build().isIntervalValid()).isTrue();
        assertThat(SeriesDefinition.builder().interval(Duration.ofDays(1).plusMillis(1).toMillis()).partition(PartitionPeriod.DAY).build().isIntervalValid()).isFalse();

        assertThat(SeriesDefinition.builder().interval(1L).partition(PartitionPeriod.MONTH).build().isIntervalValid()).isTrue();
        assertThat(SeriesDefinition.builder().interval(Duration.ofDays(28).toMillis()).partition(PartitionPeriod.MONTH).build().isIntervalValid()).isTrue();
        assertThat(SeriesDefinition.builder().interval(Duration.ofDays(28).plusMillis(1).toMillis()).partition(PartitionPeriod.MONTH).build().isIntervalValid()).isFalse();

        assertThat(SeriesDefinition.builder().interval(1L).partition(PartitionPeriod.YEAR).build().isIntervalValid()).isTrue();
        assertThat(SeriesDefinition.builder().interval(Duration.ofDays(365).toMillis()).partition(PartitionPeriod.YEAR).build().isIntervalValid()).isTrue();
        assertThat(SeriesDefinition.builder().interval(Duration.ofDays(365).plusMillis(1).toMillis()).partition(PartitionPeriod.YEAR).build().isIntervalValid()).isFalse();

    }

    @Test
    public void testGetIntervalDuration() {
        SeriesDefinition definition = SeriesDefinition.builder().interval(1000L).build();
        assertThat(definition.getIntervalDuration()).isEqualTo(Duration.ofMillis(1000));
    }

}
