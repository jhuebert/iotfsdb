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
    public void testGetIntervalDuration() {
        SeriesDefinition definition = SeriesDefinition.builder().interval(1000).build();
        assertThat(definition.getIntervalDuration()).isEqualTo(Duration.ofMillis(1000));
    }

}
