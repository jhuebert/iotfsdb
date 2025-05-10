package org.huebert.iotfsdb.schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SeriesFileTest {

    @Test
    public void testGetId() {
        SeriesDefinition definition = SeriesDefinition.builder().id("123").build();
        assertThat(SeriesFile.builder().definition(definition).build().getId()).isEqualTo("123");
    }

}
