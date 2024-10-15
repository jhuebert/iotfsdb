package org.huebert.iotfsdb.rest;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataValueTest {

    @Test
    public void testCheckValid() {
        DataValue.checkValid(new DataValue(ZonedDateTime.now(), "1"));
        DataValue.checkValid(new DataValue(ZonedDateTime.now(), null));
        assertThrows(IllegalArgumentException.class, () -> DataValue.checkValid(null));
        assertThrows(IllegalArgumentException.class, () -> DataValue.checkValid(new DataValue(null, "1")));
    }

}
