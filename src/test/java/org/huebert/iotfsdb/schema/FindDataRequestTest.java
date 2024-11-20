package org.huebert.iotfsdb.schema;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class FindDataRequestTest {

    @Test
    public void testIsRangeValid() {
        ZonedDateTime to = ZonedDateTime.now();
        ZonedDateTime from = to.minusDays(1);
        FindDataRequest request = new FindDataRequest();

        request.setFrom(null);
        request.setTo(null);
        assertThat(request.isRangeValid()).isFalse();

        request.setFrom(from);
        request.setTo(null);
        assertThat(request.isRangeValid()).isFalse();

        request.setFrom(null);
        request.setTo(to);
        assertThat(request.isRangeValid()).isFalse();

        request.setFrom(to);
        request.setTo(from);
        assertThat(request.isRangeValid()).isFalse();

        request.setFrom(from);
        request.setTo(to);
        assertThat(request.isRangeValid()).isTrue();
    }

}
