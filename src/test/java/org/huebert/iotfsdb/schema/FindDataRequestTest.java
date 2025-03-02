package org.huebert.iotfsdb.schema;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

public class FindDataRequestTest {

    @Test
    public void testIsRangeValid() {
        ZonedDateTime to = ZonedDateTime.now();
        ZonedDateTime from = to.minusDays(1);
        FindDataRequest request = new FindDataRequest();

        request.setDateTimePreset(null);

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

        request.setDateTimePreset(DateTimePreset.NONE);

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

        request.setDateTimePreset(DateTimePreset.LAST_24_HOURS);

        request.setFrom(null);
        request.setTo(null);
        assertThat(request.isRangeValid()).isTrue();

        request.setFrom(from);
        request.setTo(null);
        assertThat(request.isRangeValid()).isTrue();

        request.setFrom(null);
        request.setTo(to);
        assertThat(request.isRangeValid()).isTrue();

        request.setFrom(to);
        request.setTo(from);
        assertThat(request.isRangeValid()).isTrue();

        request.setFrom(from);
        request.setTo(to);
        assertThat(request.isRangeValid()).isTrue();
    }

    @Test
    public void testGetRange() {
        ZonedDateTime to = ZonedDateTime.now();
        ZonedDateTime from = to.minusDays(1);
        FindDataRequest request = new FindDataRequest();
        request.setFrom(from);
        request.setTo(to);

        request.setDateTimePreset(null);
        assertThat(request.getRange()).isEqualTo(Range.closed(from, to));

        request.setDateTimePreset(DateTimePreset.NONE);
        assertThat(request.getRange()).isEqualTo(Range.closed(from, to));

        request.setDateTimePreset(DateTimePreset.LAST_1_HOUR);
        Range<ZonedDateTime> result = request.getRange();
        assertThat(Duration.between(result.lowerEndpoint(), result.upperEndpoint())).isEqualTo(Duration.ofHours(1));
    }

    @Test
    public void testGetRange_WithTimezone() {
        TimeZone timezone1 = TimeZone.getTimeZone("Pacific/Honolulu");
        TimeZone timezone2 = TimeZone.getTimeZone("America/Chicago");
        ZonedDateTime to = ZonedDateTime.now().withZoneSameInstant(timezone1.toZoneId());
        ZonedDateTime from = to.minusDays(1).withZoneSameInstant(timezone1.toZoneId());
        FindDataRequest request = new FindDataRequest();
        request.setTimezone(timezone1);
        request.setFrom(from.withZoneSameInstant(timezone2.toZoneId()));
        request.setTo(to.withZoneSameInstant(timezone2.toZoneId()));

        request.setDateTimePreset(null);
        assertThat(request.getRange()).isEqualTo(Range.closed(from, to));

        request.setDateTimePreset(DateTimePreset.NONE);
        assertThat(request.getRange()).isEqualTo(Range.closed(from, to));

        request.setDateTimePreset(DateTimePreset.LAST_1_HOUR);
        Range<ZonedDateTime> result = request.getRange();
        assertThat(Duration.between(result.lowerEndpoint(), result.upperEndpoint())).isEqualTo(Duration.ofHours(1));
    }

}
