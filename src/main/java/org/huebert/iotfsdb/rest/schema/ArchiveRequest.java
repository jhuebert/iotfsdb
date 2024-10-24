package org.huebert.iotfsdb.rest.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Range;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ArchiveRequest {

    @NotNull
    private ZonedDateTime from;

    @NotNull
    private ZonedDateTime to;

    @JsonIgnore
    public Range<ZonedDateTime> getRange() {
        return Range.closed(from, to);
    }

}
