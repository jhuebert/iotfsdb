package org.huebert.iotfsdb.rest.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Range;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
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
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Request to archive series data. Only partitions that are entirely enclosed by the input date and time range will be (un)archived.")
public class ArchiveRequest {

    @Schema(description = "Earliest date and time that should be (un)archived")
    @NotNull
    private ZonedDateTime from;

    @Schema(description = "Latest date and time that should be (un)archived")
    @NotNull
    private ZonedDateTime to;

    @JsonIgnore
    public Range<ZonedDateTime> getRange() {
        return Range.closed(from, to);
    }

    @AssertTrue
    private boolean isValid() {
        if ((from == null) || (to == null)) {
            return false;
        }
        if (to.compareTo(from) <= 0) {
            return false;
        }
        return true;
    }

}
