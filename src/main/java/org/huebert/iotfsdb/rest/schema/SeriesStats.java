package org.huebert.iotfsdb.rest.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
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
@Schema(description = "Statistics for the data contained in a series")
public class SeriesStats {

    @Schema(description = "Total number of series")
    private long numSeries;

    @Schema(description = "Size on disk of uncompressed partitions")
    private long regularSize;

    @Schema(description = "Number of uncompressed partitions")
    private long regularNumPartitions;

    @Schema(description = "Size on disk of the archived partitions")
    private long archiveSize;

    @Schema(description = "Number of archived partitions")
    private long archiveNumPartitions;

    @Schema(description = "Combined size on disk of all partitions")
    private long totalSize;

    @Schema(description = "Total number of partitions")
    private long totalNumPartitions;

    @Schema(description = "Earliest date of any partition")
    private ZonedDateTime from;

    @Schema(description = "Latest date of any partition")
    private ZonedDateTime to;

}
