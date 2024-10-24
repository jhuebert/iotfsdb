package org.huebert.iotfsdb.rest.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SeriesStats {

    private long regularSize;
    private long regularNumPartitions;

    private long archiveSize;
    private long archiveNumPartitions;

    private long totalSize;
    private long totalNumPartitions;

    private ZonedDateTime from;
    private ZonedDateTime to;

}
