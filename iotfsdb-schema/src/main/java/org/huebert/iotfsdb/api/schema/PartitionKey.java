package org.huebert.iotfsdb.api.schema;

import jakarta.validation.constraints.NotBlank;

import java.time.LocalDateTime;

public record PartitionKey(
    @NotBlank String seriesId,
    @NotBlank String partitionId
) {

    public static PartitionKey getKey(String seriesId, PartitionPeriod partitionPeriod, LocalDateTime localDateTime) {
        return new PartitionKey(seriesId, partitionPeriod.getFilename(localDateTime));
    }

}
