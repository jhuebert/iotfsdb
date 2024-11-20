package org.huebert.iotfsdb.service;

import jakarta.validation.constraints.NotBlank;
import org.huebert.iotfsdb.schema.PartitionPeriod;

import java.time.LocalDateTime;

public record PartitionKey(
    @NotBlank String seriesId,
    @NotBlank String partitionId
) {

    public static PartitionKey getKey(String seriesId, PartitionPeriod partitionPeriod, LocalDateTime localDateTime) {
        return new PartitionKey(seriesId, partitionPeriod.getFilename(localDateTime));
    }

}
