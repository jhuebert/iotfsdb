package org.huebert.iotfsdb.service;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@Validated
@Slf4j
@Service
public class CloneService {

    private final DataService dataService;

    private final PartitionService partitionService;

    private final SeriesService seriesService;

    private final InsertService insertService;

    public CloneService(DataService dataService, PartitionService partitionService, SeriesService seriesService, InsertService insertService) {
        this.dataService = dataService;
        this.partitionService = partitionService;
        this.seriesService = seriesService;
        this.insertService = insertService;
    }

    public void updateDefinition(@NotBlank String id, @NotNull @Valid SeriesDefinition definition) {
        log.info("Updating series for ID: {}", id);

        SeriesFile initial = seriesService.findSeries(id).orElseThrow();
        if (initial.getDefinition().equals(definition)) {
            return;
        }

        SeriesFile updated = SeriesFile.builder()
            .definition(definition)
            .metadata(new HashMap<>(initial.getMetadata()))
            .build();

        boolean idChanged = !id.equals(definition.getId());

        String sourceId = id;
        if (!idChanged) {
            // If the ID is staying the same, we need to move the series to a new ID first
            sourceId = UUID.randomUUID().toString();
            cloneSeries(id, sourceId, true);
            seriesService.deleteSeries(id);
        }

        seriesService.createSeries(updated);

        dataService.getPartitions(sourceId).stream()
            .map(this::convertPartition)
            .filter(values -> !values.isEmpty())
            .map(values -> new InsertRequest(updated.getId(), values, Reducer.FIRST))
            .forEach(insertService::insert);

        seriesService.deleteSeries(sourceId);

    }

    private List<SeriesData> convertPartition(PartitionKey partition) {
        PartitionRange range = partitionService.getRange(partition);
        List<SeriesData> result = new ArrayList<>();
        PartitionByteBuffer buffer = dataService.getBuffer(partition).orElseThrow();
        ZonedDateTime current = TimeConverter.toUtc(range.getRange().lowerEndpoint());
        for (Number number : range.getStream(buffer)) {
            result.add(new SeriesData(current, number));
            current = current.plus(range.getInterval());
        }
        return result;
    }

    public void cloneSeries(@NotBlank String sourceId, @NotBlank String destinationId, boolean includeData) {
        log.debug("Cloning series from {} to {}", sourceId, destinationId);
        seriesService.createSeries(cloneSeriesFile(sourceId, destinationId));
        if (includeData) {
            for (PartitionKey sourceKey : dataService.getPartitions(sourceId)) {
                clonePartition(sourceKey, destinationId);
            }
        }
    }

    private SeriesFile cloneSeriesFile(String sourceId, String destinationId) {
        SeriesFile sourceSeries = seriesService.findSeries(sourceId).orElseThrow();
        return SeriesFile.builder()
            .definition(sourceSeries.getDefinition().toBuilder()
                .id(destinationId)
                .build())
            .metadata(new HashMap<>(sourceSeries.getMetadata()))
            .build();
    }

    private void clonePartition(PartitionKey sourceKey, String destinationId) {
        PartitionKey destinationKey = new PartitionKey(destinationId, sourceKey.partitionId());
        PartitionRange sourceRange = partitionService.getRange(sourceKey);
        PartitionByteBuffer sourceBuffer = dataService.getBuffer(sourceKey).orElseThrow();
        PartitionByteBuffer destinationBuffer = dataService.getBuffer(destinationKey, sourceRange.getSize(), sourceRange.getAdapter());
        destinationBuffer.withWrite(db -> sourceBuffer.withRead(db::put));
    }

}
