package org.huebert.iotfsdb.service;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.huebert.iotfsdb.schema.InsertRequest;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.Reducer;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Validated
@Service
public class InsertService {

    private final DataService dataService;

    private final PartitionService partitionService;

    private final ReducerService reducerService;

    public InsertService(@NotNull DataService dataService, @NotNull PartitionService partitionService, @NotNull ReducerService reducerService) {
        this.dataService = dataService;
        this.partitionService = partitionService;
        this.reducerService = reducerService;
    }

    public void insert(@Valid @NotNull InsertRequest request) {

        String seriesId = request.getSeries();
        PartitionPeriod partitionPeriod = dataService.getSeries(seriesId)
            .map(SeriesFile::getDefinition)
            .map(SeriesDefinition::getPartition)
            .orElseThrow();

        Map<PartitionKey, List<SeriesData>> partitionGroups = request.getValues().stream().collect(Collectors.groupingBy(value -> {
            LocalDateTime local = TimeConverter.toUtc(value.getTime());
            return PartitionKey.getKey(seriesId, partitionPeriod, local);
        }));

        Collector<Number, ?, Number> collector;
        if ((request.getReducer() == null) || (request.getReducer() == Reducer.LAST)) {
            collector = null;
        } else {
            collector = reducerService.getCollector(request.getReducer(), false, null);
        }

        partitionGroups.entrySet().parallelStream()
            .forEach(entry -> insertIntoPartition(entry.getKey(), entry.getValue(), collector));
    }

    private void insertIntoPartition(PartitionKey key, List<SeriesData> data, Collector<Number, ?, Number> collector) {
        PartitionRange details = partitionService.getRange(key);
        PartitionAdapter adapter = details.adapter();
        details.withWrite(() -> {
            ByteBuffer buffer = dataService.getBuffer(key, details.getSize(), adapter);
            for (SeriesData value : data) {
                LocalDateTime local = TimeConverter.toUtc(value.getTime());
                int index = details.getIndex(local);
                Number putValue = value.getValue();
                if (collector != null) {
                    putValue = Stream.concat(
                        adapter.getStream(buffer, index, 1),
                        Stream.of(putValue)
                    ).collect(collector);
                }
                adapter.put(buffer, index, putValue);
            }
        });
    }

}
