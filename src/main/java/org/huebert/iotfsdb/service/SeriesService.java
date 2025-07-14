package org.huebert.iotfsdb.service;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@Validated
@Slf4j
@Service
public class SeriesService {

    private final DataService dataService;

    public SeriesService(@NotNull DataService dataService) {
        this.dataService = dataService;
    }

    public void createSeries(@Valid @NotNull SeriesFile seriesFile) {
        dataService.getSeries(seriesFile.getId())
            .ifPresent(_ -> {
                throw new IllegalArgumentException(String.format("series (%s) already exists", seriesFile.getId()));
            });
        dataService.saveSeries(seriesFile);
    }

    public Optional<SeriesFile> findSeries(@NotBlank String seriesId) {
        return dataService.getSeries(seriesId);
    }

    public List<SeriesFile> findSeries(@Valid @NotNull FindSeriesRequest request) {
        return dataService.getSeries().parallelStream()
            .filter(s -> request.getPattern().matcher(s.getId()).matches())
            .filter(s -> matchesMetadata(s, request.getMetadata()))
            .sorted(Comparator.comparing(SeriesFile::getId))
            .toList();
    }

    public void updateMetadata(@NotBlank String seriesId, @NotNull Map<String, String> metadata, boolean merge) {
        SeriesFile seriesFile = dataService.getSeries(seriesId).orElseThrow();
        Map<String, String> updatedMetadata = metadata;
        if (merge) {
            updatedMetadata = new HashMap<>(seriesFile.getMetadata());
            updatedMetadata.putAll(metadata);
        }
        dataService.saveSeries(new SeriesFile(seriesFile.getDefinition(), updatedMetadata));
    }

    public void deleteSeries(@NotBlank String seriesId) {
        dataService.deleteSeries(seriesId);
    }

    private boolean matchesMetadata(@Valid @NotNull SeriesFile series, @NotNull Map<String, Pattern> metadata) {
        if (metadata.isEmpty()) {
            return true;
        }
        Map<String, String> seriesMetadata = series.getMetadata();
        if (metadata.size() > seriesMetadata.size()) {
            return false;
        }
        return metadata.entrySet().stream()
            .allMatch(entry -> {
                String seriesValue = seriesMetadata.get(entry.getKey());
                return seriesValue != null && entry.getValue().matcher(seriesValue).matches();
            });
    }
}
