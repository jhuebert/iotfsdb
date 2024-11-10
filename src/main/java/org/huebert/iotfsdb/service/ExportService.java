package org.huebert.iotfsdb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Validated
@Service
public class ExportService {

    private final DataService dataService;

    private final SeriesService seriesService;

    private final ObjectMapper objectMapper;

    public ExportService(@NotNull DataService dataService, @NotNull SeriesService seriesService, @NotNull ObjectMapper objectMapper) {
        this.dataService = dataService;
        this.seriesService = seriesService;
        this.objectMapper = objectMapper;
    }

    public void export(@NotNull @Valid FindSeriesRequest request, @NotNull OutputStream outputStream) {
        try (ZipOutputStream zos = new ZipOutputStream(outputStream)) {
            zos.setLevel(Deflater.BEST_COMPRESSION);
            WritableByteChannel channel = Channels.newChannel(zos);
            for (SeriesFile seriesFile : seriesService.findSeries(request.getPattern(), request.getMetadata())) {
                zos.putNextEntry(new ZipEntry(seriesFile.getId() + "/series.json"));
                zos.write(objectMapper.writeValueAsString(seriesFile).getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
                for (PartitionKey key : dataService.getPartitions(seriesFile.getId())) {
                    zos.putNextEntry(new ZipEntry(key.seriesId() + "/" + key.partitionId()));
                    channel.write(dataService.getBuffer(key).orElseThrow());
                    zos.closeEntry();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
