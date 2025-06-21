package org.huebert.iotfsdb.api.grpc.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.proto.TransferServiceGrpc;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.ImportService;
import org.huebert.iotfsdb.service.TimeConverter;
import org.springframework.grpc.server.service.GrpcService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

@GrpcService
public class GrpcTransferService extends TransferServiceGrpc.TransferServiceImplBase {

    private final ExportService exportService;

    private final ImportService importService;

    public GrpcTransferService(ExportService exportService, ImportService importService) {
        this.exportService = exportService;
        this.importService = importService;
    }

    @Override
    public void exportData(IotfsdbServices.ExportDataRequest request, StreamObserver<IotfsdbServices.ExportDataResponse> responseObserver) {
        String formattedTime = TimeConverter.toUtc(ZonedDateTime.now()).format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        String filename = "iotfsdb-" + formattedTime + ".zip";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        exportService.export(null, outputStream);
        responseObserver.onNext(IotfsdbServices.ExportDataResponse.newBuilder()
            .setFilename(StringValue.of(filename))
            .setData(ByteString.copyFrom(outputStream.toByteArray()))
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void importData(IotfsdbServices.ImportDataRequest request, StreamObserver<IotfsdbServices.ImportDataResponse> responseObserver) {
        try {
            Path tempFile = Files.createTempFile("iotfsdb-", ".zip");
            try {
                Files.write(tempFile, request.getData().toByteArray());
                importService.importData(tempFile);
            } finally {
                Files.deleteIfExists(tempFile);
            }
            responseObserver.onNext(IotfsdbServices.ImportDataResponse.getDefaultInstance());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        responseObserver.onCompleted();
    }
}
