package org.huebert.iotfsdb.api.grpc.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoServicesMapper;
import org.huebert.iotfsdb.api.proto.DataServiceGrpc;
import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.proto.IotfsdbTypes;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.ImportService;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.QueryService;
import org.huebert.iotfsdb.service.TimeConverter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.grpc.server.service.GrpcService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@GrpcService
public class GrpcDataService extends DataServiceGrpc.DataServiceImplBase {

    private static final ProtoServicesMapper MAPPER = Mappers.getMapper(ProtoServicesMapper.class);

    private final InsertService insertService;

    private final QueryService queryService;

    private final ExportService exportService;

    private final ImportService importService;

    public GrpcDataService(InsertService insertService, QueryService queryService, ExportService exportService, ImportService importService) {
        this.insertService = insertService;
        this.queryService = queryService;
        this.exportService = exportService;
        this.importService = importService;
    }

    @CaptureStats(group = "grpc", type = "data", operation = "find", javaClass = GrpcDataService.class, javaMethod = "findData")
    @Override
    public void findData(IotfsdbServices.FindDataRequest request, StreamObserver<IotfsdbServices.FindDataResponse> responseObserver) {
        FindDataRequest serviceRequest = MAPPER.fromGrpc(request);
        List<FindDataResponse> serviceResponse = queryService.findData(serviceRequest);
        IotfsdbServices.FindDataResponse grpcResponse = MAPPER.toGrpcFindDataResponse(serviceResponse);
        responseObserver.onNext(grpcResponse);
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "data", operation = "stream", javaClass = GrpcDataService.class, javaMethod = "insertData")
    @Override
    public StreamObserver<IotfsdbServices.InsertDataRequest> insertData(StreamObserver<IotfsdbServices.InsertDataResponse> responseObserver) {
        return new StreamObserver<>() {

            @CaptureStats(group = "grpc", type = "data", operation = "insert", javaClass = GrpcDataService.class, javaMethod = "StreamObserver.onNext")
            @Override
            public void onNext(IotfsdbServices.InsertDataRequest value) {
                try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {
                    es.submit(() -> {
                        InsertRequest serviceRequest = MAPPER.fromGrpc(value);
                        insertService.insert(serviceRequest);
                        responseObserver.onNext(IotfsdbServices.InsertDataResponse.getDefaultInstance());
                    });
                } catch (Exception e) {
                    responseObserver.onError(new RuntimeException("Error during parallel processing", e));
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }


    @CaptureStats(group = "grpc", type = "data", operation = "export", javaClass = GrpcDataService.class, javaMethod = "exportData")
    @Override
    public void exportData(IotfsdbServices.ExportDataRequest request, StreamObserver<IotfsdbServices.ExportDataResponse> responseObserver) {
        String formattedTime = TimeConverter.toUtc(ZonedDateTime.now()).format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        String filename = "iotfsdb-" + formattedTime + ".zip";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        exportService.export(null, outputStream);
        responseObserver.onNext(IotfsdbServices.ExportDataResponse.newBuilder()
            .setFile(IotfsdbTypes.File.newBuilder()
                .setFilename(StringValue.of(filename))
                .setData(ByteString.copyFrom(outputStream.toByteArray()))
                .build())
            .build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "data", operation = "import", javaClass = GrpcDataService.class, javaMethod = "importData")
    @Override
    public void importData(IotfsdbServices.ImportDataRequest request, StreamObserver<IotfsdbServices.ImportDataResponse> responseObserver) {
        try {
            Path tempFile = Files.createTempFile("iotfsdb-", ".zip");
            try {
                Files.write(tempFile, request.getFile().getData().toByteArray());
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
