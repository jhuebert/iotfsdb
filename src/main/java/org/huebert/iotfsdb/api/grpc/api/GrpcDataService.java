package org.huebert.iotfsdb.api.grpc.api;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.DataServiceGrpc;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.DataServiceProto;
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

    private static final DataServiceMapper MAPPER = Mappers.getMapper(DataServiceMapper.class);

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
    public void findData(DataServiceProto.FindDataRequest request, StreamObserver<DataServiceProto.FindDataResponse> responseObserver) {
        FindDataRequest serviceRequest = MAPPER.fromGrpc(request);
        List<FindDataResponse> serviceResponse = queryService.findData(serviceRequest);
        DataServiceProto.FindDataResponse grpcResponse = MAPPER.toGrpcFindDataResponse(serviceResponse);
        responseObserver.onNext(grpcResponse);
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "data", operation = "stream", javaClass = GrpcDataService.class, javaMethod = "insertData")
    @Override
    public StreamObserver<DataServiceProto.InsertDataRequest> insertData(StreamObserver<DataServiceProto.InsertDataResponse> responseObserver) {
        return new StreamObserver<>() {

            @CaptureStats(group = "grpc", type = "data", operation = "insert", javaClass = GrpcDataService.class, javaMethod = "StreamObserver.onNext")
            @Override
            public void onNext(DataServiceProto.InsertDataRequest value) {
                try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {
                    es.submit(() -> {
                        InsertRequest serviceRequest = MAPPER.fromGrpc(value);
                        insertService.insert(serviceRequest);
                        responseObserver.onNext(DataServiceProto.InsertDataResponse.getDefaultInstance());
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
    public void exportData(DataServiceProto.ExportDataRequest request, StreamObserver<DataServiceProto.ExportDataResponse> responseObserver) {
        String formattedTime = TimeConverter.toUtc(ZonedDateTime.now()).format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        String filename = "iotfsdb-" + formattedTime + ".zip";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        exportService.export(null, outputStream);
        responseObserver.onNext(DataServiceProto.ExportDataResponse.newBuilder()
            .setFile(CommonProto.File.newBuilder()
                .setFilename(filename)
                .setData(ByteString.copyFrom(outputStream.toByteArray()))
                .build())
            .build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "data", operation = "import", javaClass = GrpcDataService.class, javaMethod = "importData")
    @Override
    public void importData(DataServiceProto.ImportDataRequest request, StreamObserver<DataServiceProto.ImportDataResponse> responseObserver) {
        try {
            Path tempFile = Files.createTempFile("iotfsdb-", ".zip");
            try {
                Files.write(tempFile, request.getFile().getData().toByteArray());
                importService.importData(tempFile);
            } finally {
                Files.deleteIfExists(tempFile);
            }
            responseObserver.onNext(DataServiceProto.ImportDataResponse.getDefaultInstance());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        responseObserver.onCompleted();
    }

}
