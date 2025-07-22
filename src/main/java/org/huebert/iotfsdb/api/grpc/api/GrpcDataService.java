package org.huebert.iotfsdb.api.grpc.api;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.DataServiceGrpc;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.DataServiceProto;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.ImportService;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.ParallelUtil;
import org.huebert.iotfsdb.service.QueryService;
import org.huebert.iotfsdb.service.TimeConverter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.grpc.server.service.GrpcService;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
@GrpcService
@ConditionalOnExpression("${iotfsdb.api.grpc:true} and not ${iotfsdb.read-only:false}")
public class GrpcDataService extends DataServiceGrpc.DataServiceImplBase {

    private static final ServiceMapper SERVICE_MAPPER = Mappers.getMapper(ServiceMapper.class);

    private static final CommonMapper MAPPER = Mappers.getMapper(CommonMapper.class);

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
        DataServiceProto.FindDataResponse.Builder builder = DataServiceProto.FindDataResponse.newBuilder();
        try {
            builder.addAllData(queryService.findData(SERVICE_MAPPER.fromProto(request, "UTC")).stream()
                .map(SERVICE_MAPPER::toProto)
                .toList());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error finding data", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "data", operation = "insert", javaClass = GrpcDataService.class, javaMethod = "insertData")
    @Override
    public void insertData(DataServiceProto.InsertDataRequest request, StreamObserver<DataServiceProto.InsertDataResponse> responseObserver) {
        DataServiceProto.InsertDataResponse.Builder builder = DataServiceProto.InsertDataResponse.newBuilder();
        try {
            ParallelUtil.forEach(request.getDataList(), data -> insertService.insert(SERVICE_MAPPER.fromProto(data, request.getReducer())));
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error inserting data", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "data", operation = "export", javaClass = GrpcDataService.class, javaMethod = "exportData")
    @Override
    public void exportData(DataServiceProto.ExportDataRequest request, StreamObserver<DataServiceProto.ExportDataResponse> responseObserver) {
        DataServiceProto.ExportDataResponse.Builder builder = DataServiceProto.ExportDataResponse.newBuilder();
        try {
            String formattedTime = TimeConverter.toUtc(ZonedDateTime.now()).format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
            String filename = "iotfsdb-" + formattedTime + ".zip";
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            exportService.export(SERVICE_MAPPER.fromProto(request.getCriteria()), outputStream);
            builder.setFile(CommonProto.File.newBuilder()
                .setFilename(filename)
                .setData(ByteString.copyFrom(outputStream.toByteArray()))
                .build());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error exporting data", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "data", operation = "import", javaClass = GrpcDataService.class, javaMethod = "importData")
    @Override
    public void importData(DataServiceProto.ImportDataRequest request, StreamObserver<DataServiceProto.ImportDataResponse> responseObserver) {
        DataServiceProto.ImportDataResponse.Builder builder = DataServiceProto.ImportDataResponse.newBuilder();
        try {
            Path tempFile = Files.createTempFile("iotfsdb-", ".zip");
            try {
                Files.write(tempFile, request.getFile().getData().toByteArray());
                importService.importData(tempFile);
            } finally {
                Files.deleteIfExists(tempFile);
            }
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error importing data", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "data", operation = "prune", javaClass = GrpcDataService.class, javaMethod = "pruneData")
    @Override
    public void pruneData(DataServiceProto.PruneDataRequest request, StreamObserver<DataServiceProto.PruneDataResponse> responseObserver) {
        // TODO : Implement pruning logic
        super.pruneData(request, responseObserver);
    }

}
