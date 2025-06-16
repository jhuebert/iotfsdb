package org.huebert.iotfsdb.api.grpc.service;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoServicesMapper;
import org.huebert.iotfsdb.api.proto.DataServiceGrpc;
import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.QueryService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.grpc.server.service.GrpcService;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@GrpcService
public class GrpcDataService extends DataServiceGrpc.DataServiceImplBase {

    private static final ProtoServicesMapper MAPPER = Mappers.getMapper(ProtoServicesMapper.class);

    private final InsertService insertService;

    private final QueryService queryService;

    public GrpcDataService(InsertService insertService, QueryService queryService) {
        this.insertService = insertService;
        this.queryService = queryService;
    }

    @CaptureStats(
        id = "grpc-data-find",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "grpc"),
            @CaptureStats.Metadata(key = "type", value = "data"),
            @CaptureStats.Metadata(key = "operation", value = "find"),
        }
    )
    @Override
    public void findData(IotfsdbServices.FindDataRequest request, StreamObserver<IotfsdbServices.FindDataResponse> responseObserver) {
        FindDataRequest serviceRequest = MAPPER.fromGrpc(request);
        List<FindDataResponse> serviceResponse = queryService.findData(serviceRequest);
        IotfsdbServices.FindDataResponse grpcResponse = MAPPER.toGrpcFindDataResponse(serviceResponse);
        responseObserver.onNext(grpcResponse);
        responseObserver.onCompleted();
    }

    @CaptureStats(
        id = "grpc-data-insert-stream",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "grpc"),
            @CaptureStats.Metadata(key = "type", value = "data"),
            @CaptureStats.Metadata(key = "operation", value = "insert"),
        }
    )
    @Override
    public StreamObserver<IotfsdbServices.InsertDataRequest> insertData(StreamObserver<IotfsdbServices.InsertDataResponse> responseObserver) {
        return new StreamObserver<>() {

            @CaptureStats(
                id = "grpc-data-insert",
                metadata = {
                    @CaptureStats.Metadata(key = "group", value = "grpc"),
                    @CaptureStats.Metadata(key = "type", value = "data"),
                    @CaptureStats.Metadata(key = "operation", value = "insert"),
                }
            )
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
}
