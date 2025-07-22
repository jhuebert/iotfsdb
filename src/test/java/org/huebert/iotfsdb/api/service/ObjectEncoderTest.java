package org.huebert.iotfsdb.api.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import com.google.protobuf.StringValue;
import org.huebert.iotfsdb.api.ui.service.ObjectEncoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

@ExtendWith(MockitoExtension.class)
public class ObjectEncoderTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Test
    public void testEncodeDecode() throws Exception {
        ObjectEncoder objectEncoder = new ObjectEncoder(objectMapper);
        String encoded = objectEncoder.encode(Strings.repeat("test-string", 1000));
        assertThat(encoded).isEqualTo("H4sIAAAAAAAA_-3GoQ0AIBAEsF3eMxYhGAR_-4c1EK1qZXZG5-6zVFVVVVVVVVVVVVVVVVVVVVVVVVVVv209B2F7_voqAAA=");
        assertThat(objectEncoder.decode(encoded, String.class)).isEqualTo(Strings.repeat("test-string", 1000));
    }

    @Test
    public void testProto() throws Exception {
        StringValue pb = StringValue.of(Strings.repeat("test-string", 1000));
//        UiServiceProto.UiFindDataRequest pb = UiServiceProto.UiFindDataRequest.newBuilder()
//                .setTimezone("America/Chicago")
//                .setRequest(DataServiceProto.FindDataRequest.newBuilder()
//                        .setCriteria(CommonProto.SeriesCriteria.newBuilder()
//                                .putMetadata("location", "somewhere")
//                                .putMetadata("measurement", "temperature")
//                                .build())
//                        .setSize(CommonProto.Size.newBuilder()
//                                .setSize(1000)
//                                .setInterval(Duration.newBuilder()
//                                        .setSeconds(60)
//                                        .build())
//                                .build())
//                        .setNullHandler(CommonProto.NullHandler.newBuilder()
//                                .setNullValue(0.0)
//                                .build())
//                        .setTimeRange(CommonProto.TimeRange.newBuilder()
//                                .setStart(CommonProto.Time.newBuilder()
//                                        .setRelativeTime(Duration.newBuilder()
//                                                .setSeconds(0)
//                                                .build())
//                                        .build())
//                                .setEnd(CommonProto.Time.newBuilder()
//                                        .setRelativeTime(Duration.newBuilder()
//                                                .setSeconds(0)
//                                                .build())
//                                        .build())
//                                .build())
//                        .setSeriesReducer(CommonProto.Reducer.REDUCER_AVERAGE)
//                        .setTimeReducer(CommonProto.Reducer.REDUCER_SUM)
//                        .build())
//                .build();

        ObjectEncoder objectEncoder = new ObjectEncoder(objectMapper);
        System.out.println(objectEncoder.encode(pb));
        Optional<StringValue> decoded = objectEncoder.decode(objectEncoder.encode(pb), StringValue::parseFrom);
        System.out.println(decoded.get().getValue());
    }

}
