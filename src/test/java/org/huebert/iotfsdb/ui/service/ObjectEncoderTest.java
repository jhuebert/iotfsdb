package org.huebert.iotfsdb.ui.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

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

}
