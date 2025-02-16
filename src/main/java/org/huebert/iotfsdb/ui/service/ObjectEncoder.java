package org.huebert.iotfsdb.ui.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Service
public class ObjectEncoder {

    private final ObjectMapper objectMapper;

    public ObjectEncoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String encode(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (CompressedStream zos = new CompressedStream(baos)) {
            objectMapper.writeValue(zos, object);
        }
        return Base64.getUrlEncoder().encodeToString(baos.toByteArray());
    }

    public <T> T decode(String payload, Class<T> type) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getUrlDecoder().decode(payload));
        try (GZIPInputStream zis = new GZIPInputStream(bais)) {
            return objectMapper.readValue(zis, type);
        }
    }

    private static class CompressedStream extends GZIPOutputStream {

        public CompressedStream(OutputStream out) throws IOException {
            super(out);
            def.setLevel(Deflater.BEST_COMPRESSION);
        }

    }

}
