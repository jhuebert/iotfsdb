package org.huebert.iotfsdb.api.ui.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.Optional;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Slf4j
@Service
public class ObjectEncoder {

    private final ObjectMapper objectMapper;

    public ObjectEncoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String encode(Object object) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CompressedStream zos = new CompressedStream(baos)) {
            objectMapper.writeValue(zos, object);
            return Base64.getUrlEncoder().encodeToString(baos.toByteArray());
        }
    }

    public String encode(Message message) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] messageBytes = message.toByteArray();
        try (CompressedStream zos = new CompressedStream(baos)) {
            zos.write(messageBytes);
        }
        if (baos.size() > messageBytes.length) {
            return Base64.getUrlEncoder().encodeToString(messageBytes);
        }
        return Base64.getUrlEncoder().encodeToString(baos.toByteArray());
    }

    public <T> Optional<T> decode(String payload, Class<T> type) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getUrlDecoder().decode(payload));
            GZIPInputStream zis = new GZIPInputStream(bais)) {
            return Optional.of(objectMapper.readValue(zis, type));
        } catch (IOException e) {
            log.debug("Failed to decode compressed JSON payload: {}, payload = {}", e.getMessage(), payload);
            return Optional.empty();
        }
    }

    public <T> Optional<T> decode(String payload, Parser<T> parser) {
        byte[] payloadBytes = Base64.getUrlDecoder().decode(payload);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(payloadBytes);
            GZIPInputStream zis = new GZIPInputStream(bais)) {
            return Optional.of(parser.parse(zis.readAllBytes()));
        } catch (IOException e1) {
            log.debug("Failed to decode payload with compression: {}, payload = {}", e1.getMessage(), payload);
            try {
                return Optional.of(parser.parse(payloadBytes));
            } catch (IOException e2) {
                log.debug("Failed to decode payload without compression: {}, payload = {}", e2.getMessage(), payload);
                return Optional.empty();
            }
        }
    }

    public static class CompressedStream extends GZIPOutputStream {

        public CompressedStream(OutputStream out) throws IOException {
            super(out);
            def.setLevel(Deflater.BEST_COMPRESSION);
        }

    }

    public interface Parser<T> {

        T parse(byte[] bytes) throws IOException;

    }

}
