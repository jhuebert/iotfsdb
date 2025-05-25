package org.huebert.iotfsdb.mcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.execution.ToolCallResultConverter;
import org.springframework.lang.Nullable;

import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Base64;
import java.util.Map;
import javax.imageio.ImageIO;

@Slf4j
public class JsonConverter implements ToolCallResultConverter {

    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .enable(SerializationFeature.INDENT_OUTPUT)
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Override
    public String convert(@Nullable Object result, @Nullable Type returnType) {
        if (returnType == Void.TYPE) {
            log.debug("The tool has no return type. Converting to conventional response.");
            try {
                return objectMapper.writeValueAsString("Done");
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        if (result instanceof RenderedImage) {
            final var buf = new ByteArrayOutputStream(1024 * 4);
            try {
                ImageIO.write((RenderedImage) result, "PNG", buf);
            } catch (IOException e) {
                return "Failed to convert tool result to a base64 image: " + e.getMessage();
            }
            final var imgB64 = Base64.getEncoder().encodeToString(buf.toByteArray());
            try {
                return objectMapper.writeValueAsString(Map.of("mimeType", "image/png", "data", imgB64));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        } else {
            log.debug("Converting tool result to JSON.");
            try {
                return objectMapper.writeValueAsString(result);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
