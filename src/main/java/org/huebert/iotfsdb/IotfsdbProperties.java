package org.huebert.iotfsdb;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

@Data
@Component
@ConfigurationProperties("iotfsdb")
public class IotfsdbProperties {

    private Path root = Path.of("memory");

    private boolean readOnly = false;

    private boolean stats = false;

    private int maxQuerySize = 1000;

    private String partitionCache = "expireAfterAccess=5m,maximumSize=10000,softValues";

    private ApiProperties api = new ApiProperties();

    @Data
    public static class ApiProperties {

        //TODO Need to use these properties in code
        private boolean rest = true;

        private boolean ui = false;

        private boolean grpc = false;

        private boolean internalGrpc = false;

        private AiProperties ai = new AiProperties();

    }

    @Data
    public static class AiProperties {
        private boolean chat = false;
        private boolean mcp = false;
    }

}
