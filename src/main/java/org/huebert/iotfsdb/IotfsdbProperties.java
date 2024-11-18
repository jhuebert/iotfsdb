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

    private int maxQuerySize = 1000;

    private String partitionCache = "expireAfterAccess=5m,maximumSize=10000,softValues";

}
