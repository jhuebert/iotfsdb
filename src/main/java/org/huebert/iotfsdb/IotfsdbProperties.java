package org.huebert.iotfsdb;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

@Data
@Component
@ConfigurationProperties("iotfsdb")
public class IotfsdbProperties {

    private Path root;

    private boolean readOnly = true;

    private int maxQuerySize = 1000;

    private long maxIdleSeconds = 60;

}
