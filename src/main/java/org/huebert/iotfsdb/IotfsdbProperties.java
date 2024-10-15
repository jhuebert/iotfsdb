package org.huebert.iotfsdb;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.File;

@Data
@Component
@ConfigurationProperties("iotfsdb")
public class IotfsdbProperties {

    private File root;

    private boolean readOnly = true;

    private int maxQuerySize = 1000;

    private boolean writeSync = true;

    private int closeSeconds = 3600;

}
