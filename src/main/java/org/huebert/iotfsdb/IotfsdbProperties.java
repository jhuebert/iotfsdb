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

    //TODO Max records that can be returned from API
    private int maxValuesPerSeries = 10000;

}
