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

}
