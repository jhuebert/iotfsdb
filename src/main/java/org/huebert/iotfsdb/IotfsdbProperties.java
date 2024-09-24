package org.huebert.iotfsdb;

import lombok.Data;

import java.io.File;

@Data
public class IotfsdbProperties {

    private File root;

    private Boolean readOnly;
}
