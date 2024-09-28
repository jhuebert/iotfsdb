package org.huebert.iotfsdb.schema;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DataValue {
    private String seriesId;
    private LocalDateTime ts;
    private Float floatValue;
    private Integer integerValue;
    private Boolean booleanValue;
}
