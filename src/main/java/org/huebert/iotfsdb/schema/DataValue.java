package org.huebert.iotfsdb.schema;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DataValue {
    private LocalDateTime dateTime;
    private String value;
}
