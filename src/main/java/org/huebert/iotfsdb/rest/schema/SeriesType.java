package org.huebert.iotfsdb.rest.schema;

import lombok.Getter;

@Getter
public enum SeriesType {

    INTEGER(32),
    FLOAT(32),
    BOOLEAN(1);

    private final int numBits;

    SeriesType(int numBits) {
        this.numBits = numBits;
    }
}
