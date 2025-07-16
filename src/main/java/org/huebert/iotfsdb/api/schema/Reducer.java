package org.huebert.iotfsdb.api.schema;

public enum Reducer {
    AVERAGE,
    COUNT,
    COUNT_DISTINCT,
    FIRST,
    LAST,
    MAXIMUM,
    MEDIAN,
    MINIMUM,
    MODE,
    MULTIPLY,
    SQUARE_SUM,
    SUM;

    public static final String REDUCED_ID = "reduced";

}
