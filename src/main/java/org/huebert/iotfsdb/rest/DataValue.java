package org.huebert.iotfsdb.rest;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@Builder
public class DataValue {

    @NotNull
    private ZonedDateTime dateTime;

    @NotBlank
    private String value;

}
