package org.huebert.iotfsdb.properties;

import lombok.Data;

import java.util.List;

@Data
public class SecurityProperties {

    private boolean enabled;

    private List<UserProperties> users;

}
