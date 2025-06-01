package org.huebert.iotfsdb.properties;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SecurityProperties {

    private boolean enabled = false;

    private List<UserProperties> users = new ArrayList<>();

}
