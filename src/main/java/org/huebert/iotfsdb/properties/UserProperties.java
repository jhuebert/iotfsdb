package org.huebert.iotfsdb.properties;

import lombok.Data;
import org.huebert.iotfsdb.security.UserRole;

import java.util.Set;

@Data
public class UserProperties {

    private String username;

    private String password;

    private Set<UserRole> roles;

}
