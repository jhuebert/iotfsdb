package org.huebert.iotfsdb.security;

public enum UserRole {
    API_READ,
    API_WRITE,
    UI_READ,
    UI_WRITE;

    public String getRoleName() {
        return "ROLE_" + name();
    }

}
