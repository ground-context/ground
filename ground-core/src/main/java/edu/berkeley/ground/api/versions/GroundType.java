package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import edu.berkeley.ground.exceptions.GroundException;

public enum GroundType {
    STRING (String.class, "string"),
    INTEGER (Integer.class, "integer"),
    BOOLEAN (Boolean.class, "boolean");

    private final Class<?> klass;
    private final String name;

    GroundType(Class<?> klass, String name) {
        this.klass = klass;
        this.name = name;
    }

    public Class<?> getTypeClass() {
        return this.klass;
    }

    @JsonCreator
    public static GroundType fromString(String str) throws GroundException {
        if (str == null) {
            return null;
        }

        switch (str.toLowerCase()) {
            case "string": return STRING;
            case "integer": return INTEGER;
            case "boolean": return BOOLEAN;

            default: {
                throw new GroundException("Invalid type: " + str + ".");
            }
        }
    }

    @JsonValue
    public static Object stringToType(String str, GroundType groundType) {
        if (str == null) {
            return null;
        }

        switch (groundType) {
            case STRING: return str;
            case INTEGER: return Integer.parseInt(str);
            case BOOLEAN: return Boolean.parseBoolean(str);

            default: return null;
        }
    }

    public String toString() {
        return this.name;
    }
}
