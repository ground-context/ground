package edu.berkeley.ground.db;

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

public class DbDataContainer {
    // the name of the field
    private String field;

    // the type of the field
    private GroundType groundType;

    // the value of the field;
    private Object value;

    public DbDataContainer(String field, GroundType groundType, Object value) throws GroundException {
        if (value != null && !(value.getClass().equals(groundType.getTypeClass()))) {

            throw new GroundException("Value of type " + value.getClass().toString() + " does not correspond to type of " + groundType.getTypeClass().toString() + ".");
        }

        this.field = field;
        this.groundType = groundType;
        this.value = value;
    }

    public String getField() {
        return this.field;
    }

    public GroundType getGroundType() {
        return this.groundType;
    }

    public Object getValue() {
        return this.value;
    }
}
