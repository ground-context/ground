package edu.berkeley.ground.db;

import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.exceptions.GroundException;

public class DbDataContainer {
    // the name of the field
    private String field;

    // the type of the field
    private Type type;

    // the value of the field;
    private Object value;

    public DbDataContainer(String field, Type type, Object value) throws GroundException {
        if (value != null && !(value.getClass().equals(type.getTypeClass()))) {

            throw new GroundException("Value of type " + value.getClass().toString() + " does not correspond to type of " + type.getTypeClass().toString() + ".");
        }

        this.field = field;
        this.type = type;
        this.value = value;
    }

    public String getField() {
        return this.field;
    }

    public Type getType() {
        return this.type;
    }

    public Object getValue() {
        return this.value;
    }
}
