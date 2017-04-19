package db;

import exceptions.GroundDbException;
import models.versions.GroundType;

public interface DbRow {
  String getString(String field) throws GroundDbException;
  int getInt(String field) throws GroundDbException;
  boolean getBoolean(String field) throws GroundDbException;
  long getLong(String field) throws GroundDbException;
  boolean isNull(String field) throws GroundDbException;

  default Object getValue(GroundType type, String field)
      throws GroundDbException {

    if (type == null) {
      return null;
    }

    switch (type) {
      case STRING:
        return this.getString(field);
      case INTEGER:
        return this.getInt(field);
      case LONG:
        return this.getLong(field);
      case BOOLEAN:
        return this.getBoolean(field);
      default:
        // this should never happen because we've listed all types
        throw new GroundDbException("Unidentified type: " + type);
    }
  }
}
