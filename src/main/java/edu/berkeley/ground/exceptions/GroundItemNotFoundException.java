package edu.berkeley.ground.exceptions;

import edu.berkeley.ground.model.versions.Item;

public class GroundItemNotFoundException extends GroundException {

  public GroundItemNotFoundException(Class<? extends Item> itemType, String sourceKey) {
    super("Item of type ["+itemType.getName()+"] with sourceKey ["+sourceKey+"] not found");
  }

  public GroundItemNotFoundException(Class<? extends Item> itemType, String field, Object value) {
    super("Item of type [" + itemType.getName() + "]" +
        " with field [" + field + "] having value [" + value.toString() + "] not found");
  }
}
