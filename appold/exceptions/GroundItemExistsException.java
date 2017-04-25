package exceptions;

import models.versions.Item;

public class GroundItemExistsException extends GroundException {

  public GroundItemExistsException(Class<? extends Item> itemType, String sourceKey) {
    super("Item of type ["+itemType.getName()+"] with sourceKey ["+sourceKey+"] already exists");
  }
}
