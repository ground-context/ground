package exceptions;

import models.versions.Version;

public class GroundVersionNotFoundException extends GroundException {

  public GroundVersionNotFoundException(Class<? extends Version> versionType, long id) {
    super("Version of type [" + versionType.getName() + "] with id [" + id + "] not found");
  }
}
