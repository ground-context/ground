package edu.berkeley.ground.postgres.dao.versions.mock;

import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.VersionDao;
import play.db.Database;

public class TestVersionDao extends VersionDao<Version> {

  public TestVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  public Class<Version> getType() {
    return Version.class;
  }

  public Version retrieveFromDatabase(long id) {
    return new Version(id);
  }
}
