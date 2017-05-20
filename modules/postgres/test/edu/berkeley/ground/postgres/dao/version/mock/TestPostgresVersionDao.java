package edu.berkeley.ground.postgres.dao.version.mock;

import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.PostgresVersionDao;
import play.db.Database;

public class TestPostgresVersionDao extends PostgresVersionDao<Version> {

  public TestPostgresVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  public Class<Version> getType() {
    return Version.class;
  }

  @Override
  public Version retrieveFromDatabase(long id) {
    return new Version(id);
  }
}
