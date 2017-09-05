package edu.berkeley.ground.cassandra.dao.version.mock;

import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.version.CassandraVersionDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
// import play.db.Database;

public class TestCassandraVersionDao extends CassandraVersionDao<Version> {

  public TestCassandraVersionDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
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
