package edu.berkeley.ground.dao.versions.cassandra.mock;


import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.model.versions.Version;

public class TestCassandraVersionFactory extends CassandraVersionFactory<Version> {

  public TestCassandraVersionFactory(CassandraClient cassandraClient) {
    super(cassandraClient);
  }

  public Class<Version> getType() {
    return Version.class;
  }

  public Version retrieveFromDatabase(long id) {
    return new Version(id);
  }
}

