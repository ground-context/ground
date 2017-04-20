package dao.versions.cassandra.mock;


import dao.versions.cassandra.CassandraVersionFactory;
import db.CassandraClient;
import models.versions.Version;

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

