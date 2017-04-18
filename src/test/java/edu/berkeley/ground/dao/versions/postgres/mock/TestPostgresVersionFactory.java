package edu.berkeley.ground.dao.versions.postgres.mock;

import edu.berkeley.ground.dao.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.model.versions.Version;

public class TestPostgresVersionFactory extends PostgresVersionFactory<Version> {

  public TestPostgresVersionFactory(PostgresClient postgresClient) {
    super(postgresClient);
  }

  public Class<Version> getType() {
    return Version.class;
  }

  public Version retrieveFromDatabase(long id) {
    return new Version(id);
  }
}

