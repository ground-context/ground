package dao.versions.postgres.mock;

import dao.versions.postgres.PostgresVersionFactory;
import db.PostgresClient;
import models.versions.Version;

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

