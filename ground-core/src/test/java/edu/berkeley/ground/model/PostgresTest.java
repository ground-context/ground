package edu.berkeley.ground.model;

import org.junit.Before;

import java.io.File;
import java.io.IOException;

import edu.berkeley.ground.dao.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.util.IdGenerator;
import edu.berkeley.ground.util.PostgresFactories;

public class PostgresTest {
  private static String TEST_DB_NAME = "test";

  protected PostgresClient postgresClient;
  protected PostgresFactories factories;
  protected PostgresVersionFactory versionFactory;
  protected PostgresVersionSuccessorFactory versionSuccessorFactory;
  protected PostgresVersionHistoryDAGFactory versionHistoryDAGFactory;
  protected PostgresItemFactory itemFactory;
  protected PostgresRichVersionFactory richVersionFactory;
  protected PostgresTagFactory tagFactory;

  public PostgresTest() throws GroundDBException {
    this.postgresClient = new PostgresClient("localhost", 5432, "test", "test", "");
    this.factories = new PostgresFactories(this.postgresClient, 0, 1);

    this.versionFactory = new PostgresVersionFactory(this.postgresClient);
    this.versionSuccessorFactory = new PostgresVersionSuccessorFactory(this.postgresClient, new IdGenerator(0, 1, false));
    this.versionHistoryDAGFactory = new PostgresVersionHistoryDAGFactory(this.postgresClient, versionSuccessorFactory);
    this.tagFactory = new PostgresTagFactory(this.postgresClient);
    this.itemFactory = new PostgresItemFactory(this.postgresClient, versionHistoryDAGFactory, tagFactory);

    this.richVersionFactory = new PostgresRichVersionFactory(this.postgresClient, versionFactory,
        (PostgresStructureVersionFactory) factories.getStructureVersionFactory(), tagFactory);
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec("python2.7 postgres_setup.py " + TEST_DB_NAME + " test drop"
        , null, new File("scripts/postgres/"));
    p.waitFor();

    p.destroy();
  }
}
