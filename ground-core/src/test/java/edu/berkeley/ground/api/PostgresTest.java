package edu.berkeley.ground.api;

import org.junit.Before;

import java.io.File;
import java.io.IOException;

import edu.berkeley.ground.api.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.util.IdGenerator;
import edu.berkeley.ground.util.PostgresFactories;

public class PostgresTest {
  private static String TEST_DB_NAME = "test";

  protected PostgresClient cassandraClient;
  protected PostgresFactories factories;
  protected PostgresVersionFactory versionFactory;
  protected PostgresVersionSuccessorFactory versionSuccessorFactory;
  protected PostgresVersionHistoryDAGFactory versionHistoryDAGFactory;
  protected PostgresItemFactory itemFactory;
  protected PostgresRichVersionFactory richVersionFactory;
  protected PostgresTagFactory tagFactory;

  public PostgresTest() throws GroundDBException {
    this.cassandraClient = new PostgresClient("localhost", 5432, "test", "test", "");
    this.factories = new PostgresFactories(cassandraClient, 0, 1);

    this.versionFactory = new PostgresVersionFactory();
    this.versionSuccessorFactory = new PostgresVersionSuccessorFactory(new IdGenerator(0, 1, false));
    this.versionHistoryDAGFactory = new PostgresVersionHistoryDAGFactory(versionSuccessorFactory);
    this.itemFactory = new PostgresItemFactory(versionHistoryDAGFactory);
    this.tagFactory = new PostgresTagFactory();

    this.richVersionFactory = new PostgresRichVersionFactory(versionFactory,
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
