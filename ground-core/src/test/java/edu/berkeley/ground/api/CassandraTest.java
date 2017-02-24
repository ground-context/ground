package edu.berkeley.ground.api;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

import edu.berkeley.ground.api.models.cassandra.CassandraRichVersionFactory;
import edu.berkeley.ground.api.models.cassandra.CassandraStructureVersionFactory;
import edu.berkeley.ground.api.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.util.CassandraFactories;
import edu.berkeley.ground.util.IdGenerator;

public class CassandraTest {
  private static String TEST_DB_NAME = "test";

  protected static CassandraClient cassandraClient;
  protected static CassandraFactories factories;
  protected static CassandraVersionFactory versionFactory;
  protected static CassandraVersionSuccessorFactory versionSuccessorFactory;
  protected static CassandraVersionHistoryDAGFactory versionHistoryDAGFactory;
  protected static CassandraItemFactory itemFactory;
  protected static CassandraRichVersionFactory richVersionFactory;
  protected static CassandraTagFactory tagFactory;

  @BeforeClass
  public static void setup() throws GroundDBException {
    cassandraClient = new CassandraClient("localhost", 9160, "test", "test", "");
    factories = new CassandraFactories(cassandraClient, 0, 1);

    versionFactory = new CassandraVersionFactory();
    versionSuccessorFactory = new CassandraVersionSuccessorFactory(new IdGenerator(0, 1, false));
    versionHistoryDAGFactory = new CassandraVersionHistoryDAGFactory(versionSuccessorFactory);
    itemFactory = new CassandraItemFactory(versionHistoryDAGFactory);
    tagFactory = new CassandraTagFactory();

    richVersionFactory = new CassandraRichVersionFactory(versionFactory,
        (CassandraStructureVersionFactory) factories.getStructureVersionFactory(), tagFactory);
  }

  @Before
  public void setupTest() throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec("cqlsh -k " + TEST_DB_NAME + " -f truncate.cql", null, new File("scripts/cassandra/"));
    p.waitFor();

    p.destroy();
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    cassandraClient.closeCluster();
  }
}
