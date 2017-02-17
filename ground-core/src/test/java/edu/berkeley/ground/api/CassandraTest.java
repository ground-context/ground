package edu.berkeley.ground.api;

import org.junit.Before;

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

  protected CassandraClient cassandraClient;
  protected CassandraFactories factories;
  protected CassandraVersionFactory versionFactory;
  protected CassandraVersionSuccessorFactory versionSuccessorFactory;
  protected CassandraVersionHistoryDAGFactory versionHistoryDAGFactory;
  protected CassandraItemFactory itemFactory;
  protected CassandraRichVersionFactory richVersionFactory;
  protected CassandraTagFactory tagFactory;

  public CassandraTest() throws GroundDBException {
    this.cassandraClient = new CassandraClient("localhost", 9160, "test", "test", "");
    this.factories = new CassandraFactories(cassandraClient, 0, 1);

    this.versionFactory = new CassandraVersionFactory();
    this.versionSuccessorFactory = new CassandraVersionSuccessorFactory(new IdGenerator(0, 1, false));
    this.versionHistoryDAGFactory = new CassandraVersionHistoryDAGFactory(versionSuccessorFactory);
    this.itemFactory = new CassandraItemFactory(versionHistoryDAGFactory);
    this.tagFactory = new CassandraTagFactory();

    this.richVersionFactory = new CassandraRichVersionFactory(versionFactory,
        (CassandraStructureVersionFactory) factories.getStructureVersionFactory(), tagFactory);
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec("cqlsh -k " + TEST_DB_NAME + " -f truncate.cql", null, new File("scripts/cassandra/"));
    p.waitFor();

    p.destroy();
  }
}
