/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.dao;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

import edu.berkeley.ground.dao.models.cassandra.CassandraStructureVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.resources.EdgesResource;
import edu.berkeley.ground.resources.GraphsResource;
import edu.berkeley.ground.resources.LineageEdgesResource;
import edu.berkeley.ground.resources.LineageGraphsResource;
import edu.berkeley.ground.resources.NodesResource;
import edu.berkeley.ground.resources.StructuresResource;
import edu.berkeley.ground.util.CassandraFactories;
import edu.berkeley.ground.util.IdGenerator;

public class CassandraTest extends DaoTest {
  private static final String TEST_DB_NAME = "test";
  private static CassandraFactories factories;

  protected static CassandraClient cassandraClient;
  protected static CassandraVersionSuccessorFactory versionSuccessorFactory;
  protected static CassandraVersionHistoryDagFactory versionHistoryDAGFactory;
  protected static CassandraTagFactory tagFactory;

  @BeforeClass
  public static void setup() throws GroundDbException {
    cassandraClient = new CassandraClient("localhost", 9160, "test", "test", "");
    factories = new CassandraFactories(cassandraClient, 0, 1);

    versionSuccessorFactory = new CassandraVersionSuccessorFactory(cassandraClient,
        new IdGenerator(0, 1, false));
    versionHistoryDAGFactory = new CassandraVersionHistoryDagFactory(cassandraClient,
        versionSuccessorFactory);
    tagFactory = new CassandraTagFactory(cassandraClient);

    edgesResource = new EdgesResource(factories.getEdgeFactory(),
        factories.getEdgeVersionFactory(), factories.getNodeFactory(), cassandraClient);
    graphsResource = new GraphsResource(factories.getGraphFactory(),
        factories.getGraphVersionFactory(), cassandraClient);
    lineageEdgesResource = new LineageEdgesResource(factories.getLineageEdgeFactory(),
        factories.getLineageEdgeVersionFactory(), cassandraClient);
    lineageGraphsResource = new LineageGraphsResource(factories.getLineageGraphFactory(),
        factories.getLineageGraphVersionFactory(), cassandraClient);
    nodesResource = new NodesResource(factories.getNodeFactory(),
        factories.getNodeVersionFactory(), cassandraClient);
    structuresResource = new StructuresResource(factories.getStructureFactory(),
        factories.getStructureVersionFactory(), cassandraClient);
  }

  @Before
  public void setupTest() throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec("cqlsh -k " + TEST_DB_NAME + " -f truncate.cql", null, new File("scripts/cassandra/"));
    p.waitFor();

    p.destroy();
  }

  public static CassandraStructureVersionFactory getStructureVersionFactory() {
    return (CassandraStructureVersionFactory) CassandraTest.factories.getStructureVersionFactory();
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    cassandraClient.close();
  }
}
