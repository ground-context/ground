/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use file except in compliance with the License.
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

import edu.berkeley.ground.dao.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.resources.EdgesResource;
import edu.berkeley.ground.resources.GraphsResource;
import edu.berkeley.ground.resources.LineageEdgesResource;
import edu.berkeley.ground.resources.LineageGraphsResource;
import edu.berkeley.ground.resources.NodesResource;
import edu.berkeley.ground.resources.StructuresResource;
import edu.berkeley.ground.util.IdGenerator;
import edu.berkeley.ground.util.PostgresFactories;

public class PostgresTest extends DaoTest {
  private static final String TEST_DB_NAME = "test";

  private static PostgresFactories factories;

  protected static PostgresClient postgresClient;
  protected static PostgresVersionSuccessorFactory versionSuccessorFactory;
  protected static PostgresVersionHistoryDagFactory versionHistoryDAGFactory;
  protected static PostgresTagFactory tagFactory;

  @BeforeClass
  public static void setupClass() throws GroundDbException {
    postgresClient = new PostgresClient("localhost", 5432, "test", "test", "");
    factories = new PostgresFactories(postgresClient, 0, 1);

    versionSuccessorFactory = new PostgresVersionSuccessorFactory(postgresClient, new IdGenerator(0, 1, false));
    versionHistoryDAGFactory = new PostgresVersionHistoryDagFactory(postgresClient, versionSuccessorFactory);
    tagFactory = new PostgresTagFactory(postgresClient);

    edgesResource = new EdgesResource(factories.getEdgeFactory(),
        factories.getEdgeVersionFactory(), factories.getNodeFactory(), postgresClient);
    graphsResource = new GraphsResource(factories.getGraphFactory(),
        factories.getGraphVersionFactory(), postgresClient);
    lineageEdgesResource = new LineageEdgesResource(factories.getLineageEdgeFactory(),
        factories.getLineageEdgeVersionFactory(), postgresClient);
    lineageGraphsResource = new LineageGraphsResource(factories.getLineageGraphFactory(),
        factories.getLineageGraphVersionFactory(), postgresClient);
    nodesResource = new NodesResource(factories.getNodeFactory(),
        factories.getNodeVersionFactory(), postgresClient);
    structuresResource = new StructuresResource(factories.getStructureFactory(),
        factories.getStructureVersionFactory(), postgresClient);
  }

  @AfterClass
  public static void teardownClass() throws GroundDbException {
    // postgresClient.close();
  }

  public static PostgresStructureVersionFactory getStructureVersionFactory() {
    return (PostgresStructureVersionFactory) PostgresTest.factories.getStructureVersionFactory();
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec("python2.7 postgres_setup.py " + TEST_DB_NAME + " test drop"
        , null, new File("scripts/postgres/"));

    p.waitFor();
    p.destroy();
  }
}
