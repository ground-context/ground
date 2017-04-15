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

import java.io.IOException;

import edu.berkeley.ground.dao.models.TagFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jStructureVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jTagFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionSuccessorFactory;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.resources.EdgesResource;
import edu.berkeley.ground.resources.GraphsResource;
import edu.berkeley.ground.resources.LineageEdgesResource;
import edu.berkeley.ground.resources.LineageGraphsResource;
import edu.berkeley.ground.resources.NodesResource;
import edu.berkeley.ground.resources.StructuresResource;
import edu.berkeley.ground.util.IdGenerator;
import edu.berkeley.ground.util.Neo4jFactories;

public class Neo4jTest extends DaoTest {
  /* Note: In Neo4j, we don't create explicit (Rich)Versions because all of the logic is wrapped in
   * FooVersions. We are using NodeVersions as stand-ins because they are the most simple kind of
   * Versions. */

  private static Neo4jFactories factories;

  protected static Neo4jClient neo4jClient;
  protected static Neo4jVersionSuccessorFactory versionSuccessorFactory;
  protected static Neo4jVersionHistoryDagFactory versionHistoryDAGFactory;
  protected static Neo4jTagFactory tagFactory;

  @BeforeClass
  public static void setupClass() {
    neo4jClient = new Neo4jClient("localhost", "neo4j", "password");
    factories = new Neo4jFactories(neo4jClient, 0, 1);
    versionSuccessorFactory = new Neo4jVersionSuccessorFactory(neo4jClient, new IdGenerator(0, 1, true));
    versionHistoryDAGFactory = new Neo4jVersionHistoryDagFactory(neo4jClient, versionSuccessorFactory);
    tagFactory = new Neo4jTagFactory(neo4jClient);

    edgesResource = new EdgesResource(factories.getEdgeFactory(),
        factories.getEdgeVersionFactory(), factories.getNodeFactory(), neo4jClient);
    graphsResource = new GraphsResource(factories.getGraphFactory(),
        factories.getGraphVersionFactory(), neo4jClient);
    lineageEdgesResource = new LineageEdgesResource(factories.getLineageEdgeFactory(),
        factories.getLineageEdgeVersionFactory(), neo4jClient);
    lineageGraphsResource = new LineageGraphsResource(factories.getLineageGraphFactory(),
        factories.getLineageGraphVersionFactory(), neo4jClient);
    nodesResource = new NodesResource(factories.getNodeFactory(),
        factories.getNodeVersionFactory(), neo4jClient);
    structuresResource = new StructuresResource(factories.getStructureFactory(),
        factories.getStructureVersionFactory(), neo4jClient);
  }

  public static Neo4jStructureVersionFactory getStructureVersionFactory() {
    return (Neo4jStructureVersionFactory) Neo4jTest.factories.getStructureVersionFactory();
  }

  @AfterClass
  public static void teardownClass() {
    neo4jClient.close();
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    neo4jClient.dropData();
  }
}
