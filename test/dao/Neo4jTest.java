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

package dao;


import dao.models.neo4j.Neo4jStructureVersionFactory;
import dao.models.neo4j.Neo4jTagFactory;
import dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import dao.versions.neo4j.Neo4jVersionSuccessorFactory;
import db.Neo4jClient;
import exceptions.GroundException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import play.Configuration;
import util.ElasticSearch;
import util.IdGenerator;
import util.Neo4jFactories;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


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
  public static void setupClass() throws GroundException {
    factories = new Neo4jFactories(createTestConfig());
    neo4jClient = (Neo4jClient) factories.getDbClient();

    versionSuccessorFactory = new Neo4jVersionSuccessorFactory(neo4jClient, new IdGenerator(0, 1, true));
    versionHistoryDAGFactory = new Neo4jVersionHistoryDagFactory(neo4jClient, versionSuccessorFactory);
    tagFactory = new Neo4jTagFactory(neo4jClient, true);


    edgeFactory = factories.getEdgeFactory();
    graphFactory = factories.getGraphFactory();
    lineageEdgeFactory = factories.getLineageEdgeFactory();
    lineageGraphFactory = factories.getLineageGraphFactory();
    nodeFactory = factories.getNodeFactory();
    structureFactory = factories.getStructureFactory();

    edgeVersionFactory = factories.getEdgeVersionFactory();
    graphVersionFactory = factories.getGraphVersionFactory();
    lineageEdgeVersionFactory = factories.getLineageEdgeVersionFactory();
    lineageGraphVersionFactory = factories.getLineageGraphVersionFactory();
    nodeVersionFactory = factories.getNodeVersionFactory();
    structureVersionFactory = factories.getStructureVersionFactory();
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


  @AfterClass
  public static void tearDown() throws IOException {
    ElasticSearch.closeElasticSearch();
  }

  private static Configuration createTestConfig() {
    Map<String, Object> confMap = new HashMap<>();
    Map<String, Object> dbMap = new HashMap<>();
    Map<String, Object> machineMap = new HashMap<>();

    dbMap.put("host", "localhost");
    dbMap.put("user", "neo4j");
    dbMap.put("password", "password");

    machineMap.put("id", 0);
    machineMap.put("count", 1);

    confMap.put("db", dbMap);
    confMap.put("machine", machineMap) ;

    return new Configuration(confMap);
  }

}
