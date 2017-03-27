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

package edu.berkeley.ground.dao.models.neo4j;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jGraphFactoryTest extends Neo4jTest {

  public Neo4jGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    Neo4jGraphFactory edgeFactory = (Neo4jGraphFactory) super.factories.getGraphFactory();
    edgeFactory.create(testName, sourceKey, new HashMap<>());

    Graph graph = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadGraph() throws GroundException {
    String testName = "test";

    try {
      super.factories.getGraphFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No Graph found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }
}
