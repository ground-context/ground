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

package edu.berkeley.ground.dao.usage.cassandra;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;

public class CassandraLineageGraphFactoryTest extends CassandraTest {

  public CassandraLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraLineageGraphFactory graphFactory = (CassandraLineageGraphFactory)
        CassandraTest.factories.getLineageGraphFactory();
    graphFactory.create(testName, sourceKey, new HashMap<>());

    LineageGraph graph = graphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String testName = "test";

    try {
      CassandraTest.factories.getLineageGraphFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No LineageGraph found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }
}
