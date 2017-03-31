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
import edu.berkeley.ground.model.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraLineageEdgeFactoryTest extends CassandraTest {

  public CassandraLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraLineageEdgeFactory edgeFactory = (CassandraLineageEdgeFactory) super.factories.getLineageEdgeFactory();
    edgeFactory.create(testName, sourceKey, new HashMap<>());

    LineageEdge lineageEdge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, lineageEdge.getName());
    assertEquals(sourceKey, lineageEdge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String testName = "test";

    try {
      CassandraTest.factories.getLineageEdgeFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No LineageEdge found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testBadLineageEdgeVersion() throws GroundException {
    long id = 1;

    try {
      CassandraTest.factories.getLineageEdgeVersionFactory().retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals("No RichVersion found with id " + id + ".", e.getMessage());

      throw e;
    }
  }
}
