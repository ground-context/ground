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

package edu.berkeley.ground.dao.versions.neo4j;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.versions.VersionSuccessor;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jVersionSuccessorFactoryTest extends Neo4jTest {

  public Neo4jVersionSuccessorFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    try {
      long fromNodeId = super.factories.getNodeFactory().create("testFromNode", null,
          new HashMap<>()).getId();
      long toNodeId = super.factories.getNodeFactory().create("testToNode", null, new HashMap<>())
          .getId();
      long fromId = super.createNodeVersion(fromNodeId);
      long toId = super.createNodeVersion(toNodeId);

      long vsId = super.versionSuccessorFactory.create(fromId, toId).getId();

      VersionSuccessor<?> retrieved = super.versionSuccessorFactory.retrieveFromDatabase(vsId);

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());

      super.neo4jClient.commit();
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorCreation() throws GroundException {
    try {
      long toId = -1;

      try {
        String nodeName = "testNode";
        long nodeId = super.factories.getNodeFactory().create(nodeName, null, new HashMap<>())
            .getId();
        toId = super.createNodeVersion(nodeId);
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      // this statement should be fail because the fromId does not exist
      super.versionSuccessorFactory.create(9, toId).getId();

      super.neo4jClient.commit();
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorRetrieval() throws GroundException {
    try {
      super.versionSuccessorFactory.retrieveFromDatabase(10);

      super.neo4jClient.commit();
    } catch (GroundException e) {
      super.neo4jClient.abort();
      if (!e.getMessage().contains("No VersionSuccessor found with id 10.")) {
        fail();
      }

      throw e;
    }
  }
}
