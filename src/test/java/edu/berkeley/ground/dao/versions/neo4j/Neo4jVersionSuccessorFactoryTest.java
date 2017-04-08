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
      long fromNodeId = Neo4jTest.createNode("testFromNode").getId();
      long toNodeId = Neo4jTest.createNode("testToNode").getId();

      long fromId = Neo4jTest.createNodeVersion(fromNodeId).getId();
      long toId = Neo4jTest.createNodeVersion(toNodeId).getId();

      long vsId = Neo4jTest.versionSuccessorFactory.create(fromId, toId).getId();

      VersionSuccessor<?> retrieved = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(vsId);

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());

      Neo4jTest.neo4jClient.commit();
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorCreation() throws GroundException {
    try {
      long toId = -1;

      try {
        long nodeId = Neo4jTest.createNode("testNode").getId();
        toId = Neo4jTest.createNodeVersion(nodeId).getId();
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      // this statement should be fail because the fromId does not exist
      Neo4jTest.versionSuccessorFactory.create(9, toId).getId();

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorRetrieval() throws GroundException {
    try {
      Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(10);

      Neo4jTest.neo4jClient.commit();
    } catch (GroundException e) {
      Neo4jTest.neo4jClient.abort();
      if (!e.getMessage().contains("No VersionSuccessor found with id 10.")) {
        fail();
      }

      throw e;
    }
  }
}
