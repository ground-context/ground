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

package dao.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dao.Neo4jTest;
import exceptions.GroundItemNotFoundException;
import models.models.Structure;
import exceptions.GroundException;
import models.versions.GroundType;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jStructureFactoryTest extends Neo4jTest {

  public Neo4jStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      Neo4jTest.structureFactory.create(testName, sourceKey, new HashMap<>());
      Structure structure = Neo4jTest.structureFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, structure.getName());
      assertEquals(sourceKey, structure.getSourceKey());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    try {
      String sourceKey = "testStructure";
      long structureId = Neo4jTest.createStructure(sourceKey).getId();
      long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();
      long secondStructureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

      List<Long> leaves = Neo4jTest.structureFactory.getLeaves(sourceKey);

      assertTrue(leaves.contains(structureVersionId));
      assertTrue(leaves.contains(secondStructureVersionId));
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadStructure() throws GroundException {
    String sourceKey = "test";

    try {
      Neo4jTest.structureFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateStructure() throws GroundException {
    String structureName = "structureName";
    String structureKey = "structureKey";

    try {
      try {
        Neo4jTest.structureFactory.create(structureName, structureKey, new HashMap<>());
      } catch (GroundException e) {
        Neo4jTest.neo4jClient.abort();
        fail(e.getMessage());
      }

      Neo4jTest.structureFactory.create(structureName, structureKey, new HashMap<>());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }


  @Test
  public void testTruncate() throws GroundException {
    try {
      String structureName = "testStructure";
      long structureId = Neo4jTest.createStructure(structureName).getId();
      long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(structureVersionId);
      long newStructureVersionId = Neo4jTest.createStructureVersion(structureId, parents).getId();

      Neo4jTest.structureFactory.truncate(structureId, 1);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
          .retrieveFromDatabase(structureId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      Neo4jTest.neo4jClient.commit();

      assertEquals(structureId, successor.getFromId());
      assertEquals(newStructureVersionId, successor.getToId());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }
}
