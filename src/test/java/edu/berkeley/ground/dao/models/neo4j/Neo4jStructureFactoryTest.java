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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jStructureFactoryTest extends Neo4jTest {

  public Neo4jStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    Neo4jTest.structuresResource.createStructure(testName, sourceKey, new HashMap<>());
    Structure structure = Neo4jTest.structuresResource.getStructure(testName);

    assertEquals(testName, structure.getName());
    assertEquals(sourceKey, structure.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String structureName = "testStructure1";
    long structureId = Neo4jTest.createStructure(structureName).getId();
    long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();
    long secondStructureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

    List<Long> leaves = Neo4jTest.structuresResource.getLatestVersions(structureName);

    assertTrue(leaves.contains(structureVersionId));
    assertTrue(leaves.contains(secondStructureVersionId));
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadStructure() throws GroundException {
    String testName = "test";

    try {
      Neo4jTest.structuresResource.getStructure(testName);
    } catch (GroundException e) {
      assertEquals("No Structure found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test
  public void testTruncate() throws GroundException {
    String structureName = "testStructure";
    long structureId = Neo4jTest.createStructure(structureName).getId();
    long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(structureVersionId);
    long newStructureVersionId = Neo4jTest.createStructureVersion(structureId, parents).getId();

    Neo4jTest.structuresResource.truncateStructure(structureName, 1);

    VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
        .retrieveFromDatabase(structureId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    Neo4jTest.neo4jClient.commit();

    assertEquals(structureId, successor.getFromId());
    assertEquals(newStructureVersionId, successor.getToId());
  }
}
