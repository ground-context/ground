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

package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class CassandraStructureFactoryTest extends CassandraTest {

  public CassandraStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraTest.structuresResource.createStructure(testName, sourceKey, new HashMap<>());
    Structure structure = CassandraTest.structuresResource.getStructure(sourceKey);

    assertEquals(testName, structure.getName());
    assertEquals(sourceKey, structure.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String sourceKey = "testStructure";
    long structureId = CassandraTest.createStructure(sourceKey).getId();

    long structureVersionId = CassandraTest.createStructureVersion(structureId).getId();
    long secondStructureVersionId = CassandraTest.createStructureVersion(structureId).getId();

    List<Long> leaves = CassandraTest.structuresResource.getLatestVersions(sourceKey);

    assertTrue(leaves.contains(structureVersionId));
    assertTrue(leaves.contains(secondStructureVersionId));
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadStructure() throws GroundException {
    String sourceKey = "test";

    try {
      CassandraTest.structuresResource.getStructure(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateStructure() throws GroundException {
    String structureName = "structureName";
    String structureKey = "structureKey";

    try {
      CassandraTest.structuresResource.createStructure(structureName, structureKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    CassandraTest.structuresResource.createStructure(structureName, structureKey, new HashMap<>());
  }

  @Test
  public void testTruncate() throws GroundException {
    String structureName = "testStructure1";
    long structureId = CassandraTest.createStructure(structureName).getId();

    long structureVersionId = CassandraTest.createStructureVersion(structureId).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(structureVersionId);
    long newStructureVersionId = CassandraTest.createStructureVersion(structureId, parents)
        .getId();

    CassandraTest.structuresResource.truncateStructure(structureName, 1);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory
        .retrieveFromDatabase(structureId);
    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));
    CassandraTest.cassandraClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newStructureVersionId, successor.getToId());
  }
}
