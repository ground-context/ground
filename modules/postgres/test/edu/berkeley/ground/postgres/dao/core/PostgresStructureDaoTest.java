package edu.berkeley.ground.postgres.dao.core;

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Structure;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

public class PostgresStructureDaoTest extends PostgresTest {

  public PostgresStructureDaoTest() throws GroundException {
    super();
  }


  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    Structure insertStructure = new Structure(0L, testName, sourceKey, new HashMap<>());

    PostgresTest.structureDao.create(insertStructure);
    Structure structure = PostgresTest.structureDao.retrieveFromDatabase(sourceKey);

    assertEquals(testName, structure.getName());
    assertEquals(sourceKey, structure.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String sourceKey = "testStructure";
    long structureId = PostgresTest.createStructure(sourceKey).getId();

    long structureVersionId = PostgresTest.createStructureVersion(structureId).getId();
    long secondStructureVersionId = PostgresTest.createStructureVersion(structureId).getId();

    List<Long> leaves = PostgresTest.structureDao.getLeaves(sourceKey);

    assertTrue(leaves.contains(structureVersionId));
    assertTrue(leaves.contains(secondStructureVersionId));
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadStructure() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.structureDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateStructure() throws GroundException {
    String structureName = "structureName";
    String structureKey = "structureKey";

    try {
      Structure structure = new Structure(0L, structureName, structureKey, new HashMap<>());
      PostgresTest.structureDao.create(structure);
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    Structure sameStructure = new Structure(0L, structureName, structureKey, new HashMap<>());
    PostgresTest.structureDao.create(sameStructure);
  }


  @Test
  public void testTruncate() throws GroundException {
    String structureName = "testStructure1";
    long structureId = PostgresTest.createStructure(structureName).getId();

    long structureVersionId = PostgresTest.createStructureVersion(structureId).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(structureVersionId);
    long newStructureVersionId = PostgresTest.createStructureVersion(structureId, parents).getId();

    PostgresTest.structureDao.truncate(structureId, 1);

    VersionHistoryDag dag = PostgresTest.versionHistoryDagDao.retrieveFromDatabase(structureId);

    assertEquals(1, dag.getEdgeIds().size());
    VersionSuccessor successor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(0));

    assertEquals(0, successor.getFromId());
    assertEquals(newStructureVersionId, successor.getToId());
  }
}
