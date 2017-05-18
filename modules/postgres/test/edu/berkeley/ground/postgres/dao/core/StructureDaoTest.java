package edu.berkeley.ground.postgres.dao.core;

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

  import edu.berkeley.ground.common.exception.GroundException;
  import edu.berkeley.ground.common.model.core.Structure;
  import edu.berkeley.ground.common.model.version.VersionHistoryDag;
  import edu.berkeley.ground.common.model.version.VersionSuccessor;
  import edu.berkeley.ground.postgres.dao.PostgresTest;
  import org.junit.Test;

  import java.util.ArrayList;
  import java.util.HashMap;
  import java.util.List;

  import static org.junit.Assert.*;

public class StructureDaoTest extends PostgresTest {

  public StructureDaoTest() throws GroundException {
    super();
  }


  @Test
  public void testStructureCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      Structure insertStructure = new Structure(0L, testName, sourceKey, new HashMap<>());

      PostgresTest.structureDao.create(insertStructure);
      Structure structure = PostgresTest.structureDao.retrieveFromDatabase(sourceKey);

      assertEquals(testName, structure.getName());
      assertEquals(sourceKey, structure.getSourceKey());
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    try {
      String sourceKey = "testStructure";
      long structureId = PostgresTest.createStructure(sourceKey).getId();

      long structureVersionId = PostgresTest.createStructureVersion(structureId).getId();
      long secondStructureVersionId = PostgresTest.createStructureVersion(structureId).getId();

      List<Long> leaves = PostgresTest.structureDao.getLeaves(sourceKey);

      assertTrue(leaves.contains(structureVersionId));
      assertTrue(leaves.contains(secondStructureVersionId));
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadStructure() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.structureDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateStructure() throws GroundException {
    String structureName = "structureName";
    String structureKey = "structureKey";

    try {
      try {
        Structure structure = new Structure(0L, structureName, structureKey, new HashMap<>());
        PostgresTest.structureDao.create(structure);
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      Structure sameStructure = new Structure(0L, structureName, structureKey, new HashMap<>());
      PostgresTest.structureDao.create(sameStructure);
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }


  @Test
  public void testTruncate() throws GroundException {
    try {
      String structureName = "testStructure1";
      long structureId = PostgresTest.createStructure(structureName).getId();

      long structureVersionId = PostgresTest.createStructureVersion(structureId).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(structureVersionId);
      long newStructureVersionId = PostgresTest.createStructureVersion(structureId, parents)
        .getId();

      PostgresTest.structureDao.truncate(structureId, 1);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDagDao
        .retrieveFromDatabase(structureId);

      assertEquals(1, dag.getEdgeIds().size());
      VersionSuccessor<?> successor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

      //PostgresTest.postgresClient.commit();
      assertEquals(0, successor.getFromId());
      assertEquals(newStructureVersionId, successor.getToId());
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }
}
