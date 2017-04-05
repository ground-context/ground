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

    CassandraStructureFactory edgeFactory = (CassandraStructureFactory) CassandraTest.factories.getStructureFactory();
    edgeFactory.create(testName, sourceKey, new HashMap<>());

    Structure structure = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, structure.getName());
    assertEquals(sourceKey, structure.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String structureName = "testStructure1";
    String sourceKey = "testKey";

    long structureId = CassandraTest.factories.getStructureFactory().create(structureName,
        sourceKey, new HashMap<>()).getId();

    long structureVersionId = CassandraTest.factories.getStructureVersionFactory().create(structureId,
        new HashMap<>(), new ArrayList<>()).getId();
    long secondNVId = CassandraTest.factories.getStructureVersionFactory().create(structureId,
        new HashMap<>(), new ArrayList<>()).getId();

    List<Long> leaves = CassandraTest.factories.getStructureFactory().getLeaves(structureName);

    assertTrue(leaves.contains(structureVersionId));
    assertTrue(leaves.contains(secondNVId));
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadStructure() throws GroundException {
    String testName = "test";

    try {
      CassandraTest.factories.getStructureFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No Structure found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test
  public void testTruncate() throws GroundException {
    String structureName = "testStructure";
    long structureId = CassandraTest.factories.getStructureFactory().create(structureName, null,
        new HashMap<>()).getId();

    Map<String, GroundType> structureVersionAttributes = new HashMap<>();
    structureVersionAttributes.put("intfield", GroundType.INTEGER);

    long structureVersionId = CassandraTest.factories.getStructureVersionFactory().create(
        structureId, structureVersionAttributes, new ArrayList<>()).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(structureVersionId);

    long newStructureVersionId = CassandraTest.factories.getStructureVersionFactory().create(
        structureId, structureVersionAttributes, parents).getId();

    CassandraTest.factories.getStructureFactory().truncate(structureId, 1);

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
