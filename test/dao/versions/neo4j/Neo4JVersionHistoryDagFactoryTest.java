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

package dao.versions.neo4j;

import org.junit.Test;

import dao.Neo4jTest;
import models.versions.VersionHistoryDag;
import exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4JVersionHistoryDagFactoryTest extends Neo4jTest {

  public Neo4JVersionHistoryDagFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionHistoryDAGCreation() throws GroundException {
    try {
      long testId = 1;
      Neo4jTest.versionHistoryDAGFactory.create(testId);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(0, dag.getEdgeIds().size());

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }
}
