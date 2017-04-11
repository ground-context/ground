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

package edu.berkeley.ground.dao.versions.cassandra;

import org.junit.Test;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.dao.versions.cassandra.mock.TestCassandraVersionFactory;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraVersionHistoryDagFactoryTest extends CassandraTest {

  private TestCassandraVersionFactory versionFactory;

  public CassandraVersionHistoryDagFactoryTest() throws GroundDbException {
    super();

    this.versionFactory = new TestCassandraVersionFactory(CassandraTest.cassandraClient);
  }

  @Test
  public void testVersionHistoryDAGCreation() throws GroundException {
    try {
      long testId = 1;
      CassandraTest.versionHistoryDAGFactory.create(testId);

      VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(0, dag.getEdgeIds().size());
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test
  public void testAddEdge() throws GroundException {
    try {
      long testId = 1;
      CassandraTest.versionHistoryDAGFactory.create(testId);

      VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      long fromId = 123;
      long toId = 456;

      this.versionFactory.insertIntoDatabase(fromId);
      this.versionFactory.insertIntoDatabase(toId);

      CassandraTest.versionHistoryDAGFactory.addEdge(dag, fromId, toId, testId);

      VersionHistoryDag<?> retrieved = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(1, retrieved.getEdgeIds().size());
      assertEquals(toId, (long) retrieved.getLeaves().get(0));

      VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
          retrieved.getEdgeIds().get(0));

      assertEquals(fromId, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }
}
