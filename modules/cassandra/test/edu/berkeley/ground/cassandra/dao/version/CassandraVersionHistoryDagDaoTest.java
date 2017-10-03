package edu.berkeley.ground.cassandra.dao.version;

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

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.cassandra.dao.CassandraTest;
import org.junit.Test;

public class CassandraVersionHistoryDagDaoTest extends CassandraTest {

  public CassandraVersionHistoryDagDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionHistoryDAGCreation() throws GroundException {
    long testId = 1;
    CassandraTest.versionHistoryDagDao.create(testId);

    VersionHistoryDag dag = CassandraTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(0, dag.getEdgeIds().size());
  }
}
