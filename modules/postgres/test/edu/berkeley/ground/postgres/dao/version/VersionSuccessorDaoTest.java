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

package edu.berkeley.ground.postgres.dao.version;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import org.junit.Test;

import static org.junit.Assert.*;

public class VersionSuccessorDaoTest extends PostgresTest {

  private VersionDao versionDao;

  public VersionSuccessorDaoTest() throws GroundException {
    super();

    this.versionDao = new VersionDao(PostgresTest.dbSource, PostgresTest.idGenerator);
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    try {
      long fromId = 1;
      long toId = 2;

      this.versionDao.create(new Version(fromId));
      this.versionDao.create(new Version(toId));

      VersionSuccessor<?> successor = PostgresTest.versionSuccessorDao.create(fromId, toId);

      VersionSuccessor<?> retrieved = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
        successor.getId());

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());
    } finally {
      //PostgresTest.postgresClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorCreation() throws GroundException {
    try {
      long fromId = 1;
      long toId = 2;

      // Catch exceptions for these two lines because they should not fal
      try {
        // the main difference is that we're not creating a Version for the toId
        this.versionDao.create(new Version(fromId));
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      // This statement should fail because toId is not in the database
      PostgresTest.versionSuccessorDao.create(fromId, toId);
    } finally {
      //PostgresTest.postgresClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorRetrieval() throws GroundException {
    try {
      PostgresTest.versionSuccessorDao.retrieveFromDatabase(10);

      //PostgresTest.postgresClient.commit();
    } catch (GroundException e) {
      //PostgresTest.postgresClient.abort();

      if (!e.getMessage().contains("No VersionSuccessor found with id 10.")) {
        fail();
      }

      throw e;
    }
  }
}
