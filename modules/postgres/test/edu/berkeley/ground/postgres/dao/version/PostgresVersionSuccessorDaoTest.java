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

package edu.berkeley.ground.postgres.dao.version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import org.junit.Test;

public class PostgresVersionSuccessorDaoTest extends PostgresTest {

  public PostgresVersionSuccessorDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    long fromId = 1;
    long toId = 2;

    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(fromId)));
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(toId)));

    VersionSuccessor successor = ((PostgresVersionSuccessorDao) PostgresTest.versionSuccessorDao).instantiateVersionSuccessor(fromId, toId);
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.versionSuccessorDao.insert(successor));

    VersionSuccessor retrieved = PostgresTest.versionSuccessorDao.retrieveFromDatabase(successor.getId());

    assertEquals(fromId, retrieved.getFromId());
    assertEquals(toId, retrieved.getToId());
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorCreation() throws GroundException {
    long fromId = 1;
    long toId = 2;

    // Catch exceptions for these two lines because they should not fal
    try {
      // the main difference is that we're not creating a Version for the toId
      PostgresTest.postgresVersionDao.insert(new Version(fromId));
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }

    // This statement should fail because toId is not in the database
    VersionSuccessor successor = ((PostgresVersionSuccessorDao) PostgresTest.versionSuccessorDao).instantiateVersionSuccessor(fromId, toId);
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.versionSuccessorDao.insert(successor));
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorRetrieval() throws GroundException {
    try {
      PostgresTest.versionSuccessorDao.retrieveFromDatabase(10);
    } catch (GroundException e) {

      if (!e.getMessage().contains("Version Successor with id 10 does not exist.")) {
        fail();
      }

      throw e;
    }
  }
}
