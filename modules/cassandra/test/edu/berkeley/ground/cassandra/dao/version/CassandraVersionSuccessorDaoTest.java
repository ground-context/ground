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

package edu.berkeley.ground.cassandra.dao.version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.cassandra.dao.CassandraTest;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import org.junit.Test;
import play.Logger; // Andre - unnecessary

public class CassandraVersionSuccessorDaoTest extends CassandraTest {

  public CassandraVersionSuccessorDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    long fromId = 1;
    long toId = 2;

    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(fromId)));
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(toId)));

    VersionSuccessor successor = ((CassandraVersionSuccessorDao) CassandraTest.versionSuccessorDao).instantiateVersionSuccessor(fromId, toId);
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.versionSuccessorDao.insert(successor));

    VersionSuccessor retrieved = CassandraTest.versionSuccessorDao.retrieveFromDatabase(successor.getId());

    assertEquals(fromId, retrieved.getFromId());
    assertEquals(toId, retrieved.getToId());
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorCreation() throws GroundException {
    Logger.debug("\n\n\nBEGINNING TEST\n");

    long fromId = 123;
    long toId = 456;

    Logger.debug("A");
    // Catch exceptions for these two lines because they should not fal
    try {
      // the main difference is that we're not creating a Version for the toId
      CassandraTest.cassandraVersionDao.insert(new Version(fromId));
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
    Logger.debug("B");

    // This statement should cause a GroundException because toId is not in the database
    VersionSuccessor successor = ((CassandraVersionSuccessorDao) CassandraTest.versionSuccessorDao).instantiateVersionSuccessor(fromId, toId);
    Logger.debug("C");
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.versionSuccessorDao.insert(successor));
    Logger.debug("D");
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorRetrieval() throws GroundException {
    try {
      CassandraTest.versionSuccessorDao.retrieveFromDatabase(10);
    } catch (GroundException e) {

      if (!e.getMessage().contains("Version Successor with id 10 does not exist.")) {
        fail();
      }

      throw e;
    }
  }
}
