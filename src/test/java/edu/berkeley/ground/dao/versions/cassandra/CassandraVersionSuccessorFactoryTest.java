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
import edu.berkeley.ground.model.versions.VersionSuccessor;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraVersionSuccessorFactoryTest extends CassandraTest {

  public CassandraVersionSuccessorFactoryTest() throws GroundDbException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    try {
      long fromId = 123;
      long toId = 456;

      CassandraTest.versionFactory.insertIntoDatabase(fromId);
      CassandraTest.versionFactory.insertIntoDatabase(toId);

      VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.create(fromId, toId);

      VersionSuccessor<?> retrieved = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
          successor.getId());

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorCreation() throws GroundException {
    try {
      long fromId = 123;
      long toId = 456;

      // Catch exceptions for these two lines because they should not fal
      try {
        // the main difference is that we're not creating a Version for the toId
        CassandraTest.versionFactory.insertIntoDatabase(fromId);
      } catch (GroundException ge) {
        CassandraTest.cassandraClient.abort();

        fail(ge.getMessage());
      }

      // This statement should fail because toId is not in the database
      CassandraTest.versionSuccessorFactory.create(fromId, toId);
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorRetrieval() throws GroundException {
    try {
      CassandraTest.versionSuccessorFactory.retrieveFromDatabase(10);

      CassandraTest.cassandraClient.commit();
    } catch (GroundException e) {
      CassandraTest.cassandraClient.abort();

      if (!e.getMessage().contains("No VersionSuccessor found with id 10.")) {
        fail();
      }

      throw e;
    }
  }
}
