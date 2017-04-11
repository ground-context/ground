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

package edu.berkeley.ground.dao.versions.postgres;

import org.junit.Test;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.dao.versions.postgres.mock.TestPostgresVersionFactory;
import edu.berkeley.ground.model.versions.VersionSuccessor;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresVersionSuccessorFactoryTest extends PostgresTest {

  private TestPostgresVersionFactory versionFactory;

  public PostgresVersionSuccessorFactoryTest() throws GroundException {
    super();

    this.versionFactory = new TestPostgresVersionFactory(PostgresTest.postgresClient);
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    try {
      long fromId = 1;
      long toId = 2;

      this.versionFactory.insertIntoDatabase(fromId);
      this.versionFactory.insertIntoDatabase(toId);

      VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.create(fromId, toId);

      VersionSuccessor<?> retrieved = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
          successor.getId());

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());
    } finally {
      PostgresTest.postgresClient.abort();
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
        this.versionFactory.insertIntoDatabase(fromId);
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      // This statement should fail because toId is not in the database
      PostgresTest.versionSuccessorFactory.create(fromId, toId);
    } finally {
      PostgresTest.postgresClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorRetrieval() throws GroundException {
    try {
      PostgresTest.versionSuccessorFactory.retrieveFromDatabase(10);

      PostgresTest.postgresClient.commit();
    } catch (GroundException e) {
      PostgresTest.postgresClient.abort();

      if (!e.getMessage().contains("No VersionSuccessor found with id 10.")) {
        fail();
      }

      throw e;
    }
  }
}
