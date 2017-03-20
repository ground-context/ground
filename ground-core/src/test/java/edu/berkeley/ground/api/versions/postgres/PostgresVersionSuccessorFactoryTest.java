package edu.berkeley.ground.api.versions.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresVersionSuccessorFactoryTest extends PostgresTest {

  public PostgresVersionSuccessorFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    try {
      long fromId = 1;
      long toId = 2;

      super.versionFactory.insertIntoDatabase(fromId);
      super.versionFactory.insertIntoDatabase(toId);

      VersionSuccessor<?> successor = super.versionSuccessorFactory.create(fromId, toId);

      VersionSuccessor<?> retrieved = super.versionSuccessorFactory.retrieveFromDatabase(
          successor.getId());

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());
    } finally {
      super.postgresClient.abort();
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
        super.versionFactory.insertIntoDatabase(fromId);
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      // This statement should fail because toId is not in the database
      super.versionSuccessorFactory.create(fromId, toId);
    } finally {
      super.postgresClient.abort();
    }
  }
}
