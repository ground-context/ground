package edu.berkeley.ground.dao.versions.cassandra;

import org.junit.Test;

import edu.berkeley.ground.model.CassandraTest;
import edu.berkeley.ground.model.versions.VersionSuccessor;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraVersionSuccessorFactoryTest extends CassandraTest {

  public CassandraVersionSuccessorFactoryTest() throws GroundDBException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    try {
      long fromId = 123;
      long toId = 456;

      super.versionFactory.insertIntoDatabase(fromId);
      super.versionFactory.insertIntoDatabase(toId);

      VersionSuccessor<?> successor = super.versionSuccessorFactory.create(fromId, toId);

      VersionSuccessor<?> retrieved = super.versionSuccessorFactory.retrieveFromDatabase(
          successor.getId());

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());
    } finally {
      super.cassandraClient.abort();
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
        super.versionFactory.insertIntoDatabase(fromId);
      } catch (GroundException ge) {
        super.cassandraClient.abort();

        fail(ge.getMessage());
      }

      // This statement should fail because toId is not in the database
      super.versionSuccessorFactory.create(fromId, toId);
    } finally {
      super.cassandraClient.abort();
    }
  }
}
