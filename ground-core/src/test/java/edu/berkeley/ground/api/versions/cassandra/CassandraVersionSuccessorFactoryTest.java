package edu.berkeley.ground.api.versions.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraVersionSuccessorFactoryTest extends CassandraTest {

  public CassandraVersionSuccessorFactoryTest() throws GroundDBException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() {
    try {
      long fromId = 123;
      long toId = 456;

      CassandraConnection connection = super.cassandraClient.getConnection();
      super.versionFactory.insertIntoDatabase(connection, fromId);
      super.versionFactory.insertIntoDatabase(connection, toId);

      VersionSuccessor<?> successor = super.versionSuccessorFactory.create(connection, fromId, toId);

      VersionSuccessor<?> retrieved = super.versionSuccessorFactory.retrieveFromDatabase(connection,
          successor.getId());

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorCreation() throws GroundException {
    long fromId = 123;
    long toId = 456;
    CassandraConnection connection = null;

    // Catch exceptions for these two lines because they should not fal
    try {
      // the main difference is that we're not creating a Version for the toId
      connection = super.cassandraClient.getConnection();
      super.versionFactory.insertIntoDatabase(connection, fromId);
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }

    // This statement should fail because toId is not in the database
    super.versionSuccessorFactory.create(connection, fromId, toId);
  }
}
