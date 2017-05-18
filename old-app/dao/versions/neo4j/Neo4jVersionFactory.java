package dao.versions.neo4j;

import exceptions.GroundException;
import org.neo4j.driver.v1.Record;

import dao.versions.VersionFactory;
import exceptions.GroundVersionNotFoundException;
import models.versions.Version;

public abstract class Neo4jVersionFactory<T extends Version> implements VersionFactory<T> {
  public void insertIntoDatabase(long id) throws GroundException {
    // this should never be called because we never explicitly insert versions into Neo4j
    throw new GroundException("This operation should never be called");
  }

  /**
   * Verify that a result for a version is not empty.
   *
   * @param record the result to check
   * @param id the id of the version
   * @throws GroundVersionNotFoundException an exception indicating the item wasn't found
   */
  protected void verifyResultSet(Record record, long id) throws GroundVersionNotFoundException {

    if (record == null) {
      throw new GroundVersionNotFoundException(this.getType(), id);
    }
  }
}
