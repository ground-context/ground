package edu.berkeley.ground.dao.versions.postgres.mock;

import java.util.List;

import edu.berkeley.ground.dao.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionHistoryDagFactory;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.Item;

public class TestPostgresItemFactory extends PostgresItemFactory<Item> {

  public TestPostgresItemFactory(PostgresClient postgresClient,
                                  PostgresVersionHistoryDagFactory versionHistoryDagFactory,
                                  PostgresTagFactory tagFactory) {

    super(postgresClient, versionHistoryDagFactory, tagFactory);
  }

  public Class<Item> getType() {
    return Item.class;
  }

  public Item retrieveFromDatabase(long id) throws GroundException {
    return new Item(id, super.retrieveItemTags(id));
  }

  public Item retrieveFromDatabase(String sourceKey) throws GroundException {
    throw new GroundException("This method should never be called.");
  }

  public List<Long> getLeaves(String sourceKey) throws GroundException {
    throw new GroundException("This method should never be called.");
  }
}
