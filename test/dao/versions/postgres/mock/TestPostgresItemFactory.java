package dao.versions.postgres.mock;

import java.util.List;

import dao.models.postgres.PostgresTagFactory;
import dao.versions.postgres.PostgresItemFactory;
import dao.versions.postgres.PostgresVersionHistoryDagFactory;
import db.PostgresClient;
import exceptions.GroundException;
import models.versions.Item;

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
