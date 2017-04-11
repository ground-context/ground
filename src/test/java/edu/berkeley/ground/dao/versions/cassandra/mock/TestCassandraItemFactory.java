package edu.berkeley.ground.dao.versions.cassandra.mock;

import java.util.List;

import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.Item;

public class TestCassandraItemFactory extends CassandraItemFactory<Item> {

  public TestCassandraItemFactory(CassandraClient cassandraClient,
                                   CassandraVersionHistoryDagFactory versionHistoryDagFactory,
                                   CassandraTagFactory tagFactory) {

    super(cassandraClient, versionHistoryDagFactory, tagFactory);
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
