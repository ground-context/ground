package edu.berkeley.ground.cassandra.dao.version.mock;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.version.CassandraItemDao;
import edu.berkeley.ground.cassandra.dao.version.CassandraTagDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
// import play.db.Database;

public class TestCassandraItemDao extends CassandraItemDao<Item> {

  public TestCassandraItemDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Item> getType() {
    return Item.class;
  }

  @Override
  public Item retrieveFromDatabase(long id) throws GroundException {
    return new Item(id, new CassandraTagDao(dbSource).retrieveFromDatabaseByItemId(id));
  }

  @Override
  public Item retrieveFromDatabase(String sourceKey) throws GroundException {
    throw new GroundException(ExceptionType.OTHER, "This method should never be called.");
  }

  @Override
  public Item create(Item item) throws GroundException {
    CassandraUtils.executeCqlList(this.dbSource, this.insert(new Item(item.getId(), item.getTags())));

    return item;
  }
}
