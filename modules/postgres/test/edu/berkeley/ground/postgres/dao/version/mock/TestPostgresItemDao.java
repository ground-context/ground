package edu.berkeley.ground.postgres.dao.version.mock;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.dao.version.PostgresTagDao;
import play.db.Database;

public class TestPostgresItemDao extends PostgresItemDao<Item> {

  public TestPostgresItemDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Item> getType() {
    return Item.class;
  }

  @Override
  public Item retrieveFromDatabase(long id) throws GroundException {
    return new Item(id, new PostgresTagDao(dbSource).retrieveFromDatabaseByItemId(id));
  }

  @Override
  public Item retrieveFromDatabase(String sourceKey) throws GroundException {
    throw new GroundException("This method should never be called.");
  }
}
