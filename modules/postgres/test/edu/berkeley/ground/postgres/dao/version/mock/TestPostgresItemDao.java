package edu.berkeley.ground.postgres.dao.version.mock;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.dao.version.PostgresTagDao;
import edu.berkeley.ground.postgres.util.PostgresUtils;
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
    throw new GroundException(ExceptionType.OTHER, "This method should never be called.");
  }

  @Override
  public Item create(Item item) throws GroundException {
    PostgresUtils.executeSqlList(this.dbSource, this.insert(new Item(item.getId(), item.getTags())));

    return item;
  }
}
