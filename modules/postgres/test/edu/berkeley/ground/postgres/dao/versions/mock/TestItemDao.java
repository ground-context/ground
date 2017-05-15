package edu.berkeley.ground.postgres.dao.versions.mock;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.dao.version.TagDao;
import edu.berkeley.ground.postgres.dao.version.VersionHistoryDagDao;
import play.db.Database;

import java.util.List;

public class TestItemDao extends ItemDao<Item> {

  public TestItemDao(Database dbSource, IdGenerator idGenerator,
    VersionHistoryDagDao versionHistoryDagDao,
    TagDao tagDao) {

    super(dbSource, idGenerator, versionHistoryDagDao, tagDao);
  }

  public Class<Item> getType() {
    return Item.class;
  }

  public Item retrieveFromDatabase(String sourceKey) throws GroundException {
    throw new GroundException("This method should never be called.");
  }

  public List<Long> getLeaves(String sourceKey) throws GroundException {
    throw new GroundException("This method should never be called.");
  }
}
