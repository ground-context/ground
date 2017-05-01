package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.version.ItemFactory;
import edu.berkeley.ground.lib.model.version.Item;
import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import play.db.Database;

public class ItemDao<T extends Item> implements ItemFactory<T> {
  @Override
  public T retrieveFromDatabase(Database dbSource, long id) throws GroundException {
    return null;
  };

  @Override
  public T retrieveFromDatabase(Database dbSource, String sourceKey) throws GroundException {
    return null;
  };

  @Override
  public Class<T> getType() {
    return null;
  };

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    return null;
  };

  /**
   * Add a new Version to this Item. The provided parentIds will be the parents of this particular
   * version. What's provided in the default case varies based on which database we are writing
   * into.
   *
   * @param itemId the id of the Item we're updating
   * @param childId the new version's id
   * @param parentIds the ids of the parents of the child
   */
  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
  };

  /**
   * Truncate the item to only have the most recent levels.
   *
   * @param numLevels the levels to keep
   * @throws GroundException an error while removing versions
   */
  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
  };

  public void create(final Database dbSource, final T item, final IdGenerator idGenerator) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(
        String.format(
            "insert into item (id) values (%d)",
            item.getId()));
    final Map<String, Tag> tags = item.getTags();
    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);

      if (tag.getValue() != null) {
        sqlList.add(
        String.format(
            "insert into item_tag (item_id, key, value, type) values (%d, %s, %s, %s)",
            item.getId(), key, tag.getValue().toString(), tag.getValueType().toString()));
      } else {
        sqlList.add(
        String.format(
            "insert into item_tag (item_id, key, value, type) values (%d, %s, %s, %s)",
            item.getId(), key, null, null));
      }
    }
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }
}
