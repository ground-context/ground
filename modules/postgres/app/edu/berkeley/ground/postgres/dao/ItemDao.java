package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.version.Item;
import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import play.db.Database;

public class ItemDao {

  public final void create(final Database dbSource, final Item item, final IdGenerator idGenerator) throws GroundException {
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