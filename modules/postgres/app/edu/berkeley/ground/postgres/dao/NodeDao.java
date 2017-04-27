package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Node;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;

public class NodeDao {

  public final void create(final Database dbSource, final Node node) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    // Need to create a unique item id
    long uniqueItemId = 2L;

    sqlList.add(
        String.format(
            "insert into node (item_id, source_key, name) values (%s,%s,%s)",
            uniqueItemId, node.getSourceKey(), node.getName()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }
}
