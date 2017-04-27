package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.util.IdGenerator;
import edu.berkeley.ground.lib.model.core.Node;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;

public class NodeDao {

  public final void create(final Database dbSource, final Node node) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    // Call super.create(dbSource, something) to ensure that a unique item is created

    sqlList.add(
      String.format(
        "insert into node (item_id, source_key, name) values (%s,\'%s\',\'%s\')",
        node.getItemId(), node.getSourceKey(), node.getName()));

    PostgresUtils.executeSqlList(dbSource, sqlList);
  }
}
