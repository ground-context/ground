package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.NodeVersion;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;

public class NodeVersionDao {

  public final void create(final Database dbSource, final NodeVersion nodeVersion)
      throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(
        String.format(
            "insert into node_version (id, node_id) values (%s,%s)",
            nodeVersion.getId(), nodeVersion.getNodeId()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }
}
