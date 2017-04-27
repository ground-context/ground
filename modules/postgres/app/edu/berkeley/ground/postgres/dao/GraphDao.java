package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Graph;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;

public class GraphDao {

  public final void create(final Database dbSource, final Graph graph) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(
        String.format(
            "insert into graph (item_id, source_key, name) values (%d, '%s', '%s')",
            graph.getId(), graph.getSourceKey(), graph.getName()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }
}
