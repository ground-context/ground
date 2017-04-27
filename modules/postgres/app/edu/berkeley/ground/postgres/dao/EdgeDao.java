package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Edge;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class EdgeDao {

    public final void create(final Database dbSource, final Edge edge) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("insert into edge (item_id, source_key, from_node_id, to_node_id, name) values (%d, '%s', %d, %d, '%s')", edge.getId(),
                edge.getSourceKey(), edge.getFromNodeId(),edge.getToNodeId(),edge.getName()));
        PostgresUtils.executeSqlList(dbSource, sqlList);
    }
}