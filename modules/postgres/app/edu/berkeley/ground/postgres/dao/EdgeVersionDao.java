package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.EdgeVersion;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class EdgeVersionDao {

    public final void create(final Database dbSource, final EdgeVersion edgeVersion) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("insert into edge_version (id, edge_id, from_node_start_id, from_node_end_id, to_node_start_id, to_node_end_id) values (%d,%d, %d, %d, %d, %d)", edgeVersion.getId(),
                edgeVersion.getEdgeId(), edgeVersion.getFromNodeVersionStartId(), edgeVersion.getFromNodeVersionEndId(), edgeVersion.getToNodeVersionStartId(), edgeVersion.getToNodeVersionEndId()));
        PostgresUtils.executeSqlList(dbSource, sqlList);
    }
}