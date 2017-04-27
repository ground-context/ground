package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.GraphVersion;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class GraphVersionDao {

    public final void create(final Database dbSource, final GraphVersion graphVersion) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("insert into graph_version (id, graph_id) values (%d, %d)", graphVersion.getId(),
                graphVersion.getGraphId()));
        PostgresUtils.executeSqlList(dbSource, sqlList);
    }
}