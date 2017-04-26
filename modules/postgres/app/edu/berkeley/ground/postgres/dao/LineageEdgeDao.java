package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.usage.LineageEdge;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class LineageEdgeDao {

    public final void create(final Database dbSource, final LineageEdge lineageEdge) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("insert into lineage_edge (item_id, source_key, name) values (%d, '%s', '%s')",
                lineageEdge.getId(), lineageEdge.getSourceKey(), lineageEdge.getName()));
        PostgresUtils.executeSqlList(dbSource, sqlList);
    }
}

