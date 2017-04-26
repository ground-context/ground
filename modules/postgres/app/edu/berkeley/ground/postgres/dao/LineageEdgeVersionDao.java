package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class LineageEdgeVersionDao {

    public final void create(final Database dbSource, final LineageEdgeVersion lineageEdgeVersion) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("insert into lineage_edge_version (id, lineage_edge_id, from_rich_version_id," +
                        "to_rich_version_id) values (%d, %d, %d, %d)", lineageEdgeVersion.getId(),
                lineageEdgeVersion.getLineageEdgeId(), lineageEdgeVersion.getFromId(), lineageEdgeVersion.getToId()));
        PostgresUtils.executeSqlList(dbSource, sqlList);
    }

}
