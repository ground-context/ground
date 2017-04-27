package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Structure;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class StructureDao {

    public final void create(final Database dbSource, final Structure structure) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("insert into structure (item_id, source_key, name) values (%d, '%s', '%s')", structure.getId(),
                structure.getSourceKey(), structure.getName()));
        PostgresUtils.executeSqlList(dbSource, sqlList);
    }
}