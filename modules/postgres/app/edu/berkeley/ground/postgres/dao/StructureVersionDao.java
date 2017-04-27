package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.StructureVersion;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class StructureVersionDao {

    public final void create(final Database dbSource, final StructureVersion structureVersion) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("insert into structure_version (id, structure_id) values (%d, %d)", structureVersion.getId(),
                structureVersion.getStructureId()));
        PostgresUtils.executeSqlList(dbSource, sqlList);
    }
}