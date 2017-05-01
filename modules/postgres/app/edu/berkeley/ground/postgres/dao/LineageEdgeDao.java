package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.usage.LineageEdgeFactory;
import edu.berkeley.ground.lib.model.usage.LineageEdge;
import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class LineageEdgeDao extends ItemDao<LineageEdge> implements LineageEdgeFactory {

  public final LineageEdge create(final String name, final String sourceKey, final Map<String, Tag> tags) throws GroundException {
    LineageEdge lineageEdge = null;
    return lineageEdge;
  }

  @Override
  public final void create(final Database dbSource, final LineageEdge lineageEdge, final IdGenerator idGenerator) throws GroundException {
    super.create(dbSource, lineageEdge, idGenerator);
    final List<String> sqlList = new ArrayList<>();
    long uniqueId = idGenerator.generateItemId();
    sqlList.add(String.format("insert into lineage_edge (item_id, source_key, name) values (%d, '%s', '%s')",
      uniqueId, lineageEdge.getSourceKey(), lineageEdge.getName()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }

  @Override
  public LineageEdge retrieveFromDatabase(final Database dbSource, String sourceKey) throws GroundException {
    String sql = String.format("select * from lineage_edge where source_key = \'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, LineageEdge.class);
  }

  @Override
  public LineageEdge retrieveFromDatabase(final Database dbSource, long id) throws GroundException {
    String sql = String.format("select * from lineage_edge where id = %d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, LineageEdge.class);
  }

  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    return new ArrayList<>();
  }

  public void truncate(long itemId, int numLevels) throws GroundException {
  }

}

