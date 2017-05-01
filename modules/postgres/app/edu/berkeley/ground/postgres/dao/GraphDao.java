package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Graph;
import edu.berkeley.ground.lib.factory.core.GraphFactory;
import edu.berkeley.ground.postgres.dao.ItemDao;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import edu.berkeley.ground.lib.model.version.GroundType;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class GraphDao extends ItemDao<Graph> implements GraphFactory {

  @Override
  public void create(final Database dbSource, final Graph graph, final IdGenerator idGenerator) throws GroundException {
    super.create(dbSource, graph, idGenerator);
    final List<String> sqlList = new ArrayList<>();
    long uniqueId = idGenerator.generateItemId();
    sqlList.add(
        String.format(
            "insert into graph (item_id, source_key, name) values (%d, '%s', '%s')",
            uniqueId, graph.getSourceKey(), graph.getName()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }

  @Override
  public Graph retrieveFromDatabase(final Database dbSource, String sourceKey) throws GroundException {
  	String sql = String.format("select * from graph where source_key = \'%s\'", sourceKey);
  	JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
  	return Json.fromJson(json, Graph.class);
  }

  @Override
  public Graph retrieveFromDatabase(final Database dbSource, long id) throws GroundException {
  	String sql = String.format("select * from graph_version where id = %d", id);
  	JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
  	return Json.fromJson(json, Graph.class);
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
