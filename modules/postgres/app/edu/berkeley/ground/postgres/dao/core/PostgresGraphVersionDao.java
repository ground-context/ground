package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.GraphVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.GraphVersion;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;

public class PostgresGraphVersionDao extends PostgresRichVersionDao<GraphVersion> implements GraphVersionDao {

  private PostgresGraphDao postgresGraphDao;

  public PostgresGraphVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.postgresGraphDao = new PostgresGraphDao(dbSource, idGenerator);
  }

  @Override
  public GraphVersion create(final GraphVersion graphVersion, List<Long> parentIds)
    throws GroundException {

    final long uniqueId = idGenerator.generateVersionId();
    GraphVersion newGraphVersion = new GraphVersion(uniqueId, graphVersion);

    PostgresStatements updateVersionList = this.postgresGraphDao.update(newGraphVersion.getGraphId(), newGraphVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newGraphVersion);
      statements.append(String.format(SqlConstants.INSERT_GRAPH_VERSION, uniqueId, graphVersion.getGraphId()));
      statements.merge(updateVersionList);

      for (Long id : newGraphVersion.getEdgeVersionIds()) {
        statements.append(String.format(SqlConstants.INSERT_GRAPH_VERSION_EDGE, newGraphVersion.getId(), id));
      }

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    return newGraphVersion;
  }

  @Override
  public PostgresStatements delete(long id) {
    PostgresStatements statements = new PostgresStatements();
    statements.append(String.format(SqlConstants.DELETE_ALL_GRAPH_VERSION_EDGES, "graph_version_edge", "graph_version_id", id));
    statements.append(String.format(SqlConstants.DELETE_BY_ID, "graph_version", id));

    PostgresStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }

  @Override
  public GraphVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format(SqlConstants.SELECT_STAR_BY_ID, "graph_version", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.VERSION_NOT_FOUND, this.getType().getSimpleName(), String.format("%d", id));
    }

    GraphVersion graphVersion = Json.fromJson(json.get(0), GraphVersion.class);
    List<Long> edgeIds = new ArrayList<>();
    sql = String.format(SqlConstants.SELECT_GRAPH_VERSION_EDGES, id);

    JsonNode edgeJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    for (JsonNode edge : edgeJson) {
      edgeIds.add(edge.get("edgeVersionId").asLong());
    }

    RichVersion richVersion = super.retrieveFromDatabase(id);
    return new GraphVersion(id, richVersion.getTags(), richVersion.getStructureVersionId(), richVersion.getReference(), richVersion.getParameters(),
                             graphVersion.getGraphId(), edgeIds);
  }
}

