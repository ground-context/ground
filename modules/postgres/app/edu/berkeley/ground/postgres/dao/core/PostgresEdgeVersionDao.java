package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.EdgeVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.model.core.EdgeVersion;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.version.PostgresVersionHistoryDagDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;
import play.libs.Json;

public class PostgresEdgeVersionDao extends PostgresRichVersionDao<EdgeVersion> implements EdgeVersionDao {

  private PostgresEdgeDao postgresEdgeDao;

  public PostgresEdgeVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.postgresEdgeDao = new PostgresEdgeDao(dbSource, idGenerator);
  }

  @Override
  public EdgeVersion create(final EdgeVersion edgeVersion, List<Long> parentIds) throws GroundException {
    final long uniqueId = this.idGenerator.generateVersionId();
    EdgeVersion newEdgeVersion = new EdgeVersion(uniqueId, edgeVersion);

    PostgresStatements updateVersionList = this.postgresEdgeDao.update(newEdgeVersion.getEdgeId(), newEdgeVersion.getId(), parentIds);

    for (long parentId : parentIds) {
      if (parentId != 0) {
        updateVersionList.merge(this.updatePreviousVersion(newEdgeVersion, newEdgeVersion.getEdgeId(), parentId));
      }
    }

    try {
      PostgresStatements statements = super.insert(newEdgeVersion);
      Long fromEndId = edgeVersion.getFromNodeVersionEndId();
      Long toEndId = edgeVersion.getToNodeVersionEndId();

      if (fromEndId == -1) {
        fromEndId = null;
      }

      if (toEndId == -1) {
        toEndId = null;
      }

      statements.append(String.format(SqlConstants.INSERT_EDGE_VERSION, uniqueId, edgeVersion.getEdgeId(), edgeVersion.getFromNodeVersionStartId(),
        fromEndId, edgeVersion.getToNodeVersionStartId(), toEndId));

      statements.merge(updateVersionList);

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }

    return newEdgeVersion;
  }

  @Override
  public PostgresStatements delete(long id) {
    PostgresStatements statements = new PostgresStatements();
    statements.append(String.format(SqlConstants.DELETE_BY_ID, "edge_version", id));

    PostgresStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }

  /**
   * Set the from and to end versions of a previous edge version.
   *
   * @param currentVersion the new version created
   * @param edgeId the id of the edge we're updating
   * @param parentId the id of the parent we're updating
   * @return a set of statements to set the end versions
   */
  private PostgresStatements updatePreviousVersion(EdgeVersion currentVersion, long edgeId, long parentId) throws GroundException {
    PostgresStatements statements = new PostgresStatements();

    PostgresVersionHistoryDagDao versionHistoryDagDao =
      new PostgresVersionHistoryDagDao(this.dbSource, this.idGenerator);

    EdgeVersion parentVersion = this.retrieveFromDatabase(parentId);
    Edge edge = this.postgresEdgeDao.retrieveFromDatabase(edgeId);

    long fromNodeId = edge.getFromNodeId();
    long toNodeId = edge.getToNodeId();

    long fromEndId = -1;
    long toEndId = -1;

    if (parentVersion.getFromNodeVersionEndId() == -1) {
      // update from end id
      VersionHistoryDag dag = versionHistoryDagDao.retrieveFromDatabase(fromNodeId);
      fromEndId = dag.getParent(currentVersion.getFromNodeVersionStartId()).get(0);
    }

    if (parentVersion.getToNodeVersionEndId() == -1) {
      // update to end id
      VersionHistoryDag dag = versionHistoryDagDao.retrieveFromDatabase(toNodeId);
      toEndId = dag.getParent(currentVersion.getToNodeVersionStartId()).get(0);
    }

    if (fromEndId != -1 || toEndId != -1) {
      statements.append(String.format(SqlConstants.UPDATE_EDGE_VERSION, fromEndId, toEndId, parentId));
    }

    return statements;
  }

  @Override
  public EdgeVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format(SqlConstants.SELECT_STAR_BY_ID, "edge_version", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.VERSION_NOT_FOUND, this.getType().getSimpleName(), String.format("%d", id));
    }

    json = json.get(0);
    EdgeVersion edgeVersion = Json.fromJson(json, EdgeVersion.class);
    RichVersion richVersion = super.retrieveFromDatabase(id);

    return new EdgeVersion(id, richVersion, edgeVersion);
  }
}
