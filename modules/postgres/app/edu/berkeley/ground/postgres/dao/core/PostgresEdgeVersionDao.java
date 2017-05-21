package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.EdgeVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.EdgeVersion;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
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
        updateVersionList.merge(this.updatePreviousVersion(parentId, edgeVersion.getFromNodeVersionStartId(), edgeVersion.getToNodeVersionStartId()));
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

  /**
   * Set the from and to end versions of a previous edge version.
   *
   * @param id the id of the version to update
   * @param fromEndId the new from end version
   * @param toEndId the new to end version
   * @return a set of statements to set the end versions
   */
  @Override
  public PostgresStatements updatePreviousVersion(long id, long fromEndId, long toEndId) {
    PostgresStatements statements = new PostgresStatements();
    statements.append(String.format(SqlConstants.UPDATE_EDGE_VERSION, fromEndId, toEndId, id));

    return statements;
  }

  @Override
  public EdgeVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format(SqlConstants.SELECT_STAR_BY_ID, "edge_version", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(String.format("Edge Version with id %d does not exist.", id));
    }

    json = json.get(0);
    EdgeVersion edgeVersion = Json.fromJson(json, EdgeVersion.class);
    RichVersion richVersion = super.retrieveFromDatabase(id);

    // TODO: clean this up
    return new EdgeVersion(id, richVersion.getTags(), richVersion.getStructureVersionId(), richVersion.getReference(), richVersion.getParameters(),
                            edgeVersion.getEdgeId(), edgeVersion.getFromNodeVersionStartId(), edgeVersion.getFromNodeVersionEndId(),
                            edgeVersion.getToNodeVersionStartId(), edgeVersion.getToNodeVersionEndId());
  }
}
