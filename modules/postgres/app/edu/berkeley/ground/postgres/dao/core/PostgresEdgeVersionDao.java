package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.EdgeVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.EdgeVersion;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.util.IdGenerator;
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
      statements.append(String.format(
        "insert into edge_version (id, edge_id, from_node_version_start_id, from_node_version_end_id, "
          + "to_node_version_start_id, to_node_version_end_id) values (%d, %d, %d, %d, %d, %d)",
        uniqueId, edgeVersion.getEdgeId(), edgeVersion.getFromNodeVersionStartId(), fromEndId,
        edgeVersion.getToNodeVersionStartId(), toEndId));

      statements.merge(updateVersionList);

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }

    return newEdgeVersion;
  }

  /**
   * Add a new Version to this Item. The provided parentIds will be the parents of this particular
   * version. What's provided in the default case varies based on which database we are writing
   * into.
   *
   * @param parentId the ids of the parent of the child
   */
  private PostgresStatements updatePreviousVersion(long parentId, long fromEndId, long toEndId) throws GroundException {
    String sql = String.format("select * from version_successor where to_version_id=%d", fromEndId);
    JsonNode fromJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (fromJson.size() == 0) {
      throw new GroundException(String.format("Version Successor with to_version_id %d does not exist.", fromEndId));
    }

    sql = String.format("select * from version_successor where to_version_id=%d", toEndId);
    JsonNode toJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (toJson.size() == 0) {
      throw new GroundException(String.format("Version Successor with to_version_id %d does not exist.", toEndId));
    }

    PostgresStatements statements = new PostgresStatements();
    statements.append(String.format("update edge_version set from_node_version_end_id = %d, to_node_version_end_id = %d where"
                                      + " id = %d", fromJson.get(0).get("fromVersionId").asInt(), toJson.get(0).get("fromVersionId").asInt(),
      parentId));

    return statements;
  }

  @Override
  public EdgeVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from edge_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(String.format("Edge Version with id %d does not exist.", id));
    }

    json = json.get(0);
    EdgeVersion edgeVersion = Json.fromJson(json, EdgeVersion.class);
    RichVersion richVersion = super.retrieveFromDatabase(id);

    return new EdgeVersion(id, richVersion.getTags(), richVersion.getStructureVersionId(),
                            richVersion.getReference(), richVersion.getParameters(), edgeVersion.getEdgeId(),
                            edgeVersion.getFromNodeVersionStartId(), edgeVersion.getFromNodeVersionEndId(),
                            edgeVersion.getToNodeVersionStartId(), edgeVersion.getToNodeVersionEndId());
  }
}
