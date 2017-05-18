package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.EdgeVersionFactory;
import edu.berkeley.ground.common.model.core.EdgeVersion;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.dao.version.TagDao;
import edu.berkeley.ground.postgres.dao.version.VersionHistoryDagDao;
import edu.berkeley.ground.postgres.dao.version.VersionSuccessorDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.util.LinkedHashMap;
import java.util.List;

public class EdgeVersionDao extends RichVersionDao<EdgeVersion> implements EdgeVersionFactory {


  public EdgeVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  public EdgeVersion create(final EdgeVersion edgeVersion, List<Long> parentIds)
    throws GroundException {

    final long uniqueId = idGenerator.generateVersionId();
    EdgeVersion newEdgeVersion = new EdgeVersion(uniqueId, edgeVersion.getTags(), edgeVersion.getStructureVersionId(),
      edgeVersion.getReference(), edgeVersion.getParameters(), edgeVersion.getEdgeId(),
      edgeVersion.getFromNodeVersionStartId(), edgeVersion.getFromNodeVersionEndId(), edgeVersion
      .getToNodeVersionStartId(), edgeVersion.getToNodeVersionEndId());

    //TODO: I think we should consider using injection here
    VersionSuccessorDao versionSuccessorDao = new VersionSuccessorDao(dbSource, idGenerator);
    VersionHistoryDagDao versionHistoryDagDao = new VersionHistoryDagDao(dbSource, versionSuccessorDao);
    TagDao tagDao = new TagDao(dbSource, idGenerator);

    ItemDao itemDao = new ItemDao(dbSource, idGenerator, versionHistoryDagDao, tagDao);
    PostgresStatements updateVersionList = itemDao.update(newEdgeVersion.getEdgeId(), newEdgeVersion.getId(), parentIds);
    for (long parentId : parentIds) {
      if (parentId != 0) {
        updateVersionList.merge(updatePreviousVersion(parentId, edgeVersion.getFromNodeVersionStartId(),
          edgeVersion.getToNodeVersionStartId()));
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
        "insert into edge_version (id, edge_id, from_node_start_id, from_node_end_id, to_node_start_id, " +
          "to_node_end_id) values (%d, %d, %d, %d, %d, %d)",
        uniqueId, edgeVersion.getEdgeId(), edgeVersion.getFromNodeVersionStartId(), fromEndId,
        edgeVersion.getToNodeVersionStartId(), toEndId));
      statements.merge(updateVersionList);

      System.out.println("uniqueId: " + uniqueId);
      System.out.println("edgeId: " + edgeVersion.getEdgeId());

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
   * @param parentId  the ids of the parent of the child
   * @param fromEndId
   * @param toEndId
   */
  //Should return sqlList and also add edges to the versionHistoryDag
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
    statements.append(String.format("update edge_version set from_node_end_id = %d, to_node_end_id = %d where " +
      "id = %d", fromJson.get(0).get("fromVersionId").asInt(), toJson.get(0).get("fromVersionId").asInt(), parentId));
    return statements;
  }

  @Override
  public EdgeVersion retrieveFromDatabase(long id) throws GroundException {
    ObjectMapper mapper = new ObjectMapper();
    String sql = String.format("select * from edge_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Edge Version with id %d does not exist.", id));
    }

    //TODO: Should call super.retrieveRichVersionData or something
    json = json.get(0);
    sql = String.format("select * from rich_version where id=%d", id);
    JsonNode richVersionJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (richVersionJson.size() == 0) {
      throw new GroundException(String.format("Rich Version with id %d does not exist.", id));
    }
    richVersionJson = richVersionJson.get(0);
    ((ObjectNode) json).set("structureVersionId", richVersionJson.get("structureVersionId"));
    ((ObjectNode) json).set("reference", richVersionJson.get("reference"));
    sql = String.format("select * from rich_version_tag where rich_version_id=%d", id);
    JsonNode tagsJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    ((ObjectNode) json).set("tags", mapper.createObjectNode());
    for (int i = 0; i < tagsJson.size(); i++) {
      ((ObjectNode) json.get("tags")).set(tagsJson.get(i).get("key").toString(), tagsJson.get(i));
    }
    EdgeVersion edgeVersion = Json.fromJson(json, EdgeVersion.class);
    //RichVersion richVersion = super.retrieveFromDatabase(id);
    return edgeVersion;
  }
}
