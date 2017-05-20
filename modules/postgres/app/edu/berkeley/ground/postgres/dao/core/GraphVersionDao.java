package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.GraphVersionFactory;
import edu.berkeley.ground.common.model.core.GraphVersion;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.dao.version.TagDao;
import edu.berkeley.ground.postgres.dao.version.VersionHistoryDagDao;
import edu.berkeley.ground.postgres.dao.version.VersionSuccessorDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;

public class GraphVersionDao extends RichVersionDao<GraphVersion> implements GraphVersionFactory {


  public GraphVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public GraphVersion create(final GraphVersion graphVersion, List<Long> parentIds)
    throws GroundException {

    final long uniqueId = idGenerator.generateVersionId();
    GraphVersion newGraphVersion = new GraphVersion(uniqueId, graphVersion.getTags(),
      graphVersion.getStructureVersionId(),
      graphVersion.getReference(), graphVersion.getParameters(), graphVersion.getGraphId(),
      graphVersion.getEdgeVersionIds());

    //TODO: I think we should consider using injection here
    VersionSuccessorDao versionSuccessorDao = new VersionSuccessorDao(dbSource, idGenerator);
    VersionHistoryDagDao versionHistoryDagDao = new VersionHistoryDagDao(dbSource,
      versionSuccessorDao);
    TagDao tagDao = new TagDao(dbSource, idGenerator);

    //TODO: Ideally, I think this should add to the sqlList to support rollback???

    ItemDao itemDao = new ItemDao(dbSource, idGenerator, versionHistoryDagDao, tagDao);
    PostgresStatements updateVersionList = itemDao
      .update(newGraphVersion.getGraphId(), newGraphVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newGraphVersion);
      statements.append(String.format(
        "insert into graph_version (id, graph_id) values (%s,%s)",
        uniqueId, graphVersion.getGraphId()));
      statements.merge(updateVersionList);

      for (Long id : newGraphVersion.getEdgeVersionIds()) {
        statements.append(String.format(
          "insert into graph_version_edge (graph_version_id, edge_version_id) values (%d, %d) " +
            "on conflict do nothing",
          newGraphVersion.getGraphId(), id));
      }

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    return newGraphVersion;
  }

  @Override
  public GraphVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from graph_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Graph Version with id %d does not exist.", id));
    }

    GraphVersion graphVersion = Json.fromJson(json.get(0), GraphVersion.class);

    // Get EdgeIds
    List<Long> edgeIds = new ArrayList<>();
    sql = String.format("select * from graph_version_edge where graph_version_id=%d", id);
    JsonNode edgeJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    for (JsonNode edge : edgeJson) {
      edgeIds.add(edge.get("edgeVersionId").asLong());
    }

    RichVersion richVersion = super.retrieveFromDatabase(id);

    return new GraphVersion(id, richVersion.getTags(), richVersion.getStructureVersionId(),
      richVersion.getReference(),
      richVersion.getParameters(), graphVersion.getGraphId(), edgeIds);
  }
}

