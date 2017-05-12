package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.GraphVersionFactory;
import edu.berkeley.ground.common.model.core.GraphVersion;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.dao.version.TagDao;
import edu.berkeley.ground.postgres.dao.version.VersionHistoryDagDao;
import edu.berkeley.ground.postgres.dao.version.VersionSuccessorDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.util.List;

public class GraphVersionDao extends RichVersionDao<GraphVersion> implements GraphVersionFactory {


  public GraphVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  public GraphVersion create(final GraphVersion graphVersion, List<Long> parentIds)
    throws GroundException {

    final long uniqueId = idGenerator.generateVersionId();
    GraphVersion newGraphVersion = new GraphVersion(uniqueId, graphVersion.getTags(), graphVersion.getStructureVersionId(),
      graphVersion.getReference(), graphVersion.getParameters(), graphVersion.getGraphId(),
      graphVersion.getEdgeVersionIds());

    //TODO: I think we should consider using injection here
    VersionSuccessorDao versionSuccessorDao = new VersionSuccessorDao(dbSource, idGenerator);
    VersionHistoryDagDao versionHistoryDagDao = new VersionHistoryDagDao(dbSource, versionSuccessorDao);
    TagDao tagDao = new TagDao();

    //TODO: Ideally, I think this should add to the sqlList to support rollback???

    ItemDao itemDao = new ItemDao(dbSource, idGenerator, versionHistoryDagDao, tagDao);
    PostgresStatements updateVersionList = itemDao.update(newGraphVersion.getGraphId(), newGraphVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newGraphVersion);
      statements.append(String.format(
        "insert into graph_version (id, graph_id) values (%s,%s)",
        uniqueId, graphVersion.getGraphId()));
      statements.merge(updateVersionList);

      System.out.println("uniqueId: " + uniqueId);
      System.out.println("graphId: " + graphVersion.getGraphId());

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    return graphVersion;
  }

  @Override
  public GraphVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from graph_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, GraphVersion.class);
  }
}

