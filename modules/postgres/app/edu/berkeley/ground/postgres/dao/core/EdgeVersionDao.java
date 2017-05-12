package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.EdgeVersionFactory;
import edu.berkeley.ground.common.model.core.EdgeVersion;
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
    TagDao tagDao = new TagDao();

    //TODO: Ideally, I think this should add to the sqlList to support rollback???

    ItemDao itemDao = new ItemDao(dbSource, idGenerator, versionHistoryDagDao, tagDao);
    PostgresStatements updateVersionList = new PostgresStatements(itemDao.update(newEdgeVersion.getEdgeId(), newEdgeVersion.getId(), parentIds));

    try {
      PostgresStatements statements = super.insert(newEdgeVersion);
      statements.append(String.format(
        "insert into edge_version (id, edge_id) values (%s,%s)",
        uniqueId, edgeVersion.getEdgeId()));
      statements.merge(updateVersionList);

      System.out.println("uniqueId: " + uniqueId);
      System.out.println("edgeId: " + edgeVersion.getEdgeId());

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    return edgeVersion;
  }

  @Override
  public EdgeVersion retrieveFromDatabase(Database dbSource, long id) throws GroundException {
    String sql = String.format("select * from edge_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, EdgeVersion.class);
  }
}
