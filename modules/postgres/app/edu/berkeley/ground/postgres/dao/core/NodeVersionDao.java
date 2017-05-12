package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.NodeVersionFactory;
import edu.berkeley.ground.common.model.core.NodeVersion;
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

public class NodeVersionDao extends RichVersionDao<NodeVersion> implements NodeVersionFactory {

  public NodeVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  public NodeVersion create(final NodeVersion nodeVersion, List<Long> parentIds)
      throws GroundException {

    final long uniqueId = idGenerator.generateVersionId();
    NodeVersion newNodeVersion = new NodeVersion(uniqueId, nodeVersion.getTags(), nodeVersion.getStructureVersionId(),
      nodeVersion.getReference(), nodeVersion.getParameters(), nodeVersion.getNodeId());

    //TODO: I think we should consider using injection here
    VersionSuccessorDao versionSuccessorDao = new VersionSuccessorDao(dbSource, idGenerator);
    VersionHistoryDagDao versionHistoryDagDao = new VersionHistoryDagDao(dbSource, versionSuccessorDao);
    TagDao tagDao = new TagDao();

    //TODO: Ideally, I think this should add to the sqlList to support rollback???
    ItemDao itemDao = new ItemDao(dbSource, idGenerator, versionHistoryDagDao, tagDao);
    PostgresStatements updateVersionList = itemDao.update(newNodeVersion.getNodeId(), newNodeVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newNodeVersion);
      statements.append(String.format(
        "insert into node_version (id, node_id) values (%d,%d)",
        uniqueId, nodeVersion.getNodeId()));
      statements.merge(updateVersionList);

      System.out.println("uniqueId: " + uniqueId);
      System.out.println("nodeId: " + nodeVersion.getNodeId());

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      e.printStackTrace();
      throw new GroundException(e);
    }
    return newNodeVersion;
  }

  @Override
  public NodeVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from node_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, NodeVersion.class);
  }
}
