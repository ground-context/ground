package edu.berkeley.ground.postgres.dao;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.core.NodeFactory;
import edu.berkeley.ground.lib.factory.version.TagFactory;
import edu.berkeley.ground.lib.model.core.Node;
import edu.berkeley.ground.lib.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresClient;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;


public class NodeDao extends ItemDao<Node> implements NodeFactory {

  private PostgresClient dbClient;
  private VersionHistoryDagDao versionHistoryDagDao;
  private TagFactory tagFactory;

  public NodeDao() {}

  public NodeDao(PostgresClient dbClient,
                        VersionHistoryDagDao versionHistoryDagDao,
                        TagFactory tagFactory) {
    this.dbClient = dbClient;
    this.versionHistoryDagDao = versionHistoryDagDao;
    this.tagFactory = tagFactory;
  }

  @Override
  public void create(Database dbSource, Node node, IdGenerator idGenerator) throws GroundException {

    final List<String> sqlList = new ArrayList<>();
    long uniqueId = idGenerator.generateItemId();

    Node newNode = new Node(uniqueId, node.getName(), node.getSourceKey(), node.getTags());
    try {
      sqlList.addAll(super.createSqlList(newNode));
      sqlList.add(
        String.format(
          "insert into node (item_id, source_key, name) values (%s,\'%s\',\'%s\')",
          uniqueId, node.getSourceKey(), node.getName()));
    } catch (Exception e) {
      throw new GroundException(e);
    }
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }

  @Override
  public Node retrieveFromDatabase(Database dbSource, String sourceKey) throws GroundException {
    String sql =
      String.format("select * from node where source_key=\'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, Node.class);
  }

  @Override
  public Node retrieveFromDatabase(Database dbSource, long id) throws GroundException {
    String sql =
      String.format("select * from node where item_id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, Node.class);
  }

  @Override
  public void update(IdGenerator idGenerator, long itemId, long childId, List<Long> parentIds) throws GroundException {
    super.update(idGenerator, itemId, childId, parentIds);
  }

  @Override
  public List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException {
    Node node  = retrieveFromDatabase(dbSource, sourceKey);
    return super.getLeaves(dbSource, node.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }

}
