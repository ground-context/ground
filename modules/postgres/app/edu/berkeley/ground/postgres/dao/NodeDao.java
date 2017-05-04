package edu.berkeley.ground.postgres.dao;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.util.IdGenerator;
import edu.berkeley.ground.lib.factory.core.NodeFactory;
import edu.berkeley.ground.lib.model.core.Node;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;


public class NodeDao  extends ItemDao<Node> implements NodeFactory {

  @Override
  public void create(Database dbSource, Node node, IdGenerator idGenerator) throws GroundException {

    final List<String> sqlList = new ArrayList<>();
    // Call super.create(dbSource, something) to ensure that a unique item is created

    long uniqueId = idGenerator.generateItemId();
    sqlList.add(
      String.format(
        "insert into node (item_id, source_key, name) values (%s,\'%s\',\'%s\')",
        node.getItemId(), node.getSourceKey(), node.getName()));

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
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    super.update(itemId, childId, parentIds);
  }

  @Override
  public List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException {
    Node node  = retrieveFromDatabase(dbSource, sourceKey);
    return super.getLeaves(dbSource, node.getId());
  }

}
