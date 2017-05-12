package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.NodeFactory;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.util.List;


// TODO construct me with dbSource and idGenerator thanks
public class NodeDao extends ItemDao<Node> implements NodeFactory {

  public NodeDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  public Node create(Node node) throws GroundException {

    PostgresStatements postgresStatements = new PostgresStatements();
    long uniqueId = idGenerator.generateItemId();

    Node newNode = new Node(uniqueId, node.getName(), node.getSourceKey(), node.getTags());
    try {
      postgresStatements.append(String.format(
        "insert into node (item_id, source_key, name) values (%s,\'%s\',\'%s\')",
        uniqueId, node.getSourceKey(), node.getName()));

      super.insert(newNode).merge(postgresStatements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    PostgresUtils.executeSqlList(dbSource, postgresStatements);
    return newNode;
  }

  @Override
  public Node retrieveFromDatabase(String sourceKey) throws GroundException {
    String sql =
      String.format("select * from node where source_key=\'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, Node.class);
  }

  @Override
  public Node retrieveFromDatabase(long id) throws GroundException {
    String sql =
      String.format("select * from node where item_id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, Node.class);
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Node node  = retrieveFromDatabase(sourceKey);
    return super.getLeaves(node.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }

}
