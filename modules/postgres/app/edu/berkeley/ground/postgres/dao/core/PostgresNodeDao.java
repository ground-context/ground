package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.NodeDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.HashMap;
import java.util.List;
import play.db.Database;
import play.libs.Json;


public class PostgresNodeDao extends PostgresItemDao<Node> implements NodeDao {

  public PostgresNodeDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Node> getType() {
    return Node.class;
  }

  @Override
  public Node create(Node node) throws GroundException {

    PostgresStatements statements = new PostgresStatements();
    long uniqueId = idGenerator.generateItemId();
    statements.append(
      String.format("insert into node (item_id, source_key, name) values (%s,\'%s\',\'%s\')", node.getItemId(), node.getSourceKey(), node.getName()));

    Node newNode = new Node(uniqueId, node);
    try {
      statements = super.insert(newNode);
      statements.append(
        String.format("insert into node (item_id, source_key, name) values (%s,\'%s\',\'%s\')", uniqueId, node.getSourceKey(), node.getName()));
    } catch (Exception e) {
      throw new GroundException(e);
    }

    PostgresUtils.executeSqlList(dbSource, statements);
    return newNode;
  }

  @Override
  public Node retrieveFromDatabase(String sourceKey) throws GroundException {
    String sql = String.format("select * from node where source_key=\'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(String.format("Node with source_key %s does not exist.", sourceKey));
    }

    json = json.get(0);
    Node node = Json.fromJson(json, Node.class);

    // TODO: Refactor this up, and call it in all methods lol
    sql = String.format("select * from item_tag where item_id=%d", node.getId());
    JsonNode tagsJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    HashMap<String, Tag> tags = new HashMap<>();
    for (JsonNode tag : tagsJson) {
      GroundType type = GroundType.fromString(tag.get("type").asText());
      tags.put(tag.get("key").asText(), new Tag(tag.get("itemId").asLong(), tag.get("key").asText(), tag.get("value").asText(), type));
    }
    node = new Node(node.getId(), node.getName(), node.getSourceKey(), tags);
    return node;
  }

  @Override
  public Node retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from node where item_id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Node with id %d does not exist.", id));
    }
    return Json.fromJson(json.get(0), Node.class);
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Node node = retrieveFromDatabase(sourceKey);
    return super.getLeaves(node.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }

}
