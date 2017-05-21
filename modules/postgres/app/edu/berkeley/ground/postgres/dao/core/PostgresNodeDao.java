package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.NodeDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
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

    PostgresStatements statements;
    long uniqueId = idGenerator.generateItemId();

    Node newNode = new Node(uniqueId, node);
    try {
      statements = super.insert(newNode);
      statements.append(String.format(SqlConstants.INSERT_GENERIC_ITEM, "node", uniqueId, node.getSourceKey(), node.getName()));
    } catch (Exception e) {
      throw new GroundException(e);
    }

    PostgresUtils.executeSqlList(dbSource, statements);
    return newNode;
  }

  @Override
  public Node retrieveFromDatabase(String sourceKey) throws GroundException {
    String sql = String.format(SqlConstants.SELECT_STAR_BY_SOURCE_KEY, "node", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      // TODO: throw item not found exception
      throw new GroundException(String.format("Node with source_key %s does not exist.", sourceKey));
    }

    return Json.fromJson(json.get(0), Node.class);
  }

  @Override
  public Node retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format(SqlConstants.SELECT_STAR_ITEM_BY_ID, "node", id);
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
