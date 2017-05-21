package edu.berkeley.ground.postgres.dao.core;

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
    super.verifyItemNotExists(node.getSourceKey());

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
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Node node = this.retrieveFromDatabase(sourceKey);
    return super.getLeaves(node.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }

}
