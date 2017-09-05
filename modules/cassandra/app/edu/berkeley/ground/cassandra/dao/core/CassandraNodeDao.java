package edu.berkeley.ground.cassandra.dao.core;

import edu.berkeley.ground.common.dao.core.NodeDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.dao.version.CassandraItemDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.List;


public class CassandraNodeDao extends CassandraItemDao<Node> implements NodeDao {

  public CassandraNodeDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Node> getType() {
    return Node.class;
  }

  @Override
  public Node create(Node node) throws GroundException {
    super.verifyItemNotExists(node.getSourceKey());

    CassandraStatements statements;
    long uniqueId = idGenerator.generateItemId();

    Node newNode = new Node(uniqueId, node);
    try {
      statements = super.insert(newNode);

      String name = node.getName();

      if (name != null) {
        statements.append(String.format(CqlConstants.INSERT_GENERIC_ITEM_WITH_NAME, "node", uniqueId, node.getSourceKey(), name));
      } else {
        statements.append(String.format(CqlConstants.INSERT_GENERIC_ITEM_WITHOUT_NAME, "node", uniqueId, node.getSourceKey()));
      }
    } catch (Exception e) {
      throw new GroundException(e);
    }

    CassandraUtils.executeCqlList(dbSource, statements);
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
