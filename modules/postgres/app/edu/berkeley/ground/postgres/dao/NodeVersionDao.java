package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.NodeVersion;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import javax.sql.DataSource;

public class NodeVersionDao extends RichVersionDao<NodeVersion> implements NodeVersionFactory {

  public final void insertIntoDatabase(final Database dbSource, final NodeVersion nodeVersion, IdGenerator idGenerator)
      throws GroundException {
    final List<String> sqlList = new ArrayList<>();

    //Call super.create to ensure that versions exist
    final long uniqueId = idGenerator.generateVersionId();
    NodeVersion newNodeVersion = new NodeVersion(uniqueId, nodeVersion.getTags(), nodeVersion.getStructureVersionId(),
      nodeVersion.getReference(), nodeVersion.getParameters(), nodeVersion.getNodeId());

    //Call super to create 1.version, 2. structure version (need to create a node_id)?, 3. rich version, 4. node_version
    try {
      sqlList.addAll(super.createSqlList(newNodeVersion));
      sqlList.add(
        String.format(
          "insert into node_version (id, node_id) values (%s,%s)",
          uniqueId, nodeVersion.getNodeId()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
      PostgresUtils.executeSqlList(dbSource, sqlList);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public NodeVersion retrieveFromDatabase(Database dbSource, long id) throws GroundException {
    String sql = String.format("select * from node_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, NodeVersion.class);
  }
}
