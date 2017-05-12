/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.postgres.dao;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.EdgeFactory;
import edu.berkeley.ground.common.factory.version.TagFactory;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresClient;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;

public class EdgeDao extends ItemDao<Edge> implements EdgeFactory {

  private PostgresClient dbClient;
  private VersionHistoryDagDao versionHistoryDagDao;
  private TagFactory tagFactory;

  public EdgeDao() {}

  public EdgeDao(PostgresClient dbClient,
                        VersionHistoryDagDao versionHistoryDagDao,
                        TagFactory tagFactory) {
    this.dbClient = dbClient;
    this.versionHistoryDagDao = versionHistoryDagDao;
    this.tagFactory = tagFactory;
  }


	@Override
	public void create(Database dbSource, Edge edge, IdGenerator idGenerator) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        long uniqueId = idGenerator.generateItemId();
        Edge newEdge = new Edge(uniqueId, edge.getName(), edge.getSourceKey(), edge.getFromNodeId(), edge.getToNodeId(), edge.getTags());
        try {
        	sqlList.addAll(super.createSqlList(newEdge));
        	sqlList.add(
            String.format(
              "insert into edge (item_id, source_key, from_node_id, to_node_id, name) values (%d, \'%s\', %d, %d, \'%s\')", uniqueId,
                edge.getSourceKey(), edge.getFromNodeId(), edge.getToNodeId(),edge.getName()));

    	} catch (Exception e) {
    		throw new GroundException(e);
    	}
      PostgresUtils.executeSqlList(dbSource, sqlList);
   	}
   	@Override
   	public Edge retrieveFromDatabase(Database dbSource, String sourceKey) throws GroundException {
   		String sql =
        String.format("select * from edge where source_key = \'%s\'", sourceKey);
   		JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
   		return Json.fromJson(json, Edge.class);
   	}
   	@Override
  	public Edge retrieveFromDatabase(Database dbSource, long id) throws GroundException {
    	String sql =
        String.format("select * from edge where item_id = %d", id);
    	JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    	return Json.fromJson(json, Edge.class);
    }

  	//public Edge retrieveFromDatabase(Database dbSource, long fromNodeId) throws GroundException {
    	//String sql = String.format("select * from edge where from_node_id = %d", fromNodeId);
    	//JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    	//return Json.fromJson(json, Edge.class);
    //}

  	//public Edge retrieveFromDatabase(Database dbSource, long toNodeId) throws GroundException {
    	//String sql = String.format("select * from edge where to_node_id = %d", toNodeId);
    	//JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    	//return Json.fromJson(json, Edge.class);
    //}

    //@Override
    //public void update(IdGenerator idGenerator, long itemId, long childId, List<Long> parentIds) throws GroundException {
        //super.update(idGenerator, itemId, childId, parentIds);
    //}
    @Override
    public List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException {
        Edge edge  = retrieveFromDatabase(dbSource, sourceKey);
        return super.getLeaves(dbSource, edge.getId());
    }

    @Override
    public void truncate(long itemId, int numLevels) throws GroundException {
      super.truncate(itemId, numLevels);
  }
}
