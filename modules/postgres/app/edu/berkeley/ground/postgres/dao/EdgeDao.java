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

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Edge;
import edu.berkeley.ground.lib.factory.core.EdgeFactory;
import edu.berkeley.ground.postgres.dao.ItemDao;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import edu.berkeley.ground.lib.model.version.GroundType;
import play.db.Database;
import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class EdgeDao extends ItemDao<Edge> implements EdgeFactory {

	@Override
	public void create(final Database dbSource, final Edge edge, final IdGenerator idGenerator) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        long uniqueId = idGenerator.generateItemId();
        Edge newEdge = new Edge(uniqueId, edge.getName(), edge.getSourceKey(), edge.getTags());
        try {
        	sqlList.addAll(super.createSqlList(newEdge));
        	sqlList.add(String.format("insert into edge (item_id, source_key, from_node_id, to_node_id, name) values (%d, '%s', %d, %d, '%s')", uniqueId,
                edge.getSourceKey(), edge.getFromNodeId(),edge.getToNodeId(),edge.getName()));
    	    PostgresUtils.executeSqlList(dbSource, sqlList);
    	} catch (Exception e) {
    		throw new GroundException(e);
    	}
   	}
   	@Override
   	public Edge retrieveFromDatabase(final Database dbSource, String sourceKey) throws GroundException {
   		String sql = String.format("select * from edge where source_key = \'%s\'", sourceKey);
   		JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
   		return Json.fromJson(json, Edge.class);
   	}
   	@Override
  	public Edge retrieveFromDatabase(final Database dbSource, long id) throws GroundException {
    	String sql = String.format("select * from edge_version where id = %d", id);
    	JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    	return Json.fromJson(json, Edge.class);
    }
    @Override
  	public Edge retrieveFromDatabase(final Database dbSource, long fromNodeId) throws GroundException {
    	String sql = String.format("select * from edge_version where from_node_id = %d", fromNodeId);
    	JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    	return Json.fromJson(json, Edge.class);
    }
    @Override
  	public Edge retrieveFromDatabase(final Database dbSource, long toNodeId) throws GroundException {
    	String sql = String.format("select * from edge_version where to_node_id = %d", toNodeId);
    	JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    	return Json.fromJson(json, Edge.class);
    }
    @Override
    public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
        super.update(itemId, childId, parentIds);
    }
    @Override
    public List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException {
        Node node  = retrieveFromDatabase(dbSource, sourceKey);
        return super.getLeaves(dbSource, edge.getId());
    }

    public void truncate(long itemId, int numLevels) throws GroundException {
    //TODO implement
  }
}