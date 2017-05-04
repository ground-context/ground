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

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.GraphVersion;
import edu.berkeley.ground.lib.factory.core.GraphVersionFactory;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.lib.factory.core.RichVersionFactory;
import edu.berkeley.ground.lib.model.version.Tag;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import play.db.Database;
import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class GraphVersionDao extends RichVersionDao<GraphVersion> implements GraphVersionFactory{

  @Override
  public final void create(final Database dbSource, final GraphVersion graphVersion, final IdGenerator idGenerator)
      throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    long uniqueId = idGenerator.generateItemId();
    final Map<String, Tag> tags = graphVersion.getTags();
    GraphVersion newGraphVersion = new GraphVersion(uniqueId, graphVersion.getTags(), graphVersion.getStructureVersionId(), 
    	graphVersion.getReference(), graphVersion.getReferenceParameters(), graphVersion.getGraphId(), graphVersion.getEdgeVersionIds());
    try{
      sqlList.addAll(super.createSqlList(newGraphVersion));
	    sqlList.add(
	        String.format(
	            "insert into graph_version (id, graph_id) values (%d, %d)",
	            uniqueId, graphVersion.getGraphId()));
      for (long edgeVersionId : graphVersion.getEdgeVersionIds()) {
          sqlList.add(
              String.format(
                  "insert into graph_version_edge (graph_version_id, edge_version_id) values (%d, %d)",
                  uniqueId, edgeVersionId));
      }
    PostgresUtils.executeSqlList(dbSource, sqlList);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public GraphVersion retrieveFromDatabase(final Database dbSource, final long id) throws GroundException{
  	String sql = String.format("select * from graph_version where id = %d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, GraphVersion.class);
  }
}
