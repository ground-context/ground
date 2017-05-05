/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.usage.LineageEdgeFactory;
import edu.berkeley.ground.lib.factory.version.TagFactory;
import edu.berkeley.ground.lib.model.usage.LineageEdge;
import edu.berkeley.ground.lib.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresClient;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class LineageEdgeDao extends ItemDao<LineageEdge> implements LineageEdgeFactory {

  private PostgresClient dbClient;
  private VersionHistoryDagDao versionHistoryDagDao;
  private TagFactory tagFactory;

  public LineageEdgeDao() {}

  public LineageEdgeDao(PostgresClient dbClient,
                 VersionHistoryDagDao versionHistoryDagDao,
                 TagFactory tagFactory) {
    this.dbClient = dbClient;
    this.versionHistoryDagDao = versionHistoryDagDao;
    this.tagFactory = tagFactory;
  }

  public LineageEdge createLineageEdge(final Database dbSource, final LineageEdge lineageEdge, final IdGenerator idGenerator) throws GroundException {
    long uniqueId = idGenerator.generateItemId();
    LineageEdge newLineageEdge = new LineageEdge(uniqueId, lineageEdge.getName(), lineageEdge.getSourceKey(), lineageEdge.getTags());
    return create(dbSource, newLineageEdge);
  }

  @Override
  public LineageEdge create(Database dbSource, LineageEdge lineageEdge) throws GroundException {
    List<String> sqlList = new ArrayList<>();
    try {
      sqlList.addAll(super.createSqlList(lineageEdge));
      sqlList.add(String.format("insert into lineage_edge (item_id, source_key, name) values (%d, '%s', '%s')",
        lineageEdge.getId(), lineageEdge.getSourceKey(), lineageEdge.getName()));
      PostgresUtils.executeSqlList(dbSource, sqlList);
      return lineageEdge;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public LineageEdge retrieveFromDatabase(final Database dbSource, String sourceKey) throws GroundException {
    String sql = String.format("select * from lineage_edge where source_key = \'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, LineageEdge.class);
  }

  @Override
  public LineageEdge retrieveFromDatabase(final Database dbSource, long id) throws GroundException {
    String sql = String.format("select * from lineage_edge where id = %d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, LineageEdge.class);
  }

  @Override
  public void update(IdGenerator idGenerator, long itemId, long childId, List<Long> parentIds) throws GroundException {
    super.update(idGenerator, itemId, childId, parentIds);
  }

  @Override
  public List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException {
    return null;
  }

  public void truncate(long itemId, int numLevels) throws GroundException {
    //TODO implement
  }

}

