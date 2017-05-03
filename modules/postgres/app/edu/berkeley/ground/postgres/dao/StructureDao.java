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
import edu.berkeley.ground.lib.model.core.Structure;
import edu.berkeley.ground.lib.factory.core.StructureFactory;
import edu.berkeley.ground.postgres.dao.ItemDao;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import edu.berkeley.ground.lib.model.version.GroundType;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class StructureDao extends ItemDao<Structure> implements StructureFactory {

  @Override
  public void create(final Database dbSource, final Structure structure, final IdGenerator idGenerator) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    long uniqueId = idGenerator.generateItemId();
    Structure newStructure = new Structure(uniqueId, structure.getName(), structure.getSourceKey(), structure.getTags());
    try {
      sqlList.addAll(super.createSqlList(newStructure));
      sqlList.add(String.format("insert into structure (item_id, source_key, name) values (%d, '%s', '%s')",
          uniqueId, structure.getSourceKey(), structure.getName()));
      PostgresUtils.executeSqlList(dbSource, sqlList);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public Structure retrieveFromDatabase(final Database dbSource, String sourceKey) throws GroundException {
    String sql = String.format("select * from structure where source_key = \'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, Structure.class);
  }

  @Override
  public Structure retrieveFromDatabase(final Database dbSource, long id) throws GroundException {
    String sql = String.format("select * from structure_version where id = %d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, Structure.class);
  }

  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    //TODO implement
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    return new ArrayList<>();
  }

  public void truncate(long itemId, int numLevels) throws GroundException {
    //TODO implement
  }

}
