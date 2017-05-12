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
package edu.berkeley.ground.postgres.dao.core;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.factory.core.StructureVersionFactory;
import edu.berkeley.ground.postgres.dao.version.VersionDao;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import edu.berkeley.ground.common.utils.IdGenerator;

import java.util.ArrayList;
import java.util.List;

import play.db.Database;
import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class StructureVersionDao extends VersionDao<StructureVersion> implements StructureVersionFactory{

  @Override
  public final void create(final Database dbSource, final StructureVersion structureVersion, final IdGenerator idGenerator)
      throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    long uniqueId = idGenerator.generateItemId();
    StructureVersion newStructureVersion = new StructureVersion(uniqueId, structureVersion.getStructureId(), structureVersion.getAttributes());
    try{
      sqlList.addAll(super.createSqlList(newStructureVersion));
	    sqlList.add(
	        String.format(
	            "insert into structure_version (id, structure_id) values (%d, %d)",
	            uniqueId, structureVersion.getStructureId()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public StructureVersion retrieveFromDatabase(final Database dbSource, final long id) throws GroundException{
  	String sql = String.format("select * from structure_version where id = %d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, StructureVersion.class);
  }
}
