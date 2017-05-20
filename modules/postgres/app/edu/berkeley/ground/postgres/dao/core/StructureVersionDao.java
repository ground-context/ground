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

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.StructureVersionFactory;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.VersionDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import play.db.Database;
import play.libs.Json;

public class StructureVersionDao extends VersionDao<StructureVersion> implements StructureVersionFactory {

  private StructureDao structureDao;

  public StructureVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.structureDao = new StructureDao(dbSource, idGenerator);
  }

  @Override
  public final StructureVersion create(final StructureVersion structureVersion, List<Long> parentIds) throws GroundException {

    long uniqueId = idGenerator.generateItemId();
    StructureVersion newStructureVersion = new StructureVersion(uniqueId, structureVersion);
    PostgresStatements updateVersionList = this.structureDao.update(newStructureVersion.getStructureId(), newStructureVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newStructureVersion);
      statements
        .append(String.format("insert into structure_version (id, structure_id) values (%s,%s)", uniqueId, structureVersion.getStructureId()));

      for (Map.Entry<String, GroundType> attribute : structureVersion.getAttributes().entrySet()) {
        statements.append(String
                            .format("insert into structure_version_attribute (structure_version_id, key, type) values (%s,\'%s\', \'%s\')", uniqueId,
                              attribute.getKey(), attribute.getValue()));
      }

      statements.merge(updateVersionList);

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }

    return newStructureVersion;
  }

  @Override
  public StructureVersion retrieveFromDatabase(final long id) throws GroundException {
    HashMap<String, GroundType> attributes;
    try {
      String resultQuery = String.format("SELECT * FROM structure_version WHERE id = %d", id);
      JsonNode resultJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, resultQuery));
      StructureVersion structureVersion = Json.fromJson(resultJson.get(0), StructureVersion.class);

      String attributeQuery = String.format("SELECT * FROM structure_version_attribute WHERE " + "structure_version_id = %d", id);
      JsonNode attributeJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, attributeQuery));

      attributes = new HashMap<>();

      for (JsonNode attribute : attributeJson) {
        GroundType type = GroundType.fromString(attribute.get("type").asText());
        attributes.put(attribute.get("key").asText(), type);
      }

      structureVersion = new StructureVersion(structureVersion.getId(), structureVersion.getStructureId(), attributes);

      return structureVersion;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }
}
