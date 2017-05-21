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
import edu.berkeley.ground.common.dao.core.StructureVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.version.PostgresVersionDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import play.db.Database;
import play.libs.Json;

public class PostgresStructureVersionDao extends PostgresVersionDao<StructureVersion> implements StructureVersionDao {

  private PostgresStructureDao postgresStructureDao;

  public PostgresStructureVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.postgresStructureDao = new PostgresStructureDao(dbSource, idGenerator);
  }

  @Override
  public final StructureVersion create(final StructureVersion structureVersion, List<Long> parentIds) throws GroundException {

    long uniqueId = idGenerator.generateItemId();
    StructureVersion newStructureVersion = new StructureVersion(uniqueId, structureVersion);
    PostgresStatements updateVersionList = this.postgresStructureDao
                                             .update(newStructureVersion.getStructureId(), newStructureVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newStructureVersion);
      statements.append(String.format(SqlConstants.INSERT_STRUCTURE_VERSION, uniqueId, structureVersion.getStructureId()));

      for (Map.Entry<String, GroundType> attribute : structureVersion.getAttributes().entrySet()) {
        statements.append(String.format(SqlConstants.INSERT_STRUCTURE_VERSION_ATTRIBUTE, uniqueId, attribute.getKey(), attribute.getValue()));
      }

      statements.merge(updateVersionList);

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }

    return newStructureVersion;
  }

  @Override
  public PostgresStatements delete(long id) {
    PostgresStatements statements = new PostgresStatements();
    statements.append(String.format(SqlConstants.DELETE_STRUCTURE_VERSION_ATTRIBUTES, id));
    statements.append(String.format(SqlConstants.DELETE_BY_ID, "structure_version", id));

    PostgresStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }

  @Override
  public StructureVersion retrieveFromDatabase(final long id) throws GroundException {
    HashMap<String, GroundType> attributes;
    try {
      String resultQuery = String.format(SqlConstants.SELECT_STAR_BY_ID, "structure_version", id);
      JsonNode resultJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, resultQuery));

      if (resultJson.size() == 0) {
        throw new GroundException(ExceptionType.VERSION_NOT_FOUND, this.getType().getSimpleName(), String.format("%d", id));
      }

      StructureVersion structureVersion = Json.fromJson(resultJson.get(0), StructureVersion.class);

      String attributeQuery = String.format(SqlConstants.SELECT_STRUCTURE_VERSION_ATTRIBUTES, id);
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
