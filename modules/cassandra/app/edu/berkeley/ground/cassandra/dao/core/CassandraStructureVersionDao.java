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
package edu.berkeley.ground.cassandra.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.StructureVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.dao.version.CassandraVersionDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import play.db.Database;
import play.libs.Json;

public class CassandraStructureVersionDao extends CassandraVersionDao<StructureVersion> implements StructureVersionDao {

  private CassandraStructureDao cassandraStructureDao;

  public CassandraStructureVersionDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.cassandraStructureDao = new CassandraStructureDao(dbSource, idGenerator);
  }

  @Override
  public final StructureVersion create(final StructureVersion structureVersion, List<Long> parentIds) throws GroundException {

    long uniqueId = idGenerator.generateItemId();
    StructureVersion newStructureVersion = new StructureVersion(uniqueId, structureVersion);
    CassandraStatements updateVersionList = this.cassandraStructureDao
                                             .update(newStructureVersion.getStructureId(), newStructureVersion.getId(), parentIds);

    try {
      CassandraStatements statements = super.insert(newStructureVersion); // Andre - Get statements from insertion?
      statements.append(String.format(CqlConstants.INSERT_STRUCTURE_VERSION, uniqueId, structureVersion.getStructureId()));

      for (Map.Entry<String, GroundType> attribute : structureVersion.getAttributes().entrySet()) { // Andre - Change to CQL
        statements.append(String.format(CqlConstants.INSERT_STRUCTURE_VERSION_ATTRIBUTE, uniqueId, attribute.getKey(), attribute.getValue()));
      }

      statements.merge(updateVersionList); // Andre - Update some type of lineage tree of versions?

      CassandraUtils.executeCqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }

    return newStructureVersion;
  }

  @Override
  public CassandraStatements delete(long id) {
    CassandraStatements statements = new CassandraStatements();
    statements.append(String.format(CqlConstants.DELETE_STRUCTURE_VERSION_ATTRIBUTES, id)); // Andre - CQL
    statements.append(String.format(CqlConstants.DELETE_BY_ID, "structure_version", id));

    CassandraStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }

  @Override
  public StructureVersion retrieveFromDatabase(final long id) throws GroundException {
    HashMap<String, GroundType> attributes;
    try {
      String resultQuery = String.format(CqlConstants.SELECT_STAR_BY_ID, "structure_version", id);
      JsonNode resultJson = Json.parse(CassandraUtils.executeQueryToJson(dbSource, resultQuery)); // Andre - Async call? May be blocking/waiting?

      if (resultJson.size() == 0) {
        throw new GroundException(ExceptionType.VERSION_NOT_FOUND, this.getType().getSimpleName(), String.format("%d", id));
      }

      StructureVersion structureVersion = Json.fromJson(resultJson.get(0), StructureVersion.class);

      String attributeQuery = String.format(CqlConstants.SELECT_STRUCTURE_VERSION_ATTRIBUTES, id);
      JsonNode attributeJson = Json.parse(CassandraUtils.executeQueryToJson(dbSource, attributeQuery));

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
