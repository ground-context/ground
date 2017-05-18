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
import edu.berkeley.ground.common.factory.core.StructureVersionFactory;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.*;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StructureVersionDao extends VersionDao<StructureVersion> implements StructureVersionFactory{

  public StructureVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  public final StructureVersion create(final StructureVersion structureVersion, List<Long> parentIds)
      throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    long uniqueId = idGenerator.generateItemId();
    StructureVersion newStructureVersion = new StructureVersion(uniqueId, structureVersion.getStructureId(), structureVersion.getAttributes());
    VersionSuccessorDao versionSuccessorDao = new VersionSuccessorDao(dbSource, idGenerator);
    VersionHistoryDagDao
      versionHistoryDagDao = new VersionHistoryDagDao(dbSource, versionSuccessorDao);
    TagDao tagDao = new TagDao(dbSource, idGenerator);
    ItemDao itemDao = new ItemDao(dbSource, idGenerator, versionHistoryDagDao, tagDao);
    PostgresStatements updateVersionList = itemDao.update(newStructureVersion.getStructureId(), newStructureVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newStructureVersion);
      statements.append(String.format(
        "insert into structure_version (id, structure_id) values (%s,%s)",
        uniqueId, structureVersion.getStructureId()));
      for(Map.Entry<String, GroundType>attribute: structureVersion.getAttributes().entrySet()) {
        statements.append(String.format(
          "insert into structure_version_attribute (structure_version_id, key, type) values (%s,"
            + "\'%s\', \'%s\')",
          uniqueId, attribute.getKey(), attribute.getValue()));
      }
      statements.merge(updateVersionList);

      System.out.println("uniqueId: " + uniqueId);
      System.out.println("structureId: " + structureVersion.getStructureId());

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    return newStructureVersion;
  }

  @Override
  public StructureVersion retrieveFromDatabase(final long id) throws GroundException{
    //String sql = String.format("SELECT * FROM structure_version WHERE id = %d", id);
    long structureId = 0;
    HashMap<String, GroundType> attributes = null;
    try{
      Connection con = dbSource.getConnection();
      Statement stmt = con.createStatement();
      String sql = String.format("select * FROM structure_version WHERE id = %d", id);
      ResultSet resultSet = stmt.executeQuery(sql);

      String attributeQuery = String.format("SELECT * FROM structure_version_attribute WHERE "
        + "structure_version_id = %d", id);
      ResultSet attributeSet = stmt.executeQuery(attributeQuery);

      attributes = new HashMap<>();

      do {
        attributes.put(attributeSet.getString(2), GroundType.fromString(attributeSet.getString(3)));
      } while (attributeSet.next());

      structureId = resultSet.getLong(2);

    }catch(Exception e) {
      throw new GroundException(e);
    }

    return new StructureVersion(id, structureId, attributes);
  }
}
