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
package edu.berkeley.ground.postgres.dao.core;

import edu.berkeley.ground.common.dao.core.StructureDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Structure;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;


public class PostgresStructureDao extends PostgresItemDao<Structure> implements StructureDao {

  public PostgresStructureDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Structure> getType() {
    return Structure.class;
  }

  @Override
  public Structure create(Structure structure) throws GroundException {
    super.verifyItemNotExists(structure.getSourceKey());

    PostgresStatements postgresStatements;
    long uniqueId = idGenerator.generateItemId();

    Structure newStructure = new Structure(uniqueId, structure);

    try {
      postgresStatements = super.insert(newStructure);
      postgresStatements.append(String.format(SqlConstants.INSERT_GENERIC_ITEM, "structure", uniqueId, structure.getSourceKey(),
        structure.getName()));
    } catch (Exception e) {
      throw new GroundException(e);
    }

    PostgresUtils.executeSqlList(dbSource, postgresStatements);
    return newStructure;
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Structure structure = retrieveFromDatabase(sourceKey);
    return super.getLeaves(structure.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }
}
