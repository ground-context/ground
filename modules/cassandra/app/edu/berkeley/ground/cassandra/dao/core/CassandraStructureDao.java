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
package edu.berkeley.ground.cassandra.dao.core;

import edu.berkeley.ground.common.dao.core.StructureDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Structure;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.dao.version.CassandraItemDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.List;
import java.util.Map;


public class CassandraStructureDao extends CassandraItemDao<Structure> implements StructureDao {

  public CassandraStructureDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Structure> getType() {
    return Structure.class;
  }

  @Override
  public Structure create(Structure structure) throws GroundException {
    super.verifyItemNotExists(structure.getSourceKey());

    CassandraStatements cassandraStatements;
    long uniqueId = idGenerator.generateItemId();

    Structure newStructure = new Structure(uniqueId, structure);

    try {
      cassandraStatements = super.insert(newStructure);

      String name = structure.getName();

      if (name != null) {
        cassandraStatements.append(String.format(CqlConstants.INSERT_GENERIC_ITEM_WITH_NAME, "structure", uniqueId, structure.getSourceKey(), name));
      } else {
        cassandraStatements.append(String.format(CqlConstants.INSERT_GENERIC_ITEM_WITHOUT_NAME, "structure", uniqueId, structure.getSourceKey()));
      }
    } catch (GroundException e) {
      throw e;
    } catch (Exception e) {
      throw new GroundException(e);
    }

    CassandraUtils.executeCqlList(dbSource, cassandraStatements);
    return newStructure;
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Structure structure = retrieveFromDatabase(sourceKey);
    return super.getLeaves(structure.getId());
  }

  @Override
  public Map<Long, Long> getHistory(String sourceKey) throws GroundException {
    Structure structure = retrieveFromDatabase(sourceKey);
    return super.getHistory(structure.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }
}
