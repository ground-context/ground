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
package edu.berkeley.ground.postgres.dao.usage;

import edu.berkeley.ground.common.dao.usage.LineageEdgeDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;

public class PostgresLineageEdgeDao extends PostgresItemDao<LineageEdge> implements LineageEdgeDao {

  public PostgresLineageEdgeDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<LineageEdge> getType() {
    return LineageEdge.class;
  }

  @Override
  public LineageEdge create(LineageEdge lineageEdge) throws GroundException {
    super.verifyItemNotExists(lineageEdge.getSourceKey());

    long uniqueId = this.idGenerator.generateItemId();
    LineageEdge newLineageEdge = new LineageEdge(uniqueId, lineageEdge);
    PostgresStatements statements = super.insert(newLineageEdge);

    statements.append(String.format(SqlConstants.INSERT_GENERIC_ITEM, "lineage_edge", newLineageEdge.getId(), newLineageEdge.getSourceKey(),
      newLineageEdge.getName()));

    try {
      PostgresUtils.executeSqlList(this.dbSource, statements);
      return newLineageEdge;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    LineageEdge lineageEdge = retrieveFromDatabase(sourceKey);
    return super.getLeaves(lineageEdge.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }

}

