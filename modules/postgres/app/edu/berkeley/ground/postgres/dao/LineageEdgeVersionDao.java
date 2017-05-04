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
import java.util.Map;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.lib.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class LineageEdgeVersionDao extends RichVersionDao<LineageEdgeVersion> implements LineageEdgeVersionFactory {

  public LineageEdgeVersion createNewLineageEdgeVersion(final Database dbSource, final LineageEdgeVersion lineageEdgeVersion, IdGenerator idGenerator) throws GroundException {
    long uniqueId = idGenerator.generateItemId();
    System.out.println(lineageEdgeVersion.getStructureVersionId());
    LineageEdgeVersion newLineageEdgeVersion = new LineageEdgeVersion(uniqueId, lineageEdgeVersion.getTags(), lineageEdgeVersion.getStructureVersionId(),
      lineageEdgeVersion.getReference(), lineageEdgeVersion.getParameters(), lineageEdgeVersion.getFromId(), lineageEdgeVersion.getToId(),
      lineageEdgeVersion.getLineageEdgeId());
    return create(dbSource, newLineageEdgeVersion);
  }

  @Override
  public LineageEdgeVersion create(final Database dbSource, final LineageEdgeVersion lineageEdgeVersion) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    try {
      sqlList.addAll(super.createSqlList(lineageEdgeVersion));
      sqlList.add(String.format("insert into lineage_edge_version (id, lineage_edge_id, from_rich_version_id," +
          "to_rich_version_id) values (%d, %d, %d, %d)", lineageEdgeVersion.getId(),
        lineageEdgeVersion.getLineageEdgeId(), lineageEdgeVersion.getFromId(), lineageEdgeVersion.getToId()));
      PostgresUtils.executeSqlList(dbSource, sqlList);
      return lineageEdgeVersion;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }
}
