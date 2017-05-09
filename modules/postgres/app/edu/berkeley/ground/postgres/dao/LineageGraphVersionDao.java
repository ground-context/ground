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

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.lib.model.usage.LineageGraph;
import edu.berkeley.ground.lib.model.usage.LineageGraphVersion;
import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.lib.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LineageGraphVersionDao extends RichVersionDao<LineageGraphVersion> implements LineageGraphVersionFactory {

  public LineageGraphVersion createNewLineageGraphVersion(final Database dbSource, final LineageGraphVersion lineageGraphVersion, IdGenerator idGenerator) throws GroundException {
    long uniqueId = idGenerator.generateItemId();
    System.out.println(lineageGraphVersion.getStructureVersionId());
    LineageGraphVersion newLineageGraphVersion = new LineageGraphVersion(uniqueId, lineageGraphVersion.getTags(), lineageGraphVersion.getStructureVersionId(),
      lineageGraphVersion.getReference(), lineageGraphVersion.getParameters(), lineageGraphVersion.getLineageGraphId(), lineageGraphVersion.getLineageEdgeVersionIds());
    return create(dbSource, newLineageGraphVersion);
  }

  @Override
  public LineageGraphVersion create(Database dbSource, LineageGraphVersion lineageGraphVersion) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    try {
      sqlList.addAll(super.createSqlList(dbSource, lineageGraphVersion));
      sqlList.add(String.format("insert into lineage_graph_version (id, lineage_graph_id) values (%d, %d)",
        lineageGraphVersion.getId(), lineageGraphVersion.getLineageGraphId()));
      PostgresUtils.executeSqlList(dbSource, sqlList);
      return lineageGraphVersion;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }
}