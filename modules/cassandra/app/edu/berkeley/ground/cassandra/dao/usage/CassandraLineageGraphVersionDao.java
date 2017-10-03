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
package edu.berkeley.ground.cassandra.dao.usage;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.usage.LineageGraphVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.usage.LineageGraphVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.dao.core.CassandraRichVersionDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.ArrayList;
import java.util.List;
import play.libs.Json;


public class CassandraLineageGraphVersionDao extends CassandraRichVersionDao<LineageGraphVersion> implements LineageGraphVersionDao {

  private CassandraLineageGraphDao cassandraLineageGraphDao;

  public CassandraLineageGraphVersionDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.cassandraLineageGraphDao = new CassandraLineageGraphDao(dbSource, idGenerator);
  }

  @Override
  public LineageGraphVersion create(LineageGraphVersion lineageGraphVersion, List<Long> parentIds)
    throws GroundException {
    final long uniqueId = idGenerator.generateVersionId();
    LineageGraphVersion newLineageGraphVersion = new LineageGraphVersion(uniqueId, lineageGraphVersion);

    CassandraStatements updateVersionList = this.cassandraLineageGraphDao
                                             .update(newLineageGraphVersion.getLineageGraphId(), newLineageGraphVersion.getId(),
                                               parentIds);

    try {
      CassandraStatements statements = super.insert(newLineageGraphVersion);
      statements.append(String.format(CqlConstants.INSERT_LINEAGE_GRAPH_VERSION, uniqueId, newLineageGraphVersion.getLineageGraphId()));

      statements.merge(updateVersionList);

      for (Long id : newLineageGraphVersion.getLineageEdgeVersionIds()) {
        statements.append(String.format(CqlConstants.INSERT_LINEAGE_GRAPH_VERSION_EDGE, newLineageGraphVersion.getId(), id));
      }

      CassandraUtils.executeCqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }

    return newLineageGraphVersion;
  }

  @Override
  public CassandraStatements delete(long id) {
    CassandraStatements statements = new CassandraStatements();
    statements.append(String.format(CqlConstants.DELETE_ALL_GRAPH_VERSION_EDGES, "lineage_graph_version_edge", "lineage_graph_version_id", id));
    statements.append(String.format(CqlConstants.DELETE_BY_ID, "graph_version", id));

    CassandraStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }

  @Override
  public LineageGraphVersion retrieveFromDatabase(long id) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_STAR_BY_ID, "lineage_graph_version", id);
    JsonNode json = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.VERSION_NOT_FOUND, this.getType().getSimpleName(), String.format("%d", id));
    }

    LineageGraphVersion lineageGraphVersion = Json.fromJson(json.get(0), LineageGraphVersion.class);

    List<Long> edgeIds = new ArrayList<>();
    cql = String.format(CqlConstants.SELECT_LINEAGE_GRAPH_VERSION_EDGES, id);

    JsonNode edgeJson = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));
    for (JsonNode edge : edgeJson) {
      edgeIds.add(edge.get("lineageEdgeVersionId").asLong());
    }

    RichVersion richVersion = super.retrieveFromDatabase(id);
    return new LineageGraphVersion(id, richVersion.getTags(), richVersion.getStructureVersionId(), richVersion.getReference(),
                                    richVersion.getParameters(), lineageGraphVersion.getLineageGraphId(), edgeIds);
  }
}
