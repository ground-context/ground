/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.dao.usage.cassandra;

import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.CassandraResults;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageEdge;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraLineageEdgeFactory
    extends CassandraItemFactory<LineageEdge>
    implements LineageEdgeFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraLineageEdgeFactory.class);
  private final CassandraClient dbClient;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra lineage edge factory.
   *
   * @param dbClient the Cassandra client
   * @param idGenerator a unique id generator
   */
  public CassandraLineageEdgeFactory(CassandraClient dbClient,
                                     CassandraVersionHistoryDagFactory versionHistoryDagFactory,
                                     CassandraTagFactory tagFactory,
                                     IdGenerator idGenerator) {

    super(dbClient, versionHistoryDagFactory, tagFactory);

    this.dbClient = dbClient;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a lineage edge.
   *
   * @param name the name of the lineage edge
   * @param sourceKey the user generated unique id of the lineage edge
   * @param tags the tags associated with this lineage edge
   * @return the created lineage edge
   * @throws GroundException an unexpected error while creating or persisting this lineage edge
   */
  @Override
  public LineageEdge create(String name, String sourceKey, Map<String, Tag> tags)
      throws GroundException {

    super.verifyItemNotExists(sourceKey);

    long uniqueId = this.idGenerator.generateItemId();
    super.insertIntoDatabase(uniqueId, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.insert("lineage_edge", insertions);

    LOGGER.info("Created lineage edge " + name + ".");
    return new LineageEdge(uniqueId, name, sourceKey, tags);
  }

  /**
   * Retrieve the leaves of this lineage edge's DAG.
   *
   * @param sourceKey the key of the lineage edge
   * @return the list of leaves in this lineage edge's DAG
   * @throws GroundException an error while retrieving the lineage edge
   */
  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    LineageEdge structure = this.retrieveFromDatabase(sourceKey);
    return super.getLeaves(structure.getId());
  }

  /**
   * Retrieve a lineage edge from the database.
   *
   * @param sourceKey the key of the lineage edge
   * @return the retrieved lineage edge
   * @throws GroundException either the lineage edge doesn't exist or couldn't be retrieved
   */
  @Override
  public LineageEdge retrieveFromDatabase(String sourceKey) throws GroundException {
    return this.retrieveByPredicate("source_key", sourceKey, GroundType.STRING);
  }

  /**
   * Retrieves a lineage edge from the database.
   *
   * @param id the id of the lineage edge to retrieve
   * @return the retrieved lineage edge
   * @throws GroundException either the lineage edge doesn't exist or couldn't be retrieved
   */
  @Override
  public LineageEdge retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.LONG);
  }

  private LineageEdge retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    CassandraResults resultSet = this.dbClient.equalitySelect("lineage_edge",
        DbClient.SELECT_STAR,
        predicates);
    super.verifyResultSet(resultSet, fieldName, value);

    long id = resultSet.getLong("item_id");
    String name = resultSet.getString("name");
    String sourceKey = resultSet.getString("source_key");

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved lineage edge " + value + ".");
    return new LineageEdge(id, name, sourceKey, tags);
  }
}
