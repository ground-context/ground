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

package dao.usage.cassandra;

import dao.models.cassandra.CassandraTagFactory;
import dao.usage.LineageGraphFactory;
import dao.versions.cassandra.CassandraItemFactory;
import dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import db.CassandraClient;
import db.DbClient;
import db.DbDataContainer;
import db.DbResults;
import db.DbRow;
import exceptions.GroundException;
import models.models.Tag;
import models.usage.LineageGraph;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraLineageGraphFactory
    extends CassandraItemFactory<LineageGraph>
    implements LineageGraphFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraLineageGraphFactory.class);
  private final CassandraClient dbClient;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra lineage graph factory.
   *
   * @param dbClient the Cassandra client
   * @param idGenerator a unique id generator
   */
  public CassandraLineageGraphFactory(CassandraClient dbClient,
                                      CassandraVersionHistoryDagFactory versionHistoryDagFactory,
                                      CassandraTagFactory tagFactory,
                                      IdGenerator idGenerator) {

    super(dbClient, versionHistoryDagFactory, tagFactory);

    this.dbClient = dbClient;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a lineage graph.
   *
   * @param name the name of the lineage graph
   * @param sourceKey the user generated unique id of the lineage graph
   * @param tags the tags associated with this lineage graph
   * @return the created lineage graph
   * @throws GroundException an unexpected error while creating or persisting this lineage graph
   */
  @Override
  public LineageGraph create(String name, String sourceKey, Map<String, Tag> tags)
      throws GroundException {

    super.verifyItemNotExists(sourceKey);

    long uniqueId = this.idGenerator.generateItemId();
    super.insertIntoDatabase(uniqueId, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.insert("lineage_graph", insertions);

    LOGGER.info("Created lineage_graph " + name + ".");
    return new LineageGraph(uniqueId, name, sourceKey, tags);
  }

  /**
   * Retrieve the leaves of this lineage graph's DAG.
   *
   * @param sourceKey the key of the lineage graph
   * @return the list of leaves in this lineage graph's DAG
   * @throws GroundException an error while retrieving the lineage graph
   */
  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    LineageGraph structure = this.retrieveFromDatabase(sourceKey);
    return super.getLeaves(structure.getId());
  }

  /**
   * Retrieve a lineage graph from the database.
   *
   * @param sourceKey the key of the lineage graph
   * @return the retrieved lineage graph
   * @throws GroundException either the lineage graph doesn't exist or couldn't be retrieved
   */
  @Override
  public LineageGraph retrieveFromDatabase(String sourceKey) throws GroundException {
    return this.retrieveByPredicate("source_key", sourceKey, GroundType.STRING);
  }

  /**
   * Retrieves a lineage graph from the database.
   *
   * @param id the id of the lineage graph to retrieve
   * @return the retrieved lineage graph
   * @throws GroundException either the lineage graph doesn't exist or couldn't be retrieved
   */
  @Override
  public LineageGraph retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.LONG);
  }

  private LineageGraph retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    DbResults resultSet = this.dbClient.equalitySelect("lineage_graph",
        DbClient.SELECT_STAR, predicates);
    super.verifyResultSet(resultSet, fieldName, value);

    DbRow row = resultSet.one();
    long id = row.getLong("item_id");
    String name = row.getString("name");
    String sourceKey = row.getString("source_key");

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved lineage_graph " + value + ".");
    return new LineageGraph(id, name, sourceKey, tags);
  }
}
