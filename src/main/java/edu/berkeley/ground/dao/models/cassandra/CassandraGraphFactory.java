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

package edu.berkeley.ground.dao.models.cassandra;

import edu.berkeley.ground.dao.models.GraphFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.CassandraResults;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CassandraGraphFactory extends CassandraItemFactory<Graph> implements GraphFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraGraphFactory.class);
  private final CassandraClient dbClient;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra graph factory.
   *
   * @param dbClient the Cassandra client
   * @param idGenerator a unique ID generator
   */
  public CassandraGraphFactory(CassandraClient dbClient,
                               CassandraVersionHistoryDagFactory versionHistoryDagFactory,
                               CassandraTagFactory tagFactory,
                               IdGenerator idGenerator) {
    super(dbClient, versionHistoryDagFactory, tagFactory);

    this.dbClient = dbClient;
    this.idGenerator = idGenerator;
  }

  /**
   * Creates and persists a graph.
   *
   * @param name the name of the graph
   * @param sourceKey the user generated unique key for the graph
   * @param tags tags associated with this graph
   * @return the created graph
   * @throws GroundException an error while persisting the graph
   */
  @Override
  public Graph create(String name, String sourceKey, Map<String, Tag> tags) throws GroundException {
    super.verifyItemNotExists(sourceKey);
    long uniqueId = this.idGenerator.generateItemId();

    super.insertIntoDatabase(uniqueId, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.insert("graph", insertions);

    LOGGER.info("Created graph " + name + ".");

    return new Graph(uniqueId, name, sourceKey, tags);
  }

  /**
   * Retrieve the DAG leaves for this node.
   *
   * @param sourceKey the key of the node to retrieve leaves for.
   * @return the leaves of the node
   * @throws GroundException an error while retrieving the node
   */
  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Graph graph = this.retrieveFromDatabase(sourceKey);
    return super.getLeaves(graph.getId());
  }

  /**
   * Retrieves a graph from the database.
   *
   * @param sourceKey the key of the graph to retrieve
   * @return the retrieved graph
   * @throws GroundException either the graph doesn't exist or couldn't be retrieved
   */
  @Override
  public Graph retrieveFromDatabase(String sourceKey) throws GroundException {
    return this.retrieveByPredicate("source_key", sourceKey, GroundType.STRING);
  }

  /**
   * Retrieves a graph from the database.
   *
   * @param id the id of the graph to retrieve
   * @return the retrieved graph
   * @throws GroundException either the graph doesn't exist or couldn't be retrieved
   */
  @Override
  public Graph retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.LONG);
  }

  private Graph retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    CassandraResults resultSet = this.dbClient.equalitySelect("graph",
        DbClient.SELECT_STAR,
        predicates);
    super.verifyResultSet(resultSet, fieldName, value);

    long id = resultSet.getLong("item_id");
    String name = resultSet.getString("name");
    String sourceKey = resultSet.getString("source_key");

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved graph " + value + ".");

    return new Graph(id, name, sourceKey, tags);
  }
}
