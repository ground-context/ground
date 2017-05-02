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

package dao.models.postgres;

import dao.models.GraphFactory;
import dao.versions.postgres.PostgresItemFactory;
import dao.versions.postgres.PostgresVersionHistoryDagFactory;
import db.DbClient;
import db.DbCondition;
import db.DbEqualsCondition;
import db.DbResults;
import db.DbRow;
import db.PostgresClient;
import exceptions.GroundException;
import models.models.Graph;
import models.models.Tag;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresGraphFactory extends PostgresItemFactory<Graph> implements GraphFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresGraphFactory.class);
  private final PostgresClient dbClient;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Postgres graph factory.
   *
   * @param dbClient the Postgres client
   * @param idGenerator a unique ID generator
   */
  public PostgresGraphFactory(PostgresClient dbClient,
                              PostgresVersionHistoryDagFactory versionHistoryDagFactory,
                              PostgresTagFactory tagFactory,
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

    List<DbEqualsCondition> insertions = new ArrayList<>();
    insertions.add(new DbEqualsCondition("name", GroundType.STRING, name));
    insertions.add(new DbEqualsCondition("item_id", GroundType.LONG, uniqueId));
    insertions.add(new DbEqualsCondition("source_key", GroundType.STRING, sourceKey));

    this.dbClient.insert("graph", insertions);

    LOGGER.info("Created graph " + name + ".");
    return new Graph(uniqueId, name, sourceKey, tags);
  }


  /**
   * Retrieve the DAG leaves for this graph.
   *
   * @param sourceKey the key of the graph to retrieve leaves for.
   * @return the leaves of the graph
   * @throws GroundException an error while retrieving the graph
   */
  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Graph graph = this.retrieveFromDatabase(sourceKey);
    return super.getLeaves(graph.getId());
  }

  /**
   * Retrieves a graph from the database.
   *
   * @param sourceKey the source key of the graph to retrieve
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

    List<DbCondition> predicates = new ArrayList<>();
    predicates.add(new DbEqualsCondition(fieldName, valueType, value));

    DbResults resultSet = this.dbClient.select("graph",
        DbClient.SELECT_STAR, predicates);
    super.verifyResultSet(resultSet, fieldName, value);

    DbRow row = resultSet.one();
    long id = row.getLong("item_id");
    String sourceKey = row.getString("source_key");
    String name = row.getString("name");

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved graph " + value + ".");

    return new Graph(id, name, sourceKey, tags);
  }
}
