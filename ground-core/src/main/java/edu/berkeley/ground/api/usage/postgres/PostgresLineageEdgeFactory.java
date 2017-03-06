/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.usage.postgres;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PostgresLineageEdgeFactory extends LineageEdgeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresLineageEdgeFactory.class);
  private PostgresClient dbClient;
  private PostgresItemFactory itemFactory;

  private IdGenerator idGenerator;

  public PostgresLineageEdgeFactory(PostgresItemFactory itemFactory, PostgresClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  public LineageEdge create(String name, Map<String, Tag> tags) throws GroundException {
    PostgresConnection connection = this.dbClient.getConnection();

    try {
      long uniqueId = this.idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(connection, uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));

      connection.insert("lineage_edge", insertions);

      connection.commit();
      LOGGER.info("Created lineage edge " + name + ".");

      return LineageEdgeFactory.construct(uniqueId, name, tags);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public LineageEdge retrieveFromDatabase(String name) throws GroundException {
    PostgresConnection connection = this.dbClient.getConnection();

    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      QueryResults resultSet;
      try {
        resultSet = connection.equalitySelect("lineage_edge", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No LineageEdge found with name " + name + ".");
      }

      long id = resultSet.getLong(1);
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(connection, id).getTags();

      connection.commit();
      LOGGER.info("Retrieved lineage edge " + name + ".");

      return LineageEdgeFactory.construct(id, name, tags);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public void update(GroundDBConnection connection, long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(connection, itemId, childId, parentIds);
  }
}
