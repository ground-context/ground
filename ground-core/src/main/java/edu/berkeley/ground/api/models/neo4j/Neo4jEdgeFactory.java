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

package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jEdgeFactory extends EdgeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEdgeFactory.class);
  private Neo4jClient dbClient;
  private Neo4jItemFactory itemFactory;

  private IdGenerator idGenerator;

  public Neo4jEdgeFactory(Neo4jItemFactory itemFactory, Neo4jClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  public Edge create(String name, Map<String, Tag> tags) throws GroundException {
    Neo4jConnection connection = dbClient.getConnection();

    try {
      long uniqueId = idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(connection, uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));

      connection.addVertex("GroundEdge", insertions);

      connection.commit();
      LOGGER.info("Created edge " + name + ".");
      return EdgeFactory.construct(uniqueId, name, tags);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public Edge retrieveFromDatabase(String name) throws GroundException {
    Neo4jConnection connection = dbClient.getConnection();

    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      Record record;
      try {
        record = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No Edge found with name " + name + ".");
      }

      long id = record.get("v").asNode().get("id").asLong();
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(connection, id).getTags();

      connection.commit();
      LOGGER.info("Retrieved edge " + name + ".");

      return EdgeFactory.construct(id, name, tags);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }


  public void update(GroundDBConnection connection, long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(connection, itemId, childId, parentIds);
  }
}
