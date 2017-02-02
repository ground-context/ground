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

import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Neo4jStructureFactory extends StructureFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStructureFactory.class);
  private Neo4jClient dbClient;
  private Neo4jItemFactory itemFactory;

  public Neo4jStructureFactory(Neo4jClient dbClient, Neo4jItemFactory itemFactory) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
  }

  public Structure create(String name) throws GroundException {
    Neo4jConnection connection = this.dbClient.getConnection();

    try {
      String uniqueId = "Structures." + name;

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("id", GroundType.STRING, uniqueId));

      connection.addVertex("Structure", insertions);

      connection.commit();
      LOGGER.info("Created structure " + name + ".");

      return StructureFactory.construct(uniqueId, name);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public List<String> getLeaves(String name) throws GroundException {
    Neo4jConnection connection = this.dbClient.getConnection();
    List<String> leaves = this.itemFactory.getLeaves(connection, "Nodes." + name);
    connection.commit();

    return leaves;
  }

  public Structure retrieveFromDatabase(String name) throws GroundException {
    Neo4jConnection connection = this.dbClient.getConnection();

    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      Record record;
      try {
        record = connection.getVertex("Structure", predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No Structure found with name " + name + ".");
      }

      String id = Neo4jClient.getStringFromValue((StringValue) record.get("v").asNode().get("id"));

      connection.commit();
      LOGGER.info("Retrieved structure " + name + ".");

      return StructureFactory.construct(id, name);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public void update(GroundDBConnection connection, String itemId, String childId, List<String> parentIds) throws GroundException {
    this.itemFactory.update(connection, itemId, childId, parentIds);
  }

}
