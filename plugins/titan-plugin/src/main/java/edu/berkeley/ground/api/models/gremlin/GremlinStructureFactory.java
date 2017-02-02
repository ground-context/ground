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

package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.gremlin.GremlinItemFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GremlinStructureFactory extends StructureFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GremlinStructureFactory.class);
  private GremlinClient dbClient;
  private GremlinItemFactory itemFactory;

  public GremlinStructureFactory(GremlinItemFactory itemFactory, GremlinClient dbClient) {
    this.itemFactory = itemFactory;
    this.dbClient = dbClient;
  }

  public Structure create(String name) throws GroundException {
    GremlinConnection connection = this.dbClient.getConnection();

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
    GremlinConnection connection = this.dbClient.getConnection();
    List<String> leaves = this.itemFactory.getLeaves(connection, "Nodes." + name);
    connection.commit();

    return leaves;
  }

  public Structure retrieveFromDatabase(String name) throws GroundException {
    GremlinConnection connection = this.dbClient.getConnection();

    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      Vertex vertex = null;
      try {
        vertex = connection.getVertex("Structure", predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No Structure found with name " + name + ".");
      }

      String id = (String) vertex.property("id").value();

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
