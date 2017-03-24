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

package edu.berkeley.ground.dao.models.neo4j;

import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.dao.models.StructureFactory;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jStructureFactory extends StructureFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStructureFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jItemFactory itemFactory;

  private final IdGenerator idGenerator;

  public Neo4jStructureFactory(Neo4jClient dbClient, Neo4jItemFactory itemFactory, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  public Structure create(String name, String sourceKey, Map<String, Tag> tags)
      throws GroundException {
    try {
      long uniqueId = this.idGenerator.generateItemId();

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));
      insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

      this.dbClient.addVertex("Structure", insertions);

      this.dbClient.commit();
      LOGGER.info("Created structure " + name + ".");
      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      return StructureFactory.construct(uniqueId, name, sourceKey, tags);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public List<Long> getLeaves(String name) throws GroundException {
    Structure structure = this.retrieveFromDatabase(name);

    List<Long> leaves = this.itemFactory.getLeaves(structure.getId());
    this.dbClient.commit();

    return leaves;
  }

  public Structure retrieveFromDatabase(String name) throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      Record record;
      try {
        record = this.dbClient.getVertex("Structure", predicates);
      } catch (EmptyResultException e) {
        throw new GroundDBException("No Structure found with name " + name + ".");
      }

      long id = record.get("v").asNode().get("id").asLong();
      String sourceKey = record.get("v").asNode().get("source_key").asString();

      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

      this.dbClient.commit();
      LOGGER.info("Retrieved structure " + name + ".");

      return StructureFactory.construct(id, name, sourceKey, tags);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
  }

}
