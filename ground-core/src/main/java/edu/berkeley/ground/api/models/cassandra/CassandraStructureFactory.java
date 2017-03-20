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

package edu.berkeley.ground.api.models.cassandra;

import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CassandraStructureFactory extends StructureFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStructureFactory.class);
  private final CassandraClient dbClient;
  private final CassandraItemFactory itemFactory;

  private final IdGenerator idGenerator;

  public CassandraStructureFactory(CassandraItemFactory itemFactory, CassandraClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  public Structure create(String name, Map<String, Tag> tags) throws GroundException {
    try {
      long uniqueId = this.idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));

      this.dbClient.insert("structure", insertions);

      this.dbClient.commit();
      LOGGER.info("Created structure " + name + ".");

      return StructureFactory.construct(uniqueId, name, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public List<Long> getLeaves(String name) throws GroundException {
    Structure structure = this.retrieveFromDatabase(name);

    try {
      List<Long> leaves = this.itemFactory.getLeaves(structure.getId());
      this.dbClient.commit();

      return leaves;
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public Structure retrieveFromDatabase(String name) throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("structure", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        this.dbClient.abort();

        throw new GroundException("No Structure found with name " + name + ".");
      }

      if (!resultSet.next()) {
        this.dbClient.abort();

        throw new GroundException("No Structure found with name " + name + ".");
      }

      long id = resultSet.getLong(0);
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

      this.dbClient.commit();
      LOGGER.info("Retrieved structure " + name + ".");

      return StructureFactory.construct(id, name, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
  }
}
