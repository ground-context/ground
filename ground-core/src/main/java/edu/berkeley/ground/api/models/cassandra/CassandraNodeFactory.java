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

import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.util.IdGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CassandraNodeFactory extends NodeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraNodeFactory.class);
  private final CassandraClient dbClient;
  private final CassandraItemFactory itemFactory;

  private final IdGenerator idGenerator;

  public CassandraNodeFactory(CassandraItemFactory itemFactory, CassandraClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  public Node create(String name, Map<String, Tag> tags) throws GroundDBException {
    try {
      long uniqueId = this.idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));

      this.dbClient.insert("node", insertions);

      this.dbClient.commit();
      LOGGER.info("Created node " + name + ".");

      return NodeFactory.construct(uniqueId, name, tags);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public List<Long> getLeaves(String name) throws GroundDBException {
    Node node = this.retrieveFromDatabase(name);

    try {
      List<Long> leaves = this.itemFactory.getLeaves(node.getId());
      this.dbClient.commit();

      return leaves;
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public Node retrieveFromDatabase(String name) throws GroundDBException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("node", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        this.dbClient.abort();

        throw new GroundDBException("No Node found with name " + name + ".");
      }

      if (!resultSet.next()) {
        this.dbClient.abort();

        throw new GroundDBException("No Node found with name " + name + ".");
      }

      long id = resultSet.getLong(0);
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

      this.dbClient.commit();
      LOGGER.info("Retrieved node " + name + ".");

      return NodeFactory.construct(id, name, tags);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public void update(long itemId, long childId, List<Long> parentIds) throws GroundDBException {
    this.itemFactory.update(itemId, childId, parentIds);
  }
}
