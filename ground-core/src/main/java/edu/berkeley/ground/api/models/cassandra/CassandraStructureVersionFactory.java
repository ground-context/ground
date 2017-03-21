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

import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CassandraStructureVersionFactory extends StructureVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStructureVersionFactory.class);
  private final CassandraClient dbClient;
  private final CassandraStructureFactory structureFactory;
  private final CassandraVersionFactory versionFactory;

  private final IdGenerator idGenerator;

  public CassandraStructureVersionFactory(CassandraStructureFactory structureFactory, CassandraVersionFactory versionFactory, CassandraClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.structureFactory = structureFactory;
    this.versionFactory = versionFactory;
    this.idGenerator = idGenerator;
  }

  public StructureVersion create(long structureId,
                                 Map<String, GroundType> attributes,
                                 List<Long> parentIds) throws GroundException {

    try {
      long id = this.idGenerator.generateVersionId();

      this.versionFactory.insertIntoDatabase(id);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("structure_id", GroundType.LONG, structureId));

      this.dbClient.insert("structure_version", insertions);

      for (String key : attributes.keySet()) {
        List<DbDataContainer> itemInsertions = new ArrayList<>();
        itemInsertions.add(new DbDataContainer("structure_version_id", GroundType.LONG, id));
        itemInsertions.add(new DbDataContainer("key", GroundType.STRING, key));
        itemInsertions.add(new DbDataContainer("type", GroundType.STRING, attributes.get(key).toString()));

        this.dbClient.insert("structure_version_attribute", itemInsertions);
      }

      this.structureFactory.update(structureId, id, parentIds);

      this.dbClient.commit();
      LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

      return StructureVersionFactory.construct(id, structureId, attributes);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public StructureVersion retrieveFromDatabase(long id) throws GroundException {
    try {

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("structure_version", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        this.dbClient.abort();

        throw new GroundException("No StructureVersion found with id " + id + ".");
      }

      if (!resultSet.next()) {
        this.dbClient.abort();

        throw new GroundException("No StructureVersion found with id " + id + ".");
      }

      Map<String, GroundType> attributes = new HashMap<>();

      try {
        List<DbDataContainer> attributePredicates = new ArrayList<>();
        attributePredicates.add(new DbDataContainer("structure_version_id", GroundType.LONG, id));
        QueryResults attributesSet = this.dbClient.equalitySelect("structure_version_attribute", DBClient.SELECT_STAR, attributePredicates);

        while (attributesSet.next()) {
          attributes.put(attributesSet.getString(1), GroundType.fromString(attributesSet.getString(2)));
        }
      } catch (EmptyResultException e) {
        this.dbClient.abort();

        throw new GroundException("No StructureVersion attributes found for id " + id + ".");
      }

      long structureId = resultSet.getLong(1);

      this.dbClient.commit();
      LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");

      return StructureVersionFactory.construct(id, structureId, attributes);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
