package edu.berkeley.ground.api.usage.cassandra;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.usage.LineageGraph;
import edu.berkeley.ground.api.usage.LineageGraphFactory;
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

public class CassandraLineageGraphFactory extends LineageGraphFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraLineageGraphFactory.class);
  private final CassandraClient dbClient;
  private final CassandraItemFactory itemFactory;

  private final IdGenerator idGenerator;

  public CassandraLineageGraphFactory(CassandraItemFactory itemFactory, CassandraClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  public LineageGraph create(String name, Map<String, Tag> tags) throws GroundException {
    try {
      long uniqueId = this.idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));

      this.dbClient.insert("lineage_graph", insertions);

      this.dbClient.commit();
      LOGGER.info("Created lineage_graph " + name + ".");

      return LineageGraphFactory.construct(uniqueId, name, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public LineageGraph retrieveFromDatabase(String name) throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("lineage_graph", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        this.dbClient.abort();

        throw new GroundException("No LineageGraph found with name " + name + ".");
      }

      if (!resultSet.next()) {
        this.dbClient.abort();

        throw new GroundException("No LineageGraph found with name " + name + ".");
      }

      long id = resultSet.getLong(0);
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

      this.dbClient.commit();
      LOGGER.info("Retrieved lineage_graph " + name + ".");

      return LineageGraphFactory.construct(id, name, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
  }
}
