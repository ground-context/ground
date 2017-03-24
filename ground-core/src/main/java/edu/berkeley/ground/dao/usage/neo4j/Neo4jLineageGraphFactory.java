package edu.berkeley.ground.dao.usage.neo4j;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.dao.usage.LineageGraphFactory;
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

public class Neo4jLineageGraphFactory extends LineageGraphFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jLineageGraphFactory.class);

  private final Neo4jClient dbClient;
  private final Neo4jItemFactory itemFactory;

  private final IdGenerator idGenerator;

  public Neo4jLineageGraphFactory(Neo4jClient dbClient, Neo4jItemFactory itemFactory, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  public LineageGraph create(String name, Map<String, Tag> tags) throws GroundException {
    try {
      long uniqueId = this.idGenerator.generateItemId();

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));

      this.dbClient.addVertex("LineageGraph", insertions);
      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      this.dbClient.commit();
      LOGGER.info("Created lineage graph " + name + ".");

      return LineageGraphFactory.construct(uniqueId, name, tags);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public LineageGraph retrieveFromDatabase(String name) throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      Record record;
      try {
        record = this.dbClient.getVertex(predicates);
      } catch (EmptyResultException e) {
        throw new GroundDBException("No LineageGraph found with name " + name + ".");
      }

      long id = record.get("v").asNode().get("id").asLong();
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

      this.dbClient.commit();
      LOGGER.info("Retrieved lineage graph " + name + ".");

      return LineageGraphFactory.construct(id, name, tags);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
  }
}
