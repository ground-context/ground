package dao.versions.neo4j.mock;

import java.util.List;

import dao.models.neo4j.Neo4jTagFactory;
import dao.versions.neo4j.Neo4jItemFactory;
import dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import db.Neo4jClient;
import exceptions.GroundException;
import models.versions.Item;

public class TestNeo4jItemFactory extends Neo4jItemFactory<Item> {

  public TestNeo4jItemFactory(Neo4jClient neo4jClient,
                               Neo4jVersionHistoryDagFactory versionHistoryDagFactory,
                               Neo4jTagFactory tagFactory) {

    super(neo4jClient, versionHistoryDagFactory, tagFactory);
  }

  public Class<Item> getType() {
    return Item.class;
  }

  public Item retrieveFromDatabase(long id) throws GroundException {
    return new Item(id, super.retrieveItemTags(id));
  }

  public Item retrieveFromDatabase(String sourceKey) throws GroundException {
    throw new GroundException("This method should never be called.");
  }

  public List<Long> getLeaves(String sourceKey) throws GroundException {
    throw new GroundException("This method should never be called.");
  }
}


