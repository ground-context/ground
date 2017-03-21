package edu.berkeley.ground.api.usage;

import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;

public abstract class LineageGraphFactory {
  public abstract LineageGraph create(String name, Map<String, Tag> tags) throws GroundException;

  public abstract LineageGraph retrieveFromDatabase(String name) throws GroundException;

  public abstract void update(long itemId, long childId, List<Long> parentIds) throws GroundException;

  protected static LineageGraph construct(long id, String name, Map<String, Tag> tags) {
    return new LineageGraph(id, name, tags);
  }
}
