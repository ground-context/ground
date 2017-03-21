package edu.berkeley.ground.api.usage;

import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;

public abstract class LineageGraphVersionFactory {
  public abstract LineageGraphVersion create(Map<String, Tag> tags,
                                      long structureVersionId,
                                      String reference,
                                      Map<String, String> referenceParameters,
                                      long lineageGraphId,
                                      List<Long> lineageEdgeVersionIds,
                                      List<Long> parentIds) throws GroundException;

  public abstract LineageGraphVersion retrieveFromDatabase(long id) throws GroundException;

  protected static LineageGraphVersion construct(long id,
                                          Map<String, Tag> tags,
                                          long structureVersionId,
                                          String reference,
                                          Map<String, String> parameters,
                                          long lineageGraphId,
                                          List<Long> lineageEdgeVersionIds) {

    return new LineageGraphVersion(id, tags, structureVersionId, reference, parameters,
        lineageGraphId, lineageEdgeVersionIds);
  }
}
