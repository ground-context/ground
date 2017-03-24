package edu.berkeley.ground.dao.usage.neo4j;

import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.dao.models.neo4j.Neo4jRichVersionFactory;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.model.versions.GroundType;
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
import java.util.stream.Collectors;

public class Neo4jLineageGraphVersionFactory extends LineageGraphVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jLineageGraphVersionFactory.class);
  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  private final Neo4jLineageGraphFactory lineageGraphFactory;
  private final Neo4jRichVersionFactory richVersionFactory;

  public Neo4jLineageGraphVersionFactory(Neo4jClient dbClient,
                                         Neo4jLineageGraphFactory lineageGraphFactory,
                                         Neo4jRichVersionFactory richVersionFactory,
                                         IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.lineageGraphFactory = lineageGraphFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }

  public LineageGraphVersion create(Map<String, Tag> tags,
                                    long structureVersionId,
                                    String reference,
                                    Map<String, String> referenceParameters,
                                    long lineageGraphId,
                                    List<Long> lineageEdgeVersionIds,
                                    List<Long> parentIds) throws GroundException {

    try {
      long id = this.idGenerator.generateVersionId();

      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag
          .getKey(), tag.getValue(), tag.getValueType())));

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("lineage_graph_id", GroundType.LONG, lineageGraphId));

      this.dbClient.addVertex("LineageGraphVersion", insertions);
      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

      for (long lineageEdgeVersionId : lineageEdgeVersionIds) {
        this.dbClient.addEdge("LineageGraphVersionEdge", id, lineageEdgeVersionId, new ArrayList<>());
      }

      this.lineageGraphFactory.update(lineageGraphId, id, parentIds);


      this.dbClient.commit();
      LOGGER.info("Created graph version " + id + " in graph " + lineageGraphId + ".");

      return LineageGraphVersionFactory.construct(id, tags, structureVersionId, reference,
          referenceParameters, lineageGraphId, lineageEdgeVersionIds);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public LineageGraphVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      Record versionRecord;
      try {
        versionRecord = this.dbClient.getVertex(predicates);
      } catch (EmptyResultException e) {
        throw new GroundDBException("No LineageGraphVersion found with id " + id + ".");
      }

      long lineageGraphId = versionRecord.get("v") .asNode().get("lineage_graph_id").asLong();

      List<String> returnFields = new ArrayList<>();
      returnFields.add("id");

      List<Record> lineageEdgeVersionVertices = this.dbClient.getAdjacentVerticesByEdgeLabel
          ("LineageGraphVersionEdge", id, returnFields);
      List<Long> lineageEdgeVersionIds = new ArrayList<>();

      lineageEdgeVersionVertices.forEach(edgeVersionVertex -> lineageEdgeVersionIds.add
          (edgeVersionVertex.get("id").asLong()));

      this.dbClient.commit();
      LOGGER.info("Retrieved lineage graph version " + id + " in lineage graph " + lineageGraphId + ".");

      return LineageGraphVersionFactory.construct(id, version.getTags(), version
          .getStructureVersionId(), version.getReference(), version.getParameters(),
          lineageGraphId, lineageEdgeVersionIds);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
