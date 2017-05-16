package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import dao.models.GraphFactory;
import dao.models.GraphVersionFactory;
import db.DbClient;
import exceptions.GroundException;
import exceptions.GroundItemNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import models.models.Graph;
import models.models.GraphVersion;
import models.models.Tag;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import util.ControllerUtils;
import util.FactoryGenerator;

public class GraphController extends Controller {
  private final GraphFactory graphFactory;
  private final GraphVersionFactory graphVersionFactory;

  private final DbClient dbClient;

  @Inject
  public GraphController(FactoryGenerator generator) throws GroundException {
    this.dbClient = generator.getDbClient();
    this.graphFactory = generator.getGraphFactory();
    this.graphVersionFactory = generator.getGraphVersionFactory();
  }

  public Result getGraph(String sourceKey) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.graphFactory.retrieveFromDatabase(sourceKey));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result getGraphVersion(Long id) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.graphVersionFactory.retrieveFromDatabase(id));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result createGraph(String sourceKey, String name) throws GroundException {
    Graph graph;
    try {
      JsonNode requestBody = request().body().asJson();
      Map<String, Tag> tags = ControllerUtils.getTagsFromJson(requestBody);

      graph = this.graphFactory.create(name, sourceKey, tags);
      this.dbClient.commit();
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }

    JsonNode json = Json.toJson(graph);

    return ok(json);
  }

  public Result createGraphVersion(String sourceKey) throws GroundException {
    try {
      long graphId;

      try {
        graphId = this.graphFactory.retrieveFromDatabase(sourceKey).getId();

      } catch (GroundException e) {
        if (e instanceof GroundItemNotFoundException) {
          graphId = this.graphFactory.create(null, sourceKey, new HashMap<>()).getId();
        } else {
          throw e;
        }
      }

      JsonNode requestBody = request().body().asJson();
      Map<String, Tag> tags = ControllerUtils.getTagsFromJson(requestBody);
      Map<String, String> referenceParameters = ControllerUtils.getParametersFromJson(requestBody);
      List<Long> parents = ControllerUtils.getListFromJson(requestBody, "parents");
      long structureVersionId = ControllerUtils.getLongFromJson(requestBody, "structureVersionId");
      String reference = ControllerUtils.getStringFromJson(requestBody, "reference");

      List<Long> edgeVersionIds = ControllerUtils.getListFromJson(requestBody, "edgeVersionIds");


      GraphVersion created = this.graphVersionFactory.create(tags, structureVersionId, reference,
          referenceParameters, graphId, edgeVersionIds, parents);

      this.dbClient.commit();
      return ok(Json.toJson(created));
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
