package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import dao.usage.LineageGraphFactory;
import dao.usage.LineageGraphVersionFactory;
import db.DbClient;
import exceptions.GroundException;
import exceptions.GroundItemNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import models.models.Tag;
import models.usage.LineageGraph;
import models.usage.LineageGraphVersion;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import util.ControllerUtils;
import util.FactoryGenerator;

public class LineageGraphController extends Controller {
  private final LineageGraphFactory lineageGraphFactory;
  private final LineageGraphVersionFactory lineageGraphVersionFactory;

  private final DbClient dbClient;

  @Inject
  public LineageGraphController(FactoryGenerator generator) throws GroundException {
    this.dbClient = generator.getDbClient();
    this.lineageGraphFactory = generator.getLineageGraphFactory();
    this.lineageGraphVersionFactory = generator.getLineageGraphVersionFactory();
  }

  public Result getLineageGraph(String sourceKey) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.lineageGraphFactory.retrieveFromDatabase(sourceKey));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result getLineageGraphVersion(Long id) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.lineageGraphVersionFactory.retrieveFromDatabase(id));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result createLineageGraph(String sourceKey, String name) throws GroundException {
    LineageGraph lineageGraph;
    try {
      JsonNode requestBody = request().body().asJson();
      Map<String, Tag> tags = ControllerUtils.getTagsFromJson(requestBody);

      lineageGraph = this.lineageGraphFactory.create(name, sourceKey, tags);
      this.dbClient.commit();
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }

    JsonNode json = Json.toJson(lineageGraph);

    return ok(json);
  }

  public Result createLineageGraphVersion(String sourceKey) throws GroundException {
    try {
      long lineageGraphId;

      try {
        lineageGraphId = this.lineageGraphFactory.retrieveFromDatabase(sourceKey).getId();

      } catch (GroundException e) {
        if (e instanceof GroundItemNotFoundException) {
          lineageGraphId = this.lineageGraphFactory.create(null, sourceKey, new HashMap<>()).getId();
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

      List<Long> lineageEdgeVersionIds = ControllerUtils.getListFromJson(requestBody,
          "lineageEdgeVersionIds");


      LineageGraphVersion created = this.lineageGraphVersionFactory.create(tags, structureVersionId,
          reference, referenceParameters, lineageGraphId, lineageEdgeVersionIds, parents);

      this.dbClient.commit();
      return ok(Json.toJson(created));
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
