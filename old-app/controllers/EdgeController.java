package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import dao.models.EdgeFactory;
import dao.models.EdgeVersionFactory;
import db.DbClient;
import exceptions.GroundException;
import exceptions.GroundItemNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import models.models.Edge;
import models.models.EdgeVersion;
import models.models.Tag;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import util.ControllerUtils;
import util.FactoryGenerator;

public class EdgeController extends Controller {
  private final EdgeFactory edgeFactory;
  private final EdgeVersionFactory edgeVersionFactory;

  private final DbClient dbClient;

  @Inject
  public EdgeController(FactoryGenerator generator) throws GroundException {
    this.dbClient = generator.getDbClient();
    this.edgeFactory = generator.getEdgeFactory();
    this.edgeVersionFactory = generator.getEdgeVersionFactory();
  }

  public Result getEdge(String sourceKey) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.edgeFactory.retrieveFromDatabase(sourceKey));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result getEdgeVersion(Long id) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.edgeVersionFactory.retrieveFromDatabase(id));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result createEdge(String sourceKey, String name, long fromNodeId, long toNodeId)
      throws GroundException {

    Edge edge;

    try {
      JsonNode requestBody = request().body().asJson();
      Map<String, Tag> tags = ControllerUtils.getTagsFromJson(requestBody);

      edge = this.edgeFactory.create(name, sourceKey, fromNodeId, toNodeId, tags);
      this.dbClient.commit();
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }

    JsonNode json = Json.toJson(edge);

    return ok(json);
  }


  public Result createEdgeVersion(String sourceKey) throws GroundException {
    try {
      long edgeId;
      edgeId = this.edgeFactory.retrieveFromDatabase(sourceKey).getId();


      JsonNode requestBody = request().body().asJson();
      Map<String, Tag> tags = ControllerUtils.getTagsFromJson(requestBody);
      Map<String, String> referenceParameters = ControllerUtils.getParametersFromJson(requestBody);
      List<Long> parents = ControllerUtils.getListFromJson(requestBody, "parents");
      long structureVersionId = ControllerUtils.getLongFromJson(requestBody, "structureVersionId");
      String reference = ControllerUtils.getStringFromJson(requestBody, "reference");

      long fromNodeVersionStartId = ControllerUtils.getLongFromJson(requestBody,
          "fromNodeVersionStartId");
      long fromNodeVersionEndId = ControllerUtils.getLongFromJson(requestBody,
          "fromNodeVersionEndId");
      long toNodeVersionStartId = ControllerUtils.getLongFromJson(requestBody,
          "toNodeVersionStartId");
      long toNodeVersionEndId = ControllerUtils.getLongFromJson(requestBody, "toNodeVersionEndId");

      EdgeVersion created = this.edgeVersionFactory.create(tags, structureVersionId, reference,
          referenceParameters, edgeId, fromNodeVersionStartId, fromNodeVersionEndId,
          toNodeVersionStartId, toNodeVersionEndId, parents);

      this.dbClient.commit();
      return ok(Json.toJson(created));
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
