package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import dao.usage.LineageEdgeFactory;
import dao.usage.LineageEdgeVersionFactory;
import db.DbClient;
import exceptions.GroundException;
import exceptions.GroundItemNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import models.models.Tag;
import models.usage.LineageEdge;
import models.usage.LineageEdgeVersion;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import util.ControllerUtils;
import util.FactoryGenerator;

public class LineageEdgeController extends Controller {
  private final LineageEdgeFactory lineageEdgeFactory;
  private final LineageEdgeVersionFactory lineageEdgeVersionFactory;

  private final DbClient dbClient;

  @Inject
  public LineageEdgeController(FactoryGenerator generator) throws GroundException {
    this.dbClient = generator.getDbClient();
    this.lineageEdgeFactory = generator.getLineageEdgeFactory();
    this.lineageEdgeVersionFactory = generator.getLineageEdgeVersionFactory();
  }

  public Result getLineageEdge(String sourceKey) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.lineageEdgeFactory.retrieveFromDatabase(sourceKey));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result getLineageEdgeVersion(Long id) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.lineageEdgeVersionFactory.retrieveFromDatabase(id));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result createLineageEdge(String sourceKey, String name) throws GroundException {
    LineageEdge lineageEdge;
    try {
      JsonNode requestBody = request().body().asJson();
      Map<String, Tag> tags = ControllerUtils.getTagsFromJson(requestBody);

      lineageEdge = this.lineageEdgeFactory.create(name, sourceKey, tags);
      this.dbClient.commit();
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }

    JsonNode json = Json.toJson(lineageEdge);

    return ok(json);
  }

  public Result createLineageEdgeVersion(String sourceKey) throws GroundException {
    try {
      long lineageEdgeId;

      try {
        lineageEdgeId = this.lineageEdgeFactory.retrieveFromDatabase(sourceKey).getId();

      } catch (GroundException e) {
        if (e instanceof GroundItemNotFoundException) {
          lineageEdgeId = this.lineageEdgeFactory.create(null, sourceKey, new HashMap<>()).getId();
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

      long fromRichVersionId = ControllerUtils.getLongFromJson(requestBody, "fromId");
      long toRichVersionId = ControllerUtils.getLongFromJson(requestBody, "toId") ;


      LineageEdgeVersion created = this.lineageEdgeVersionFactory.create(tags, structureVersionId,
          reference, referenceParameters, fromRichVersionId, toRichVersionId, lineageEdgeId,
          parents);

      this.dbClient.commit();
      return ok(Json.toJson(created));
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
