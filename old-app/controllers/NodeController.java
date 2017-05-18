
package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import dao.models.NodeFactory;
import dao.models.NodeVersionFactory;
import db.DbClient;
import exceptions.GroundException;
import exceptions.GroundItemNotFoundException;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import models.models.Node;
import models.models.NodeVersion;
import models.models.Tag;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import java.util.HashMap;
import util.ControllerUtils;
import util.FactoryGenerator;

public class NodeController extends Controller {
  private final NodeFactory nodeFactory;
  private final NodeVersionFactory nodeVersionFactory;

  private final DbClient dbClient;

  @Inject
  public NodeController(FactoryGenerator generator) throws GroundException {
    this.dbClient = generator.getDbClient();
    this.nodeFactory = generator.getNodeFactory();
    this.nodeVersionFactory = generator.getNodeVersionFactory();
  }

  public Result getNode(String sourceKey) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.nodeFactory.retrieveFromDatabase(sourceKey));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result getNodeVersion(Long id) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.nodeVersionFactory.retrieveFromDatabase(id));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result createNode(String sourceKey, String name) throws GroundException {
    Node node;
    try {
      JsonNode requestBody = request().body().asJson();
      Map<String, Tag> tags = ControllerUtils.getTagsFromJson(requestBody);

      node = this.nodeFactory.create(name, sourceKey, tags);
      this.dbClient.commit();
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }

    JsonNode json = Json.toJson(node);

    return ok(json);
  }

  public Result createNodeVersion(String sourceKey) throws GroundException {
    try {
      long nodeId;

      try {
        nodeId = this.nodeFactory.retrieveFromDatabase(sourceKey).getId();

      } catch (GroundException e) {
        if (e instanceof GroundItemNotFoundException) {
          nodeId = this.nodeFactory.create(null, sourceKey, new HashMap<>()).getId();
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

      NodeVersion created = this.nodeVersionFactory.create(tags, structureVersionId, reference,
          referenceParameters, nodeId, parents);

      this.dbClient.commit();
      return ok(Json.toJson(created));
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}