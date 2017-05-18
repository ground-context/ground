package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import dao.models.StructureFactory;
import dao.models.StructureVersionFactory;
import db.DbClient;
import exceptions.GroundException;
import exceptions.GroundItemNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import models.models.Structure;
import models.models.StructureVersion;
import models.models.Tag;
import models.versions.GroundType;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import util.ControllerUtils;
import util.FactoryGenerator;

public class StructureController extends Controller {
  private final StructureFactory structureFactory;
  private final StructureVersionFactory structureVersionFactory;

  private final DbClient dbClient;

  @Inject
  public StructureController(FactoryGenerator generator) throws GroundException {
    this.dbClient = generator.getDbClient();
    this.structureFactory = generator.getStructureFactory();
    this.structureVersionFactory = generator.getStructureVersionFactory();
  }

  public Result getStructure(String sourceKey) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.structureFactory.retrieveFromDatabase(sourceKey));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result getStructureVersion(Long id) throws GroundException {
    try {
      JsonNode json = Json.toJson(this.structureVersionFactory.retrieveFromDatabase(id));

      return ok(json);
    } finally {
      this.dbClient.commit();
    }
  }

  public Result createStructure(String sourceKey, String name) throws GroundException {
    Structure structure;
    try {
      JsonNode requestBody = request().body().asJson();
      Map<String, Tag> tags = ControllerUtils.getTagsFromJson(requestBody);

      structure = this.structureFactory.create(name, sourceKey, tags);
      this.dbClient.commit();
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }

    JsonNode json = Json.toJson(structure);

    return ok(json);
  }

  public Result createStructureVersion(String sourceKey) throws GroundException {
    try {
      long structureId;

      try {
        structureId = this.structureFactory.retrieveFromDatabase(sourceKey).getId();

      } catch (GroundException e) {
        if (e instanceof GroundItemNotFoundException) {
          structureId = this.structureFactory.create(null, sourceKey, new HashMap<>()).getId();
        } else {
          throw e;
        }
      }

      JsonNode requestBody = request().body().asJson();
      List<Long> parents = ControllerUtils.getListFromJson(requestBody, "parents");

      Map<String, GroundType> attributes = new HashMap<>();
      JsonNode attributesNode = requestBody.get("attributes");
      if (attributesNode != null) {
        // don't use .foreachRemaining for this because GroundType.fromString throws an exception
        // which the closure doesn't like
        Iterator<String> fieldNames = attributesNode.fieldNames();
        while (fieldNames.hasNext()) {
          String fieldName = fieldNames.next();
          attributes.put(fieldName, GroundType.fromString(attributesNode.get(fieldName).asText()));
        }
      }

      StructureVersion created = this.structureVersionFactory.create(structureId, attributes,
          parents);

      this.dbClient.commit();
      return ok(Json.toJson(created));
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
