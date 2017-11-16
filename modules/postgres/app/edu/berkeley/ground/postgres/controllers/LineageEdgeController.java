package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageEdgeDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageEdgeVersionDao;
import edu.berkeley.ground.postgres.util.*;
import play.cache.CacheApi;
import play.db.Database;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Results;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

public class LineageEdgeController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private PostgresLineageEdgeDao postgresLineageEdgeDao;
  private PostgresLineageEdgeVersionDao postgresLineageEdgeVersionDao;
  private SharedFileState sharedFileState;

  @Inject
  final void injectUtils(final CacheApi cache, final Database dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.postgresLineageEdgeDao = new PostgresLineageEdgeDao(dbSource, idGenerator);
    this.postgresLineageEdgeVersionDao = new PostgresLineageEdgeVersionDao(dbSource, idGenerator);
    this.sharedFileState = new SharedFileState(new Tree(new TreeNode("root", "root")));
  }

  public final CompletionStage<Result> getLineageEdge(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_edges." + sourceKey,
            () -> Json.toJson(this.postgresLineageEdgeDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getLineageEdgeVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_edge_versions." + id,
            () -> Json.toJson(this.postgresLineageEdgeVersionDao.retrieveFromDatabase(id)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> createLineageEdge() {
    String currentPath = sharedFileState.getCwd();
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        String newName = currentPath + "/" + String.valueOf(json.get("name"));
        ((ObjectNode) json).put("name", newName);
        LineageEdge lineageEdge = Json.fromJson(json, LineageEdge.class);
        try {
          lineageEdge = this.postgresLineageEdgeDao.create(lineageEdge);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(lineageEdge);
      },
      PostgresUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> createLineageEdgeVersion() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();

        List<Long> parentIds = GroundUtils.getListFromJson(json, "parentIds");
        ((ObjectNode) json).remove("parentIds");

        LineageEdgeVersion lineageEdgeVersion = Json.fromJson(json, LineageEdgeVersion.class);

        try {
          lineageEdgeVersion = this.postgresLineageEdgeVersionDao.create(lineageEdgeVersion, parentIds);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(lineageEdgeVersion);
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getLatest(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_edge_leaves." + sourceKey,
            () -> Json.toJson(this.postgresLineageEdgeDao.getLeaves(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getHistory(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_edge_history." + sourceKey,
            () -> Json.toJson(this.postgresLineageEdgeDao.getHistory(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }
}
