package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.model.usage.LineageGraphVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageGraphDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageGraphVersionDao;
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

public class LineageGraphController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private PostgresLineageGraphDao postgresLineageGraphDao;
  private PostgresLineageGraphVersionDao postgresLineageGraphVersionDao;
  private SharedFileState sharedFileState;

  @Inject
  final void injectUtils(final CacheApi cache, final Database dbSource,
                          final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.postgresLineageGraphDao = new PostgresLineageGraphDao(dbSource, idGenerator);
    this.postgresLineageGraphVersionDao = new PostgresLineageGraphVersionDao(dbSource, idGenerator);
    this.sharedFileState = new SharedFileState(new Tree(new TreeNode("root", "root")));
  }

  public final CompletionStage<Result> getLineageGraph(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_graphs." + sourceKey,
            () -> Json.toJson(this.postgresLineageGraphDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getLineageGraphVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_graph_versions." + id,
            () -> Json.toJson(this.postgresLineageGraphVersionDao.retrieveFromDatabase(id)),
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
  public final CompletionStage<Result> createLineageGraph() {
    String currentPath = sharedFileState.getCwd();
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        String newName = currentPath + "/" + String.valueOf(json.get("name"));
        ((ObjectNode) json).put("name", newName);
        LineageGraph lineageGraph = Json.fromJson(json, LineageGraph.class);
        try {
          lineageGraph = this.postgresLineageGraphDao.create(lineageGraph);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(lineageGraph);
      },
      PostgresUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> createLineageGraphVersion() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();

        List<Long> parentIds = GroundUtils.getListFromJson(json, "parentIds");
        ((ObjectNode) json).remove("parentIds");

        LineageGraphVersion lineageGraphVersion = Json.fromJson(json, LineageGraphVersion.class);

        try {
          lineageGraphVersion = this.postgresLineageGraphVersionDao.create(lineageGraphVersion, parentIds);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(lineageGraphVersion);
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
            "lineage_graph_leaves." + sourceKey,
            () -> Json.toJson(this.postgresLineageGraphDao.getLeaves(sourceKey)),
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
            "lineage_graph_history." + sourceKey,
            () -> Json.toJson(this.postgresLineageGraphDao.getHistory(sourceKey)),
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
