package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.model.core.EdgeVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.PostgresEdgeDao;
import edu.berkeley.ground.postgres.dao.core.PostgresEdgeVersionDao;
import edu.berkeley.ground.postgres.util.GroundUtils;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;
import play.cache.CacheApi;
import play.db.Database;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Results;

public class EdgeController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private PostgresEdgeDao postgresEdgeDao;
  private PostgresEdgeVersionDao postgresEdgeVersionDao;

  @Inject
  final void injectUtils(final CacheApi cache, final Database dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.postgresEdgeDao = new PostgresEdgeDao(dbSource, idGenerator);
    this.postgresEdgeVersionDao = new PostgresEdgeVersionDao(dbSource, idGenerator);
  }

  public final CompletionStage<Result> getEdge(final String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "edges",
            () -> Json.toJson(this.postgresEdgeDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> addEdge() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        Edge edge = Json.fromJson(json, Edge.class);

        try {
          edge = this.postgresEdgeDao.create(edge);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }

        return Json.toJson(edge);
      },
      PostgresUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getEdgeVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "edge_versions",
            () -> Json.toJson(this.postgresEdgeVersionDao.retrieveFromDatabase(id)),
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
  public final CompletionStage<Result> addEdgeVersion() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        List<Long> parentIds = GroundUtils.getListFromJson(json, "parentIds");

        ((ObjectNode) json).remove("parentIds");
        EdgeVersion edgeVersion = Json.fromJson(json, EdgeVersion.class);

        try {
          edgeVersion = this.postgresEdgeVersionDao.create(edgeVersion, parentIds);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }

        return Json.toJson(edgeVersion);
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }
}
