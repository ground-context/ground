package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.model.usage.LineageGraphVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.usage.LineageGraphDao;
import edu.berkeley.ground.postgres.dao.usage.LineageGraphVersionDao;
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

public class LineageGraphController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private LineageGraphDao lineageGraphDao;
  private LineageGraphVersionDao lineageGraphVersionDao;

  @Inject
  final void injectUtils(final CacheApi cache, final Database dbSource,
                          final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.lineageGraphDao = new LineageGraphDao(dbSource, idGenerator);
  }

  public final CompletionStage<Result> getLineageGraph(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_graphs",
            () -> Json.toJson(this.lineageGraphDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> internalServerError(GroundUtils.getServerError(request(), e)));
  }

  public final CompletionStage<Result> getLineageGraphVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_graph_versions",
            () -> Json.toJson(this.lineageGraphVersionDao.retrieveFromDatabase(id)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> internalServerError(GroundUtils.getServerError(request(), e)));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> createLineageGraph() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        LineageGraph lineageGraph = Json.fromJson(json, LineageGraph.class);
        try {
          lineageGraph = this.lineageGraphDao.create(lineageGraph);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(lineageGraph);
      },
      PostgresUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> {
               if (e.getCause() instanceof GroundException) {
                 // TODO: fix
                 return badRequest(GroundUtils.getClientError(request(), e, GroundException.exceptionType.ITEM_NOT_FOUND));
               } else {
                 return internalServerError(GroundUtils.getServerError(request(), e));
               }
             });
  }

  public final CompletionStage<Result> createLineageGraphVersion() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();

        List<Long> parentIds = GroundUtils.getListFromJson(json, "parentIds");
        ((ObjectNode) json).remove("parentIds");

        LineageGraphVersion lineageGraphVersion = Json.fromJson(json, LineageGraphVersion.class);

        try {
          lineageGraphVersion = this.lineageGraphVersionDao.create(lineageGraphVersion, parentIds);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(lineageGraphVersion);
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> {
               if (e.getCause() instanceof GroundException) {
                 // TODO: fix
                 return badRequest(GroundUtils.getClientError(request(), e, GroundException.exceptionType.ITEM_NOT_FOUND));
               } else {
                 return internalServerError(GroundUtils.getServerError(request(), e));
               }
             });
  }
}
