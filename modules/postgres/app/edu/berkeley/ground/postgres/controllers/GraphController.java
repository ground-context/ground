package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Graph;
import edu.berkeley.ground.lib.model.core.GraphVersion;
import edu.berkeley.ground.postgres.dao.GraphDao;
import edu.berkeley.ground.postgres.dao.GraphVersionDao;
import edu.berkeley.ground.postgres.utils.GroundUtils;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
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

public class GraphController extends Controller {
  private CacheApi cache;
  private Database dbSource;
  private ActorSystem actorSystem;

  @Inject
  final void injectUtils(
      final CacheApi cache, final Database dbSource, final ActorSystem actorSystem) {
    this.dbSource = dbSource;
    this.actorSystem = actorSystem;
    this.cache = cache;
  }

  public final CompletionStage<Result> getGraph(final String sourceKey) {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  String sql =
                      String.format("select * from graph where source_key = \'%s\'", sourceKey);
                  try {
                    return cache.getOrElse(
                        "graphs",
                        () -> PostgresUtils.executeQueryToJson(dbSource, sql),
                        Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
                  } catch (Exception e) {
                    throw new CompletionException(e);
                  }
                },
                PostgresUtils.getDbSourceHttpContext(actorSystem))
            .thenApply(output -> ok(output))
            .exceptionally(
                e -> {
                  return internalServerError(GroundUtils.getServerError(request(), e));
                });
    return results;
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> addGraph() {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  JsonNode json = request().body().asJson();
                  Graph graph = Json.fromJson(json, Graph.class);
                  try {
                    new GraphDao().create(dbSource, graph);
                  } catch (GroundException e) {
                    throw new CompletionException(e);
                  }
                  return String.format("New Graph Created Successfully");
                },
                PostgresUtils.getDbSourceHttpContext(actorSystem))
            .thenApply(output -> created(output))
            .exceptionally(
                e -> {
                  if (e.getCause() instanceof GroundException) {
                    return badRequest(
                        GroundUtils.getClientError(
                            request(), e, GroundException.exceptionType.ITEM_NOT_FOUND));
                  } else {
                    return internalServerError(GroundUtils.getServerError(request(), e));
                  }
                });
    return results;
  }

  public final CompletionStage<Result> getGraphVersion(final long id) {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  String sql = String.format("select * from graph_version where id = %d", id);
                  try {
                    return cache.getOrElse(
                        "graphs",
                        () -> PostgresUtils.executeQueryToJson(dbSource, sql),
                        Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
                  } catch (Exception e) {
                    throw new CompletionException(e);
                  }
                },
                PostgresUtils.getDbSourceHttpContext(actorSystem))
            .thenApply(output -> ok(output))
            .exceptionally(
                e -> {
                  return internalServerError(GroundUtils.getServerError(request(), e));
                });
    return results;
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> addGraphVersion() {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  JsonNode json = request().body().asJson();
                  GraphVersion graphVersion = Json.fromJson(json, GraphVersion.class);
                  try {
                    new GraphVersionDao().create(dbSource, graphVersion);
                  } catch (GroundException e) {
                    throw new CompletionException(e);
                  }
                  return String.format("New Graph Version Created Successfully");
                },
                PostgresUtils.getDbSourceHttpContext(actorSystem))
            .thenApply(output -> created(output))
            .exceptionally(
                e -> {
                  if (e.getCause() instanceof GroundException) {
                    return badRequest(
                        GroundUtils.getClientError(
                            request(), e, GroundException.exceptionType.ITEM_NOT_FOUND));
                  } else {
                    return internalServerError(GroundUtils.getServerError(request(), e));
                  }
                });
    return results;
  }
}