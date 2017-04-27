package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.NodeVersion;
import edu.berkeley.ground.postgres.dao.ExampleVersionDao;
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

public class ExampleController extends Controller {
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

  public final CompletionStage<Result> getNode() {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  String sql = "select * from node_version";
                  try {
                    return cache.getOrElse(
                        "nodes",
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
  public final CompletionStage<Result> addNode() {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  JsonNode json = request().body().asJson();
                  NodeVersion nodeVersion = Json.fromJson(json, NodeVersion.class);
                  try {
                    new ExampleVersionDao().create(dbSource, nodeVersion);
                  } catch (GroundException e) {
                    throw new CompletionException(e);
                  }
                  return String.format("New Node Created Successfully");
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
