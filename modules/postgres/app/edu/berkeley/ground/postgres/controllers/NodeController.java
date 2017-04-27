package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Node;
import edu.berkeley.ground.lib.model.core.NodeVersion;
import edu.berkeley.ground.postgres.dao.NodeDao;
import edu.berkeley.ground.postgres.dao.NodeVersionDao;
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

public class NodeController extends Controller {
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

  public final CompletionStage<Result> getNode(String sourceKey) {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  String sql =
                      String.format("select * from node where source_key=\'%s\'", sourceKey);
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
  public final CompletionStage<Result> createNode() {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  JsonNode json = request().body().asJson();
                  Node node = Json.fromJson(json, Node.class);
                  try {
                    new NodeDao().create(dbSource, node);
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

  public final CompletionStage<Result> getNodeVersion(Long id) {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  String sql =
                      String.format(
                          "select * from node_version where node_id=\'%s\'", id.toString());
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

  public final CompletionStage<Result> createNodeVersion() {
    CompletableFuture<Result> results =
        CompletableFuture.supplyAsync(
                () -> {
                  JsonNode json = request().body().asJson();
                  NodeVersion nodeVersion = Json.fromJson(json, NodeVersion.class);
                  try {
                    new NodeVersionDao().create(dbSource, nodeVersion);
                  } catch (GroundException e) {
                    throw new CompletionException(e);
                  }
                  return String.format("New Node Version Created Successfully");
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
