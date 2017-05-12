package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.model.core.EdgeVersion;
import edu.berkeley.ground.postgres.dao.EdgeDao;
import edu.berkeley.ground.postgres.dao.EdgeVersionDao;
import edu.berkeley.ground.postgres.utils.GroundUtils;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import edu.berkeley.ground.postgres.utils.ControllerUtils;

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

public class EdgeController extends Controller {
    private CacheApi cache;
    private Database dbSource;
    private ActorSystem actorSystem;
    private IdGenerator idGenerator;

    @Inject
    final void injectUtils(
        final CacheApi cache, final Database dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
        this.dbSource = dbSource;
        this.actorSystem = actorSystem;
        this.cache = cache;
        this.idGenerator = idGenerator;
    }

    public final CompletionStage<Result> getEdge(final String sourceKey) {
        CompletableFuture<Result> results =
            CompletableFuture.supplyAsync(
                () -> {
                    String sql =
                        String.format("select * from edge where source_key = \'%s\'", sourceKey);
            //"select * from graph where source_key = \'%s'"
            try {
                return cache.getOrElse(
                    "edges",
                    () -> PostgresUtils.executeQueryToJson(dbSource, sql),
                        Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, PostgresUtils.getDbSourceHttpContext(actorSystem))
        .thenApply(output -> ok(output)).exceptionally(e -> {
            return internalServerError(GroundUtils.getServerError(request(), e));
        });
        return results;
    }

    @BodyParser.Of(BodyParser.Json.class)
    public final CompletionStage<Result> addEdge() {
        CompletableFuture<Result> results =
            CompletableFuture.supplyAsync(
                () -> {
                    JsonNode json = request().body().asJson();
                    Edge edge = Json.fromJson(json, Edge.class);
                    try {
                        new EdgeDao().create(dbSource, edge, idGenerator);
                    } catch (GroundException e) {
                        throw new CompletionException(e);
                    }
                    return String.format("New Edge Created Successfully");
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

    public final CompletionStage<Result> getEdgeVersion(Long id) {
        CompletableFuture<Result> results =
            CompletableFuture.supplyAsync(
                () -> {
                    String sql =
                        String.format(
                            "select * from edge_version where id = %d", id);
            //"select * from graph where source_key = \'%s'"
                    try {
                        return cache.getOrElse(
                            "edge_verisons",
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
    public final CompletionStage<Result> addEdgeVersion() {
        CompletableFuture<Result> results =
            CompletableFuture.supplyAsync(
                () -> {
                    JsonNode json = request().body().asJson();
                    EdgeVersion edgeVersion = Json.fromJson(json, EdgeVersion.class);
                    try {
                        new EdgeVersionDao().create(dbSource, edgeVersion, idGenerator, ControllerUtils.getListFromJson(json, "parents"));
                    } catch (GroundException e) {
                        throw new CompletionException(e);
                    }
                    return String.format("New Edge Version Created Successfully");
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
