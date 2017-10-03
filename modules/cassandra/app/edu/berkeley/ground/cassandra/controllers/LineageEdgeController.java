package edu.berkeley.ground.cassandra.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.usage.CassandraLineageEdgeDao;
import edu.berkeley.ground.cassandra.dao.usage.CassandraLineageEdgeVersionDao;
import edu.berkeley.ground.cassandra.util.GroundUtils;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;
import play.cache.CacheApi;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Results;

public class LineageEdgeController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private CassandraLineageEdgeDao cassandraLineageEdgeDao;
  private CassandraLineageEdgeVersionDao cassandraLineageEdgeVersionDao;

  @Inject
  final void injectUtils(final CacheApi cache, final CassandraDatabase dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.cassandraLineageEdgeDao = new CassandraLineageEdgeDao(dbSource, idGenerator);
    this.cassandraLineageEdgeVersionDao = new CassandraLineageEdgeVersionDao(dbSource, idGenerator);
  }

  public final CompletionStage<Result> getLineageEdge(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_edges",
            () -> Json.toJson(this.cassandraLineageEdgeDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      CassandraUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getLineageEdgeVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "lineage_edge_versions",
            () -> Json.toJson(this.cassandraLineageEdgeVersionDao.retrieveFromDatabase(id)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      CassandraUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> createLineageEdge() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        LineageEdge lineageEdge = Json.fromJson(json, LineageEdge.class);
        try {
          lineageEdge = this.cassandraLineageEdgeDao.create(lineageEdge);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(lineageEdge);
      },
      CassandraUtils.getDbSourceHttpContext(this.actorSystem))
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
          lineageEdgeVersion = this.cassandraLineageEdgeVersionDao.create(lineageEdgeVersion, parentIds);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(lineageEdgeVersion);
      },
      CassandraUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }
}
