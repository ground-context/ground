package edu.berkeley.ground.cassandra.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.model.core.NodeVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.core.CassandraNodeDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraNodeVersionDao;
import edu.berkeley.ground.cassandra.util.GroundUtils;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;
import play.cache.CacheApi;
// import play.db.Database;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Results;

public class NodeController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private CassandraNodeDao cassandraNodeDao;
  private CassandraNodeVersionDao cassandraNodeVersionDao;

  @Inject
  final void injectUtils(final CacheApi cache, final CassandraDatabase dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.cassandraNodeDao = new CassandraNodeDao(dbSource, idGenerator);
    this.cassandraNodeVersionDao = new CassandraNodeVersionDao(dbSource, idGenerator);
  }

  public final CompletionStage<Result> getNode(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "nodes",
            () -> Json.toJson(this.cassandraNodeDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> addNode() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        Node node = Json.fromJson(json, Node.class);
        try {
          node = this.cassandraNodeDao.create(node);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(node);
      },
      CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getNodeVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "node_versions",
            () -> Json.toJson(this.cassandraNodeVersionDao.retrieveFromDatabase(id)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> addNodeVersion() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();

        List<Long> parentIds = GroundUtils.getListFromJson(json, "parentIds");
        ((ObjectNode) json).remove("parentIds");
        NodeVersion nodeVersion = Json.fromJson(json, NodeVersion.class);

        try {
          nodeVersion = this.cassandraNodeVersionDao.create(nodeVersion, parentIds);
        } catch (GroundException e) {
          e.printStackTrace();
          throw new CompletionException(e);
        }
        return Json.toJson(nodeVersion);
      },
      CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }
}
