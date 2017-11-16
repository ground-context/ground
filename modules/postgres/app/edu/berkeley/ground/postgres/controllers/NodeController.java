package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.model.core.NodeVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.PostgresNodeDao;
import edu.berkeley.ground.postgres.dao.core.PostgresNodeVersionDao;
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

public class NodeController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private PostgresNodeDao postgresNodeDao;
  private PostgresNodeVersionDao postgresNodeVersionDao;
  private SharedFileState sharedFileState;

  @Inject
  final void injectUtils(final CacheApi cache, final Database dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.postgresNodeDao = new PostgresNodeDao(dbSource, idGenerator);
    this.postgresNodeVersionDao = new PostgresNodeVersionDao(dbSource, idGenerator);
    this.sharedFileState = new SharedFileState(new Tree(new TreeNode("root", "root")));
  }

  public final CompletionStage<Result> getNode(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "nodes." + sourceKey,
            () -> Json.toJson(this.postgresNodeDao.retrieveFromDatabase(sourceKey)),
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
  public final CompletionStage<Result> addNode() {
    String currentPath = sharedFileState.getCwd();
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        String newName = currentPath + "/" + String.valueOf(json.get("name"));
        ((ObjectNode) json).put("name", newName);
        Node node = Json.fromJson(json, Node.class);
        try {
          node = this.postgresNodeDao.create(node);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(node);
      },
      PostgresUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getNodeVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "node_versions." + id,
            () -> Json.toJson(this.postgresNodeVersionDao.retrieveFromDatabase(id)),
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
  public final CompletionStage<Result> addNodeVersion() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();

        List<Long> parentIds = GroundUtils.getListFromJson(json, "parentIds");
        ((ObjectNode) json).remove("parentIds");
        NodeVersion nodeVersion = Json.fromJson(json, NodeVersion.class);

        try {
          nodeVersion = this.postgresNodeVersionDao.create(nodeVersion, parentIds);
        } catch (GroundException e) {
          e.printStackTrace();
          throw new CompletionException(e);
        }
        return Json.toJson(nodeVersion);
      },
      PostgresUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getLatest(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "node_leaves." + sourceKey,
            () -> Json.toJson(this.postgresNodeDao.getLeaves(sourceKey)),
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
            "node_history." + sourceKey,
            () -> Json.toJson(this.postgresNodeDao.getHistory(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getAdjacentLineage(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "node_version_adj_lineage." + id,
            () -> Json.toJson(this.postgresNodeVersionDao.retrieveAdjacentLineageEdgeVersion(id)),
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
