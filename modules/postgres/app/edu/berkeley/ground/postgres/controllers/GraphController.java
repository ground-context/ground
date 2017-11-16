/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Graph;
import edu.berkeley.ground.common.model.core.GraphVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.PostgresGraphDao;
import edu.berkeley.ground.postgres.dao.core.PostgresGraphVersionDao;
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

public class GraphController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private PostgresGraphDao postgresGraphDao;
  private PostgresGraphVersionDao postgresGraphVersionDao;
  private SharedFileState sharedFileState;

  @Inject
  final void injectUtils(final CacheApi cache, final Database dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;
    this.postgresGraphDao = new PostgresGraphDao(dbSource, idGenerator);
    this.postgresGraphVersionDao = new PostgresGraphVersionDao(dbSource, idGenerator);
    this.sharedFileState = new SharedFileState(new Tree(new TreeNode("root", "root")));
  }

  public final CompletionStage<Result> getGraph(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "graphs." + sourceKey,
            () -> Json.toJson(this.postgresGraphDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      PostgresUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getGraphVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse("graph_versions." + id,
            () -> Json.toJson(this.postgresGraphVersionDao.retrieveFromDatabase(id)),
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
  public final CompletionStage<Result> addGraph() {
    String currentPath = sharedFileState.getCwd();
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        String newName = currentPath + "/" + String.valueOf(json.get("name"));
        ((ObjectNode) json).put("name", newName);
        Graph graph = Json.fromJson(json, Graph.class);

        try {
          graph = this.postgresGraphDao.create(graph);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }

        return Json.toJson(graph);
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> addGraphVersion() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        List<Long> parentIds = GroundUtils.getListFromJson(json, "parentIds");
        ((ObjectNode) json).remove("parentIds");
        GraphVersion graphVersion = Json.fromJson(json, GraphVersion.class);

        try {
          graphVersion = this.postgresGraphVersionDao.create(graphVersion, parentIds);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(graphVersion);
      },
      PostgresUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getLatest(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "graph_leaves." + sourceKey,
            () -> Json.toJson(this.postgresGraphDao.getLeaves(sourceKey)),
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
            "graph_history." + sourceKey,
            () -> Json.toJson(this.postgresGraphDao.getHistory(sourceKey)),
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
