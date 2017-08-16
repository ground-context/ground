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
package edu.berkeley.ground.cassandra.controllers;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Graph;
import edu.berkeley.ground.common.model.core.GraphVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.core.CassandraGraphDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraGraphVersionDao;
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

public class GraphController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private CassandraGraphDao cassandraGraphDao;
  private CassandraGraphVersionDao cassandraGraphVersionDao;

  @Inject
  final void injectUtils(final CacheApi cache, final CassandraDatabase dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.cassandraGraphDao = new CassandraGraphDao(dbSource, idGenerator);

    this.cassandraGraphVersionDao = new CassandraGraphVersionDao(dbSource, idGenerator);
  }

  public final CompletionStage<Result> getGraph(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "graphs",
            () -> Json.toJson(this.cassandraGraphDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      },
      CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getGraphVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse("graph_versions",
            () -> Json.toJson(this.cassandraGraphVersionDao.retrieveFromDatabase(id)),
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
  public final CompletionStage<Result> addGraph() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        Graph graph = Json.fromJson(json, Graph.class);

        try {
          graph = this.cassandraGraphDao.create(graph);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }

        return Json.toJson(graph);
      },
      CassandraUtils.getDbSourceHttpContext(actorSystem))
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
          graphVersion = this.cassandraGraphVersionDao.create(graphVersion, parentIds);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(graphVersion);
      },
      CassandraUtils.getDbSourceHttpContext(actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }
}
