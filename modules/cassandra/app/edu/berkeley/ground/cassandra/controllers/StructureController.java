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
import edu.berkeley.ground.common.model.core.Structure;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.core.CassandraStructureDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraStructureVersionDao;
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

public class StructureController extends Controller {

  private CacheApi cache;
  private ActorSystem actorSystem;

  private CassandraStructureDao cassandraStructureDao;
  private CassandraStructureVersionDao cassandraStructureVersionDao;

  @Inject
  final void injectUtils(final CacheApi cache, final CassandraDatabase dbSource, final ActorSystem actorSystem, final IdGenerator idGenerator) {
    this.actorSystem = actorSystem;
    this.cache = cache;

    this.cassandraStructureDao = new CassandraStructureDao(dbSource, idGenerator);
    this.cassandraStructureVersionDao = new CassandraStructureVersionDao(dbSource, idGenerator);
  }

  public final CompletionStage<Result> getStructure(String sourceKey) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "structures",
            () -> Json.toJson(this.cassandraStructureDao.retrieveFromDatabase(sourceKey)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      }, CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  public final CompletionStage<Result> getStructureVersion(Long id) {
    return CompletableFuture.supplyAsync(
      () -> {
        try {
          return this.cache.getOrElse(
            "structure_versions",
            () -> Json.toJson(this.cassandraStructureVersionDao.retrieveFromDatabase(id)),
            Integer.parseInt(System.getProperty("ground.cache.expire.secs")));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      }, CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::ok)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> addStructure() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();
        Structure structure = Json.fromJson(json, Structure.class);

        try {
          structure = this.cassandraStructureDao.create(structure);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(structure);
      },

      CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }

  @BodyParser.Of(BodyParser.Json.class)
  public final CompletionStage<Result> addStructureVersion() {
    return CompletableFuture.supplyAsync(
      () -> {
        JsonNode json = request().body().asJson();

        List<Long> parentIds = GroundUtils.getListFromJson(json, "parentIds");
        ((ObjectNode) json).remove("parentIds");

        StructureVersion structureVersion = Json.fromJson(json, StructureVersion.class);

        try {
          structureVersion = this.cassandraStructureVersionDao.create(structureVersion, parentIds);
        } catch (GroundException e) {
          throw new CompletionException(e);
        }
        return Json.toJson(structureVersion);
      },
      CassandraUtils.getDbSourceHttpContext(this.actorSystem))
             .thenApply(Results::created)
             .exceptionally(e -> GroundUtils.handleException(e, request()));
  }
}
