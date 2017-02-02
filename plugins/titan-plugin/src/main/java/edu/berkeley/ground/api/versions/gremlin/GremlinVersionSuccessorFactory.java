/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.versions.gremlin;

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.api.versions.VersionSuccessorFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;

public class GremlinVersionSuccessorFactory extends VersionSuccessorFactory {
  public <T extends Version> VersionSuccessor<T> create(GroundDBConnection connectionPointer, String fromId, String toId) throws GroundException {
    GremlinConnection connection = (GremlinConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.STRING, fromId));

    Vertex source = null;
    try {
      source = connection.getVertex(predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No source vertex found with id " + fromId + ".");
    }
    predicates.clear();

    predicates.add(new DbDataContainer("id", GroundType.STRING, toId));

    Vertex destination = null;
    try {
      destination = connection.getVertex(predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No destination vertex found with id " + toId + ".");
    }

    String dbId = IdGenerator.generateId(fromId + toId);
    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("successor_id", GroundType.STRING, dbId));
    connection.addEdge("VersionSuccessor", source, destination, insertions);

    return VersionSuccessorFactory.construct(dbId, toId, fromId);
  }

  public <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connectionPointer, String dbId) throws GroundException {
    GremlinConnection connection = (GremlinConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("successor_id", GroundType.STRING, dbId));

    Edge edge;
    try {
      edge = connection.getEdge(predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No VersionSuccessor found with id " + dbId + ".");
    }

    return VersionSuccessorFactory.construct(dbId, (String) edge.outVertex().property("id").value(), (String) edge.inVertex().property("id").value());
  }
}
