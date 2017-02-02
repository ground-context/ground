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

package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class GremlinTagFactory extends TagFactory {
  public Map<String, Tag> retrieveFromDatabaseById(GroundDBConnection connectionPointer, String id) throws GroundException {
    GremlinConnection connection = (GremlinConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.STRING, id));

    Vertex versionVertex;
    try {
      versionVertex = connection.getVertex(predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No RichVersion found with id " + id + ".");
    }

    List<Vertex> tagVertices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "TagConnection");

    Map<String, Tag> tags = new HashMap<>();

    if (!tagVertices.isEmpty()) {
      for (Vertex tag : tagVertices) {
        String key = tag.property("tkey").value().toString();

        Object value;
        GroundType type;
        if (tag.property("value").isPresent()) {
          type = GroundType.fromString(tag.property("type").value().toString());
          value = GroundType.stringToType(tag.property("value").value().toString(), type);
        } else {
          value = null;
          type = null;
        }

        tags.put(key, new Tag(id, key, value, type));
      }
    }
    return tags;
  }

  public List<String> getIdsByTag(GroundDBConnection connectionPointer, String tag) throws GroundException {
    GremlinConnection connection = (GremlinConnection) connectionPointer;

    List<Vertex> taggedVertices = connection.getVerticesByAttribute("tkey", tag);

    List<String> ids = new ArrayList<>();
    taggedVertices.forEach(vertex -> {
      ids.add(vertex.property("id").value().toString());
    });

    return ids;
  }
}
