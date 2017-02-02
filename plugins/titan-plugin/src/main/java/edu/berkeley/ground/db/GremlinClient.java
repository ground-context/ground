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

package edu.berkeley.ground.db;

import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;

import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class GremlinClient implements DBClient {
  private Graph graph;

  public GremlinClient() {
    this.graph = GraphFactory.open("/Users/Vikram/Code/titan/conf/titan-cassandra.properties");
  }

  public GremlinConnection getConnection() throws GroundDBException {
    return new GremlinConnection(this.graph);
  }

  public class GremlinConnection extends GroundDBConnection {
    private Graph graph;

    protected GremlinConnection(Graph graph) {
      this.graph = graph;
      graph.tx();
    }

    /**
     * Add a new vertex to the graph.
     *
     * @param label      the vertex label
     * @param attributes the vertex's attributes
     * @return the created vertex
     */
    public Vertex addVertex(String label, List<DbDataContainer> attributes) {
      Object[] attributesArray = new Object[(attributes.size() + 1) * 2];
      for (int i = 1; i < attributes.size() + 1; i++) {
        attributesArray[2 * i] = attributes.get(i - 1).getField();
        attributesArray[2 * i + 1] = attributes.get(i - 1).getValue();
      }

      attributesArray[0] = T.label;
      attributesArray[1] = label;

      return this.graph.addVertex(attributesArray);
    }

    /**
     * Add a new edge to the graph.
     *
     * @param label       the edge label
     * @param source      the source of the edge
     * @param destination the destination of the edge
     * @param attributes  the edge's attributes
     */
    public void addEdge(String label, Vertex source, Vertex destination, List<DbDataContainer> attributes) {
      Object[] attributesArray = new Object[attributes.size() * 2];
      for (int i = 0; i < attributes.size(); i++) {
        attributesArray[2 * i] = attributes.get(i).getField();
        attributesArray[2 * i + 1] = attributes.get(i).getValue();
      }

      source.addEdge(label, destination, attributesArray);
    }

    /**
     * Retrieve a vertex without a label.
     *
     * @param predicates the predicates to check
     * @return a vertex if one exists
     */
    public Vertex getVertex(List<DbDataContainer> predicates) throws EmptyResultException {
      GraphTraversal traversal = this.graph.traversal().V();

      for (DbDataContainer predicate : predicates) {
        traversal = traversal.has(predicate.getField(), predicate.getValue());
      }

      if (traversal.hasNext()) {
        return (Vertex) traversal.next();
      }

      throw new EmptyResultException("No matches for query.");
    }

    /**
     * Retrieve a vertex with a label.
     *
     * @param label      the vertex label
     * @param predicates the predicates to check
     * @return a vertex if one exists
     */
    public Vertex getVertex(String label, List<DbDataContainer> predicates) throws EmptyResultException {
      GraphTraversal traversal = this.graph.traversal().V();

      traversal.has(T.label, label);
      for (DbDataContainer predicate : predicates) {
        traversal = traversal.has(predicate.getField(), predicate.getValue());
      }

      if (traversal.hasNext()) {
        return (Vertex) traversal.next();
      }

      throw new EmptyResultException("No matches for query.");
    }

    public Edge getEdge(List<DbDataContainer> predicates) throws EmptyResultException {
      GraphTraversal traversal = this.graph.traversal().E();

      for (DbDataContainer predicate : predicates) {
        traversal = traversal.has(predicate.getField(), predicate.getValue());
      }

      if (traversal.hasNext()) {
        return (Edge) traversal.next();
      }

      throw new EmptyResultException("No matches for quesry");
    }

    /**
     * Get all vertices that have a particular attribute.
     *
     * @param key   the key of the attribute
     * @param value the value of the attribute
     * @return the vertices, if any exist
     */
    public List<Vertex> getVerticesByAttribute(String key, String value) {
      GraphTraversal traversal = this.graph.traversal().V();
      List<Vertex> vertices = new ArrayList<>();

      traversal.has(key, value).forEachRemaining(vertex -> {
        vertices.add((Vertex) vertex);
      });

      return vertices;
    }

    /**
     * Get all the edges with a particular label that are reachable from a particular
     * starting vertex.
     *
     * @param vertex the starting point for the query
     * @param label  the edge label we are looking for
     * @return the list of valid edges
     */
    public List<Edge> getDescendantEdgesWithLabel(Vertex vertex, String label) {
      List<Edge> result = new ArrayList<>();
      LinkedList<Vertex> queue = new LinkedList<>();
      queue.add(vertex);

      while (!queue.isEmpty()) {
        Vertex current = queue.pop();
        Iterator<Edge> currentEdges = current.edges(Direction.OUT, label);

        currentEdges.forEachRemaining(edge -> {
          queue.add(edge.inVertex());
          result.add(edge);
        });
      }

      return result;
    }

    /**
     * Get all vertices that are one edge away, where the edge connecting them has a
     * particular label.
     *
     * @param vertex    the vertex to start from
     * @param edgeLabel the edge label we are looking for
     * @return a list of adjacent vertices related by edgeLabel
     */
    public List<Vertex> getAdjacentVerticesByEdgeLabel(Vertex vertex, String edgeLabel) {
      List<Vertex> result = new ArrayList<>();
      vertex.vertices(Direction.OUT, edgeLabel).forEachRemaining(result::add);

      return result;
    }

    public List<String> adjacentNodes(String nodeVersionId, String edgeNameRegex) throws GroundException {
      GraphTraversal traversal = this.graph.traversal().V().has("id", nodeVersionId);
      List<String> result = new ArrayList<>();

      traversal.outE("EdgeVersionConnection").inV().forEachRemaining(object -> {
        CacheVertex edgeVertex = (CacheVertex) object;

        if (edgeVertex.property("edge_id").value().toString().contains(edgeNameRegex)) {
          Vertex dst = edgeVertex.edges(Direction.OUT, "EdgeVersionConnection").next().inVertex();
          result.add(dst.property("id").value().toString());
        }
      });

      return result;
    }

    public List<String> transitiveClosure(String nodeVersionId) {
      GraphTraversal traversal = this.graph.traversal().V().has("id", nodeVersionId);

      traversal = traversal.repeat(__.outE("EdgeVersionConnection").inV().outE("EdgeVersionConnection").inV());
      traversal = traversal.emit();
      traversal = traversal.until(__.outE("EdgeVersionConnection").count().is(0)).values("id");

      List<String> result = new ArrayList<>();

      traversal.forEachRemaining(object -> {
        result.add(((String) object));
      });

      return result;
    }

    public void commit() throws GroundDBException {
      this.graph.tx().commit();
    }

    public void abort() throws GroundDBException {
      this.graph.tx().rollback();
    }
  }
}
