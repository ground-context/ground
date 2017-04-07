package edu.berkeley.ground.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.model.models.GraphVersion;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.NodeVersion;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageEdge;
import edu.berkeley.ground.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.resources.EdgesResource;
import edu.berkeley.ground.resources.GraphsResource;
import edu.berkeley.ground.resources.LineageEdgesResource;
import edu.berkeley.ground.resources.LineageGraphsResource;
import edu.berkeley.ground.resources.NodesResource;
import edu.berkeley.ground.resources.StructuresResource;

public class DaoTest {
  protected static EdgesResource edgesResource;
  protected static GraphsResource graphsResource;
  protected static LineageEdgesResource lineageEdgesResource;
  protected static LineageGraphsResource lineageGraphsResource;
  protected static NodesResource nodesResource;
  protected static StructuresResource structuresResource;

  public static Node createNode(String sourceKey) throws GroundException {
    return nodesResource.createNode(null, sourceKey, new HashMap<>());
  }

  public static NodeVersion createNodeVersion(long nodeId) throws GroundException {
    return createNodeVersion(nodeId, new ArrayList<>());
  }

  public static NodeVersion createNodeVersion(long nodeId, List<Long> parents)
      throws GroundException {
    return nodesResource.createNodeVersion(nodeId, new HashMap<>(), new HashMap<>(), -1, null,
        parents);
  }

  public static Edge createEdge(String sourceKey, String fromNode, String toNode)
      throws GroundException {
    return edgesResource.createEdge(null, fromNode, toNode, sourceKey, new HashMap<>());
  }

  public static EdgeVersion createEdgeVersion(long edgeId, long fromStart, long toStart)
      throws GroundException {

    return createEdgeVersion(edgeId, fromStart, toStart, new ArrayList<>());
  }

  public static EdgeVersion createEdgeVersion(long edgeId,
                                              long fromStart,
                                              long toStart,
                                              List<Long> parents)
      throws GroundException {
    return edgesResource.createEdgeVersion(edgeId, new HashMap<>(), new HashMap<>(), -1, null,
        fromStart, -1, toStart, -1, parents);
  }

  public static Graph createGraph(String sourceKey) throws GroundException {
    return graphsResource.createGraph(null, sourceKey, new HashMap<>());
  }

  public static GraphVersion createGraphVersion(long graphId, List<Long> edgeVersionIds)
      throws GroundException {

    return createGraphVersion(graphId, edgeVersionIds, new ArrayList<>());
  }

  public static GraphVersion createGraphVersion(long graphId,
                                                List<Long> edgeVersionIds,
                                                List<Long> parents)
      throws GroundException {
    return graphsResource.createGraphVersion(graphId, new HashMap<>(), new HashMap<>(), -1, null,
        edgeVersionIds, parents);
  }

  public static LineageEdge createLineageEdge(String sourceKey) throws GroundException {
    return lineageEdgesResource.createLineageEdge(null, sourceKey, new HashMap<>());
  }

  public static LineageEdgeVersion createLineageEdgeVersion(long lineageEdgeId,
                                                            long fromId,
                                                            long toId) throws GroundException {

    return createLineageEdgeVersion(lineageEdgeId, fromId, toId, new ArrayList<>());
  }

  public static LineageEdgeVersion createLineageEdgeVersion(long lineageEdgeId,
                                                            long fromId,
                                                            long toId,
                                                            List<Long> parents)
      throws GroundException {
    return lineageEdgesResource.createLineageEdgeVersion(lineageEdgeId, new HashMap<>(),
        new HashMap<>(), -1, null, fromId, toId, parents);
  }

  public static LineageGraph createLineageGraph(String sourceKey) throws GroundException {
    return lineageGraphsResource.createLineageGraph(null, sourceKey, new HashMap<>());
  }

  public static LineageGraphVersion createLineageGraphVersion(long lineageGraphId,
                                                              List<Long> lineageEdgeVersionIds)
    throws GroundException {

    return createLineageGraphVersion(lineageGraphId, lineageEdgeVersionIds, new ArrayList<>());
  }

  public static LineageGraphVersion createLineageGraphVersion(long lineageGraphId,
                                                              List<Long> lineageEdgeVersionIds,
                                                              List<Long> parents)
      throws GroundException {

    return lineageGraphsResource.createLineageGraphVersion(lineageGraphId, new HashMap<>(), new
        HashMap<>(), -1, null, lineageEdgeVersionIds, parents);
  }


  public static Structure createStructure(String sourceKey) throws GroundException {
    return structuresResource.createStructure(null, sourceKey, new HashMap<>());
  }

  public static StructureVersion createStructureVersion(long structureId) throws GroundException {
    return createStructureVersion(structureId, new ArrayList<>());
  }

  public static StructureVersion createStructureVersion(long structureId, List<Long> parents)
      throws GroundException {

    Map<String, GroundType> structureVersionAttributes = new HashMap<>();
    structureVersionAttributes.put("intfield", GroundType.INTEGER);
    structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
    structureVersionAttributes.put("strfield", GroundType.STRING);

    return structuresResource.createStructureVersion(structureId, structureVersionAttributes,
        parents);
  }

  public static Map<String, Tag> createTags() throws GroundException {
    Map<String, Tag> tags = new HashMap<>();
    tags.put("intfield", new Tag(-1, "intfield", 1, GroundType.INTEGER));
    tags.put("strfield", new Tag(-1, "strfield", "1", GroundType.STRING));
    tags.put("boolfield", new Tag(-1, "boolfield", true, GroundType.BOOLEAN));

    return tags;
  }

  public static long createTwoNodesAndEdge() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId).getId();

    String edgeName = "testEdge";
    long edgeId = CassandraTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();

    long edgeVersionId = CassandraTest.createEdgeVersion(edgeId, firstNodeVersionId,
        secondNodeVersionId).getId();

    return edgeVersionId;
  }
}
