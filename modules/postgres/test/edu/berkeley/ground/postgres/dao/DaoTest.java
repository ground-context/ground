package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.EdgeFactory;
import edu.berkeley.ground.common.factory.core.EdgeVersionFactory;
import edu.berkeley.ground.common.factory.core.GraphFactory;
import edu.berkeley.ground.common.factory.core.GraphVersionFactory;
import edu.berkeley.ground.common.factory.core.NodeFactory;
import edu.berkeley.ground.common.factory.core.NodeVersionFactory;
import edu.berkeley.ground.common.factory.core.StructureFactory;
import edu.berkeley.ground.common.factory.core.StructureVersionFactory;
import edu.berkeley.ground.common.factory.usage.LineageEdgeFactory;
import edu.berkeley.ground.common.factory.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.common.factory.usage.LineageGraphFactory;
import edu.berkeley.ground.common.factory.usage.LineageGraphVersionFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.model.core.EdgeVersion;
import edu.berkeley.ground.common.model.core.Graph;
import edu.berkeley.ground.common.model.core.GraphVersion;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.model.core.NodeVersion;
import edu.berkeley.ground.common.model.core.Structure;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.model.usage.LineageGraphVersion;
import edu.berkeley.ground.common.model.version.GroundType;

import javax.sound.sampled.Line;

public class DaoTest {

  protected static EdgeFactory edgeFactory;
  protected static GraphFactory graphFactory;
  protected static LineageEdgeFactory lineageEdgeFactory;
  protected static LineageGraphFactory lineageGraphFactory;
  protected static NodeFactory nodeFactory;
  protected static StructureFactory structureFactory;

  protected static EdgeVersionFactory edgeVersionFactory;
  protected static GraphVersionFactory graphVersionFactory;
  protected static LineageEdgeVersionFactory lineageEdgeVersionFactory;
  protected static LineageGraphVersionFactory lineageGraphVersionFactory;
  protected static NodeVersionFactory nodeVersionFactory;
  protected static StructureVersionFactory structureVersionFactory;

  public static Node createNode(String sourceKey) throws GroundException {
    Node node = new Node(-1, null, sourceKey, new HashMap<>());
    return nodeFactory.create(node);
  }

  public static NodeVersion createNodeVersion(long nodeId) throws GroundException {
    return createNodeVersion(nodeId, new ArrayList<>());
  }

  public static NodeVersion createNodeVersion(long nodeId, List<Long> parents)
    throws GroundException {
    return nodeVersionFactory.create(new HashMap<>(), -1, null, new HashMap<>(), nodeId,
      parents);
  }

  public static Edge createEdge(String sourceKey, String fromNode, String toNode)
    throws GroundException {

    long fromNodeId = nodeFactory.retrieveFromDatabase(fromNode).getId();
    long toNodeId = nodeFactory.retrieveFromDatabase(toNode).getId();

    return edgeFactory.create(null, sourceKey, fromNodeId, toNodeId, new HashMap<>());
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
    return edgeVersionFactory.create(new HashMap<>(), -1, null, new HashMap<>(),
      edgeId, fromStart, -1, toStart, -1, parents);
  }

  public static Graph createGraph(String sourceKey) throws GroundException {
    return graphFactory.create(null, sourceKey, new HashMap<>());
  }

  public static GraphVersion createGraphVersion(long graphId, List<Long> edgeVersionIds)
    throws GroundException {

    return createGraphVersion(graphId, edgeVersionIds, new ArrayList<>());
  }

  public static GraphVersion createGraphVersion(long graphId,
                                                List<Long> edgeVersionIds,
                                                List<Long> parents)
    throws GroundException {
    return graphVersionFactory.create(new HashMap<>(), -1, null,  new HashMap<>(), graphId,
      edgeVersionIds, parents);
  }

  public static LineageEdge createLineageEdge(String sourceKey) throws GroundException {
    LineageEdge lineageEdge = new LineageEdge(0, "", sourceKey, new HashMap<>());
    return lineageEdgeFactory.create(lineageEdge);
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
    return lineageEdgeVersionFactory.create(new HashMap<>(), -1, null,  new HashMap<>(),
      fromId, toId, lineageEdgeId, parents);
  }

  public static LineageGraph createLineageGraph(String sourceKey) throws GroundException {
    LineageGraph lineageGraph = new LineageGraph(0, "", sourceKey, new HashMap<>());
    return lineageGraphFactory.create(lineageGraph);
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

    return lineageGraphVersionFactory.create(new HashMap<>(), -1, null,  new HashMap<>(),
      lineageGraphId, lineageEdgeVersionIds, parents);
  }


  public static Structure createStructure(String sourceKey) throws GroundException {
    return structureFactory.create(null, sourceKey, new HashMap<>());
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

    return structureVersionFactory.create(structureId, structureVersionAttributes,
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


  protected static void runScript(String scriptFile, Consumer<String> executor)  {
    final String SQL_COMMENT_START = "--";

    try (Stream<String> lines = Files.lines(Paths.get(scriptFile))) {
      String data = lines.filter(line -> !line.startsWith(SQL_COMMENT_START)).collect(Collectors.joining());
      Arrays.stream(data.split(";"))
        .map(chunk -> chunk + ";")
        .forEach(statement -> executor.accept(statement));
    }catch (IOException e) {
      throw new RuntimeException("Unable to read script file: "+ scriptFile);
    }
  }
}
