package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.*;
import edu.berkeley.ground.common.factory.usage.LineageEdgeFactory;
import edu.berkeley.ground.common.factory.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.common.factory.usage.LineageGraphFactory;
import edu.berkeley.ground.common.factory.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.common.factory.version.TagFactory;
import edu.berkeley.ground.common.factory.version.VersionHistoryDagFactory;
import edu.berkeley.ground.common.factory.version.VersionSuccessorFactory;
import edu.berkeley.ground.common.model.core.*;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.model.usage.LineageGraphVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.RichVersionDao;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.dao.version.TagDao;
import edu.berkeley.ground.postgres.dao.version.VersionDao;
import play.db.Database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DaoTest {
  private static final String DROP_SCRIPT = "./scripts/postgres/drop_postgres.sql";
  private static final String CREATE_SCHEMA_SCRIPT = "./scripts/postgres/postgres.sql";

  protected static Database dbSource;
  protected static IdGenerator idGenerator;
  protected static ItemDao itemDao;
  protected static VersionDao versionDao;
  protected static RichVersionDao richVersionDao;
  protected static VersionSuccessorFactory versionSuccessorDao;
  protected static VersionHistoryDagFactory versionHistoryDagDao;
  protected static TagFactory tagDao;
  protected static EdgeFactory edgeDao;
  protected static GraphFactory graphDao;
  protected static LineageEdgeFactory lineageEdgeDao;
  protected static LineageGraphFactory lineageGraphDao;
  protected static NodeFactory nodeDao;
  protected static StructureFactory structureDao;
  protected static EdgeVersionFactory edgeVersionDao;
  protected static GraphVersionFactory graphVersionDao;
  protected static LineageEdgeVersionFactory lineageEdgeVersionDao;
  protected static LineageGraphVersionFactory lineageGraphVersionDao;
  protected static NodeVersionFactory nodeVersionDao;
  protected static StructureVersionFactory structureVersionDao;

  public static Node createNode(String sourceKey) throws GroundException {
    // id should be replaced by output of IdGenerator
    return nodeDao.create(new Node(0L, null, sourceKey, new HashMap<String, Tag>()));
  }

  public static NodeVersion createNodeVersion(long nodeId) throws GroundException {
    return createNodeVersion(nodeId, new ArrayList<>());
  }

  public static NodeVersion createNodeVersion(long nodeId, List<Long> parents)
    throws GroundException {
    NodeVersion nodeVersion = new NodeVersion(0L, new HashMap<>(), -1, null, new HashMap<>(), nodeId);
    return nodeVersionDao.create(nodeVersion, parents);
  }

  public static Edge createEdge(String sourceKey, String fromNode, String toNode)
    throws GroundException {

    long fromNodeId = nodeDao.retrieveFromDatabase(fromNode).getId();
    long toNodeId = nodeDao.retrieveFromDatabase(toNode).getId();
    Edge edge = new Edge(0L, null, sourceKey, fromNodeId, toNodeId, new HashMap<>());
    return edgeDao.create(edge);
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
    EdgeVersion edgeVersion = new EdgeVersion(0L, new HashMap<>(), -1, null, new HashMap<>(),
      edgeId, fromStart, -1, toStart, -1);
    return edgeVersionDao.create(edgeVersion, parents);
  }

  public static EdgeVersion createEdgeVersion(long edgeId,
                                              long fromStart,
                                              long fromEnd,
                                              long toStart,
                                              long toEnd,
                                              List<Long> parents)
    throws GroundException {
    EdgeVersion edgeVersion = new EdgeVersion(0L, new HashMap<>(), -1, null, new HashMap<>(),
      edgeId, fromStart, fromEnd, toStart, toEnd);
    return edgeVersionDao.create(edgeVersion, parents);
  }

  public static Graph createGraph(String sourceKey) throws GroundException {
    Graph graph = new Graph(0L, null, sourceKey, new HashMap<>());
    return graphDao.create(graph);
  }

  public static GraphVersion createGraphVersion(long graphId, List<Long> edgeVersionIds)
    throws GroundException {

    return createGraphVersion(graphId, edgeVersionIds, new ArrayList<>());
  }

  public static GraphVersion createGraphVersion(long graphId,
                                                List<Long> edgeVersionIds,
                                                List<Long> parents)
    throws GroundException {
    GraphVersion graphVersion = new GraphVersion(0L, new HashMap<>(), -1, null,
      new HashMap<>(), graphId, edgeVersionIds);
    return graphVersionDao.create(graphVersion, parents);
  }

  public static LineageEdge createLineageEdge(String sourceKey) throws GroundException {
    LineageEdge lineageEdge = new LineageEdge(0L, null, sourceKey, new HashMap<>());
    return lineageEdgeDao.create(lineageEdge);
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
    LineageEdgeVersion lineageEdgeVersion = new LineageEdgeVersion(0L, new HashMap<String, Tag>(), (long) -1,
      null, new HashMap<>(), fromId, toId, lineageEdgeId);
    return lineageEdgeVersionDao.create(lineageEdgeVersion, parents);
  }

  public static LineageGraph createLineageGraph(String sourceKey) throws GroundException {
    LineageGraph lineageGraph = new LineageGraph(0L, null, sourceKey, new HashMap<>());
    return lineageGraphDao.create(lineageGraph);
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

    LineageGraphVersion lineageGraphVersion = new LineageGraphVersion(0L, new HashMap<>(), -1, null,
      new HashMap<>(), lineageGraphId, lineageEdgeVersionIds);
    return lineageGraphVersionDao.create(lineageGraphVersion, parents);
  }


  public static Structure createStructure(String sourceKey) throws GroundException {
    Structure structure = new Structure(0L, null, sourceKey, new HashMap<>());
    return structureDao.create(structure);
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

    StructureVersion structureVersion = new StructureVersion(0L, structureId, structureVersionAttributes);
    return structureVersionDao.create(structureVersion, parents);
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
    long firstTestNodeId = DaoTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = DaoTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = DaoTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = DaoTest.createNodeVersion(secondTestNodeId).getId();

    String edgeName = "testEdge";
    long edgeId = DaoTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();

    long edgeVersionId = DaoTest.createEdgeVersion(edgeId, firstNodeVersionId,
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
