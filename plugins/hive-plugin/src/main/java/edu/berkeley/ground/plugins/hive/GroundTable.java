/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.plugins.hive;

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.NodeVersion;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.plugins.hive.util.JsonUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroundTable {
  private static final Logger LOG = LoggerFactory.getLogger(GroundTable.class.getName());

  private GroundReadWrite groundReadWrite = null;
  private GroundPartition groundPartition = null;

  GroundTable(GroundReadWrite ground) {
    groundReadWrite = ground;
    groundPartition = new GroundPartition(ground);
  }

  /**
   * Retrieve the node for a ground table.
   *
   * @param tableName the name of the table
   * @return the node for the table
   * @throws GroundException an exception while retrieving the node
   */
  public Node getNode(String tableName) throws GroundException {
    try {
      LOG.debug("Fetching table node: {}", tableName);
      return groundReadWrite.getNodeFactory().retrieveFromDatabase(tableName);
    } catch (GroundException ge1) {
      LOG.debug("Not found - Creating table node: " + tableName);

      Node node = groundReadWrite.getNodeFactory().create(tableName, null, new HashMap<>());
      Structure nodeStruct = groundReadWrite.getStructureFactory().create(node.getName(), null,
          new HashMap<>());

      return node;
    }
  }

  /**
   * Retrieve the structure for nodes.
   *
   * @param tableName the name of the table
   * @return the table structure
   * @throws GroundException an error while retrieving the structure
   */
  public Structure getNodeStructure(String tableName) throws GroundException {
    try {
      Node node = this.getNode(tableName);
      return groundReadWrite.getStructureFactory().retrieveFromDatabase(tableName);
    } catch (GroundException e) {
      LOG.error("Unable to fetch table node structure");
      throw e;
    }
  }

  /**
   * Retrieve the table edge.
   *
   * @param nodeVersion the table's node version
   * @return the edge for this table
   * @throws GroundException an error while retrieving the edge
   */
  public Edge getEdge(NodeVersion nodeVersion) throws GroundException {
    try {
      LOG.debug("Fetching table partition edge: " + nodeVersion.getNodeId());
      return groundReadWrite.getEdgeFactory().retrieveFromDatabase("" + nodeVersion.getNodeId());
    } catch (GroundException ge1) {
      LOG.debug("Not found - Creating table partition edge: " + nodeVersion.getNodeId());

      Edge edge = groundReadWrite.getEdgeFactory().create("" + nodeVersion.getNodeId(), null, 1, 2,
          new HashMap<>());
      Structure edgeStruct = groundReadWrite.getStructureFactory().create(""
          + nodeVersion.getNodeId(), null, new HashMap<>());
      return edge;
    }
  }

  /**
   * Retreive the structure for edges.
   *
   * @param nodeVersion the table's node version
   * @return the structure for edges
   * @throws GroundException an error while retrieving the structure
   */
  public Structure getEdgeStructure(NodeVersion nodeVersion) throws GroundException {
    try {
      Edge edge = getEdge(nodeVersion);
      return groundReadWrite.getStructureFactory().retrieveFromDatabase(""
          + nodeVersion.getNodeId());
    } catch (GroundException e) {
      LOG.error("Unable to fetch table partition edge structure");
      throw e;
    }
  }

  /**
   * Create the node version for a new table.
   *
   * @param table the table object
   * @return the table's node version
   * @throws InvalidObjectException an invalid table
   * @throws MetaException an error while creating the node version
   */
  public NodeVersion createTableNodeVersion(Table table)
      throws InvalidObjectException, MetaException {

    if (table == null) {
      throw new InvalidObjectException("Database object passed is null");
    }
    try {
      String tableName = table.getTableName();
      final Node tableNode = this.getNode(tableName);
      Structure tableStruct = this.getNodeStructure(tableName);

      Tag tableTag = new Tag(0, tableName, JsonUtil.toJson(table), GroundType.STRING);

      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      structVersionAttribs.put(tableName, GroundType.STRING);

      final StructureVersion sv = groundReadWrite.getStructureVersionFactory().create(
          tableStruct.getId(), structVersionAttribs, new ArrayList<>());

      final String reference = table.getDbName();
      HashMap<String, Tag> tags = new HashMap<>();
      tags.put(tableName, tableTag);

      Map<String, String> tableParamMap = table.getParameters();
      if (tableParamMap == null) {
        tableParamMap = new HashMap<>();
      }
      Map<String, String> parameters = tableParamMap;

      List<Long> parent = new ArrayList<>();
      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(tableName);
      if (!versions.isEmpty()) {
        parent.add(versions.get(0));
      }

      NodeVersion tableNodeVersion = groundReadWrite.getNodeVersionFactory().create(tags,
          sv.getId(), reference, parameters, tableNode.getId(), parent);
      long tableVersionId = tableNodeVersion.getId();

      return tableNodeVersion;
    } catch (GroundException e) {
      LOG.error("Failure to create a table node: {}", table.getTableName(), e);
      throw new MetaException(e.getMessage());
    }
  }

  Table getTable(String dbName, String tableName) throws MetaException {
    try {
      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(tableName);
      if (versions.isEmpty()) {
        throw new MetaException("Table node not found: " + tableName);
      }

      NodeVersion latestVersion = groundReadWrite.getNodeVersionFactory()
          .retrieveFromDatabase(versions.get(0));
      Map<String, Tag> dbTag = latestVersion.getTags();

      return JsonUtil.fromJson((String) dbTag.get(tableName).getValue(), Table.class);
    } catch (GroundException ex) {
      throw new MetaException(ex.getMessage());
    }
  }

  List<String> getTables(String dbName, String pattern) throws MetaException {
    List<String> tables = new ArrayList<String>();
    try {
      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(dbName);

      if (!versions.isEmpty()) {
        long metaVersionId = versions.get(0);
        List<Long> tableNodeIds = groundReadWrite.getNodeVersionFactory()
            .getAdjacentNodes(metaVersionId, pattern);

        for (long tableNodeId : tableNodeIds) {
          NodeVersion tableNodeVersion = groundReadWrite.getNodeVersionFactory()
              .retrieveFromDatabase(tableNodeId);
          // create an edge for a dbname only if was not created
          // earlier
          Edge edge = groundReadWrite.getEdgeFactory().retrieveFromDatabase(""
              + tableNodeVersion.getNodeId());
          tables.add(edge.getName().split("Nodes.")[1]);
        }
      }
    } catch (GroundException ex) {
      LOG.error("Get tables failed for pattern {}", pattern);
      throw new MetaException(ex.getMessage());
    }
    return tables;
  }

  NodeVersion addPartitions(String dbName, String tableName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    try {
      Table prevTable = this.getTable(dbName, tableName);

      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(tableName);

      NodeVersion tableNodeVersion = createTableNodeVersion(prevTable);
      long tableNodeVersionId = tableNodeVersion.getId();

      if (!versions.isEmpty() && versions.size() > 0) {
        long prevVersionId = versions.get(0);
        List<Long> nodeIds = groundReadWrite.getNodeVersionFactory()
            .getAdjacentNodes(prevVersionId, "");
        for (long nodeId : nodeIds) {
          NodeVersion oldNodeVersion = groundReadWrite.getNodeVersionFactory()
              .retrieveFromDatabase(nodeId);
          Edge edge = this.getEdge(oldNodeVersion);
          Structure structure = this.getEdgeStructure(oldNodeVersion);

          Map<String, GroundType> structVersionAttribs = new HashMap<>();
          for (String key : oldNodeVersion.getTags().keySet()) {
            structVersionAttribs.put(key, GroundType.STRING);
          }

          // create an edge version for a dbname
          StructureVersion sv = groundReadWrite.getStructureVersionFactory().create(
              structure.getId(), structVersionAttribs, new ArrayList<>());
          groundReadWrite.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
              oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
              tableNodeVersionId, -1, oldNodeVersion.getId(), -1, new ArrayList<>());
        }
      }

      for (Partition part : parts) {
        NodeVersion nv = groundPartition.createPartition(dbName, tableName, part);

        Edge edge = this.getEdge(nv);
        Structure structure = this.getEdgeStructure(nv);
        Map<String, GroundType> structVersionAttribs = new HashMap<>();
        for (String key : nv.getTags().keySet()) {
          structVersionAttribs.put(key, GroundType.STRING);
        }
        StructureVersion sv = groundReadWrite.getStructureVersionFactory().create(structure.getId(),
            structVersionAttribs, new ArrayList<>());

        groundReadWrite.getEdgeVersionFactory().create(nv.getTags(), sv.getId(),
            tableNodeVersion.getReference(), tableNodeVersion.getParameters(), edge.getId(),
            tableNodeVersionId, -1, nv.getId(), -1, new ArrayList<>());
      }

      return tableNodeVersion;
    } catch (InvalidObjectException | MetaException ex) {
      LOG.error("Unable to create partition to table {} database {}", tableName, dbName);
      throw ex;
    } catch (GroundException ex) {
      LOG.error("Unable to create partition to table {} database {}", tableName, dbName);
      throw new MetaException(ex.getMessage());
    }
  }

  Partition getPartition(String dbName, String tableName, String partName)
      throws MetaException, NoSuchObjectException {
    try {
      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(tableName);
      if (!versions.isEmpty() && versions.size() > 0) {
        long tableNodeVersionId = versions.get(0);
        List<Long> partNodeVersionIds = new ArrayList<>();

        String partNodeName = "Nodes." + partName;
        for (long partNodeVersionId : partNodeVersionIds) {
          long version = groundReadWrite.getNodeFactory().getLeaves("" + partNodeVersionId).get(0);
          NodeVersion nv = groundReadWrite.getNodeVersionFactory().retrieveFromDatabase(version);
          if (("" + nv.getNodeId()).equals(partNodeName)) {
            return JsonUtil.fromJson((String) nv.getTags().get(partName).getValue(),
                Partition.class);
          }
        }
      }
      throw new NoSuchObjectException(
          "Unable to find partition " + partName + " for table " + tableName + " in database "
              + dbName);
    } catch (GroundException ex) {
      LOG.error("Unable to find partition {} for table {} in database {}", partName, tableName,
          dbName);
      throw new MetaException(ex.getMessage());
    }
  }

  boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    // TODO Auto-generated method stub
    return false;
  }

  List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    try {
      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(tableName);
      List<Partition> parts = new ArrayList<Partition>();
      if (!versions.isEmpty()) {
        long prevVersionId = versions.get(0);
        List<Long> nodeIds = groundReadWrite.getNodeVersionFactory().getAdjacentNodes(prevVersionId,
            "");
        for (long nodeId : nodeIds) {
          NodeVersion oldNodeVersion = groundReadWrite.getNodeVersionFactory()
              .retrieveFromDatabase(nodeId);
          parts.add(JsonUtil
              .fromJson((String) oldNodeVersion.getTags().get(
                  oldNodeVersion.getNodeId()).getValue(), Partition.class));
        }
      }
      return parts;
    } catch (GroundException ex) {
      LOG.error("Get partitions failed on table {} in database {}", tableName, dbName);
      throw new MetaException(ex.getMessage());
    }

  }

}