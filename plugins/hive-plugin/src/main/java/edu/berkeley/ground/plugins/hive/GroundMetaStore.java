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

import com.google.common.annotations.VisibleForTesting;

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.NodeVersion;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GroundMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(GroundMetaStore.class.getName());

  private static final HashMap<String, String> EMPTY_MAP = new HashMap<>();
  private static final String TIMESTAMP = "TIMESTAMP";
  private static final String METASTORE_NODE = "_METASTORE";

  private GroundReadWrite ground = null;
  private GroundDatabase groundDatabase = null;

  @VisibleForTesting
  GroundMetaStore(GroundReadWrite ground) {
    this.ground = ground;
    groundDatabase = new GroundDatabase(ground);
  }

  /**
   * Retrieve the metastore node.
   *
   * @return the metastore node
   * @throws GroundException an exception while retrieving the node
   */
  public Node getNode() throws GroundException {
    try {
      LOG.debug("Fetching metastore node: {}", METASTORE_NODE);
      return ground.getNodeFactory().retrieveFromDatabase(METASTORE_NODE);
    } catch (GroundException ge1) {
      LOG.debug("Not found - Creating metastore node: {}", METASTORE_NODE);
      Node node = ground.getNodeFactory().create(METASTORE_NODE, null, new HashMap<>());
      Structure nodeStruct = ground.getStructureFactory().create(node.getName(), null,
          new HashMap<>());
      LOG.debug("node struct created {}", nodeStruct.getName());
      return node;
    }
  }

  Structure getNodeStructure() throws GroundException {
    try {
      LOG.debug("Fetching metastore node structure: " + METASTORE_NODE);
      Node node = this.getNode();
      return ground.getStructureFactory().retrieveFromDatabase(node.getName());
    } catch (GroundException e) {
      LOG.debug("Not found - metastore node structure: " + METASTORE_NODE);
      throw e;
    }
  }

  Edge getEdge(NodeVersion nodeVersion) throws GroundException {
    String edgeId = "" + nodeVersion.getId();
    try {
      LOG.debug("Fetching metastore database edge: " + edgeId);
      return ground.getEdgeFactory().retrieveFromDatabase(edgeId);
    } catch (GroundException e) {
      LOG.debug("Not found - Creating metastore table edge: {}", edgeId);
      Edge edge = ground.getEdgeFactory().create(edgeId, null, getNode().getId(),
          nodeVersion.getNodeId(), new HashMap<>());
      Structure edgeStruct = ground.getStructureFactory().create(edge.getName(), null,
          new HashMap<>());
      return edge;
    }
  }

  Structure getEdgeStructure(NodeVersion nodeVersion) throws GroundException {
    try {
      LOG.debug("Fetching metastore database edge structure: " + nodeVersion.getNodeId());
      Edge edge = this.getEdge(nodeVersion);
      return ground.getStructureFactory().retrieveFromDatabase(edge.getName());
    } catch (GroundException e) {
      LOG.debug("Not found - metastore table edge structure: " + nodeVersion.getNodeId());
      throw e;
    }
  }

  /**
   * Given an entity name retrieve its node version from database.
   */
  NodeVersion getNodeVersion() throws GroundException {
    try {
      Node node = this.getNode();
      long nodeVersionId = ground.getNodeFactory().getLeaves(node.getName()).get(0);
      return ground.getNodeVersionFactory().retrieveFromDatabase(nodeVersionId);
    } catch (GroundException ex) {
      throw ex;
    }
  }

  NodeVersion createNodeVersion() throws GroundException {
    try {
      Node node = this.getNode();
      final long nodeId = node.getId();

      Map<String, GroundType> structureVersionAttributes = new HashMap<>();
      structureVersionAttributes.put(TIMESTAMP, GroundType.STRING);

      Structure structure = this.getNodeStructure();
      StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
          structureVersionAttributes, new ArrayList<>());

      Map<String, Tag> tags = new HashMap<>();
      String timestamp = Date.from(Instant.now()).toString();
      tags.put(METASTORE_NODE, new Tag(0, TIMESTAMP, timestamp, GroundType.STRING));

      List<Long> parent = new ArrayList<>();
      List<Long> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);
      if (!versions.isEmpty()) {
        parent.add(versions.get(0));
      }

      // TODO: put version and other information in tags
      return ground.getNodeVersionFactory().create(tags, sv.getId(), "" + nodeId, EMPTY_MAP, nodeId,
          parent);
    } catch (GroundException ex) {
      LOG.error("Unable to initialize Ground Metastore: " + ex);
      throw ex;
    }
  }

  // Database related functions

  Database getDatabase(String name) throws NoSuchObjectException {
    try {
      List<String> dbNames = this.getDatabases(name);
      if (dbNames.contains(name)) {
        return groundDatabase.getDatabase(name);
      }
      return null;
    } catch (NoSuchObjectException e) {
      LOG.error("Unable to locate table {}", name);
      throw e;
    }
  }

  List<String> getDatabases(String dbPattern) throws NoSuchObjectException {
    List<String> databases = new ArrayList<String>();
    try {
      List<Long> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

      if (!versions.isEmpty()) {
        long metaVersionId = versions.get(0);
        List<Long> dbNodeIds = ground.getNodeVersionFactory().getAdjacentNodes(metaVersionId,
            dbPattern);

        for (long dbNodeId : dbNodeIds) {
          NodeVersion dbNodeVersion = ground.getNodeVersionFactory().retrieveFromDatabase(dbNodeId);
          // fetch the edge for the dbname
          Edge edge = ground.getEdgeFactory().retrieveFromDatabase("" + dbNodeVersion.getId());
          databases.add(edge.getName().split("Nodes.")[1]);
        }
      }
    } catch (GroundException ex) {
      LOG.error("Get databases failed for pattern {}", dbPattern);
      throw new NoSuchObjectException(ex.getMessage());
    }
    return databases;
  }

  void createDatabase(Database db) throws InvalidObjectException, MetaException {
    try {
      NodeVersion nv = groundDatabase.createDatabase(db);

      List<Long> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

      NodeVersion metaNodeVersion = this.createNodeVersion();
      long metaVersionId = metaNodeVersion.getId();
      Edge edge = this.getEdge(nv);
      Structure structure = this.getEdgeStructure(nv);
      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      for (String key : nv.getTags().keySet()) {
        structVersionAttribs.put(key, GroundType.STRING);
      }
      StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
          structVersionAttribs, new ArrayList<>());

      ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), nv.getReference(),
          nv.getParameters(), edge.getId(), metaVersionId, -1, nv.getId(), -1, new ArrayList<>());

      if (!versions.isEmpty()) {
        if (versions.size() != 0) {
          long prevVersionId = versions.get(0);
          List<Long> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
          for (long nodeId : nodeIds) {
            NodeVersion oldNodeVersion = ground.getNodeVersionFactory()
                .retrieveFromDatabase(nodeId);
            edge = this.getEdge(oldNodeVersion);

            structVersionAttribs = new HashMap<>();
            for (String key : oldNodeVersion.getTags().keySet()) {
              structVersionAttribs.put(key, GroundType.STRING);
            }

            // create an edge version for a dbname
            sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                new ArrayList<>());
            ground.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
                oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
                metaVersionId, -1 , oldNodeVersion.getId(), -1, new ArrayList<>());
          }
        }
      }

    } catch (InvalidObjectException | MetaException e) {
      throw e;
    } catch (GroundException e) {
      throw new MetaException("Failed to create database: " + db.getName() + " because "
          + e.getMessage());
    }
  }

  boolean dropDatabase(String dbName) throws GroundException {
    try {
      boolean found = false;
      List<Long> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

      if (versions.isEmpty()) {
        LOG.error("Could not find datbase to drop named " + dbName);
        return false;
      } else {
        long prevVersionId = versions.get(0);
        List<Long> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");

        if (nodeIds.size() == 0) {
          LOG.error("Failed to drop database {}", dbName);
          return false;
        }
        NodeVersion metaNodeVersion = this.createNodeVersion();
        long metaVersionId = metaNodeVersion.getId();
        String dbNodeId = "Nodes." + dbName;

        for (long nodeId : nodeIds) {
          NodeVersion oldNodeVersion = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);

          if (oldNodeVersion.getNodeId() != 0) {
            Edge edge = this.getEdge(oldNodeVersion);
            Structure structure = this.getEdgeStructure(oldNodeVersion);

            LOG.error("Found edge with name {}", oldNodeVersion.getNodeId());

            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : oldNodeVersion.getTags().keySet()) {
              structVersionAttribs.put(key, GroundType.STRING);
            }
            // create an edge for each dbname other than the one
            // being deleted
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
                structVersionAttribs, new ArrayList<>());
            ground.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
                oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
                metaVersionId, -1, oldNodeVersion.getId(), -1, new ArrayList<>());
          } else {
            found = true;
          }
        }
      }
      return found;
    } catch (GroundException e) {
      LOG.error("Failed to drop database {}", dbName);
      throw e;
    }
  }

  // Table related functions
  void createTable(Table table) throws InvalidObjectException, MetaException {
    try {
      String dbName = table.getDbName();

      NodeVersion nv = groundDatabase.createTableComponents(table);

      List<Long> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

      NodeVersion metaNodeVersion = this.createNodeVersion();
      long metaVersionId = metaNodeVersion.getId();
      Edge edge = this.getEdge(nv);
      Structure structure = this.getEdgeStructure(nv);
      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      for (String key : nv.getTags().keySet()) {
        structVersionAttribs.put(key, GroundType.STRING);
      }
      StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
          structVersionAttribs, new ArrayList<>());

      ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), nv.getReference(),
          nv.getParameters(), edge.getId(), metaVersionId, -1, nv.getId(), -1, new ArrayList<>());

      String dbNodeId = "Nodes." + dbName;

      if (!versions.isEmpty()) {
        if (versions.size() != 0) {
          long prevVersionId = versions.get(0);
          List<Long> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
          for (long nodeId : nodeIds) {
            NodeVersion oldNodeVersion = ground.getNodeVersionFactory()
                .retrieveFromDatabase(nodeId);
            edge = this.getEdge(oldNodeVersion);

            if (oldNodeVersion.getNodeId() != 0) {
              structVersionAttribs = new HashMap<>();
              for (String key : oldNodeVersion.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
              }

              // create an edge version for a dbname
              sv = ground.getStructureVersionFactory().create(structure.getId(),
                  structVersionAttribs, new ArrayList<>());
              ground.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
                  oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
                  metaVersionId, -1, oldNodeVersion.getId(), -1, new ArrayList<>());
            }
          }
        }
      }

    } catch (InvalidObjectException | MetaException e) {
      throw e;
    } catch (GroundException e) {
      throw new MetaException("Failed to create table: " + table.getTableName() + " because "
          + e.getMessage());
    }
  }

  boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try {
      NodeVersion nv = groundDatabase.dropTableNodeVersion(dbName, tableName);
      if (nv == null) {
        throw new NoSuchObjectException("Table not found: " + tableName);
      }

      List<Long> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

      NodeVersion metaNodeVersion = this.createNodeVersion();
      long metaVersionId = metaNodeVersion.getId();
      Edge edge = this.getEdge(nv);
      Structure structure = this.getEdgeStructure(nv);
      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      for (String key : nv.getTags().keySet()) {
        structVersionAttribs.put(key, GroundType.STRING);
      }
      StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
          structVersionAttribs, new ArrayList<>());

      ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), nv.getReference(),
          nv.getParameters(), edge.getId(), metaVersionId, -1, nv.getId(), -1, new ArrayList<>());

      String dbNodeId = "Nodes." + dbName;

      if (!versions.isEmpty()) {
        if (versions.size() != 0) {
          long prevVersionId = versions.get(0);
          List<Long> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
          for (long nodeId : nodeIds) {
            NodeVersion oldNodeVersion = ground.getNodeVersionFactory()
                .retrieveFromDatabase(nodeId);
            edge = this.getEdge(oldNodeVersion);

            if (oldNodeVersion.getNodeId() != 0) {
              structVersionAttribs = new HashMap<>();
              for (String key : oldNodeVersion.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
              }

              // create an edge version for a dbname
              sv = ground.getStructureVersionFactory().create(structure.getId(),
                  structVersionAttribs, new ArrayList<>());
              ground.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
                  oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
                  metaVersionId, -1, oldNodeVersion.getId(), -1, new ArrayList<>());
            }
          }
        }
      }
      return true;
    } catch (InvalidObjectException | MetaException e) {
      throw e;
    } catch (GroundException e) {
      throw new MetaException("Failed to create table: " + tableName + " because "
          + e.getMessage());
    }
  }

  Table getTable(String dbName, String tableName) throws MetaException {
    try {
      return groundDatabase.getTable(dbName, tableName);
    } catch (MetaException ex) {
      LOG.error("Unalbe to find table {} for database {}", tableName, dbName);
      throw ex;
    }
  }

  List<String> getTables(String dbName, String pattern) throws MetaException {
    return groundDatabase.getTables(dbName, pattern);
  }

  boolean addPartitions(String dbName, String tableName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    try {
      NodeVersion nv = groundDatabase.addPartitions(dbName, tableName, parts);
      if (nv == null) {
        LOG.error("Unable to create partition for table " + tableName + " in database " + dbName);
        throw new InvalidObjectException(
            "Unable to create partition for table " + tableName + " in database " + dbName);
      }

      List<Long> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

      NodeVersion metaNodeVersion = this.createNodeVersion();
      long metaVersionId = metaNodeVersion.getId();
      Edge edge = this.getEdge(nv);
      Structure structure = this.getEdgeStructure(nv);
      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      for (String key : nv.getTags().keySet()) {
        structVersionAttribs.put(key, GroundType.STRING);
      }
      StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
          structVersionAttribs, new ArrayList<>());

      ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), nv.getReference(),
          nv.getParameters(), edge.getId(), metaVersionId, -1, nv.getId(), -1, new ArrayList<>());

      String dbNodeId = "Nodes." + dbName;

      if (!versions.isEmpty()) {
        if (versions.size() != 0) {
          long prevVersionId = versions.get(0);
          List<Long> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
          for (long nodeId : nodeIds) {
            NodeVersion oldNodeVersion = ground.getNodeVersionFactory()
                .retrieveFromDatabase(nodeId);
            edge = this.getEdge(oldNodeVersion);

            if (oldNodeVersion.getNodeId() != 0) {
              structVersionAttribs = new HashMap<>();
              for (String key : oldNodeVersion.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
              }

              // create an edge version for a dbname
              sv = ground.getStructureVersionFactory().create(structure.getId(),
                  structVersionAttribs, new ArrayList<>());
              ground.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
                  oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
                  metaVersionId, -1, oldNodeVersion.getId(), -1, new ArrayList<>());
            }
          }
        }
      }
      return true;

    } catch (InvalidObjectException | MetaException ex) {
      LOG.error("Unable to add partition to table {} database {} with error: ", tableName, dbName,
          ex.getMessage());
      throw ex;
    } catch (GroundException e) {
      throw new MetaException(
          "Failed to add partition to " + tableName + " in " + dbName + " because "
              + e.getMessage());
    }
  }

  Partition getPartition(String dbName, String tableName, String partName)
      throws MetaException, NoSuchObjectException {
    return this.groundDatabase.getPartition(dbName, tableName, partName);
  }

  List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    try {
      return this.groundDatabase.getPartitions(dbName, tableName, max);
    } catch (MetaException | NoSuchObjectException ex) {
      throw ex;
    }
  }
}
