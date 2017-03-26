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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroundDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(GroundDatabase.class.getName());

  static final String DATABASE_NODE = "_DATABASE";

  private GroundReadWrite groundReadWrite = null;
  private GroundTable groundTable = null;

  GroundDatabase(GroundReadWrite ground) {
    groundReadWrite = ground;
    groundTable = new GroundTable(ground);
  }

  Node getNode(String dbName) throws GroundException {
    try {
      LOG.debug("Fetching database node: " + dbName);
      return groundReadWrite.getNodeFactory().retrieveFromDatabase(dbName);
    } catch (GroundException ge1) {
      LOG.debug("Not found - Creating databsae node: {}", dbName);

      Node node = groundReadWrite.getNodeFactory().create(dbName, null, new HashMap<>());
      Structure nodeStruct = groundReadWrite.getStructureFactory().create(node.getName(), null,
          new HashMap<>());
      LOG.debug("node structure created {}", nodeStruct);
      return node;
    }
  }

  Structure getNodeStructure(String dbName) throws GroundException {
    try {
      Node node = this.getNode(dbName);
      return groundReadWrite.getStructureFactory().retrieveFromDatabase(dbName);
    } catch (GroundException e) {
      LOG.error("Unable to fetch database node structure");
      throw e;
    }
  }

  Edge getEdge(NodeVersion nodeVersion) throws GroundException {
    String edgeId = "" + nodeVersion.getNodeId();
    try {
      LOG.debug("Fetching database table edge: " + edgeId);
      return groundReadWrite.getEdgeFactory().retrieveFromDatabase(edgeId);
    } catch (GroundException e) {
      LOG.debug("Not found - Creating database table edge: " + edgeId);
      Edge edge = groundReadWrite.getEdgeFactory().create(edgeId, null, getNode(DATABASE_NODE)
              .getId(),
          nodeVersion.getNodeId(), new HashMap<>());
      Structure edgeStruct = groundReadWrite.getStructureFactory().create(edge.getName(), null,
          new HashMap<>());
      return edge;
    }
  }

  Structure getEdgeStructure(NodeVersion nodeVersion) throws GroundException {
    try {
      LOG.debug("Fetching database table edge structure: " + nodeVersion.getNodeId());
      Edge edge = this.getEdge(nodeVersion);
      return groundReadWrite.getStructureFactory().retrieveFromDatabase(edge.getName());
    } catch (GroundException e) {
      LOG.debug("Not found - database table edge structure: " + nodeVersion.getNodeId());
      throw e;
    }
  }

  Database getDatabase(String dbName) throws NoSuchObjectException {
    try {
      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(dbName);
      if (versions.isEmpty()) {
        throw new GroundException("Database node not found: " + dbName);
      }

      NodeVersion latestVersion = groundReadWrite.getNodeVersionFactory()
          .retrieveFromDatabase(versions.get(0));
      Map<String, Tag> dbTag = latestVersion.getTags();

      return JsonUtil.fromJson((String) dbTag.get(dbName).getValue(), Database.class);
    } catch (GroundException e) {
      throw new NoSuchObjectException(e.getMessage());
    }
  }

  NodeVersion createDatabase(Database db) throws InvalidObjectException, MetaException {
    if (db == null) {
      throw new InvalidObjectException("Database object passed is null");
    }
    try {
      String dbName = db.getName();
      Node dbNode = this.getNode(dbName);
      Structure dbStruct = groundReadWrite.getStructureFactory().create(dbNode.getName(), null,
          new HashMap<>());
      LOG.debug("Node and Structure {}, {}", dbNode.getId(), dbStruct.getId());
      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      structVersionAttribs.put(dbName, GroundType.STRING);
      Tag dbTag = new Tag(0, dbName, JsonUtil.toJson(db), GroundType.STRING);

      HashMap<String, Tag> tags = new HashMap<>();
      tags.put(dbName, dbTag);

      Map<String, String> dbParamMap = db.getParameters();
      if (dbParamMap == null) {
        dbParamMap = new HashMap<>();
      }
      List<Long> parent = new ArrayList<>();
      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(dbName);
      if (!versions.isEmpty()) {
        LOG.debug("leaves {}", versions.get(0));
        parent.add(versions.get(0));
      }

      String reference = db.getLocationUri();
      StructureVersion sv = groundReadWrite.getStructureVersionFactory().create(dbStruct.getId(),
          structVersionAttribs, new ArrayList<Long>());

      return groundReadWrite.getNodeVersionFactory().create(tags, sv.getId(), reference,
          dbParamMap, dbNode.getId(), parent);
    } catch (GroundException e) {
      LOG.error("Failure to create a database node: {}", e);
      throw new MetaException(e.getMessage());
    }
  }

  // Table related functions
  NodeVersion createTableComponents(Table table) throws InvalidObjectException, MetaException {
    try {
      String dbName = table.getDbName();
      NodeVersion tableNodeVersion = groundTable.createTableNodeVersion(table);
      Database prevDb = this.getDatabase(dbName);

      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(dbName);

      NodeVersion dbNodeVersion = this.createDatabase(prevDb);
      long dbNodeVersionId = dbNodeVersion.getId();

      Edge edge = this.getEdge(tableNodeVersion);
      Structure structure = this.getEdgeStructure(tableNodeVersion);
      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      for (String key : tableNodeVersion.getTags().keySet()) {
        structVersionAttribs.put(key, GroundType.STRING);
      }
      StructureVersion sv = groundReadWrite.getStructureVersionFactory().create(structure.getId(),
          structVersionAttribs, new ArrayList<>());

      groundReadWrite.getEdgeVersionFactory().create(tableNodeVersion.getTags(), sv.getId(),
          tableNodeVersion.getReference(), tableNodeVersion.getParameters(), edge.getId(),
          dbNodeVersionId, -1, tableNodeVersion.getId(), -1, new ArrayList<Long>());

      if (!versions.isEmpty() && versions.size() != 0) {
        long prevVersionId = versions.get(0);
        List<Long> nodeIds = groundReadWrite.getNodeVersionFactory().getAdjacentNodes(prevVersionId,
            "");
        for (long nodeId : nodeIds) {
          NodeVersion oldNodeVersion = groundReadWrite.getNodeVersionFactory()
              .retrieveFromDatabase(nodeId);
          edge = this.getEdge(oldNodeVersion);

          structVersionAttribs = new HashMap<>();
          for (String key : oldNodeVersion.getTags().keySet()) {
            structVersionAttribs.put(key, GroundType.STRING);
          }

          // create an edge version for a dbname
          sv = groundReadWrite.getStructureVersionFactory().create(structure.getId(),
              structVersionAttribs, new ArrayList<>());
          groundReadWrite.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
              oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
              dbNodeVersionId, -1, oldNodeVersion.getId(), -1, new ArrayList<>());
        }
      }

      return dbNodeVersion;
    } catch (GroundException ex) {
      LOG.error(ex.getMessage());
      throw new MetaException(ex.getMessage());
    } catch (NoSuchObjectException ex) {
      throw new MetaException(ex.getMessage());
    }
  }

  NodeVersion dropTableNodeVersion(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try {
      boolean found = false;
      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(dbName);

      if (versions.isEmpty()) {
        LOG.error("Could not find table to drop named {}", tableName);
        return null;
      } else {
        long prevVersionId = versions.get(0);
        List<Long> nodeIds = groundReadWrite.getNodeVersionFactory().getAdjacentNodes(prevVersionId,
            "");

        if (nodeIds.size() == 0) {
          LOG.error("Failed to drop table {}", dbName);
          return null;
        }
        Database db = this.getDatabase(dbName);
        NodeVersion dbNodeVersion = this.createDatabase(db);
        long dbVersionId = dbNodeVersion.getId();
        String tableNodeId = "Nodes." + tableName;

        for (long nodeId : nodeIds) {
          NodeVersion oldNodeVersion = groundReadWrite.getNodeVersionFactory()
              .retrieveFromDatabase(nodeId);

          if (!("" + oldNodeVersion.getNodeId()).equals(tableNodeId)) {
            Edge edge = this.getEdge(oldNodeVersion);
            Structure structure = this.getEdgeStructure(oldNodeVersion);

            LOG.error("Found edge with name {}", oldNodeVersion.getNodeId());

            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : oldNodeVersion.getTags().keySet()) {
              structVersionAttribs.put(key, GroundType.STRING);
            }
            // create an edge for each table other than the one
            // being deleted
            StructureVersion sv = groundReadWrite.getStructureVersionFactory()
                .create(structure.getId(),
                structVersionAttribs, new ArrayList<>());
            groundReadWrite.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
                oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
                dbVersionId, -1, oldNodeVersion .getId(), -1, new ArrayList<>());
          }
        }
        return dbNodeVersion;
      }
    } catch (GroundException ex) {
      LOG.error("Failed to drop table {}", tableName);
      throw new MetaException("Failed to drop table: " + ex.getMessage());
    }
  }

  Table getTable(String dbName, String tableName) throws MetaException {
    return groundTable.getTable(dbName, tableName);
  }

  List<String> getTables(String dbName, String pattern) throws MetaException {
    return groundTable.getTables(dbName, pattern);
  }

  NodeVersion addPartitions(String dbName, String tableName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    try {
      NodeVersion tableNodeVersion = groundTable.addPartitions(dbName, tableName, parts);
      Database prevDb = this.getDatabase(dbName);

      List<Long> versions = groundReadWrite.getNodeFactory().getLeaves(dbName);

      NodeVersion dbNodeVersion = this.createDatabase(prevDb);
      long dbNodeVersionId = dbNodeVersion.getId();

      Edge edge = this.getEdge(tableNodeVersion);
      Structure structure = this.getEdgeStructure(tableNodeVersion);
      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      for (String key : tableNodeVersion.getTags().keySet()) {
        structVersionAttribs.put(key, GroundType.STRING);
      }
      StructureVersion sv = groundReadWrite.getStructureVersionFactory().create(structure.getId(),
          structVersionAttribs, new ArrayList<>());

      groundReadWrite.getEdgeVersionFactory().create(tableNodeVersion.getTags(), sv.getId(),
          tableNodeVersion.getReference(), tableNodeVersion.getParameters(), edge.getId(),
          dbNodeVersionId, -1, tableNodeVersion.getId(), -1, new ArrayList<Long>());

      if (!versions.isEmpty() && versions.size() > 0) {
        long prevVersionId = versions.get(0);
        List<Long> nodeIds = groundReadWrite.getNodeVersionFactory().getAdjacentNodes(prevVersionId,
            "");
        for (long nodeId : nodeIds) {
          NodeVersion oldNodeVersion = groundReadWrite.getNodeVersionFactory()
              .retrieveFromDatabase(nodeId);
          edge = this.getEdge(oldNodeVersion);

          structVersionAttribs = new HashMap<>();
          for (String key : oldNodeVersion.getTags().keySet()) {
            structVersionAttribs.put(key, GroundType.STRING);
          }

          // create an edge version for a dbname
          sv = groundReadWrite.getStructureVersionFactory().create(structure.getId(),
              structVersionAttribs, new ArrayList<>());
          groundReadWrite.getEdgeVersionFactory().create(oldNodeVersion.getTags(), sv.getId(),
              oldNodeVersion.getReference(), oldNodeVersion.getParameters(), edge.getId(),
              dbNodeVersionId, -1, oldNodeVersion.getId(), -1, new ArrayList<>());
        }
      }
      return dbNodeVersion;
    } catch (GroundException ex) {
      LOG.error("Unable to add partition to table {} database {}", tableName, dbName, ex);
      throw new MetaException(ex.getMessage());
    } catch (NoSuchObjectException ex) {
      LOG.error("Database {} not found", dbName);
      throw new MetaException(ex.getMessage());
    } catch (InvalidObjectException | MetaException ex) {
      LOG.error("Unable to add partition to table {} database {}", tableName, dbName);
      throw ex;
    }
  }

  Partition getPartition(String dbName, String tableName, String partName)
      throws NoSuchObjectException, MetaException {
    try {
      return groundTable.getPartition(dbName, tableName, partName);
    } catch (MetaException | NoSuchObjectException ex) {
      LOG.error("Unable to ger partition {} fro table {} database {}", partName, tableName, dbName);
      throw ex;
    }
  }

  List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    try {
      return groundTable.getPartitions(dbName, tableName, max);
    } catch (MetaException | NoSuchObjectException ex) {
      throw ex;
    }
  }
}
