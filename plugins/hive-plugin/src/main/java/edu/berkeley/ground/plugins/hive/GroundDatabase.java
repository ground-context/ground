/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.plugins.hive;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import com.google.gson.Gson;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroundDatabase {
    static final private Logger LOG = LoggerFactory.getLogger(GroundDatabase.class.getName());

    static final String DATABASE_NODE = "_DATABASE";

    static final String DATABASE_TABLE_EDGE = "_DATABASE_TABLE";

    private static final List<String> EMPTY_PARENT_LIST = new ArrayList<String>();

    private GroundReadWrite ground = null;
    private GroundTable table = null;

    GroundDatabase(GroundReadWrite ground) {
        this.ground = ground;
        this.table = new GroundTable(ground);
    }

    Node getNode(String dbName) throws GroundException {
        try {
            LOG.debug("Fetching database node: " + dbName);
            return ground.getNodeFactory().retrieveFromDatabase(dbName);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating databsae node: {}", dbName);

            Node node = ground.getNodeFactory().create(dbName);
            Structure nodeStruct = ground.getStructureFactory().create(node.getName());
            LOG.debug("node structure created {}", nodeStruct);
            return node;
        }
    }

    public Structure getNodeStructure(String dbName) throws GroundException {
        try {
            Node node = this.getNode(dbName);
            return ground.getStructureFactory().retrieveFromDatabase(dbName);
        } catch (GroundException e) {
            LOG.error("Unable to fetch database node structure");
            throw e;
        }
    }

    public Edge getEdge(NodeVersion nodeVersion) throws GroundException {
        String edgeId = nodeVersion.getNodeId();
        try {
            LOG.debug("Fetching database table edge: " + edgeId);
            return ground.getEdgeFactory().retrieveFromDatabase(edgeId);
        } catch (GroundException e) {
            LOG.debug("Not found - Creating database table edge: " + edgeId);
            Edge edge = ground.getEdgeFactory().create(edgeId);
            Structure edgeStruct = ground.getStructureFactory().create(edge.getName());
            return edge;
        }
    }

    Structure getEdgeStructure(NodeVersion nodeVersion) throws GroundException {
        try {
            LOG.debug("Fetching database table edge structure: " + nodeVersion.getNodeId());
            Edge edge = this.getEdge(nodeVersion);
            return ground.getStructureFactory().retrieveFromDatabase(edge.getName());
        } catch (GroundException e) {
            LOG.debug("Not found - database table edge structure: " + nodeVersion.getNodeId());
            throw e;
        }
    }

    Database fromJSON(String json) {
        Gson gson = new Gson();
        return (Database) gson.fromJson(json, Database.class);
    }

    String toJSON(Database db) {
        Gson gson = new Gson();
        return gson.toJson(db);
    }

    Database getDatabase(String dbName) throws NoSuchObjectException {
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);
            if (versions.isEmpty()) {
                throw new GroundException("Database node not found: " + dbName);
            }

            NodeVersion latestVersion = ground.getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
            Map<String, Tag> dbTag = latestVersion.getTags();

            return this.fromJSON((String) dbTag.get(dbName).getValue());
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
            Structure dbStruct = this.getNodeStructure(dbName);

            Tag dbTag = new Tag("1.0.0", dbName, toJSON(db), GroundType.STRING);

            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            structVersionAttribs.put(dbName, GroundType.STRING);

            StructureVersion sv = ground.getStructureVersionFactory().create(dbStruct.getId(), structVersionAttribs,
                    EMPTY_PARENT_LIST);

            String reference = db.getLocationUri();
            HashMap<String, Tag> tags = new HashMap<>();
            tags.put(dbName, dbTag);

            Map<String, String> dbParamMap = db.getParameters();
            if (dbParamMap == null) {
                dbParamMap = new HashMap<String, String>();
            }
            Map<String, String> parameters = dbParamMap;

            List<String> parent = new ArrayList<String>();
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);
            if (!versions.isEmpty()) {
                parent.add(versions.get(0));
            }

            NodeVersion dbNodeVersion = ground.getNodeVersionFactory().create(tags, sv.getId(), reference, parameters,
                    dbNode.getId(), parent);

            return dbNodeVersion;
        } catch (GroundException e) {
            LOG.error("Failure to create a database node: " + db.getName());
            throw new MetaException(e.getMessage());
        }
    }

    // Table related functions
    NodeVersion createTable(Table table) throws InvalidObjectException, MetaException {
        try {
            String dbName = table.getDbName();
            NodeVersion tableNodeVersion = this.table.createTable(table);
            Database prevDb = this.getDatabase(dbName);

            List<String> versions = ground.getNodeFactory().getLeaves(dbName);

            NodeVersion dbNodeVersion = this.createDatabase(prevDb);
            String dbNodeVersionId = dbNodeVersion.getId();

            Edge edge = this.getEdge(tableNodeVersion);
            Structure structure = this.getEdgeStructure(tableNodeVersion);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : tableNodeVersion.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                    new ArrayList<>());

            ground.getEdgeVersionFactory().create(tableNodeVersion.getTags(), sv.getId(),
                    tableNodeVersion.getReference(), tableNodeVersion.getParameters(), edge.getId(), dbNodeVersionId,
                    tableNodeVersion.getId(), EMPTY_PARENT_LIST);

            if (!versions.isEmpty() && versions.size() != 0) {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                    edge = this.getEdge(oldNV);

                    structVersionAttribs = new HashMap<>();
                    for (String key : oldNV.getTags().keySet()) {
                        structVersionAttribs.put(key, GroundType.STRING);
                    }

                    // create an edge version for a dbname
                    sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                            new ArrayList<>());
                    ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                            oldNV.getParameters(), edge.getId(), dbNodeVersionId, oldNV.getId(),
                            new ArrayList<String>());
                }
            }

            return dbNodeVersion;
        } catch (GroundException ex) {
            throw new MetaException(ex.getMessage());
        } catch (NoSuchObjectException ex) {
            throw new MetaException(ex.getMessage());
        }
    }

    NodeVersion dropTable(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        try {
            boolean found = false;
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);

            if (versions.isEmpty()) {
                LOG.error("Could not find table to drop named {}", tableName);
                return null;
            } else {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");

                if (nodeIds.size() == 0) {
                    LOG.error("Failed to drop table {}", dbName);
                    return null;
                }
                Database db = this.getDatabase(dbName);
                NodeVersion dbNodeVersion = this.createDatabase(db);
                String dbVersionId = dbNodeVersion.getId();
                String tableNodeId = "Nodes." + tableName;

                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);

                    if (!oldNV.getNodeId().equals(tableNodeId)) {
                        Edge edge = this.getEdge(oldNV);
                        Structure structure = this.getEdgeStructure(oldNV);

                        LOG.error("Found edge with name {}", oldNV.getNodeId());

                        Map<String, GroundType> structVersionAttribs = new HashMap<>();
                        for (String key : oldNV.getTags().keySet()) {
                            structVersionAttribs.put(key, GroundType.STRING);
                        }
                        // create an edge for each table other than the one
                        // being deleted
                        StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
                                structVersionAttribs, new ArrayList<>());
                        ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                                oldNV.getParameters(), edge.getId(), dbVersionId, oldNV.getId(),
                                new ArrayList<String>());
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
        return this.table.getTable(dbName, tableName);
    }

    List<String> getTables(String dbName, String pattern) throws MetaException {
        return this.table.getTables(dbName, pattern);
    }

    NodeVersion addPartitions(String dbName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        try {
            NodeVersion tableNodeVersion = this.table.addPartitions(dbName, tableName, parts);
            Database prevDb = this.getDatabase(dbName);

            List<String> versions = ground.getNodeFactory().getLeaves(dbName);

            NodeVersion dbNodeVersion = this.createDatabase(prevDb);
            String dbNodeVersionId = dbNodeVersion.getId();

            Edge edge = this.getEdge(tableNodeVersion);
            Structure structure = this.getEdgeStructure(tableNodeVersion);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : tableNodeVersion.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                    new ArrayList<>());

            ground.getEdgeVersionFactory().create(tableNodeVersion.getTags(), sv.getId(),
                    tableNodeVersion.getReference(), tableNodeVersion.getParameters(), edge.getId(), dbNodeVersionId,
                    tableNodeVersion.getId(), EMPTY_PARENT_LIST);

            if (!versions.isEmpty() && versions.size() > 0) {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                    edge = this.getEdge(oldNV);

                    structVersionAttribs = new HashMap<>();
                    for (String key : oldNV.getTags().keySet()) {
                        structVersionAttribs.put(key, GroundType.STRING);
                    }

                    // create an edge version for a dbname
                    sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                            new ArrayList<>());
                    ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                            oldNV.getParameters(), edge.getId(), dbNodeVersionId, oldNV.getId(),
                            new ArrayList<String>());
                }
            }

            return dbNodeVersion;
        } catch (GroundException ex) {
            LOG.error("Unable to add partition to table {} database {}", tableName, dbName);
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
            return this.table.getPartition(dbName, tableName, partName);
        } catch (MetaException | NoSuchObjectException ex) {
            LOG.error("Unable to ger partition {} fro table {} database {}", partName, tableName, dbName);
            throw ex;
        }
    }

    List<Partition> getPartitions(String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        try {
            return this.table.getPartitions(dbName, tableName, max);
        } catch (MetaException | NoSuchObjectException ex) {
            throw ex;
        }
    }
}
