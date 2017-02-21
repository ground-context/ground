/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import edu.berkeley.ground.plugins.hive.util.JsonUtil;

import org.apache.commons.httpclient.HttpException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroundDatabase {
    static final private Logger LOG = LoggerFactory.getLogger(GroundDatabase.class.getName());

    static final String DATABASE_NODE = "_DATABASE";

    static final String DATABASE_TABLE_EDGE = "_DATABASE_TABLE";

    private static final List<String> EMPTY_PARENT_LIST = new ArrayList<String>();

    private GroundReadWrite groundReadWrite = null;
    private GroundTable groundTable = null;

    GroundDatabase(GroundReadWrite ground) {
        groundReadWrite = ground;
        groundTable = new GroundTable(ground);
    }

    Node getNode(String dbName) throws GroundException {
        LOG.debug("Fetching database node: " + dbName);
        return groundReadWrite.getNode(dbName);
    }

    Structure getNodeStructure(String dbName) throws GroundException {
        return groundReadWrite.getStructure(dbName);
    }

    Edge getEdge(NodeVersion nodeVersion) throws GroundException {
        String edgeId = nodeVersion.getNodeId();
        LOG.debug("Fetching database table edge: " + edgeId);
        return groundReadWrite.getEdge(edgeId);
    }

    Structure getEdgeStructure(NodeVersion nodeVersion) throws GroundException {
        try {
            LOG.debug("Fetching database table edge structure: " + nodeVersion.getNodeId());
            Edge edge = this.getEdge(nodeVersion);
            return groundReadWrite.getStructure(edge.getName());
        } catch (GroundException e) {
            LOG.debug("Not found - database table edge structure: " + nodeVersion.getNodeId());
            throw new GroundException(e);
        }
    }

    Database getDatabase(String dbName) throws GroundException {
        List<String> versions = groundReadWrite.getLatestVersions(dbName);
        if (versions.isEmpty()) {
            throw new GroundException("Database node not found: " + dbName);
        }
        NodeVersion latestVersion = groundReadWrite.getNodeVersion(versions.get(0));
        Map<String, Tag> dbTag = latestVersion.getTags();
        return JsonUtil.fromJSON((String) dbTag.get(dbName).getValue(), Database.class);
    }

    NodeVersion createDatabase(Database db) throws InvalidObjectException, MetaException {
        if (db == null) {
            throw new InvalidObjectException("Database object passed is null");
        }
        try {
            String dbName = db.getName();
            Node dbNode = this.getNode(dbName);
            Structure dbStruct = groundReadWrite.createStructure(dbName);
            LOG.debug("Node and Structure {}, {}", dbNode.getId(), dbStruct.getId());
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            structVersionAttribs.put(dbName, GroundType.STRING);
            StructureVersion sv = groundReadWrite.createStructureVersion(dbStruct.getName(), dbStruct.getId(),
                    structVersionAttribs);

            Tag dbTag = new Tag("1.0.0", dbName, JsonUtil.toJSON(db), GroundType.STRING);
            String reference = db.getLocationUri();
            HashMap<String, Tag> tags = new HashMap<>();
            tags.put(dbName, dbTag);

            Map<String, String> referenceParameterMap = db.getParameters();
            if (referenceParameterMap == null) {
                referenceParameterMap = new HashMap<String, String>();
            }
            List<String> parent = new ArrayList<String>();
            List<String> versions = groundReadWrite.getLatestVersions(dbName);
            if (!versions.isEmpty()) {
                LOG.debug("leaves {}", versions.get(0));
                parent.add(versions.get(0));
            }
            return groundReadWrite.createNodeVersion(dbNode.getName(), tags, sv.getId(), reference,
                    referenceParameterMap, dbNode.getId());
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

            List<String> versions = groundReadWrite.getLatestVersions(dbName);

            NodeVersion dbNodeVersion = this.createDatabase(prevDb);
            String dbNodeVersionId = dbNodeVersion.getId();

            Edge edge = this.getEdge(tableNodeVersion);
            Structure structure = this.getEdgeStructure(tableNodeVersion);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : tableNodeVersion.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = groundReadWrite.createStructureVersion(structure.getName(), structure.getId(),
                    structVersionAttribs);

            groundReadWrite.createEdgeVersion(edge.getName(), tableNodeVersion.getTags(), sv.getId(),
                    tableNodeVersion.getReference(), tableNodeVersion.getParameters(), edge.getId(), dbNodeVersionId,
                    tableNodeVersion.getId());
            if (!versions.isEmpty() && versions.size() != 0) {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = groundReadWrite.getAdjacentNodes(prevVersionId, "");
                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = groundReadWrite.getNodeVersion(nodeId);
                    edge = this.getEdge(oldNV);

                    structVersionAttribs = new HashMap<>();
                    for (String key : oldNV.getTags().keySet()) {
                        structVersionAttribs.put(key, GroundType.STRING);
                    }

                    // create an edge version for a dbname
                    sv = groundReadWrite.getStructureVersion(structure.getId());
                    groundReadWrite.createEdgeVersion(edge.getName(), oldNV.getTags(), sv.getId(), oldNV.getReference(),
                            oldNV.getParameters(), edge.getId(), dbNodeVersionId, oldNV.getId());
                }
            }

            return dbNodeVersion;
        } catch (GroundException ex) {
            LOG.error(ex.getMessage());
            throw new MetaException(ex.getMessage());
        }
    }

    NodeVersion dropTableNodeVersion(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        try {
            boolean found = false;
            List<String> versions = groundReadWrite.getLatestVersions(dbName);

            if (versions.isEmpty()) {
                LOG.error("Could not find table to drop named {}", tableName);
                return null;
            } else {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = groundReadWrite.getAdjacentNodes(prevVersionId, "");

                if (nodeIds.size() == 0) {
                    LOG.error("Failed to drop table {}", dbName);
                    return null;
                }
                Database db = this.getDatabase(dbName);
                NodeVersion dbNodeVersion = this.createDatabase(db);
                String dbVersionId = dbNodeVersion.getId();
                String tableNodeId = "Nodes." + tableName;

                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = groundReadWrite.getNodeVersion(nodeId);

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
                        StructureVersion sv = groundReadWrite.createStructureVersion(structure.getName(), structure.getId(),
                                structVersionAttribs);
                        groundReadWrite.createEdgeVersion(edge.getName(), oldNV.getTags(), sv.getId(),
                                oldNV.getReference(), oldNV.getParameters(), edge.getId(), dbVersionId, oldNV.getId());
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

            List<String> versions = groundReadWrite.getLatestVersions(dbName);

            NodeVersion dbNodeVersion = this.createDatabase(prevDb);
            String dbNodeVersionId = dbNodeVersion.getId();

            Edge edge = this.getEdge(tableNodeVersion);
            Structure structure = this.getEdgeStructure(tableNodeVersion);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : tableNodeVersion.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = groundReadWrite.createStructureVersion(structure.getName(), structure.getId(),
                    structVersionAttribs);

            groundReadWrite.createEdgeVersion(edge.getName(), tableNodeVersion.getTags(), sv.getId(),
                    tableNodeVersion.getReference(), tableNodeVersion.getParameters(), edge.getId(), dbNodeVersionId,
                    tableNodeVersion.getId());

            if (!versions.isEmpty() && versions.size() > 0) {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = groundReadWrite.getAdjacentNodes(prevVersionId, "");
                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = groundReadWrite.getNodeVersion(nodeId);
                    edge = this.getEdge(oldNV);

                    structVersionAttribs = new HashMap<>();
                    for (String key : oldNV.getTags().keySet()) {
                        structVersionAttribs.put(key, GroundType.STRING);
                    }

                    // create an edge version for a dbname
                    sv = groundReadWrite.createStructureVersion(structure.getName(), structure.getId(), structVersionAttribs);
                    groundReadWrite.createEdgeVersion(edge.getName(), oldNV.getTags(), sv.getId(), oldNV.getReference(),
                            oldNV.getParameters(), edge.getId(), dbNodeVersionId, oldNV.getId());
                }
            }
            return dbNodeVersion;
        } catch (GroundException | MetaException | InvalidObjectException ex) {
            LOG.error("Unable to add partition to table {} database {}", tableName, dbName, ex);
            throw new MetaException(ex.getMessage());
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
