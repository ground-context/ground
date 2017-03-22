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
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.GroundStore.EntityState;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

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
    private static final String DATABASE = "database";

    static final private Logger LOG = LoggerFactory.getLogger(GroundDatabase.class.getName());

    static final String DATABASE_NODE = "_DATABASE";

    static final String DATABASE_TABLE_EDGE = "_DATABASE_TABLE";

    private static final String DB_STATE = "_DATABASE_STATE";

    private static final String DATABASE_PARENT_NODE = "_DATABASE_PARENT_NODE";

    private GroundReadWrite groundReadWrite = null;
    private GroundTable groundTable = null;
    private NodeVersion metaDatabaseNodeVersion =null;

    GroundDatabase(GroundReadWrite groundReadWrite) {
        this.groundReadWrite = groundReadWrite;
        // create a parent for all databases - its edges will determine the
        // number of databases
        if (metaDatabaseNodeVersion == null) {
            try {
                Database metaDatabase = new Database(DATABASE_PARENT_NODE, "metanode", "locationurl",
                        new HashMap<String, String>());
                metaDatabaseNodeVersion = createDatabaseNodeVersion(metaDatabase, EntityState.ACTIVE.name());
            } catch (InvalidObjectException | MetaException ex) {
                LOG.error("error creating metadatabase: {}", ex);
            }
        }
    }

    Node getNode(String dbName, Map<String, Tag> tagMap) throws GroundException {
        LOG.debug("Fetching database node: " + dbName);
        Node node = this.groundReadWrite.getGroundReadWriteNodeResource().getNode(dbName);
        if (node == null) {
            return this.groundReadWrite.getGroundReadWriteNodeResource().createNode(dbName, tagMap);
        }
        return node;
    }

    Database getDatabase(String dbName) throws GroundException {
        NodeVersion latestVersion = getDatabaseNodeVersion(dbName);
        LOG.info("database versions id: {}", latestVersion.getNodeId());
        LOG.info("database versions structureVersion: {}", latestVersion.getStructureVersionId());
        Map<String, Tag> dbTag = latestVersion.getTags();
        return PluginUtil.fromJson((String) dbTag.get(dbName).getValue(), Database.class);
    }

    NodeVersion getDatabaseNodeVersion(String dbName) throws GroundException {
        List<Long> versions = (List<Long>) PluginUtil.getLatestVersions(dbName, "nodes");
        if (versions == null || versions.isEmpty()) {
            throw new GroundException("Database node not found: " + dbName);
        }
        LOG.info("database versions size: {}", versions.size());
        return this.groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(versions.get(0));
    }

    NodeVersion createDatabaseNodeVersion(Database db, String state) throws InvalidObjectException, MetaException {
        if (db == null) {
            throw new InvalidObjectException("Database object passed is null");
        }
        try {
            // create a new tag for new database node version entry
            String dbName = db.getName();
            String reference = dbName;
            Map<String, Tag> tags = new HashMap<>();
            Tag stateTag = new Tag(1L, dbName + DB_STATE, state, GroundType.STRING);
            tags.put(DB_STATE, stateTag);
            Tag dbTag = new Tag(1L, dbName, PluginUtil.toJson(db), GroundType.STRING);
            tags.put(dbName, dbTag);
            Map<String, String> referenceParameterMap = db.getParameters();
            if (referenceParameterMap == null) {
                referenceParameterMap = new HashMap<String, String>();
            }
            StructureVersion sv = this.getDatabaseStructureVersion(state);
            NodeVersion dbNodeVersion = this.groundReadWrite.getGroundReadWriteNodeResource().createNodeVersion(1L,
                    tags, sv.getId(), reference, referenceParameterMap, dbName);
            if (dbName.equals(DATABASE_PARENT_NODE)) {
                LOG.info("created metanode: {}",  dbNodeVersion.getId());
                return dbNodeVersion;
            }
            // create a new edge from just created database to metadatabase node
            // useful for getting all edges
            Edge edge = groundReadWrite.getGroundReadWriteEdgeResource().createEdge(DATABASE_PARENT_NODE + dbName,
                    tags);
            groundReadWrite.getGroundReadWriteEdgeResource().createEdgeVersion(edge.getId(), tags, sv.getId(),
                    reference, referenceParameterMap, edge.getId(), metaDatabaseNodeVersion.getId(),
                    dbNodeVersion.getId());
            return dbNodeVersion;
        } catch (GroundException e) {
            LOG.error("Failure to create a database node: {}", e);
            throw new MetaException(e.getMessage());
        }
    }

    private StructureVersion getDatabaseStructureVersion(String state) throws GroundException {
        return groundReadWrite.getGroundReadWriteStructureResource().getStructureVersion(DATABASE, state);
    }

    // Table related functions
    NodeVersion createTableComponents(Table table) throws InvalidObjectException, MetaException {
        return null;
    }

    NodeVersion dropTableNodeVersion(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return null;
    }

    Table getTable(String dbName, String tableName) throws MetaException {
        return groundTable.getTable(dbName, tableName);
    }

    List<String> getTables(String dbName, String pattern) throws MetaException {
        return groundTable.getTables(dbName, pattern);
    }

    NodeVersion addPartitions(String dbName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        return null;
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

    NodeVersion dropDatabase(String dbName, String state) throws GroundException {
        NodeVersion databaseNodeVersion = getDatabaseNodeVersion(dbName);
        Map<String, Tag> dbTagMap = databaseNodeVersion.getTags();
        if (dbTagMap == null) {
            LOG.info("node version getTags failed");
            return null;
        }
        StructureVersion sv = getDatabaseStructureVersion(state);
        Tag stateTag = new Tag(1L, DB_STATE, state, GroundType.STRING);
        dbTagMap.put(DB_STATE, stateTag); // update state to deleted
        // Node dbNode = getNode(dbName, dbTagMap);
        LOG.info("database deleted: {}, {}", dbName, databaseNodeVersion.getNodeId());
        return this.groundReadWrite.getGroundReadWriteNodeResource().createNodeVersion(1L, dbTagMap, sv.getId(), "",
                new HashMap<String, String>(), dbName);
    }

    public List<String> getDatabases(String pattern) throws GroundException {
        List<String> list = new ArrayList<>();
        List<Long> metaDatabaseClosureList = groundReadWrite.getGroundReadWriteNodeResource()
                .getTransitiveClosure(metaDatabaseNodeVersion.getId());
        for (long i : metaDatabaseClosureList) {
            NodeVersion nodeVersion = this.groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(i);
            list.add(nodeVersion.getReference());
        }
        return list;
    }
}
