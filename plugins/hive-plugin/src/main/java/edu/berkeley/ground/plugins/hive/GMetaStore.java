package edu.berkeley.ground.plugins.hive;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

public class GMetaStore {
    static final private Logger LOG = LoggerFactory.getLogger(GroundStore.class.getName());

    private static final String DUMMY_NOT_USED = "DUMMY_NOT_USED";

    private static final String _TIMESTAMP = "_TIMESTAMP";

    private static String METASTORE_NODE = "_METASTORE";
    private GroundReadWrite ground = null;

    final String APACHE_HIVE_URL = "HIVE_INFO";
    HashMap<String, String> APACHE_HIVE_URL_MAP = new HashMap<String, String>();
    {
        APACHE_HIVE_URL_MAP.put("URL", "http://hive.apache.org/");
    }

    public GMetaStore(GroundReadWrite ground) {
        this.ground = ground;
    }

    public Node getNode() throws GroundException {
        try {
            LOG.debug("Fetching metastore node: " + METASTORE_NODE);
            return ground.getNodeFactory().retrieveFromDatabase(METASTORE_NODE);
        } catch (GroundException ex) {
            LOG.debug("Not found - Creating metastore node: " + METASTORE_NODE);
            throw ex;
        }
    }

    public Node createNode() throws GroundException {
        try {
            LOG.debug("Fetching metastore node: " + METASTORE_NODE);
            return ground.getNodeFactory().retrieveFromDatabase(METASTORE_NODE);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating metastore node: " + METASTORE_NODE);
            Node node = ground.getNodeFactory().create(METASTORE_NODE);
            Structure structure = ground.getStructureFactory().create(node.getId());
            return node;
        }
    }

    public NodeVersion getNodeVersion() throws GroundException {
        try {
            Node node = this.getNode();
            String nodeVersionId = ground.getNodeFactory().getLeaves(node.getId()).get(0);
            return ground.getNodeVersionFactory().retrieveFromDatabase(nodeVersionId);
        } catch (GroundException ex) {
            throw ex;
        }
    }

    public NodeVersion createNodeVersion() throws GroundException {
        try {
            Node node = this.createNode();
            String nodeId = node.getId();

            Map<String, GroundType> structureVersionAttributes = new HashMap<>();
            structureVersionAttributes.put(_TIMESTAMP, GroundType.STRING);
            Structure structure = ground.getStructureFactory().retrieveFromDatabase(nodeId);
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
                    structureVersionAttributes, new ArrayList<>());

            Map<String, Tag> tags = new HashMap<>();
            String timestamp = Date.from(Instant.now()).toString();
            tags.put(METASTORE_NODE, new Tag(DUMMY_NOT_USED, _TIMESTAMP, timestamp, GroundType.STRING));

            // TODO: put version and other information in tags
            return ground.getNodeVersionFactory().create(tags, sv.getId(), nodeId, APACHE_HIVE_URL_MAP, nodeId,
                    new ArrayList<>());
        } catch (GroundException ex) {
            LOG.error("Unable to initialize Ground Metastore: " + ex);
            throw ex;
        }
    }

    public void addDatabase(Node n, NodeVersion nv) throws GroundException {
        try {
            Edge edge = null;
            String dbName = n.getId();
            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

            if (!versions.isEmpty()) {
                String metaVersionId = versions.get(0);
                edge = ground.getEdgeFactory().create(dbName);
                ground.getEdgeVersionFactory().create(nv.getTags(), nv.getStructureVersionId(), nv.getReference(),
                        nv.getParameters(), edge.getId(), metaVersionId, nv.getId(), new ArrayList<String>());
                if (versions.size() > 1) {
                    String prevVersionId = versions.get(1);
                    List<String> dbNodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                    for (String dbNodeId : dbNodeIds) {
                        NodeVersion dbNodeVersion = ground.getNodeVersionFactory().retrieveFromDatabase(dbNodeId);
                        // create an edge for a dbname only if was not created earlier
                        edge = ground.getEdgeFactory().retrieveFromDatabase(dbNodeVersion.getNodeId());
                        ground.getEdgeVersionFactory().create(nv.getTags(), nv.getStructureVersionId(),
                                nv.getReference(), nv.getParameters(), edge.getId(), metaVersionId, nv.getId(),
                                new ArrayList<String>());
                    }
                }
            }
        } catch (GroundException ex) {
            LOG.error("Failed to register database {} version {}", n.getId(), nv.getId());
            throw ex;
        }
    }

}
