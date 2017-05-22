package edu.berkeley.ground.postgres.dao;

public class SqlConstants {

  /* General insert statements */
  public static final String INSERT_GENERIC_ITEM = "INSERT INTO %s (item_id, source_key, name) VALUES (%d, \'%s\', \'%s\');";

  /* General select statements */
  public static final String SELECT_STAR_BY_SOURCE_KEY = "SELECT * FROM %s WHERE source_key = \'%s\'";
  public static final String SELECT_STAR_ITEM_BY_ID = "SELECT * FROM %s WHERE item_id = %d;";
  public static final String SELECT_STAR_BY_ID = "SELECT * FROM %s WHERE id = %d;";
  public static final String DELETE_BY_ID = "DELETE FROM %s WHERE id = %d";

  /* Version-specific statements */
  public static final String INSERT_VERSION = "INSERT INTO version (id) VALUES (%d);";

  /* Version Successor-specific statements */
  public static final String INSERT_VERSION_SUCCESSOR = "INSERT INTO version_successor (id, from_version_id, to_version_id) VALUES (%d, %d, %d);";
  public static final String SELECT_VERSION_SUCCESSOR = "SELECT * FROM version_successor where id = %d;";
  public static final String SELECT_VERSION_SUCCESSOR_BY_ENDPOINT = "SELECT * FROM version_successor WHERE to_version_id = %d;";
  public static final String DELETE_VERSION_SUCCESSOR = "DELETE FROM version_successor WHERE id = %d;";

  /* Version History DAG-specific statements */
  public static final String INSERT_VERSION_HISTORY_DAG_EDGE = "INSERT INTO version_history_dag (item_id, version_successor_id) VALUES (%d, %d);";
  public static final String SELECT_VERSION_HISTORY_DAG = "SELECT * FROM version_history_dag WHERE item_id = %d;";
  public static final String DELETE_SUCCESSOR_FROM_DAG = "DELETE FROM version_history_dag WHERE version_successor_id = %d;";

  /* Item-specific statements */
  public static final String INSERT_ITEM = "INSERT INTO ITEM (id) VALUES (%d);";
  public static final String INSERT_ITEM_TAG_WITH_VALUE =
    "INSERT INTO item_tag (item_id, key, value, type) VALUES (%d, " + "\'%s\', \'%s\', \'%s\');";
  public static final String INSERT_ITEM_TAG_NO_VALUE = "INSERT INTO item_tag (item_id, key, value, type) VALUES (%d, \'%s\', null, null);";
  public static final String SELECT_ITEM_TAGS = "SELECT * FROM item_tag WHERE item_id = %d;";
  public static final String SELECT_ITEM_TAGS_BY_KEY = "SELECT * FROM item_tag WHERE key = \'%s\';";

  /* Edge-specific statements */
  public static final String INSERT_EDGE =
    "INSERT INTO edge (item_id, source_key, from_node_id, to_node_id, name) VALUES (%d, \'%s\', %d, %d, \'%s\');";
  public static final String INSERT_EDGE_VERSION = "INSERT INTO edge_version (id, edge_id, from_node_version_start_id, from_node_version_end_id, "
                                                     + "to_node_version_start_id, to_node_version_end_id) VALUES (%d, %d, %d, %d, %d, %d);";
  public static final String UPDATE_EDGE_VERSION = "UPDATE edge_version SET from_node_version_end_id = %d, to_node_version_end_id = %d WHERE id = "
                                                     + "%d;";

  /* Graph-specific statements */
  public static final String INSERT_GRAPH_VERSION = "INSERT INTO graph_version (id, graph_id) VALUES (%d, %d);";
  public static final String INSERT_GRAPH_VERSION_EDGE = "INSERT INTO graph_version_edge (graph_version_id, edge_version_id) VALUES (%d, %d);";
  public static final String SELECT_GRAPH_VERSION_EDGES = "SELECT * FROM graph_version_edge WHERE graph_version_id = %d;";
  public static final String DELETE_ALL_GRAPH_VERSION_EDGES = "DELETE FROM %s WHERE %s_version_id = %d";

  /* Node-specific statements */
  public static final String INSERT_NODE_VERSION = "INSERT INTO node_version (id, node_id) VALUES (%d, %d);";

  /* Rich Version-specific statements */
  public static final String INSERT_RICH_VERSION = "INSERT INTO rich_version (id, structure_version_id, reference) VALUES (%d, %d, \'%s\');";
  public static final String INSERT_RICH_VERSION_TAG_WITH_VALUE = "INSERT INTO rich_version_tag (rich_version_id, key, value, type) VALUES (%d, "
                                                                    + "\'%s\', \'%s\', \'%s\');";
  public static final String INSERT_RICH_VERSION_TAG_NO_VALUE = "INSERT INTO rich_version_tag (rich_version_id, key, value, type) VALUES (%d, "
                                                                  + "\'%s\', null, null);";
  public static final String INSERT_RICH_VERSION_EXTERNAL_PARAMETER = "INSERT INTO rich_version_external_parameter (rich_version_id, key, value) "
                                                                        + "VALUES (%d, \'%s\', \'%s\');";
  public static final String SELECT_RICH_VERSION_EXTERNAL_PARAMETERS = "SELECT * FROM rich_version_external_parameter WHERE rich_version_id = %d;";
  public static final String SELECT_RICH_VERSION_TAGS = "SELECT * FROM rich_version_tag WHERE rich_version_id = %d;";
  public static final String SELECT_RICH_VERSION_TAGS_BY_KEY = "SELECT * FROM rich_version_tag WHERE key = \'%s\';";
  public static final String DELETE_RICH_VERSION_TAGS = "DELETE FROM rich_version_tag WHERE rich_version_id = %d";
  public static final String DELETE_RICH_EXTERNAL_PARAMETERS = "DELETE FROM rich_version_external_parameter WHERE rich_version_id = %d";

  /* Structure-specific statements */
  public static final String INSERT_STRUCTURE_VERSION = "INSERT INTO structure_version (id, structure_id) VALUES (%d, %d);";
  public static final String INSERT_STRUCTURE_VERSION_ATTRIBUTE = "INSERT INTO structure_version_attribute (structure_version_id, key, type) "
                                                                    + "VALUES (%d, \'%s\', \'%s\');";
  public static final String SELECT_STRUCTURE_VERSION_ATTRIBUTES = "SELECT * FROM structure_version_attribute WHERE structure_version_id = %d;";
  public static final String DELETE_STRUCTURE_VERSION_ATTRIBUTES = "DELETE FROM structure_version_attribute WHERE structure_version_id = %d";

  /* Lineage Edge-specific statements */
  public static final String INSERT_LINEAGE_EDGE_VERSION = "INSERT INTO lineage_edge_version (id, lineage_edge_id, from_rich_version_id, "
                                                             + "to_rich_version_id, principal_id) VALUES (%d, %d, %d, %d, %d);";

  /* Lineage Graph-specific statements */
  public static final String INSERT_LINEAGE_GRAPH_VERSION = "INSERT INTO lineage_graph_version (id, lineage_graph_id) VALUES (%d, %d);";
  public static final String INSERT_LINEAGE_GRAPH_VERSION_EDGE = "INSERT INTO lineage_graph_version_edge (lineage_graph_version_id, "
                                                                   + "lineage_edge_version_id) VALUES (%d, %d);";
  public static final String SELECT_LINEAGE_GRAPH_VERSION_EDGES = "SELECT * FROM lineage_graph_version_edge WHERE lineage_graph_version_id = %d;";
}
