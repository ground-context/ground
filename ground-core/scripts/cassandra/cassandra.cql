/* VERSIONS */

create table version (
    id bigint PRIMARY KEY
);

create table version_successor (
    id bigint PRIMARY KEY,
    from_version_id bigint,
    to_version_id bigint
);

create table item (
    id bigint PRIMARY KEY
);

create table version_history_dag (
    item_id bigint,
    version_successor_id bigint,
    PRIMARY KEY(item_id, version_successor_id)
);

/* MODELS */

create table structure (
    item_id bigint,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table structure_version (
    id bigint PRIMARY KEY,
    structure_id bigint
);

create table structure_version_attribute (
    structure_version_id bigint,
    key varchar,
    type varchar,
    PRIMARY KEY (structure_version_id, key)
);

create table rich_version (
    id bigint PRIMARY KEY,
    structure_version_id bigint,
    reference varchar
);

create table rich_version_external_parameter (
    rich_version_id bigint,
    key varchar,
    value varchar,
    PRIMARY KEY (rich_version_id, key)
);

create table tag (
    rich_version_id bigint,
    key varchar,
    value varchar,
    type varchar,
    PRIMARY KEY (rich_version_id, key)
);

create table edge (
    item_id bigint,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table node (
    item_id bigint,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table graph (
    item_id bigint,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table node_version (
    id bigint PRIMARY KEY,
    node_id bigint
);

create table edge_version (
    id bigint PRIMARY KEY,
    edge_id bigint,
    from_node_version_id bigint,
    to_node_version_id bigint
);

create table graph_version (
    id bigint PRIMARY KEY,
    graph_id bigint
);

create table graph_version_edge (
    graph_version_id bigint,
    edge_version_id bigint,
    PRIMARY KEY (graph_version_id, edge_version_id)
);

/* USAGE */

create table workflow (
    graph_id bigint,
    name varchar,
    PRIMARY KEY (graph_id, name)
);

create table principal (
    node_id bigint,
    name varchar,
    PRIMARY KEY (node_id, name)
);

create table lineage_edge (
    item_id bigint,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table lineage_edge_version (
    id bigint PRIMARY KEY,
    lineage_edge_id bigint,
    from_rich_version_id bigint,
    to_rich_version_id bigint,
    workflow_id bigint,
    principal_id bigint,
);

/* CREATE EMPTY VERSION */

insert into version(id) values (0);
