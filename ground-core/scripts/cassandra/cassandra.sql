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
    id varchar PRIMARY KEY
);

create table version_history_dag (
    item_id varchar,
    version_successor_id bigint,
    PRIMARY KEY(item_id, version_successor_id)
);

/* MODELS */

create table structure (
    item_id varchar,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table structure_version (
    id bigint primary key,
    structure_id varchar
);

create table structure_version_attribute (
    structure_version_id bigint,
    key varchar,
    type varchar,
    PRIMARY KEY (structure_version_id, key)
);

create table rich_version (
    id bigint PRIMARY KEY,
    structure_version_id varchar,
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
    item_id varchar,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table node (
    item_id varchar,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table graph (
    item_id varchar,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table node_version (
    id bigint PRIMARY KEY,
    node_id varchar
);

create table edge_version (
    id bigint PRIMARY KEY,
    edge_id varchar,
    from_node_version_id varchar,
    to_node_version_id varchar
);

create table graph_version (
    id bigint primary key,
    graph_id varchar,
);

create table graph_version_edge (
    graph_version_id bigint,
    edge_version_id bigint,
    PRIMARY KEY (graph_version_id, edge_version_id)
);

/* USAGE */

create table workflow (
    graph_id varchar,
    name varchar,
    PRIMARY KEY (graph_id, name)
);

create table principal (
    node_id varchar,
    name varchar,
    PRIMARY KEY (node_id, name)
);

create table lineage_edge (
    item_id varchar,
    name varchar,
    PRIMARY KEY (item_id, name)
);

create table lineage_edge_version (
    id bigint PRIMARY KEY,
    lineage_edge_id varchar,
    from_rich_version_id bigint,
    to_rich_version_id bigint,
    workflow_id varchar,
    principal_id varchar,
);

/* CREATE EMPTY VERSION */

insert into version(id) values (0);
