/* VERSIONS */

create table version (
    id bigint NOT NULL PRIMARY KEY
);

create table version_successor (
    id bigint NOT NULL PRIMARY KEY,
    from_version_id bigint NOT NULL REFERENCES version(id),
    to_version_id bigint NOT NULL REFERENCES version(id),
    CONSTRAINT version_successor_unique_endpoints UNIQUE (from_version_id, to_version_id)
);

create table item (
    id bigint NOT NULL PRIMARY KEY
);

create table version_history_dag (
    item_id bigint NOT NULL REFERENCES item(id),
    version_successor_id bigint NOT NULL REFERENCES version_successor(id),
    CONSTRAINT version_history_dag_pkey PRIMARY KEY (item_id, version_successor_id)
);


/* MODELS */
create type data_type as enum ('integer', 'string', 'boolean');


create table structure (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    name varchar NOT NULL UNIQUE
);

create table structure_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES version(id),
    structure_id bigint NOT NULL REFERENCES structure(item_id)
);

create table structure_version_attribute (
    structure_version_id bigint NOT NULL REFERENCES structure_version(id),
    key varchar NOT NULL,
    type varchar NOT NULL,
    CONSTRAINT structure_version_attribute_pkey PRIMARY KEY(structure_version_id, key)
);

create table rich_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES version(id),
    structure_version_id bigint REFERENCES structure_version(id),
    reference varchar
);

create table rich_version_external_parameter (
    rich_version_id bigint NOT NULL REFERENCES rich_version(id),
    key varchar NOT NULL,
    value varchar NOT NULL,
    CONSTRAINT rich_version_external_parameter_pkey PRIMARY KEY (rich_version_id, key)
);

create table tag (
    rich_version_id bigint REFERENCES rich_version(id),
    key varchar NOT NULL,
    value varchar,
    type data_type,
    CONSTRAINT tag_pkey PRIMARY KEY (rich_version_id, key)
);

create table edge (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    name varchar NOT NULL UNIQUE
);

create table node (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    name varchar NOT NULL UNIQUE
);

create table graph (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    name varchar NOT NULL UNIQUE
);

create table node_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    node_id bigint NOT NULL REFERENCES node(item_id)
);

create table edge_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    edge_id bigint NOT NULL REFERENCES edge(item_id),
    from_node_version_id bigint NOT NULL REFERENCES node_version(id),
    to_node_version_id bigint NOT NULL REFERENCES node_version(id)
);

create table graph_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    graph_id bigint NOT NULL REFERENCES graph(item_id)
);

create table graph_version_edge (
    graph_version_id bigint NOT NULL REFERENCES graph_version(id),
    edge_version_id bigint NOT NULL REFERENCES edge_version(id),
    CONSTRAINT graph_version_edge_pkey PRIMARY KEY (graph_version_id, edge_version_id)
);

/* USAGE */

create table workflow (
    graph_id bigint NOT NULL PRIMARY KEY REFERENCES graph(item_id),
    name varchar NOT NULL UNIQUE REFERENCES graph(name)
);

create table principal (
    node_id bigint NOT NULL PRIMARY KEY REFERENCES node(item_id),
    name varchar NOT NULL UNIQUE REFERENCES node(name)
);

create table lineage_edge (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    name varchar NOT NULL UNIQUE
);

create table lineage_edge_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    lineage_edge_id bigint NOT NULL REFERENCES lineage_edge(item_id),
    from_rich_version_id bigint NOT NULL REFERENCES rich_version(id),
    to_rich_version_id bigint NOT NULL REFERENCES rich_version(id),
    workflow_id bigint REFERENCES graph_version(id),
    principal_id bigint REFERENCES node_version(id)
);

/* CREATE EMPTY VERSION */

insert into version(id) values (0);
