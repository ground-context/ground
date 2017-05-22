-- noinspection SqlDialectInspectionForFile

-- noinspection SqlNoDataSourceInspectionForFile

-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- VERSIONS
CREATE TYPE data_type as enum ('integer', 'string', 'boolean');

CREATE TABLE IF NOT EXISTS version (
    id bigint NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS version_successor (
    id bigint NOT NULL PRIMARY KEY,
    from_version_id bigint NOT NULL REFERENCES version(id),
    to_version_id bigint NOT NULL REFERENCES version(id),
    CONSTRAINT version_successor_unique_endpoints UNIQUE (from_version_id, to_version_id)
);

CREATE TABLE IF NOT EXISTS item (
    id bigint NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS item_tag (
    item_id bigint NOT NULL REFERENCES item(id),
    key varchar NOT NULL,
    value varchar,
    type data_type,
    CONSTRAINT item_tag_pkey PRIMARY KEY (item_id, key)
);

CREATE TABLE IF NOT EXISTS version_history_dag (
    item_id bigint NOT NULL REFERENCES item(id),
    version_successor_id bigint NOT NULL REFERENCES version_successor(id),
    CONSTRAINT version_history_dag_pkey PRIMARY KEY (item_id, version_successor_id)
);

-- MODELS

CREATE TABLE IF NOT EXISTS structure (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    source_key varchar UNIQUE,
    name varchar
);

CREATE TABLE IF NOT EXISTS structure_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES version(id),
    structure_id bigint NOT NULL REFERENCES structure(item_id)
);

CREATE TABLE IF NOT EXISTS structure_version_attribute (
    structure_version_id bigint NOT NULL REFERENCES structure_version(id),
    key varchar NOT NULL,
    type varchar NOT NULL,
    CONSTRAINT structure_version_attribute_pkey PRIMARY KEY(structure_version_id, key)
);

CREATE TABLE IF NOT EXISTS rich_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES version(id),
    structure_version_id bigint REFERENCES structure_version(id),
    reference varchar
);

CREATE TABLE IF NOT EXISTS rich_version_external_parameter (
    rich_version_id bigint NOT NULL REFERENCES rich_version(id),
    key varchar NOT NULL,
    value varchar NOT NULL,
    CONSTRAINT rich_version_external_parameter_pkey PRIMARY KEY (rich_version_id, key)
);

CREATE TABLE IF NOT EXISTS rich_version_tag (
    rich_version_id bigint REFERENCES rich_version(id),
    key varchar NOT NULL,
    value varchar,
    type data_type,
    CONSTRAINT rich_version_tag_pkey PRIMARY KEY (rich_version_id, key)
);

CREATE TABLE IF NOT EXISTS node (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    source_key varchar UNIQUE,
    name varchar
);

CREATE TABLE IF NOT EXISTS edge (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    source_key varchar UNIQUE,
    from_node_id bigint NOT NULL REFERENCES node(item_id),
    to_node_id bigint NOT NULL REFERENCES node(item_id),
    name varchar
);


CREATE TABLE IF NOT EXISTS graph (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    source_key varchar UNIQUE,
    name varchar
);

CREATE TABLE IF NOT EXISTS node_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    node_id bigint NOT NULL REFERENCES node(item_id)
);

CREATE TABLE IF NOT EXISTS edge_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    edge_id bigint NOT NULL REFERENCES edge(item_id),
    from_node_version_start_id bigint NOT NULL REFERENCES node_version(id),
    from_node_version_end_id bigint REFERENCES node_version(id),
    to_node_version_start_id bigint NOT NULL REFERENCES node_version(id),
    to_node_version_end_id bigint REFERENCES node_version(id)
);

CREATE TABLE IF NOT EXISTS graph_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    graph_id bigint NOT NULL REFERENCES graph(item_id)
);

CREATE TABLE IF NOT EXISTS graph_version_edge (
    graph_version_id bigint NOT NULL REFERENCES graph_version(id),
    edge_version_id bigint NOT NULL REFERENCES edge_version(id),
    CONSTRAINT graph_version_edge_pkey PRIMARY KEY (graph_version_id, edge_version_id)
);

-- USAGE

CREATE TABLE IF NOT EXISTS principal (
    node_id bigint NOT NULL PRIMARY KEY REFERENCES node(item_id),
    source_key varchar UNIQUE,
    name varchar
);

CREATE TABLE IF NOT EXISTS lineage_edge (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    source_key varchar UNIQUE,
    name varchar
);

CREATE TABLE IF NOT EXISTS lineage_edge_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    lineage_edge_id bigint NOT NULL REFERENCES lineage_edge(item_id),
    from_rich_version_id bigint NOT NULL REFERENCES rich_version(id),
    to_rich_version_id bigint NOT NULL REFERENCES rich_version(id),
    principal_id bigint REFERENCES node_version(id)
);

CREATE TABLE IF NOT EXISTS lineage_graph (
    item_id bigint NOT NULL PRIMARY KEY REFERENCES item(id),
    source_key varchar UNIQUE,
    name varchar
);

CREATE TABLE IF NOT EXISTS lineage_graph_version (
    id bigint NOT NULL PRIMARY KEY REFERENCES rich_version(id),
    lineage_graph_id bigint NOT NULL REFERENCES lineage_graph(item_id)
);

CREATE TABLE IF NOT EXISTS lineage_graph_version_edge (
    lineage_graph_version_id bigint NOT NULL REFERENCES lineage_graph_version(id),
    lineage_edge_version_id bigint NOT NULL REFERENCES lineage_edge_version(id),
    CONSTRAINT lineage_graph_version_edge_pkey PRIMARY KEY (lineage_graph_version_id, lineage_edge_version_id)
);

-- CREATE EMPTY VERSION

INSERT INTO version(id) values (0);
