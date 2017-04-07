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

create table item_tag (
    item_id bigint,
    key varchar,
    value varchar,
    type varchar,
    PRIMARY KEY (item_id, key)
);

create table version_history_dag (
    item_id bigint,
    version_successor_id bigint,
    PRIMARY KEY(item_id, version_successor_id)
);

/* MODELS */

create table structure (
    item_id bigint,
    source_key varchar,
    name varchar,
    PRIMARY KEY (item_id, source_key)
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

create table rich_version_tag (
    rich_version_id bigint,
    key varchar,
    value varchar,
    type varchar,
    PRIMARY KEY (rich_version_id, key)
);

create table edge (
    item_id bigint,
    source_key varchar,
    from_node_id bigint,
    to_node_id bigint,
    name varchar,
    PRIMARY KEY (item_id, source_key)
);

create table node (
    item_id bigint,
    source_key varchar,
    name varchar,
    PRIMARY KEY (item_id, source_key)
);

create table graph (
    item_id bigint,
    source_key varchar,
    name varchar,
    PRIMARY KEY (item_id, source_key)
);

create table node_version (
    id bigint PRIMARY KEY,
    node_id bigint
);

create table edge_version (
    id bigint PRIMARY KEY,
    edge_id bigint,
    from_node_start_id bigint,
    from_node_end_id bigint,
    to_node_start_id bigint,
    to_node_end_id bigint
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

create table principal (
    node_id bigint,
    source_key varchar,
    name varchar,
    PRIMARY KEY (node_id, source_key)
);

create table lineage_edge (
    item_id bigint,
    source_key varchar,
    name varchar,
    PRIMARY KEY (item_id, source_key)
);

create table lineage_edge_version (
    id bigint PRIMARY KEY,
    lineage_edge_id bigint,
    from_rich_version_id bigint,
    to_rich_version_id bigint,
    principal_id bigint,
);

create table lineage_graph (
    item_id bigint,
    source_key varchar,
    name varchar,
    PRIMARY KEY (item_id, source_key)
);

create table lineage_graph_version (
    id bigint PRIMARY KEY,
    lineage_graph_id bigint
);

create table lineage_graph_version_edge (
    lineage_graph_version_id bigint,
    lineage_edge_version_id bigint,
    PRIMARY KEY (lineage_graph_version_id, lineage_edge_version_id)
);

/* CREATE EMPTY VERSION */

insert into version(id) values (0);
