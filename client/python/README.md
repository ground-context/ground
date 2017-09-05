# Ground Python Client

## Requirements.

Python 2.7 and 3.4+

## Installation & Usage
### pip install

If the python package is hosted on Github, you can install directly from Github

```sh
pip install git+https://github.com/GIT_USER_ID/GIT_REPO_ID.git
```
(you may need to run `pip` with root permission: `sudo pip install git+https://github.com/GIT_USER_ID/GIT_REPO_ID.git`)

Then import the package:
```python
import groundclient 
```

### Setuptools

Install via [Setuptools](http://pypi.python.org/pypi/setuptools).

```sh
python setup.py install --user
```
(or `sudo python setup.py install` to install the package for all users)

Then import the package:
```python
import groundclient
```

## Getting Started

Please follow the [installation procedure](#installation--usage) and then run the following:

```python
from __future__ import print_function
import time
import groundclient
from groundclient.rest import ApiException
from pprint import pprint
# create an instance of the API class
api_instance = groundclient.DefaultApi()

try:
    api_instance.edges_post()
except ApiException as e:
    print("Exception when calling DefaultApi->edges_post: %s\n" % e)

```

## Documentation for API Endpoints

All URIs are relative to *http://localhost:9000*

Method | HTTP request
------------- | -------------
[**edges_post**](docs/DefaultApi.md#edges_post) | **POST** /edges
[**edges_source_key_get**](docs/DefaultApi.md#edges_source_key_get) | **GET** /edges/{sourceKey}
[**graphs_post**](docs/DefaultApi.md#graphs_post) | **POST** /graphs
[**graphs_source_key_get**](docs/DefaultApi.md#graphs_source_key_get) | **GET** /graphs/{sourceKey}
[**lineage_edges_post**](docs/DefaultApi.md#lineage_edges_post) | **POST** /lineage_edges
[**lineage_edges_source_key_get**](docs/DefaultApi.md#lineage_edges_source_key_get) | **GET** /lineage_edges/{sourceKey}
[**lineage_graphs_post**](docs/DefaultApi.md#lineage_graphs_post) | **POST** /lineage_graphs
[**lineage_graphs_source_key_get**](docs/DefaultApi.md#lineage_graphs_source_key_get) | **GET** /lineage_graphs/{sourceKey}
[**nodes_post**](docs/DefaultApi.md#nodes_post) | **POST** /nodes
[**nodes_source_key_get**](docs/DefaultApi.md#nodes_source_key_get) | **GET** /nodes/{sourceKey}
[**root_get**](docs/DefaultApi.md#root_get) | **GET** /
[**structures_post**](docs/DefaultApi.md#structures_post) | **POST** /structures
[**structures_source_key_get**](docs/DefaultApi.md#structures_source_key_get) | **GET** /structures/{sourceKey}
[**versions_edges_id_get**](docs/DefaultApi.md#versions_edges_id_get) | **GET** /versions/edges/{id}
[**versions_edges_post**](docs/DefaultApi.md#versions_edges_post) | **POST** /versions/edges
[**versions_graphs_id_get**](docs/DefaultApi.md#versions_graphs_id_get) | **GET** /versions/graphs/{id}
[**versions_graphs_post**](docs/DefaultApi.md#versions_graphs_post) | **POST** /versions/graphs
[**versions_lineage_edges_id_get**](docs/DefaultApi.md#versions_lineage_edges_id_get) | **GET** /versions/lineage_edges/{id}
[**versions_lineage_edges_post**](docs/DefaultApi.md#versions_lineage_edges_post) | **POST** /versions/lineage_edges
[**versions_lineage_graphs_id_get**](docs/DefaultApi.md#versions_lineage_graphs_id_get) | **GET** /versions/lineage_graphs/{id}
[**versions_lineage_graphs_post**](docs/DefaultApi.md#versions_lineage_graphs_post) | **POST** /versions/lineage_graphs
[**versions_nodes_id_get**](docs/DefaultApi.md#versions_nodes_id_get) | **GET** /versions/nodes/{id}
[**versions_nodes_post**](docs/DefaultApi.md#versions_nodes_post) | **POST** /versions/nodes
[**versions_structures_id_get**](docs/DefaultApi.md#versions_structures_id_get) | **GET** /versions/structures/{id}
[**versions_structures_post**](docs/DefaultApi.md#versions_structures_post) | **POST** /versions/structures

