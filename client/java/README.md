# Ground Java Client

## Requirements

Building the API client library requires [Maven](https://maven.apache.org/) to be installed.

## Installation

To install the API client library to your local Maven repository, simply execute:

```shell
mvn install
```

To deploy it to a remote Maven repository instead, configure the settings of the repository and execute:

```shell
mvn deploy
```

Refer to the [official documentation](https://maven.apache.org/plugins/maven-deploy-plugin/usage.html) for more information.

### Maven users

Add this dependency to your project's POM:

```xml
<dependency>
    <groupId>edu.berkeley.ground</groupId>
    <artifactId>ground-java-client</artifactId>
    <version>1.0.0</version>
    <scope>compile</scope>
</dependency>
```

### Others

At first generate the JAR by executing:

    mvn package

Then manually install the following JARs:

* target/swagger-java-client-1.0.0.jar
* target/lib/*.jar

## Getting Started

Please follow the [installation](#installation) instruction and execute the following Java code:

```java

import io.swagger.client.*;
import io.swagger.client.auth.*;
import io.swagger.client.model.*;
import io.swagger.client.api.DefaultApi;

import java.io.File;
import java.util.*;

public class DefaultApiExample {

    public static void main(String[] args) {
        
        DefaultApi apiInstance = new DefaultApi();
        try {
            apiInstance.edgesPost();
        } catch (ApiException e) {
            System.err.println("Exception when calling DefaultApi#edgesPost");
            e.printStackTrace();
        }
    }
}

```

## Documentation for API Endpoints

All URIs are relative to *http://localhost:9000*

Method | HTTP request
------------- | -------------
[**edgesPost**](docs/DefaultApi.md#edgesPost) | **POST** /edges
[**edgesSourceKeyGet**](docs/DefaultApi.md#edgesSourceKeyGet) | **GET** /edges/{sourceKey}
[**graphsPost**](docs/DefaultApi.md#graphsPost) | **POST** /graphs
[**graphsSourceKeyGet**](docs/DefaultApi.md#graphsSourceKeyGet) | **GET** /graphs/{sourceKey}
[**lineageEdgesPost**](docs/DefaultApi.md#lineageEdgesPost) | **POST** /lineage_edges
[**lineageEdgesSourceKeyGet**](docs/DefaultApi.md#lineageEdgesSourceKeyGet) | **GET** /lineage_edges/{sourceKey}
[**lineageGraphsPost**](docs/DefaultApi.md#lineageGraphsPost) | **POST** /lineage_graphs
[**lineageGraphsSourceKeyGet**](docs/DefaultApi.md#lineageGraphsSourceKeyGet) | **GET** /lineage_graphs/{sourceKey}
[**nodesPost**](docs/DefaultApi.md#nodesPost) | **POST** /nodes
[**nodesSourceKeyGet**](docs/DefaultApi.md#nodesSourceKeyGet) | **GET** /nodes/{sourceKey}
[**rootGet**](docs/DefaultApi.md#rootGet) | **GET** /
[**structuresPost**](docs/DefaultApi.md#structuresPost) | **POST** /structures
[**structuresSourceKeyGet**](docs/DefaultApi.md#structuresSourceKeyGet) | **GET** /structures/{sourceKey}
[**versionsEdgesIdGet**](docs/DefaultApi.md#versionsEdgesIdGet) | **GET** /versions/edges/{id}
[**versionsEdgesPost**](docs/DefaultApi.md#versionsEdgesPost) | **POST** /versions/edges
[**versionsGraphsIdGet**](docs/DefaultApi.md#versionsGraphsIdGet) | **GET** /versions/graphs/{id}
[**versionsGraphsPost**](docs/DefaultApi.md#versionsGraphsPost) | **POST** /versions/graphs
[**versionsLineageEdgesIdGet**](docs/DefaultApi.md#versionsLineageEdgesIdGet) | **GET** /versions/lineage_edges/{id}
[**versionsLineageEdgesPost**](docs/DefaultApi.md#versionsLineageEdgesPost) | **POST** /versions/lineage_edges
[**versionsLineageGraphsIdGet**](docs/DefaultApi.md#versionsLineageGraphsIdGet) | **GET** /versions/lineage_graphs/{id}
[**versionsLineageGraphsPost**](docs/DefaultApi.md#versionsLineageGraphsPost) | **POST** /versions/lineage_graphs
[**versionsNodesIdGet**](docs/DefaultApi.md#versionsNodesIdGet) | **GET** /versions/nodes/{id}
[**versionsNodesPost**](docs/DefaultApi.md#versionsNodesPost) | **POST** /versions/nodes
[**versionsStructuresIdGet**](docs/DefaultApi.md#versionsStructuresIdGet) | **GET** /versions/structures/{id}
[**versionsStructuresPost**](docs/DefaultApi.md#versionsStructuresPost) | **POST** /versions/structures

## Recommendation

It's recommended to create an instance of `ApiClient` per thread in a multithreaded environment to avoid any potential issues.

