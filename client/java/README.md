# swagger-java-client

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
    <groupId>io.swagger</groupId>
    <artifactId>swagger-java-client</artifactId>
    <version>1.0.0</version>
    <scope>compile</scope>
</dependency>
```

### Gradle users

Add this dependency to your project's build file:

```groovy
compile "io.swagger:swagger-java-client:1.0.0"
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

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DefaultApi* | [**edgesPost**](docs/DefaultApi.md#edgesPost) | **POST** /edges | 
*DefaultApi* | [**edgesSourceKeyGet**](docs/DefaultApi.md#edgesSourceKeyGet) | **GET** /edges/{sourceKey} | 
*DefaultApi* | [**graphsPost**](docs/DefaultApi.md#graphsPost) | **POST** /graphs | 
*DefaultApi* | [**graphsSourceKeyGet**](docs/DefaultApi.md#graphsSourceKeyGet) | **GET** /graphs/{sourceKey} | 
*DefaultApi* | [**lineageEdgesPost**](docs/DefaultApi.md#lineageEdgesPost) | **POST** /lineage_edges | 
*DefaultApi* | [**lineageEdgesSourceKeyGet**](docs/DefaultApi.md#lineageEdgesSourceKeyGet) | **GET** /lineage_edges/{sourceKey} | 
*DefaultApi* | [**lineageGraphsPost**](docs/DefaultApi.md#lineageGraphsPost) | **POST** /lineage_graphs | 
*DefaultApi* | [**lineageGraphsSourceKeyGet**](docs/DefaultApi.md#lineageGraphsSourceKeyGet) | **GET** /lineage_graphs/{sourceKey} | 
*DefaultApi* | [**nodesPost**](docs/DefaultApi.md#nodesPost) | **POST** /nodes | 
*DefaultApi* | [**nodesSourceKeyGet**](docs/DefaultApi.md#nodesSourceKeyGet) | **GET** /nodes/{sourceKey} | 
*DefaultApi* | [**rootGet**](docs/DefaultApi.md#rootGet) | **GET** / | 
*DefaultApi* | [**structuresPost**](docs/DefaultApi.md#structuresPost) | **POST** /structures | 
*DefaultApi* | [**structuresSourceKeyGet**](docs/DefaultApi.md#structuresSourceKeyGet) | **GET** /structures/{sourceKey} | 
*DefaultApi* | [**versionsEdgesIdGet**](docs/DefaultApi.md#versionsEdgesIdGet) | **GET** /versions/edges/{id} | 
*DefaultApi* | [**versionsEdgesPost**](docs/DefaultApi.md#versionsEdgesPost) | **POST** /versions/edges | 
*DefaultApi* | [**versionsGraphsIdGet**](docs/DefaultApi.md#versionsGraphsIdGet) | **GET** /versions/graphs/{id} | 
*DefaultApi* | [**versionsGraphsPost**](docs/DefaultApi.md#versionsGraphsPost) | **POST** /versions/graphs | 
*DefaultApi* | [**versionsLineageEdgesIdGet**](docs/DefaultApi.md#versionsLineageEdgesIdGet) | **GET** /versions/lineage_edges/{id} | 
*DefaultApi* | [**versionsLineageEdgesPost**](docs/DefaultApi.md#versionsLineageEdgesPost) | **POST** /versions/lineage_edges | 
*DefaultApi* | [**versionsLineageGraphsIdGet**](docs/DefaultApi.md#versionsLineageGraphsIdGet) | **GET** /versions/lineage_graphs/{id} | 
*DefaultApi* | [**versionsLineageGraphsPost**](docs/DefaultApi.md#versionsLineageGraphsPost) | **POST** /versions/lineage_graphs | 
*DefaultApi* | [**versionsNodesIdGet**](docs/DefaultApi.md#versionsNodesIdGet) | **GET** /versions/nodes/{id} | 
*DefaultApi* | [**versionsNodesPost**](docs/DefaultApi.md#versionsNodesPost) | **POST** /versions/nodes | 
*DefaultApi* | [**versionsStructuresIdGet**](docs/DefaultApi.md#versionsStructuresIdGet) | **GET** /versions/structures/{id} | 
*DefaultApi* | [**versionsStructuresPost**](docs/DefaultApi.md#versionsStructuresPost) | **POST** /versions/structures | 


## Documentation for Models



## Documentation for Authorization

All endpoints do not require authorization.
Authentication schemes defined for the API:

## Recommendation

It's recommended to create an instance of `ApiClient` per thread in a multithreaded environment to avoid any potential issues.

## Author

andreaskari@berkeley.edu

