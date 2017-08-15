# DefaultApi

All URIs are relative to *http://localhost:9000*

Method | HTTP request | Description
------------- | ------------- | -------------
[**edgesPost**](DefaultApi.md#edgesPost) | **POST** /edges | 
[**edgesSourceKeyGet**](DefaultApi.md#edgesSourceKeyGet) | **GET** /edges/{sourceKey} | 
[**graphsPost**](DefaultApi.md#graphsPost) | **POST** /graphs | 
[**graphsSourceKeyGet**](DefaultApi.md#graphsSourceKeyGet) | **GET** /graphs/{sourceKey} | 
[**lineageEdgesPost**](DefaultApi.md#lineageEdgesPost) | **POST** /lineage_edges | 
[**lineageEdgesSourceKeyGet**](DefaultApi.md#lineageEdgesSourceKeyGet) | **GET** /lineage_edges/{sourceKey} | 
[**lineageGraphsPost**](DefaultApi.md#lineageGraphsPost) | **POST** /lineage_graphs | 
[**lineageGraphsSourceKeyGet**](DefaultApi.md#lineageGraphsSourceKeyGet) | **GET** /lineage_graphs/{sourceKey} | 
[**nodesPost**](DefaultApi.md#nodesPost) | **POST** /nodes | 
[**nodesSourceKeyGet**](DefaultApi.md#nodesSourceKeyGet) | **GET** /nodes/{sourceKey} | 
[**rootGet**](DefaultApi.md#rootGet) | **GET** / | 
[**structuresPost**](DefaultApi.md#structuresPost) | **POST** /structures | 
[**structuresSourceKeyGet**](DefaultApi.md#structuresSourceKeyGet) | **GET** /structures/{sourceKey} | 
[**versionsEdgesIdGet**](DefaultApi.md#versionsEdgesIdGet) | **GET** /versions/edges/{id} | 
[**versionsEdgesPost**](DefaultApi.md#versionsEdgesPost) | **POST** /versions/edges | 
[**versionsGraphsIdGet**](DefaultApi.md#versionsGraphsIdGet) | **GET** /versions/graphs/{id} | 
[**versionsGraphsPost**](DefaultApi.md#versionsGraphsPost) | **POST** /versions/graphs | 
[**versionsLineageEdgesIdGet**](DefaultApi.md#versionsLineageEdgesIdGet) | **GET** /versions/lineage_edges/{id} | 
[**versionsLineageEdgesPost**](DefaultApi.md#versionsLineageEdgesPost) | **POST** /versions/lineage_edges | 
[**versionsLineageGraphsIdGet**](DefaultApi.md#versionsLineageGraphsIdGet) | **GET** /versions/lineage_graphs/{id} | 
[**versionsLineageGraphsPost**](DefaultApi.md#versionsLineageGraphsPost) | **POST** /versions/lineage_graphs | 
[**versionsNodesIdGet**](DefaultApi.md#versionsNodesIdGet) | **GET** /versions/nodes/{id} | 
[**versionsNodesPost**](DefaultApi.md#versionsNodesPost) | **POST** /versions/nodes | 
[**versionsStructuresIdGet**](DefaultApi.md#versionsStructuresIdGet) | **GET** /versions/structures/{id} | 
[**versionsStructuresPost**](DefaultApi.md#versionsStructuresPost) | **POST** /versions/structures | 


<a name="edgesPost"></a>
# **edgesPost**
> edgesPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.edgesPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#edgesPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="edgesSourceKeyGet"></a>
# **edgesSourceKeyGet**
> edgesSourceKeyGet(sourceKey)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
String sourceKey = "sourceKey_example"; // String | 
try {
    apiInstance.edgesSourceKeyGet(sourceKey);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#edgesSourceKeyGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sourceKey** | **String**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="graphsPost"></a>
# **graphsPost**
> graphsPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.graphsPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#graphsPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="graphsSourceKeyGet"></a>
# **graphsSourceKeyGet**
> graphsSourceKeyGet(sourceKey)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
String sourceKey = "sourceKey_example"; // String | 
try {
    apiInstance.graphsSourceKeyGet(sourceKey);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#graphsSourceKeyGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sourceKey** | **String**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="lineageEdgesPost"></a>
# **lineageEdgesPost**
> lineageEdgesPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.lineageEdgesPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#lineageEdgesPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="lineageEdgesSourceKeyGet"></a>
# **lineageEdgesSourceKeyGet**
> lineageEdgesSourceKeyGet(sourceKey)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
String sourceKey = "sourceKey_example"; // String | 
try {
    apiInstance.lineageEdgesSourceKeyGet(sourceKey);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#lineageEdgesSourceKeyGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sourceKey** | **String**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="lineageGraphsPost"></a>
# **lineageGraphsPost**
> lineageGraphsPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.lineageGraphsPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#lineageGraphsPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="lineageGraphsSourceKeyGet"></a>
# **lineageGraphsSourceKeyGet**
> lineageGraphsSourceKeyGet(sourceKey)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
String sourceKey = "sourceKey_example"; // String | 
try {
    apiInstance.lineageGraphsSourceKeyGet(sourceKey);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#lineageGraphsSourceKeyGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sourceKey** | **String**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="nodesPost"></a>
# **nodesPost**
> nodesPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.nodesPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#nodesPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="nodesSourceKeyGet"></a>
# **nodesSourceKeyGet**
> nodesSourceKeyGet(sourceKey)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
String sourceKey = "sourceKey_example"; // String | 
try {
    apiInstance.nodesSourceKeyGet(sourceKey);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#nodesSourceKeyGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sourceKey** | **String**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="rootGet"></a>
# **rootGet**
> rootGet()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.rootGet();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#rootGet");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="structuresPost"></a>
# **structuresPost**
> structuresPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.structuresPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#structuresPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="structuresSourceKeyGet"></a>
# **structuresSourceKeyGet**
> structuresSourceKeyGet(sourceKey)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
String sourceKey = "sourceKey_example"; // String | 
try {
    apiInstance.structuresSourceKeyGet(sourceKey);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#structuresSourceKeyGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sourceKey** | **String**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsEdgesIdGet"></a>
# **versionsEdgesIdGet**
> versionsEdgesIdGet(id)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
Long id = 789L; // Long | 
try {
    apiInstance.versionsEdgesIdGet(id);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsEdgesIdGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **Long**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsEdgesPost"></a>
# **versionsEdgesPost**
> versionsEdgesPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.versionsEdgesPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsEdgesPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsGraphsIdGet"></a>
# **versionsGraphsIdGet**
> versionsGraphsIdGet(id)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
Long id = 789L; // Long | 
try {
    apiInstance.versionsGraphsIdGet(id);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsGraphsIdGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **Long**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsGraphsPost"></a>
# **versionsGraphsPost**
> versionsGraphsPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.versionsGraphsPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsGraphsPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsLineageEdgesIdGet"></a>
# **versionsLineageEdgesIdGet**
> versionsLineageEdgesIdGet(id)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
Long id = 789L; // Long | 
try {
    apiInstance.versionsLineageEdgesIdGet(id);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsLineageEdgesIdGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **Long**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsLineageEdgesPost"></a>
# **versionsLineageEdgesPost**
> versionsLineageEdgesPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.versionsLineageEdgesPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsLineageEdgesPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsLineageGraphsIdGet"></a>
# **versionsLineageGraphsIdGet**
> versionsLineageGraphsIdGet(id)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
Long id = 789L; // Long | 
try {
    apiInstance.versionsLineageGraphsIdGet(id);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsLineageGraphsIdGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **Long**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsLineageGraphsPost"></a>
# **versionsLineageGraphsPost**
> versionsLineageGraphsPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.versionsLineageGraphsPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsLineageGraphsPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsNodesIdGet"></a>
# **versionsNodesIdGet**
> versionsNodesIdGet(id)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
Long id = 789L; // Long | 
try {
    apiInstance.versionsNodesIdGet(id);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsNodesIdGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **Long**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsNodesPost"></a>
# **versionsNodesPost**
> versionsNodesPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.versionsNodesPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsNodesPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsStructuresIdGet"></a>
# **versionsStructuresIdGet**
> versionsStructuresIdGet(id)



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
Long id = 789L; // Long | 
try {
    apiInstance.versionsStructuresIdGet(id);
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsStructuresIdGet");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **Long**|  |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

<a name="versionsStructuresPost"></a>
# **versionsStructuresPost**
> versionsStructuresPost()



### Example
```java
// Import classes:
//import io.swagger.client.ApiException;
//import io.swagger.client.api.DefaultApi;


DefaultApi apiInstance = new DefaultApi();
try {
    apiInstance.versionsStructuresPost();
} catch (ApiException e) {
    System.err.println("Exception when calling DefaultApi#versionsStructuresPost");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

