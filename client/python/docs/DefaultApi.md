# swagger_client.DefaultApi

All URIs are relative to *http://localhost:9000*

Method | HTTP request | Description
------------- | ------------- | -------------
[**edges_post**](DefaultApi.md#edges_post) | **POST** /edges | 
[**edges_source_key_get**](DefaultApi.md#edges_source_key_get) | **GET** /edges/{sourceKey} | 
[**graphs_post**](DefaultApi.md#graphs_post) | **POST** /graphs | 
[**graphs_source_key_get**](DefaultApi.md#graphs_source_key_get) | **GET** /graphs/{sourceKey} | 
[**lineage_edges_post**](DefaultApi.md#lineage_edges_post) | **POST** /lineage_edges | 
[**lineage_edges_source_key_get**](DefaultApi.md#lineage_edges_source_key_get) | **GET** /lineage_edges/{sourceKey} | 
[**lineage_graphs_post**](DefaultApi.md#lineage_graphs_post) | **POST** /lineage_graphs | 
[**lineage_graphs_source_key_get**](DefaultApi.md#lineage_graphs_source_key_get) | **GET** /lineage_graphs/{sourceKey} | 
[**nodes_post**](DefaultApi.md#nodes_post) | **POST** /nodes | 
[**nodes_source_key_get**](DefaultApi.md#nodes_source_key_get) | **GET** /nodes/{sourceKey} | 
[**root_get**](DefaultApi.md#root_get) | **GET** / | 
[**structures_post**](DefaultApi.md#structures_post) | **POST** /structures | 
[**structures_source_key_get**](DefaultApi.md#structures_source_key_get) | **GET** /structures/{sourceKey} | 
[**versions_edges_id_get**](DefaultApi.md#versions_edges_id_get) | **GET** /versions/edges/{id} | 
[**versions_edges_post**](DefaultApi.md#versions_edges_post) | **POST** /versions/edges | 
[**versions_graphs_id_get**](DefaultApi.md#versions_graphs_id_get) | **GET** /versions/graphs/{id} | 
[**versions_graphs_post**](DefaultApi.md#versions_graphs_post) | **POST** /versions/graphs | 
[**versions_lineage_edges_id_get**](DefaultApi.md#versions_lineage_edges_id_get) | **GET** /versions/lineage_edges/{id} | 
[**versions_lineage_edges_post**](DefaultApi.md#versions_lineage_edges_post) | **POST** /versions/lineage_edges | 
[**versions_lineage_graphs_id_get**](DefaultApi.md#versions_lineage_graphs_id_get) | **GET** /versions/lineage_graphs/{id} | 
[**versions_lineage_graphs_post**](DefaultApi.md#versions_lineage_graphs_post) | **POST** /versions/lineage_graphs | 
[**versions_nodes_id_get**](DefaultApi.md#versions_nodes_id_get) | **GET** /versions/nodes/{id} | 
[**versions_nodes_post**](DefaultApi.md#versions_nodes_post) | **POST** /versions/nodes | 
[**versions_structures_id_get**](DefaultApi.md#versions_structures_id_get) | **GET** /versions/structures/{id} | 
[**versions_structures_post**](DefaultApi.md#versions_structures_post) | **POST** /versions/structures | 


# **edges_post**
> edges_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.edges_post()
except ApiException as e:
    print("Exception when calling DefaultApi->edges_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **edges_source_key_get**
> edges_source_key_get(source_key)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
source_key = 'source_key_example' # str | 

try: 
    api_instance.edges_source_key_get(source_key)
except ApiException as e:
    print("Exception when calling DefaultApi->edges_source_key_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **source_key** | **str**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **graphs_post**
> graphs_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.graphs_post()
except ApiException as e:
    print("Exception when calling DefaultApi->graphs_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **graphs_source_key_get**
> graphs_source_key_get(source_key)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
source_key = 'source_key_example' # str | 

try: 
    api_instance.graphs_source_key_get(source_key)
except ApiException as e:
    print("Exception when calling DefaultApi->graphs_source_key_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **source_key** | **str**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **lineage_edges_post**
> lineage_edges_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.lineage_edges_post()
except ApiException as e:
    print("Exception when calling DefaultApi->lineage_edges_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **lineage_edges_source_key_get**
> lineage_edges_source_key_get(source_key)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
source_key = 'source_key_example' # str | 

try: 
    api_instance.lineage_edges_source_key_get(source_key)
except ApiException as e:
    print("Exception when calling DefaultApi->lineage_edges_source_key_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **source_key** | **str**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **lineage_graphs_post**
> lineage_graphs_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.lineage_graphs_post()
except ApiException as e:
    print("Exception when calling DefaultApi->lineage_graphs_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **lineage_graphs_source_key_get**
> lineage_graphs_source_key_get(source_key)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
source_key = 'source_key_example' # str | 

try: 
    api_instance.lineage_graphs_source_key_get(source_key)
except ApiException as e:
    print("Exception when calling DefaultApi->lineage_graphs_source_key_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **source_key** | **str**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **nodes_post**
> nodes_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.nodes_post()
except ApiException as e:
    print("Exception when calling DefaultApi->nodes_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **nodes_source_key_get**
> nodes_source_key_get(source_key)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
source_key = 'source_key_example' # str | 

try: 
    api_instance.nodes_source_key_get(source_key)
except ApiException as e:
    print("Exception when calling DefaultApi->nodes_source_key_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **source_key** | **str**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **root_get**
> root_get()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.root_get()
except ApiException as e:
    print("Exception when calling DefaultApi->root_get: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **structures_post**
> structures_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.structures_post()
except ApiException as e:
    print("Exception when calling DefaultApi->structures_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **structures_source_key_get**
> structures_source_key_get(source_key)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
source_key = 'source_key_example' # str | 

try: 
    api_instance.structures_source_key_get(source_key)
except ApiException as e:
    print("Exception when calling DefaultApi->structures_source_key_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **source_key** | **str**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_edges_id_get**
> versions_edges_id_get(id)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
id = 789 # int | 

try: 
    api_instance.versions_edges_id_get(id)
except ApiException as e:
    print("Exception when calling DefaultApi->versions_edges_id_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_edges_post**
> versions_edges_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.versions_edges_post()
except ApiException as e:
    print("Exception when calling DefaultApi->versions_edges_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_graphs_id_get**
> versions_graphs_id_get(id)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
id = 789 # int | 

try: 
    api_instance.versions_graphs_id_get(id)
except ApiException as e:
    print("Exception when calling DefaultApi->versions_graphs_id_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_graphs_post**
> versions_graphs_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.versions_graphs_post()
except ApiException as e:
    print("Exception when calling DefaultApi->versions_graphs_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_lineage_edges_id_get**
> versions_lineage_edges_id_get(id)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
id = 789 # int | 

try: 
    api_instance.versions_lineage_edges_id_get(id)
except ApiException as e:
    print("Exception when calling DefaultApi->versions_lineage_edges_id_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_lineage_edges_post**
> versions_lineage_edges_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.versions_lineage_edges_post()
except ApiException as e:
    print("Exception when calling DefaultApi->versions_lineage_edges_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_lineage_graphs_id_get**
> versions_lineage_graphs_id_get(id)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
id = 789 # int | 

try: 
    api_instance.versions_lineage_graphs_id_get(id)
except ApiException as e:
    print("Exception when calling DefaultApi->versions_lineage_graphs_id_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_lineage_graphs_post**
> versions_lineage_graphs_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.versions_lineage_graphs_post()
except ApiException as e:
    print("Exception when calling DefaultApi->versions_lineage_graphs_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_nodes_id_get**
> versions_nodes_id_get(id)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
id = 789 # int | 

try: 
    api_instance.versions_nodes_id_get(id)
except ApiException as e:
    print("Exception when calling DefaultApi->versions_nodes_id_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_nodes_post**
> versions_nodes_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.versions_nodes_post()
except ApiException as e:
    print("Exception when calling DefaultApi->versions_nodes_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_structures_id_get**
> versions_structures_id_get(id)



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()
id = 789 # int | 

try: 
    api_instance.versions_structures_id_get(id)
except ApiException as e:
    print("Exception when calling DefaultApi->versions_structures_id_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **int**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **versions_structures_post**
> versions_structures_post()



### Example 
```python
from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.DefaultApi()

try: 
    api_instance.versions_structures_post()
except ApiException as e:
    print("Exception when calling DefaultApi->versions_structures_post: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

