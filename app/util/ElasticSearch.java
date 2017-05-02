package util;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import exceptions.GroundElasticSearchException;
import models.models.Tag;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ElasticSearch {
  private static Node node;
  private static Client client;
  private static String clusterName = "groundv1";



  public static boolean connectElasticSearch() throws GroundElasticSearchException {
    try {
      node = nodeBuilder().clusterName(clusterName).node();
      client = node.client();
    } catch (Exception e) {
      throw new GroundElasticSearchException("ElasticSearch encountered an error while trying to connect");
    }
    return !node.isClosed();
  }

  public static void insertElasticSearch(Tag tag, String table) throws GroundElasticSearchException {
    ObjectMapper mapper = new ObjectMapper();
    try {
      String json = mapper.writeValueAsString(tag);
      IndexResponse response = client.prepareIndex(clusterName, table, Long.toString(tag.getId()))
        .setSource(json).get();

    } catch (JsonProcessingException e) {

      throw new GroundElasticSearchException("ObjectMapper failed to parse Tag object");
    }
  }

  public static List<Long> getSearchResponse(String type, String searchQuery) throws GroundElasticSearchException {
    client.admin().indices().prepareRefresh().execute().actionGet(); // need to refresh index with new inserted item
    SearchResponse response = client.prepareSearch().setTypes(type).setQuery(QueryBuilders.matchQuery("key", searchQuery)).get();
    SearchHit[] hits = response.getHits().hits();
    ObjectMapper mapper = new ObjectMapper();
    List<Long> tagIds = new ArrayList<>();
    try {
      for (SearchHit hit : hits) {
        String source = hit.getSourceAsString();
        Tag tag = mapper.readValue(source, Tag.class);
        tagIds.add(tag.getId());

      }
    } catch (JsonParseException e) {
      throw new GroundElasticSearchException("ObjectMapper failed to parse json string into Tag object");
    } catch (JsonMappingException e) {
      throw new GroundElasticSearchException("ObjectMapper failed to map json string into Tag object");
    } catch (IOException e) {
      throw new GroundElasticSearchException("ObjectMapper detected an IOException");
    }
    return tagIds;
  }

  public static void closeElasticSearch() throws IOException {
    node.close();
    client.close();
  }
}
